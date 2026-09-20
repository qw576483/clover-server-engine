package globalstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/etcd"
	ilog "github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// EtcdBackend 以 etcd 为后端的全局 KV 实现，用于生产集群。
// 复用既有的 internal/transport/etcd.Client（统一连接/认证/TLS/租约管理），连接生命周期由该 Client 负责，
// 因此本后端的 Close 为 no-op。CAS/Incr 经 Txn 保证原子性；Watch 经 Raw().Watch 解析事件类型。
type EtcdBackend struct {
	c      *etcd.Client
	prefix string
	wg     sync.WaitGroup // 追踪异步 goroutine（Watch）生命周期
}

// NewEtcdBackend 用既有 etcd 客户端构造后端。
// 前缀无关设计——prefix 允许为空（由上层 Store 统一负责命名空间）。
// 若直接单独使用本 Backend，可传入自定义 prefix，Backend 只做 key 拼接、不做剥离。
func NewEtcdBackend(c *etcd.Client, prefix string) *EtcdBackend {
	return &EtcdBackend{c: c, prefix: prefix}
}

// NewEtcdStore 构造以 etcd 为后端的带前缀 Store（生产环境全局 KV）。
//
// 命名空间前缀的责任统一归 Store 层——Backend 保持前缀无关（与 MemBackend 一致）。
// 因此这里 Backend 传空前缀，避免 Store 与 Backend 双方各加一次前缀造成「双前缀写入 / 双剥离」。
func NewEtcdStore(c *etcd.Client, prefix string) *Store {
	return NewStore(NewEtcdBackend(c, ""), prefix)
}

func (b *EtcdBackend) Get(ctx context.Context, key string) (string, error) {
	v, err := b.c.Get(ctx, b.prefix+key)
	if err != nil {
		// 明确区分「key 不存在」与「底层故障」两类错误，
		// 前者归一化为 gstore 的 ErrKeyNotFound，后者原样上抛供调用方感知/重试。
		// 必须用 errors.Is：底层一旦对该错误做包装（fmt.Errorf("%w")），
		// `==` 立刻漏判，会把「键不存在」当成底层故障上抛。
		if errors.Is(err, etcd.ErrKeyNotFound) {
			return "", ErrKeyNotFound
		}
		return "", err
	}
	return v, nil
}

func (b *EtcdBackend) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	full := b.prefix + key
	if ttl > 0 {
		grantCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		// etcd 租约 TTL 以秒为单位；直接 int64(ttl.Seconds()) 会把亚秒级 TTL
		// （如 500ms）截断为 0 → 变成「永久租约」。改用向上取整并保证至少 1 秒。
		secs := int64(math.Ceil(ttl.Seconds()))
		if secs < 1 {
			secs = 1
		}
		lease, err := b.c.Raw().Grant(grantCtx, secs)
		cancel()
		if err != nil {
			return err
		}
		_, err = b.c.Raw().Put(ctx, full, value, clientv3.WithLease(lease.ID))
		if err != nil {
			// Put 失败时回收已授予的租约，避免租约泄漏。
			revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 3*time.Second)
			if _, err := b.c.Raw().Revoke(revokeCtx, lease.ID); err != nil {
				ilog.LogError("gstore etcd revoke lease", zap.String("lease", fmt.Sprintf("%d", lease.ID)), zap.Error(err))
			}
			revokeCancel()
		}
		return err
	}
	return b.c.Put(ctx, full, value)
}

func (b *EtcdBackend) Delete(ctx context.Context, key string) error {
	return b.c.Delete(ctx, b.prefix+key)
}

func (b *EtcdBackend) Exists(ctx context.Context, key string) (bool, error) {
	return b.c.Exists(ctx, b.prefix+key)
}

func (b *EtcdBackend) CAS(ctx context.Context, key, old, new string) (bool, error) {
	full := b.prefix + key
	cli := b.c.Raw()
	var cmp clientv3.Cmp
	var putOpts []clientv3.OpOption
	if old == "" {
		// put-if-absent：仅当键不存在（CreateRevision==0）时写入，新键本就没有租约可用
		cmp = clientv3.Compare(clientv3.CreateRevision(full), "=", 0)
	} else {
		cmp = clientv3.Compare(clientv3.Value(full), "=", old)
		// 裸 OpPut 会解绑键上已有的租约，让带 TTL 的键变成永不过期；
		// 改用 WithIgnoreLease 沿用当前租约。比较条件已保证键存在，不会触发「键不存在」错误。
		putOpts = append(putOpts, clientv3.WithIgnoreLease())
	}
	txnCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := cli.Txn(txnCtx).
		If(cmp).
		Then(clientv3.OpPut(full, new, putOpts...)).
		Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func (b *EtcdBackend) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	full := b.prefix + key
	cli := b.c.Raw()
	const maxAttempts = 16
	for range maxAttempts {
		getCtx, gc := context.WithTimeout(ctx, 3*time.Second)
		gr, err := cli.Get(getCtx, full)
		gc()
		if err != nil {
			return 0, err
		}
		var cur int64
		if len(gr.Kvs) > 0 {
			n, perr := strconv.ParseInt(string(gr.Kvs[0].Value), 10, 64)
			if perr != nil {
				return 0, ErrNotInteger
			}
			cur = n
		}
		cur += delta
		next := conv.FormatInt(cur)
		txnCtx, tc := context.WithTimeout(ctx, 3*time.Second)
		var cmp clientv3.Cmp
		var putOpts []clientv3.OpOption
		if len(gr.Kvs) == 0 {
			cmp = clientv3.Compare(clientv3.CreateRevision(full), "=", 0)
		} else {
			cmp = clientv3.Compare(clientv3.Value(full), "=", string(gr.Kvs[0].Value))
			// 沿用键当前绑定的租约：裸 OpPut 会解绑租约，一次自增就让带 TTL 的键永不过期。
			// 值比较已确认键仍存在，故 WithIgnoreLease 不会落到「键不存在」的错误分支。
			putOpts = append(putOpts, clientv3.WithIgnoreLease())
		}
		resp, terr := cli.Txn(txnCtx).
			If(cmp).
			Then(clientv3.OpPut(full, next, putOpts...)).
			Commit()
		tc()
		if terr != nil {
			return 0, terr
		}
		if resp.Succeeded {
			return cur, nil
		}
	}
	return 0, ErrIncrConflict
}

// List 列出 prefix 下全部键值；返回键为带此前缀的完整键；无匹配返回空 map。
func (b *EtcdBackend) List(ctx context.Context, prefix string) (map[string]string, error) {
	getCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := b.c.Raw().Get(getCtx, b.prefix+prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if kv.Key == nil {
			continue
		}
		out[string(kv.Key)] = string(kv.Value)
	}
	return out, nil
}

// BatchPut 原子批量写入：单个 Txn 提交，要么整批生效，要么整批不生效。
func (b *EtcdBackend) BatchPut(ctx context.Context, kv map[string]string) error {
	if len(kv) == 0 {
		return nil
	}
	ops := make([]clientv3.Op, 0, len(kv))
	for k, v := range kv {
		ops = append(ops, clientv3.OpPut(b.prefix+k, v))
	}
	txnCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := b.c.Raw().Txn(txnCtx).Then(ops...).Commit()
	return err
}

// Watch 监听 prefix 下的键变更，把事件按发生顺序交给 cb；ctx 取消即停止。
//
// 重连骨架复用 etcd 客户端的**唯一**实现（WatchPrefixLoop：lastRev 续看 / 压缩重置 /
// 指数退避 / 回调 panic 回收）。此前本后端自写了一套，退避策略（固定 1s vs 指数 2s→30s）
// 与压缩判定方式（CompactRevision 字段 vs 错误文案匹配）已经与客户端那套漂移。
//
// 本方法只负责两件事：事件类型映射（clientv3 事件类型 → 本包 Event）与 goroutine 归属
// ——挂到 b.wg 上，使 Close 能等到 watch 真正退出（连接生命周期仍由 etcd.Client 管理）。
func (b *EtcdBackend) Watch(ctx context.Context, prefix string, cb func(Event)) error {
	full := b.prefix + prefix
	b.wg.Go(func() {
		b.c.WatchPrefixLoopResync(ctx, full, func(evs []etcd.WatchEvent) {
			for _, ev := range evs {
				t := EventPut
				if ev.IsDelete {
					t = EventDelete
				}
				cb(Event{Type: t, Key: ev.Key, Value: ev.Value})
			}
		}, func() {
			// 压缩重置：lastRev 已归零，中间事件永久缺失，只能补一份全量快照。
			b.resync(ctx, prefix, cb)
		})
	})
	return nil
}

// resync 在「事件段被压缩」之后补发完整快照：先一条 EventResync 标记，再逐个键补 EventPut。
// 键空间与 Watch 事件一致（都是带 b.prefix 的完整键），调用方可直接覆盖本地镜像。
// 取快照失败只记日志：watch 循环仍在跑，下一次压缩重置或后续事件还会给它机会，
// 不因一次 List 失败就把 watch 协程打死。
func (b *EtcdBackend) resync(ctx context.Context, prefix string, cb func(Event)) {
	ilog.LogWarn("gstore etcd watch compacted, resyncing full snapshot",
		zap.String("prefix", b.prefix+prefix))
	kv, err := b.List(ctx, prefix)
	if err != nil {
		ilog.LogError("gstore etcd resync list failed",
			zap.String("prefix", b.prefix+prefix), zap.Error(err))
		return
	}
	cb(Event{Type: EventResync})
	for k, v := range kv {
		cb(Event{Type: EventPut, Key: k, Value: v})
	}
}

// Close 等待所有 Watch goroutine 退出后返回。连接生命周期由 etcd.Client 管理。
//
// 等待有超时兜底：Watch goroutine 仅在调用方传入的 ctx 取消后才退出，
// 调用方不取消（取自长存活 Background ctx）时无上限等待会让 Close 永久阻塞
// ——兜底口径与 etcd.Client.Close 一致（5s）。
func (b *EtcdBackend) Close() error {
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(etcdBackendCloseTimeout):
		return errors.New("gstore: etcd backend close timed out waiting for watch goroutines")
	}
}

// etcdBackendCloseTimeout Close 等待 Watch goroutine 退出的上限。
const etcdBackendCloseTimeout = 5 * time.Second
