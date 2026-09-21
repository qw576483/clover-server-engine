// #nosec G115 -- 同上：Xxhash64(key) % uint64(total) 的结果恒小于 total，total 为正分片数。

// master 分片的 etcd 服务发现。
//
// 与 discovery.go 的「注册值为裸地址、按轮询选实例」约定**不同**：master 分片是
// 「按 key 定归属」，因此走独立前缀与独立解析器：
//
//	clover/services/master/<index> = {"addr":"127.0.0.1:8021","total":2}
//
// 值为 JSON 的原因：调用方除了地址，还必须知道总分片数才能算归属
// （index = Xxhash64(key) % total）；serviceResolver 只认裸地址，语义不匹配。
//
// 未配 etcd（ec == nil）或分片表为空时，pick 回落静态 master_addr，
// 行为与未启用分片完全一致（单机 / 单 master 部署零感知）。
package app

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/transport/etcd"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/qw576483/clover-server-engine/pkg/shared/util"
)

// roleMaster master 角色名（同时是 etcd 前缀的路径段）。
const roleMaster = "master"

// masterShardValue master 分片注册值。
// Addr 为该分片的可调用地址；Total 为集群总分片数（>=1）。
type masterShardValue struct {
	Addr  string `json:"addr"`
	Total int    `json:"total"`
}

// masterShardKey 返回某分片在 etcd 中的注册键：clover/services/master/<index>。
func masterShardKey(index int) string {
	return servicePrefix(roleMaster) + strconv.Itoa(index)
}

// registerMasterShard 把本分片的可调用地址以「租约 + 自动续租」注册到 etcd。
//
// 返回的 unregister 在进程退出时调用（撤销注册、回收租约），可安全重复调用。
// ec 为 nil（未配 etcd）或 addr 为空时返回空操作——此时分片无法被 game 发现，
// game 侧会回落到静态 master_addr（单分片部署即如此，行为与未启用分片一致）。
func registerMasterShard(ctx context.Context, ec *etcd.Client, index, total int, addr string) (func(), error) {
	if ec == nil || addr == "" {
		return func() {}, nil
	}
	value, err := ujson.Marshal(masterShardValue{Addr: addr, Total: total})
	if err != nil {
		return nil, fmt.Errorf("discovery: marshal master shard value: %w", err)
	}
	key := masterShardKey(index)
	ttl := ec.Config().RegisterTTL
	stop, err := ec.Register(ctx, key, string(value), ttl)
	if err != nil {
		return nil, fmt.Errorf("discovery: register master shard %d/%d (%s) failed: %w", index, total, addr, err)
	}
	logger.Infof("discovery: master shard %d/%d registered at %s (key=%s ttl=%s)", index, total, addr, key, ttl)
	return stop, nil
}

// masterShardResolver 解析 etcd 中的 master 分片表，并按 key 计算归属（uid / 榜名 → 分片地址）。
//
// 归属规则（与 master 侧一致，两端唯一契约）：index = Xxhash64(key) % total。
type masterShardResolver struct {
	ec     *etcd.Client
	static string // 兜底静态地址（未配 etcd / 分片表为空时使用）
	mu     sync.RWMutex
	addrs  map[int]string // index → addr
	total  int
	// missingWarned 记录已告警过的「缺失分片序号」，避免 pick 在
	// 高频路径（每次跨节点投递）上对同一个缺失分片反复打日志。
	// 分片表刷新后清空，使下一次变更能重新告警。
	missingWarned map[int]bool
}

// newMasterShardResolver 构造解析器。staticAddr 为配置里的静态 master 地址。
func newMasterShardResolver(staticAddr string, ec *etcd.Client) *masterShardResolver {
	return &masterShardResolver{
		ec:            ec,
		static:        staticAddr,
		addrs:         make(map[int]string),
		missingWarned: make(map[int]bool),
	}
}

// start 首次拉取分片表并注册前缀监听（分片上下线自动刷新）。
// ec 为 nil 时不做任何事：pick 只回落静态地址。
func (r *masterShardResolver) start(ctx context.Context) {
	if r == nil || r.ec == nil {
		return
	}
	r.refresh(ctx)
	if err := r.ec.WatchPrefix(ctx, servicePrefix(roleMaster), func() { r.refresh(context.WithoutCancel(ctx)) }); err != nil {
		// 监听失败只降级为「只在启动时拉一次」，不阻断启动。
		logger.Warnf("discovery: watch master shards %s: %v", servicePrefix(roleMaster), err)
	}
}

// refresh 重新拉取分片表；失败保留旧值（宁可短暂过期，也不要让查询直接失败）。
// total 取各分片声明值的最大值：配置不一致时以最大值为准并告警。
func (r *masterShardResolver) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, defaultDiscoveryTimeout)
	defer cancel()
	m, err := r.ec.GetPrefix(ctx, servicePrefix(roleMaster))
	if err != nil {
		// 列表长度读加锁：r.addrs 由其它 goroutine 的 refresh 在锁内整体替换。
		r.mu.RLock()
		n := len(r.addrs)
		r.mu.RUnlock()
		logger.Warnf("discovery: list master shards failed: %v (keep %d cached)", err, n)
		return
	}
	addrs := make(map[int]string, len(m))
	total := 0
	prefix := servicePrefix(roleMaster)
	for k, raw := range m {
		if raw == "" {
			continue
		}
		index, convErr := strconv.Atoi(strings.TrimPrefix(k, prefix))
		if convErr != nil {
			logger.Warnf("discovery: master shard key %q is not <prefix><index>, ignored", k)
			continue
		}
		var v masterShardValue
		if jsonErr := ujson.Unmarshal([]byte(raw), &v); jsonErr != nil || v.Addr == "" {
			logger.Warnf("discovery: master shard %d value %q invalid, ignored", index, raw)
			continue
		}
		addrs[index] = v.Addr
		if v.Total > total {
			total = v.Total
		}
	}
	r.mu.Lock()
	// 变更判定不能只看数量与 total：某个分片换了地址（数量不变）同样要留变更日志。
	changed := total != r.total || !sameMasterShardAddrs(addrs, r.addrs)
	r.addrs, r.total = addrs, total
	r.missingWarned = make(map[int]bool) // 表已更新：允许对新的缺失分片重新告警
	r.mu.Unlock()
	if changed {
		logger.Infof("discovery: master shards updated: total=%d addrs=%v", total, addrs)
	}
}

// sameMasterShardAddrs 比较两份分片表是否等价（索引与地址都一致）。
// 只比较数量的写法会漏掉「某分片换地址」的变更日志。
func sameMasterShardAddrs(a, b map[int]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// pick 返回 key 归属分片的地址。
//
// 分片表为空（未配 etcd / 尚未拉到 / 全部 master 下线）时回落静态地址；
// 分片表非空但目标分片缺失时返回空串——**不**回落别的分片，
// 否则会把请求发给不持有该 key 的 master，读到错误数据。
func (r *masterShardResolver) pick(key string) string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	total, addrs := r.total, r.addrs
	r.mu.RUnlock()
	if total <= 0 || len(addrs) == 0 {
		return r.static
	}
	index := int(util.Xxhash64(key) % uint64(total))
	addr, ok := addrs[index]
	if !ok {
		// 单个缺失分片只告警一次（pick 在每次跨节点投递前都会被调用）。
		r.mu.Lock()
		first := !r.missingWarned[index]
		r.missingWarned[index] = true
		r.mu.Unlock()
		if first {
			logger.Warnf("discovery: master shard %d/%d is not registered, requests for its keys will fail "+
				"(only reported once per shard)", index, total)
		}
		return ""
	}
	return addr
}

// allAddrs 返回当前已知的全部分片地址（按 index 升序），供「所有分片」类操作使用
// （如排行榜 BackupAll：每个分片只持有自己的榜，必须逐个调用）。
func (r *masterShardResolver) allAddrs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.addrs) == 0 {
		return nil
	}
	indexes := make([]int, 0, len(r.addrs))
	for i := range r.addrs {
		indexes = append(indexes, i)
	}
	sort.Ints(indexes)
	out := make([]string, 0, len(indexes))
	for _, i := range indexes {
		out = append(out, r.addrs[i])
	}
	return out
}
