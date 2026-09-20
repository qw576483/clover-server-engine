package etcd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/internal/shared/retry"
	ilog "github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/validate"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Client ETCD 客户端封装，收口连接、KV、Watch、分布式锁等通用能力
type Client struct {
	cli       *clientv3.Client
	cfg       EtcdConfig
	wg        sync.WaitGroup // 追踪异步 goroutine（Watch/WatchPrefix/Register 续租）生命周期
	watchesMu sync.Mutex
	watches   map[string]context.CancelFunc // prefix watch 去重，key->cancel
	closed    chan struct{}                 // Close 时关闭：唤醒 Register 续租/重注册循环立即退出
	closeOnce sync.Once
}

// NewClient 创建 ETCD 客户端，并在返回前探测首节点连通性
func NewClient(conf EtcdConfig) (*Client, error) {
	if err := validate.Struct(conf); err != nil {
		return nil, fmt.Errorf("etcd: config invalid: %w", err)
	}
	// 默认值在此一次性归一化进 conf：Config() 返回的应是**生效值**。
	// 否则调用方（如服务发现注册时的租约 TTL）还得各自再兜一次默认值，
	// 同一个 10s / 5s 就会散落到多个包（应用层曾为此各写一份常量）。
	if conf.DialTimeout <= 0 {
		conf.DialTimeout = defaultDialTimeout
	}
	if conf.RegisterTTL <= 0 {
		conf.RegisterTTL = defaultRegisterTTL
	}

	cfg := clientv3.Config{
		Endpoints:   conf.Endpoints,
		DialTimeout: conf.DialTimeout,
		Username:    conf.Username,
		Password:    conf.Password,
	}
	if conf.TLS != nil {
		tlsCfg, err := buildTLSConfig(conf.TLS)
		if err != nil {
			return nil, fmt.Errorf("etcd: build tls: %w", err)
		}
		cfg.TLS = tlsCfg
	}

	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("etcd: create client: %w", err)
	}

	// 探测连通性，避免返回不可用的客户端。
	// 集群内任意节点可达即可正常工作（clientv3 自带故障转移与 endpoint 重选），
	// 只探首个端点会把「某台机器宕机」误判成「整个集群不可用」。
	probeCtx, cancel := context.WithTimeout(context.Background(), conf.DialTimeout)
	defer cancel()
	if err := probeEndpoints(probeCtx, cli, conf.Endpoints); err != nil {
		_ = cli.Close()
		return nil, err
	}

	c := &Client{cli: cli, cfg: conf, watches: make(map[string]context.CancelFunc), closed: make(chan struct{})}
	ilog.LogInfo("etcd client connected",
		zap.Any("endpoints", conf.Endpoints),
		zap.String("config_key", conf.ConfigKey))
	return c, nil
}

// probeEndpoints 逐个探测端点连通性：任一成功即视为集群可用并返回 nil；
// 全部不可达时返回最后一个错误（endpoints 为空时返回显式错误）。
func probeEndpoints(ctx context.Context, cli *clientv3.Client, endpoints []string) error {
	var lastErr error
	for _, ep := range endpoints {
		if _, err := cli.Status(ctx, ep); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr == nil {
		return errors.New("etcd: no endpoints configured")
	}
	return fmt.Errorf("etcd: connect %v failed: %w", endpoints, lastErr)
}

// Config 返回创建客户端时的连接参数
func (c *Client) Config() EtcdConfig { return c.cfg }

// ConfigKey 返回业务配置在 ETCD 中的存储 key
func (c *Client) ConfigKey() string { return c.cfg.ConfigKey }

// Raw 返回底层 clientv3 实例（高级场景如租约、选主）
func (c *Client) Raw() *clientv3.Client { return c.cli }

// Close 释放连接资源，避免泄漏；应在进程退出前调用
func (c *Client) Close() error {
	// 先广播关闭信号：唤醒 Register 续租/重注册循环（可能睡在退避 time.After 中）立即退出，
	// 否则 cli.Close 后重注册对已关闭客户端无限退避重试，wg.Wait 将永久阻塞。
	c.closeOnce.Do(func() { close(c.closed) })
	// 再取消所有活跃 watch，最后关闭连接。
	c.watchesMu.Lock()
	for _, cancel := range c.watches {
		cancel()
	}
	c.watchesMu.Unlock()
	// 关闭底层连接，触发所有 watch channel 关闭，goroutine 收到 ctx.Done / channel close 退出
	err := c.cli.Close()

	// 等待所有 Watch/WatchPrefix/续租 goroutine 退出。
	// etcd 不可达时清理调用可能久拖不决，加超时兜底，避免退出流程永久阻塞。
	waited := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(defaultCloseTimeout):
		ilog.LogWarn("etcd close: wait goroutines timeout", zap.Duration("timeout", defaultCloseTimeout))
		// 不覆盖 c.cli.Close() 的原始错误：底层关闭失败的真实信息不可丢，两者一并返回。
		err = errors.Join(err, fmt.Errorf("etcd close: wait goroutines timeout after %v", defaultCloseTimeout))
	}
	ilog.LogInfo("etcd client closed", zap.Any("endpoints", c.cfg.Endpoints))
	return err
}

// KV 基础封装 =
// Get 读取 key 对应的字符串值；key 不存在返回 ErrKeyNotFound
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		ilog.LogError("etcd get failed", zap.String("key", key), zap.String("err", err.Error()))
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", ErrKeyNotFound
	}
	return string(resp.Kvs[0].Value), nil
}

// GetPrefix 列出前缀下所有 key → value（服务发现用：一个前缀下挂多个实例）。
// 无匹配返回空 map（非错误）；调用方自行决定选哪一个（轮询 / 随机 / 哈希）。
func (c *Client) GetPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	resp, err := c.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		ilog.LogError("etcd get prefix failed", zap.String("prefix", prefix), zap.String("err", err.Error()))
		return nil, err
	}
	out := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out[string(kv.Key)] = string(kv.Value)
	}
	return out, nil
}

// Put 写入 key-value
func (c *Client) Put(ctx context.Context, key, value string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	if _, err := c.cli.Put(ctx, key, value); err != nil {
		ilog.LogError("etcd put failed", zap.String("key", key), zap.String("err", err.Error()))
		return err
	}
	return nil
}

// Delete 删除 key
func (c *Client) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	if _, err := c.cli.Delete(ctx, key); err != nil {
		ilog.LogError("etcd delete failed", zap.String("key", key), zap.String("err", err.Error()))
		return err
	}
	return nil
}

// Register 以「租约 + 自动续租」方式注册一个服务发现节点。

// 与 Put 的区别：写入的 key 绑定一个带 TTL 的租约，并启动后台 KeepAlive 续租；
// 进程崩溃 / 网络断开时续租停止，租约到期后 etcd 自动删除该 key，避免留下僵尸节点。

// 返回的 cancel 需在进程退出时调用，用于主动撤销注册（停续租 + 删 key + 回收租约）。
// ttl 为租约生存时间，<=0 使用 defaultRegisterTTL。
func (c *Client) Register(ctx context.Context, key, value string, ttl time.Duration) (func(), error) {
	if ttl <= 0 {
		ttl = defaultRegisterTTL
	}
	// 向上取整：etcd 租约以秒为单位，截断会让实际 TTL 短于配置值
	//（如 1500ms → 1s），续租间隔不变时更容易在续租窗口内过期。
	ttlSec := int(math.Ceil(ttl.Seconds()))
	if ttlSec <= 0 {
		ttlSec = 1
	}
	// 初始注册重试：etcd 偶发不可达（网络瞬断 / 探活波峰）时不应直接让服务启动失败。
	// 用引擎统一重试器（退避曲线只有一处定义，见 internal/shared/retry）：
	// 3 次尝试、100ms 起步、倍率 2、无上限截断——与原实现逐次延迟一致。
	const maxAttempts = 3
	policy := retry.Policy{
		MaxAttempts: maxAttempts,
		BaseDelay:   100 * time.Millisecond,
		Multiplier:  2,
	}
	var stop func()
	if err := retry.Do(ctx, policy, func(attempt int) error {
		// 客户端已关闭：重试没有意义，直接终止，不再空等退避。
		select {
		case <-c.closed:
			return retry.Permanent(errors.New("etcd: register aborted: client closed"))
		default:
		}
		leaseID, kaCh, kcancel, terr := c.tryGrantAndPut(ctx, key, value, ttlSec)
		if terr != nil {
			if attempt < maxAttempts {
				ilog.LogWarn("etcd: register attempt failed, retrying",
					zap.Int("attempt", attempt), zap.Int("max", maxAttempts), zap.Error(terr),
					zap.Duration("backoff", retry.NextDelay(policy, attempt)))
			}
			return terr
		}
		// 成功：启动续租 goroutine。
		stop = c.startKeepAliveLoop(ctx, key, value, ttlSec, leaseID, kaCh, kcancel)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("etcd: register: %w", err)
	}
	return stop, nil
}

// tryGrantAndPut 尝试一次 Grant + Put 初始注册，成功返回 leaseID/keepaliveCh/cancel。
func (c *Client) tryGrantAndPut(ctx context.Context, key, value string, ttl int) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, context.CancelFunc, error) {
	grantCtx, gcancel := context.WithTimeout(context.WithoutCancel(ctx), defaultOpTimeout)
	resp, err := c.cli.Grant(grantCtx, int64(ttl))
	gcancel()
	if err != nil {
		return 0, nil, nil, fmt.Errorf("etcd: grant lease: %w", err)
	}
	leaseID := resp.ID
	putCtx, pcancel := context.WithTimeout(context.WithoutCancel(ctx), defaultOpTimeout)
	_, perr := c.cli.Put(putCtx, key, value, clientv3.WithLease(leaseID))
	pcancel()
	if perr != nil {
		_, _ = c.cli.Revoke(context.Background(), leaseID)
		return 0, nil, nil, fmt.Errorf("etcd: put with lease: %w", perr)
	}
	kaCtx, kcancel := context.WithCancel(context.Background())
	ch, err := c.cli.KeepAlive(kaCtx, leaseID)
	if err != nil {
		kcancel()
		_, _ = c.cli.Delete(context.Background(), key)
		_, _ = c.cli.Revoke(context.Background(), leaseID)
		return 0, nil, nil, fmt.Errorf("etcd: keepalive: %w", err)
	}
	return leaseID, ch, kcancel, nil
}

// startKeepAliveLoop 启动续租后台 goroutine，返回 stop 函数供外部撤销注册。
func (c *Client) startKeepAliveLoop(ctx context.Context, key, value string, ttl int, leaseID clientv3.LeaseID, ch <-chan *clientv3.LeaseKeepAliveResponse, kcancel context.CancelFunc) func() {

	stopCh := make(chan struct{})
	done := make(chan struct{})
	c.wg.Add(1) // 纳入客户端 goroutine 追踪：Close 时 wg.Wait 保证续租协程已退出
	go func() {
		defer c.wg.Done()
		defer close(done)
		// 使用 WithoutCancel 保留 request-scoped ctx 的 values，同时避免父 ctx 取消影响清理操作。
		bgCtx := context.WithoutCancel(ctx)
		curCh, curLease, curCancel := ch, leaseID, kcancel
		defer func() { curCancel() }()
		for {
			select {
			case <-c.closed:
				// 客户端整体关闭：不再访问 etcd（连接即将/已经关闭），租约到期自动清理。
				return
			case <-stopCh:
				// 主动撤销：删除 key 并回收租约，注册随之消失。
				// 必须带超时，否则 etcd 不可达时 cancel() 的 <-done 会永久阻塞。
				c.cleanupRegistration(bgCtx, key, curLease)
				return
			case resp, ok := <-curCh:
				// 两种「续租丢失」都要重建：① 通道关闭（连接断开/租约过期）；
				// ② 服务端回报 TTL<=0（租约已失效、通道可能未关）——不检查则注册 key
				// 静默过期、节点从服务发现消失而进程无感知。
				if ok && resp != nil && resp.TTL <= 0 {
					ilog.LogWarn("etcd keepalive: server reported lease expired (ttl<=0), re-registering",
						zap.String("key", key), zap.Int64("ttl", resp.TTL))
				}
				leaseExpired := ok && resp != nil && resp.TTL <= 0
				if !ok || leaseExpired {
					// 续租通道关闭（连接断开/租约过期）：自动重注册直至成功或被撤销。
					// 不能退出循环——否则进程还活着却从服务发现中永久消失（网络闪断
					// 超过 TTL 后无人再写回 key），必须带退避循环重建租约。
					curCancel()
					newCh, newLease, newCancel, ok2 := c.reregister(bgCtx, key, value, ttl, stopCh)
					if !ok2 {
						curCancel = func() {} // 已无活跃 keepalive
						select {
						case <-c.closed:
							// 客户端已关闭：跳过清理（对关闭连接的操作无意义）。
						default:
							// stopCh 已触发：走撤销清理路径，key 与租约都要回收。
							c.cleanupRegistration(bgCtx, key, curLease)
						}
						return
					}
					curCh, curLease, curCancel = newCh, newLease, newCancel
				}
			}
		}
	}()

	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() { close(stopCh) })
		<-done
	}
	return cancel
}

// cleanupRegistration 撤销注册：删 key + 回收租约，两步都带超时，
// 避免 etcd 不可达时清理调用无限期挂起、拖死退出流程。
func (c *Client) cleanupRegistration(bgCtx context.Context, key string, leaseID clientv3.LeaseID) {
	delCtx, dcancel := context.WithTimeout(bgCtx, defaultOpTimeout)
	_, _ = c.cli.Delete(delCtx, key)
	dcancel()
	if leaseID == 0 {
		return
	}
	revCtx, rcancel := context.WithTimeout(bgCtx, defaultOpTimeout)
	_, _ = c.cli.Revoke(revCtx, leaseID)
	rcancel()
}

// reregister 续租断开后的自动重注册：指数退避重建「租约+Put+KeepAlive」。
// stopCh 触发时放弃并返回 ok=false。
func (c *Client) reregister(bgCtx context.Context, key, value string, ttl int, stopCh <-chan struct{}) (<-chan *clientv3.LeaseKeepAliveResponse, clientv3.LeaseID, context.CancelFunc, bool) {
	// 退避曲线统一走 retry.Backoff（与 watch / accept / read 循环同一条策略，
	// 不再各处手写 `*= 2` 与自定上限）。
	bo := retry.NewBackoff(retry.Policy{
		BaseDelay:  time.Second,
		MaxDelay:   30 * time.Second,
		Multiplier: 2,
		Jitter:     0.2,
	})
	for {
		select {
		case <-stopCh:
			return nil, 0, nil, false
		case <-c.closed:
			return nil, 0, nil, false
		default:
		}
		grantCtx, gcancel := context.WithTimeout(bgCtx, defaultOpTimeout)
		resp, err := c.cli.Grant(grantCtx, int64(ttl))
		gcancel()
		if err == nil {
			putCtx, pcancel := context.WithTimeout(bgCtx, defaultOpTimeout)
			_, perr := c.cli.Put(putCtx, key, value, clientv3.WithLease(resp.ID))
			pcancel()
			if perr == nil {
				kaCtx, kcancel := context.WithCancel(context.Background())
				ch, kerr := c.cli.KeepAlive(kaCtx, resp.ID)
				if kerr == nil {
					ilog.LogInfo("etcd re-registered after keepalive lost", zap.String("key", key))
					return ch, resp.ID, kcancel, true
				}
				// KeepAlive 失败：本轮 Grant 出来的租约必须回收，否则每轮重试泄漏一个租约。
				// Revoke 自身失败同样要留日志：否则「回收未闭环」的泄漏依旧不可观测。
				kcancel()
				revCtx, rcancel := context.WithTimeout(bgCtx, defaultOpTimeout)
				if _, rerr := c.cli.Revoke(revCtx, resp.ID); rerr != nil {
					ilog.LogWarn("etcd: revoke lease after keepalive failure failed",
						zap.String("key", key), zap.Int64("lease_id", int64(resp.ID)), zap.Error(rerr))
				}
				rcancel()
			} else {
				// Put 失败：同样要回收刚 Grant 的租约（Revoke 失败留日志，见上）。
				revCtx, rcancel := context.WithTimeout(bgCtx, defaultOpTimeout)
				if _, rerr := c.cli.Revoke(revCtx, resp.ID); rerr != nil {
					ilog.LogWarn("etcd: revoke lease after put failure failed",
						zap.String("key", key), zap.Int64("lease_id", int64(resp.ID)), zap.Error(rerr))
				}
				rcancel()
			}
		}
		select {
		case <-stopCh:
			return nil, 0, nil, false
		case <-c.closed:
			return nil, 0, nil, false
		case <-time.After(bo.Next()):
		}
	}
}

// Exists 判断 key 是否存在；仅当 key 不存在（ErrKeyNotFound）时返回 false 且无错
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.Get(ctx, key)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
