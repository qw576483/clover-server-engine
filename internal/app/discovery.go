// #nosec G115 -- 取模结果恒小于模数（n/total 为正数且非 0），乘 1 之后仍在 int 范围内：转换不会溢出（报告 §四「已排除」）。

// 引擎**唯一的** etcd 服务发现设施，全角色共用（logic / auth / log …）。
//
// 约定：每个实例以「前缀 + 每实例唯一 key」注册自己的**可调用地址**，值为地址字符串：
//
//	clover/services/logic/<role>-<host>-<pid> = 127.0.0.1:8011   // 逻辑服（网关上游）
//	clover/services/auth/<role>-<host>-<pid>  = 127.0.0.1:8061   // 账号服消息通道
//	clover/services/log/<role>-<host>-<pid>   = 127.0.0.1:8031   // 日志服
//
// 为什么用前缀而不是单 key：单 key 单值下第二个实例注册会**覆盖**第一个，
// 调用方永远只能看到一个实例——多实例横向扩展直接失效。前缀 + 唯一 key 才能列出全部实例。
//
// 注册走「租约 + 自动续租」：进程崩溃 / 断网 → 租约到期，etcd 自动摘除该实例，不留僵尸节点。
// 调用方按前缀拉全量列表，WatchPrefix 感知增删，调用时**轮询**选一个实例。
//
// 消费方：网关上游（logic，启动时选一次）、Game.CallAuth（每次调用轮询）、
// Game.CallLog（每次调用轮询）、业务日志管道 Game.AddLog（logbuf 按片轮询，每实例一条长连接）。
//
// 「静态地址」的角色差异（别一刀切）：逻辑服上游是**静态优先**——配了 logic.listen_addr
// 就直接用、不查 etcd（见 bootstrap 的网关上游解析）；auth / log 是**发现优先、静态兜底**
// ——resolver.pick 先看实例列表，列表为空才回落到 auth.rpc_addr / log_addr。
//
// 未配置 etcd（endpoints 为空）时全部退化为「配置里的静态地址」，行为与未接入发现时一致。
package app

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/etcd"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/util"
)

// 服务角色名（同时是 etcd 前缀的路径段）。
const (
	roleLogic = "logic" // 逻辑服：网关的上游
	roleAuth  = "auth"  // 账号服：消息通道
	roleLog   = "log"   // 日志服
)

// serviceKeyRoot 所有服务发现 key 的根前缀。
const serviceKeyRoot = "clover/services/"

// servicePrefix 返回某角色的实例注册前缀（注意以 "/" 结尾）。
func servicePrefix(role string) string { return serviceKeyRoot + role + "/" }

// defaultDiscoveryTimeout 单次实例列表拉取的超时。
// （注册租约 TTL 的默认值不在这里——它由 etcd 客户端统一归一化，见其 NewClient。）
const defaultDiscoveryTimeout = 3 * time.Second

// serviceInstanceID 生成本实例在 etcd 中的唯一 key 后缀。
// 用「角色-主机名-PID」：同机多实例靠 PID 区分，跨机靠主机名区分，且人肉可读。
func serviceInstanceID(role string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%s-%d", role, host, os.Getpid())
}

// registerService 把本实例的 addr 以「租约 + 自动续租」注册到该角色的 etcd 前缀下。
//
// 返回的 unregister 在进程退出时调用（撤销注册、回收租约），可安全重复调用。
// ec / addr 为空时返回空操作——未配 etcd 的部署保持「静态地址直连」行为。
func registerService(ctx context.Context, ec *etcd.Client, role, addr string) (func(), error) {
	if ec == nil || addr == "" {
		return func() {}, nil
	}
	// TTL 不再在此兜默认值：etcd 客户端已把 <=0 归一化为 defaultRegisterTTL，
	// Register 自身也对 <=0 兜底，应用层再写一份默认值只会多一处需要同步的地方。
	ttl := ec.Config().RegisterTTL
	key := servicePrefix(role) + serviceInstanceID(role)
	stop, err := ec.Register(ctx, key, addr, ttl)
	if err != nil {
		return nil, fmt.Errorf("discovery: register %s (%s) failed: %w", role, addr, err)
	}
	logger.Infof("discovery: %s registered at %s (key=%s ttl=%s)", role, addr, key, ttl)
	return stop, nil
}

// serviceResolver 按前缀发现某角色下的多个实例地址，并在实例增删时自动刷新。
//
// 未启用 etcd 时退化为「固定返回 static」；启用但列表为空时同样回退 static
// （首次拉取尚未完成 / etcd 抖动都不应让调用方直接失败）。
type serviceResolver struct {
	role   string
	prefix string
	static string
	ec     *etcd.Client

	mu    sync.RWMutex
	addrs []string
	next  atomic.Uint64
}

// newServiceResolver 构造解析器。ec 为 nil（未配 etcd）时只走 staticAddr。
//
// 轮询游标用「纳秒时间戳 ^ PID」播种：多个调用方进程（如多网关）若都从 0 开始，
// 首次 pick 会全部选中同一个实例，扩了等于没扩。
func newServiceResolver(role, staticAddr string, ec *etcd.Client) *serviceResolver {
	r := &serviceResolver{role: role, prefix: servicePrefix(role), static: staticAddr, ec: ec}
	r.next.Store(uint64(time.Now().UnixNano()) ^ uint64(os.Getpid())<<16)
	return r
}

// start 首次拉取实例列表并注册前缀监听（实例增删时自动刷新）。
// 首次拉取失败不阻断启动：降级为 static，后续 watch 回调会自愈。
func (r *serviceResolver) start(ctx context.Context) {
	if r == nil || r.ec == nil {
		return
	}
	r.refresh(ctx)
	if err := r.ec.WatchPrefix(ctx, r.prefix, func() { r.refresh(context.WithoutCancel(ctx)) }); err != nil {
		// ErrWatchExists 表示同前缀已有监听（多个解析器共用），不是故障。
		logger.Warnf("discovery: %s watch prefix %s: %v", r.role, r.prefix, err)
	}
}

// refresh 重新拉取实例列表。失败保留旧列表（宁可短暂过期，也不要让调用直接失败）。
func (r *serviceResolver) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, defaultDiscoveryTimeout)
	defer cancel()
	m, err := r.ec.GetPrefix(ctx, r.prefix)
	if err != nil {
		// 列表长度读加锁：r.addrs 由其它 goroutine 的 refresh 在锁内整体替换。
		r.mu.RLock()
		n := len(r.addrs)
		r.mu.RUnlock()
		logger.Warnf("discovery: %s list %s failed: %v (keep %d cached)", r.role, r.prefix, err, n)
		return
	}
	addrs := make([]string, 0, len(m))
	for _, v := range m {
		if v != "" {
			addrs = append(addrs, v)
		}
	}
	// 排序保证各调用方看到一致顺序（便于排障，也让轮询可预期）。
	sort.Strings(addrs)
	r.mu.Lock()
	changed := !util.Equal(addrs, r.addrs)
	r.addrs = addrs
	r.mu.Unlock()
	if changed {
		logger.Infof("discovery: %s instances updated: %v", r.role, addrs)
	}
}

// pick 轮询选一个实例地址；无可用实例时回退 static。返回空串表示确实无处可去。
func (r *serviceResolver) pick() string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	n := len(r.addrs)
	var addr string
	if n > 0 {
		// 先按 uint64 取模再转 int：游标旧值为 2^64-1 时 `int(旧值)%n` 会得负数
		//（Go 的负数取模结果仍为负），导致切片索引越界 panic。
		addr = r.addrs[int((r.next.Add(1)-1)%uint64(n))]
	}
	r.mu.RUnlock()
	if addr != "" {
		return addr
	}
	// 缓存为空：etcd 未启用 / 首次拉取未完成 / 实例全部下线 → 回退静态地址。
	if r.static != "" {
		return r.static
	}
	if r.ec != nil {
		logger.Warnf("discovery: %s has no available instance (etcd empty), call will fail", r.role)
	}
	return ""
}

// newEtcdClientForRole 按全局 etcd 配置创建客户端；未配置 etcd 时返回 (nil, noop)。
//
// 与 game 的 etcd 处理不同，这里**不阻断启动**：game 侧 etcd 用于把自己登记给网关
// （强依赖，失败即启动失败）；auth / log 的注册是「对外提供可发现性」的增强能力，
// etcd 抖动不应让账号服 / 日志服起不来（它们单实例也能正常工作）。
func newEtcdClientForRole(cfg *Config, role string) (*etcd.Client, func()) {
	if len(cfg.Etcd.Endpoints) == 0 {
		return nil, func() {}
	}
	ec, err := etcd.NewClient(cfg.Etcd)
	if err != nil {
		logger.Warnf("discovery: %s: etcd unavailable (%v), running without service discovery", role, err)
		return nil, func() {}
	}
	return ec, func() { _ = ec.Close() }
}

// resolverDialer 把 *serviceResolver 适配成 logbuf.Dialer，供日志缓冲器注入。
//
// 为什么要这层薄适配：pick 是包内小写方法（resolver 的调用方都在 app 包内），
// 而 logbuf 需要的是它自己的 Dialer 接口。多一层适配比把 pick 导出、
// 从而放大 resolver 的公开面更保守。
type resolverDialer struct{ r *serviceResolver }

// Pick 见 logbuf.Dialer。r 为 nil 时返回空串，调用方会回退静态地址。
func (d resolverDialer) Pick() string {
	if d.r == nil {
		return ""
	}
	return d.r.pick()
}
