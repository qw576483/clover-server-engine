package app

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nethttp "github.com/qw576483/clover-server-engine/internal/transport/net/http"
	apptypes "github.com/qw576483/clover-server-engine/pkg/app/types"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/foundation/metrics"
)

// 常量
const (
	// DefaultListenAddr admin HTTP 控制面默认监听地址（127.0.0.1:8041，只绑定回环，
	// 避免默认配置下把运维接口暴露到公网）。真身在 pkg/app/types，这里是别名；
	// 要关闭 admin 请用 admin.disable=true，留空会让 net.Listen 监听所有网卡。
	DefaultListenAddr = apptypes.DefaultListenAddr
	// DefaultShutdownTimeout admin server 关闭时等待在途请求的超时上限。
	// 与 pkg/app/types 的默认值同源——AdminConfig 的真身在那里（这里是别名），
	// 两处各写一个 5s 会在改默认值时漂移。
	DefaultShutdownTimeout = apptypes.DefaultShutdownTimeout
	// DefaultReadHeaderTimeout 读取请求头超时，防 Slowloris。
	DefaultReadHeaderTimeout = 5 * time.Second
)

// 编译期断言：admin 的默认关闭超时与传输层 HTTP 的兜底值必须一致。
// 两者一个是「配置默认值」、一个是「调用方不传超时时的兜底」，分处 pkg 与 internal
// 无法共用一个常量；用数组下标越界把「不一致」变成编译错误，避免改一处漏一处。
var _ = [1]struct{}{}[apptypes.DefaultShutdownTimeout-nethttp.DefaultShutdownTimeout]

// AdminConfig 内置 admin HTTP 控制面配置。

// 零值即「启用 + 监听 127.0.0.1:8041」，符合仓库「零值能回落到默认」的约定；
// 需要关闭时显式设置 Disable: true。
type AdminConfig = apptypes.AdminConfig

// Server
// AdminServer 内置运维 HTTP 服务。

// 内置端点：
//   - /ping     → 自检（admin活着+列出路由）
//   - /routes   → 列出已注册路由
//   - /log/level→ 动态调整日志级别
//   - /metrics  → Prometheus 指标暴露
//   - /deadletter → 跨服死信队列人工介入（读 + 重投/删除；**需令牌**，见下「鉴权」）
//   - /debug/pprof/* → Go 运行时性能分析（需 Pprof: true）
//
// 鉴权：配置 admin.token 后，高危端点（/admin/*、/deadletter、/log/level、/debug/pprof*）必须带同一
// 令牌（X-Admin-Token 或 Authorization: Bearer）；未配置 token 时 AdminConfig.Normalize
// **要求监听地址必须是回环**（否则构造即返回错误、启动被拒），即「要么只回环，要么带令牌」。

// Handle 必须在 Start 之前调用；Start 之后再注册会被忽略并记警告日志
// （http.ServeMux 在服务运行中注册路由不是并发安全的）。
type AdminServer struct {
	cfg      AdminConfig
	mu       sync.Mutex
	mux      *http.ServeMux
	routes   []string
	srv      *http.Server
	ln       net.Listener
	started  bool
	stopOnce sync.Once
	// lastRejectLog 上一次因鉴权失败打日志的秒级时间戳（拒绝日志降频，防刷屏）。
	lastRejectLog atomic.Int64
}

// newAdminServer 构造 admin HTTP 服务（此时尚未监听）。
// 返回 (nil, nil) 表示配置为禁用；调用方对 nil 接收者调用 Start/Stop/Handle 是安全的（空操作）。
//
// 返回 error 表示配置不安全（未配 admin.token 却要绑非回环地址）。**调用方必须中止启动**：
// 该配置会让 /admin/shutdown、/admin/drain、/admin/gateway/upstream、/deadletter、
// /log/level、/debug/pprof 在无任何鉴权的状态下对所有同网可达者开放。
func newAdminServer(cfg *AdminConfig) (*AdminServer, error) {
	if cfg.Disable {
		return nil, nil
	}
	if err := cfg.Normalize(); err != nil {
		// 拒绝路径必须留日志：这条是「启动被配置挡住」的唯一线索，
		// 只把 err 返回给上层会让日志里只剩一句 wrap 文案，缺了「怎么修」。
		logger.Errorf("admin: 拒绝启动 —— %v；请把 admin.listen_addr 改回回环（127.0.0.1），"+
			"或同时配置 admin.token 以在非回环地址上启用令牌鉴权（控制面含 /admin/shutdown、"+
			"/admin/drain、/admin/gateway/upstream、/deadletter、/log/level、/debug/pprof 等高危端点）", err)
		return nil, err
	}
	if cfg.Token != "" {
		logger.Infof("admin: token 鉴权已启用（/admin/*、/deadletter、/log/level、/debug/pprof 需带 X-Admin-Token 或 Authorization: Bearer）")
	}
	as := &AdminServer{
		cfg: *cfg,
		mux: http.NewServeMux(),
	}
	// 内置探针：确认 admin server 本身活着 + 列出已注册路由。
	// 内置 pattern 一并记入 routes：Handle 的重复注册检测依赖它，
	// 漏记会让业务注册同名 pattern 时打到 ServeMux 的重复注册 panic。
	as.mux.HandleFunc("/ping", as.handlePing)
	as.mux.HandleFunc("/routes", as.handleRoutes)
	// 日志等级动态调整
	as.mux.Handle("/log/level", logger.LevelHandler())
	// Prometheus 指标暴露
	as.mux.Handle("/metrics", as.instrument(metrics.Handler()))
	as.routes = append(as.routes, "/ping", "/routes", "/log/level", "/metrics")
	// Go 运行时性能分析（可选）
	if cfg.Pprof {
		as.registerPprof()
	}
	return as, nil
}

// Handle 注册一个 admin 路由。nil handler 或空 pattern 被忽略。
// 重复注册同一 pattern 时后者被忽略（避免 ServeMux 因重复注册 panic）。
func (as *AdminServer) Handle(pattern string, h http.Handler) {
	if as == nil || h == nil || pattern == "" {
		return
	}
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.started {
		logger.Warnf("admin: register %s after Start, ignored", pattern)
		return
	}
	// 重复注册（含与内置端点重名）必须在此拦下：ServeMux.Handle 对重复
	// pattern 直接 panic，会击穿启动流程。
	for _, p := range as.routes {
		if p == pattern {
			logger.Warnf("admin: duplicate route %s, ignored", pattern)
			return
		}
	}
	as.mux.Handle(pattern, as.instrument(h))
	as.routes = append(as.routes, pattern)
}

// HandleFunc 是 Handle 的函数形式。
func (as *AdminServer) HandleFunc(pattern string, fn func(http.ResponseWriter, *http.Request)) {
	if fn == nil {
		return
	}
	as.Handle(pattern, http.HandlerFunc(fn))
}

// Routes 返回已注册路由快照（排序后，便于日志与自检稳定输出）。
func (as *AdminServer) Routes() []string {
	if as == nil {
		return nil
	}
	as.mu.Lock()
	defer as.mu.Unlock()
	out := make([]string, len(as.routes))
	copy(out, as.routes)
	sort.Strings(out)
	return out
}

// Addr 返回实际监听地址（Start 之后有效，支持 :0 自动分配端口场景）。
func (as *AdminServer) Addr() string {
	if as == nil {
		return ""
	}
	// as.ln 由 Start 在 as.mu 内写入：无锁读与 Start 并发构成数据竞争
	//（gateway / all 模式会在运行期用它判断 admin 是否已启动）。
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.ln == nil {
		return ""
	}
	return as.ln.Addr().String()
}

// Start 开始监听（后台 goroutine 提供服务，不阻塞调用方）。
// 端口占用等监听失败会立即返回错误；nil 接收者（禁用）直接返回 nil。
func (as *AdminServer) Start() error {
	if as == nil {
		return nil
	}
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.started {
		return errors.New("admin: already started")
	}
	ln, err := net.Listen("tcp", as.cfg.ListenAddr)
	if err != nil {
		return err
	}
	logger.Infof("admin: listening on %s (token_auth=%t)", ln.Addr().String(), as.cfg.Token != "")
	// 非回环绑定必须显式告警：端口上挂着 /admin/shutdown、/admin/drain、/admin/gateway/upstream、
	// /deadletter、/log/level、/debug/pprof 等高危端点。走到这里时 token 必定非空
	//（无 token + 非回环已在 Normalize 被拒、构造期就返回错误），故只需提醒再加一层网络隔离；
	// 下面仍保留「无 token」的分支做纵深防御：调用方若绕过 Normalize 直接构造本结构体，
	// 这条 Error 是最后一道可见线索。
	if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok && !tcpAddr.IP.IsLoopback() {
		if as.cfg.Token == "" {
			logger.Errorf("admin: listening on non-loopback address %s WITHOUT authentication; anyone who can "+
				"reach it can call /admin/shutdown, /admin/drain or /admin/gateway/upstream — bind to 127.0.0.1, "+
				"restrict the port with a firewall, or set admin.token", tcpAddr.String())
		} else {
			logger.Warnf("admin: listening on non-loopback address %s WITH token auth — keep the port behind a "+
				"firewall / private network as well", tcpAddr.String())
		}
	}
	as.ln = ln
	as.srv = &http.Server{
		// guard：admin.token 配置后，高危端点（/admin/*、/deadletter、/log/level、/debug/pprof）必须带令牌。
		Handler:           as.guard(as.mux),
		ReadHeaderTimeout: DefaultReadHeaderTimeout,
	}
	as.started = true
	go func() {
		if err := as.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf("admin: serve error: %v", err)
		}
	}()
	return nil
}

// Stop 关闭 admin 服务：等待在途请求结束，超时后强制关闭。幂等。
func (as *AdminServer) Stop(ctx context.Context) {
	if as == nil {
		return
	}
	as.stopOnce.Do(func() {
		as.mu.Lock()
		srv := as.srv
		timeout := as.cfg.ShutdownTimeout
		// 外层 ctx 带截止时间且更紧时取剩余时间：关闭流程不应超出调用方给的预算。
		if deadline, ok := ctx.Deadline(); ok {
			if remain := time.Until(deadline); remain < timeout {
				timeout = remain
			}
		}
		as.mu.Unlock()
		// Shutdown 放在锁外：它会等所有在途请求结束（最长 timeout），
		// 持 as.mu 执行会让正在处理的 /ping、/routes 等请求卡在取锁上，
		// 本该「优雅关闭」变成「全体卡到超时」。
		// 关闭流程与传输层共用一份实现（Shutdown 内含「优雅关闭 → 超时强制 Close」兜底）。
		if err := nethttp.Shutdown(srv, timeout, "admin"); err != nil {
			logger.Warnf("admin: force close failed: %v", err)
		}
		logger.Infof("admin: stopped")
	})
}

// NewAdminServer 构造 admin HTTP 服务 + 挂载跨服死信端点。
//
// 返回 (nil, nil) 表示 admin 被配置禁用；返回 error 表示配置不安全（见 newAdminServer），
// 调用方必须中止启动而不是忽略它。
func NewAdminServer(cfg AdminConfig) (*AdminServer, error) {
	s, err := newAdminServer(&cfg)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	// 死信队列人工介入：请求到达时再从 currentGame 取实时 bus，
	// 避免「构造期 bus 尚未存在」与「运行中改 mux」两种情况。
	s.Handle("/deadletter", deadLetterDispatcher())
	s.Handle("/deadletter/", deadLetterDispatcher())
	// 灰度下线 / 滚动重启端点（drain / gateway upstream / shutdown）。
	InstallDrainRoutes(s)
	return s, nil
}

// 鉴权
// adminTokenHeader 约定的令牌请求头（也可用 Authorization: Bearer <token>）。
const adminTokenHeader = "X-Admin-Token"

// guardedAdminPaths 必须鉴权的高危端点前缀：
// 能关服 / 切上游 / 改日志级别 / 读运行时内存 / 改跨服死信队列。
//
// /ping、/routes、/metrics 不在此列 —— 它们是探针与指标抓取，只读且无副作用，不该被令牌挡住。
//
// /deadletter/dlq/retry 与 /deadletter/dlq/remove 会**重投 / 删除**跨服事件（有业务副作用），
// 只读的 /deadletter/dlq 与 /deadletter/pending 也直接暴露事件体与玩家标识，
// 仅在回环之外可达就等于把这些能力交给所有同网可达者，故整组纳入门禁。
var guardedAdminPaths = []string{"/admin/", "/deadletter", "/log/level", "/debug/pprof"}

// guard 按 guardedAdminPaths 对高危端点做令牌校验。
// token 为空（未配置）时**放行**：此时 Normalize 已保证监听地址只可能是回环
// （非回环 + 无 token 的配置在构造期就被 ErrNonLoopbackWithoutToken 挡下、根本起不来），
// 二者是「或」关系——要么只回环，要么带令牌，两者都不满足的配置不会走到这里。
func (as *AdminServer) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if as == nil || as.cfg.Token == "" || !isGuardedAdminPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if subtle.ConstantTimeCompare([]byte(extractAdminToken(r)), []byte(as.cfg.Token)) == 1 {
			next.ServeHTTP(w, r)
			return
		}
		// 非预期分支必须留痕（但要防日志刷屏：同一个来源每秒最多记一条）。
		if as.allowRejectLog() {
			logger.Warnf("admin: rejected unauthorized %s %s from %s (missing/incorrect admin token)",
				r.Method, r.URL.Path, r.RemoteAddr)
		}
		adminAuthRejected.Inc()
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized: admin token required", http.StatusUnauthorized)
	})
}

// isGuardedAdminPath 判断路径是否落在受保护前缀内。
func isGuardedAdminPath(path string) bool {
	for _, p := range guardedAdminPaths {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// extractAdminToken 取请求携带的令牌：优先自定义头，其次 `Authorization: Bearer <token>`。
func extractAdminToken(r *http.Request) string {
	if t := r.Header.Get(adminTokenHeader); t != "" {
		return t
	}
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(auth) > len(prefix) && strings.EqualFold(auth[:len(prefix)], prefix) {
		return strings.TrimSpace(auth[len(prefix):])
	}
	return ""
}

// allowRejectLog 拒绝日志的降频闸门（每秒最多一条）。
func (as *AdminServer) allowRejectLog() bool {
	now := time.Now().Unix()
	last := as.lastRejectLog.Load()
	if last >= now {
		return false
	}
	return as.lastRejectLog.CompareAndSwap(last, now)
}

// 内部方法
// adminReqTotal admin HTTP 请求总计数器。
var adminReqTotal = metrics.CounterOf("clover_admin_http_requests_total")

// adminAuthRejected 被令牌鉴权拒绝的请求数（>0 说明有人在探控制面）。
var adminAuthRejected = metrics.CounterOf("clover_admin_auth_rejected_total")

// instrument 给 handler 包一层计数埋点，每次请求递增总计数器。
func (as *AdminServer) instrument(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		adminReqTotal.Inc()
		h.ServeHTTP(w, r)
	})
}

// handlePing 内置占位探针：返回 admin server 状态与已注册路由列表。
func (as *AdminServer) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"routes": as.Routes(),
		"time":   time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		logger.Warnf("admin: encode ping response: %v", err)
	}
}

// handleRoutes 返回所有已注册路由列表。
func (as *AdminServer) handleRoutes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"routes": as.Routes(),
	}); err != nil {
		logger.Warnf("admin: encode routes response: %v", err)
	}
}

// registerPprof 注册 Go 运行时性能分析端点。
func (as *AdminServer) registerPprof() {
	// 复用 net/http/pprof 的标准 handler。
	as.mux.HandleFunc("/debug/pprof/", pprof.Index)
	as.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	as.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	as.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	as.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	as.routes = append(as.routes, "/debug/pprof/")
}
