// Package http 实现 clover 通用「HTTP 传输层」。
//
// 定位：与 net/tcp、net/ws 平级，是 base 的一等公民传输原语；
// 它只负责「监听 TCP → 每来一个 HTTP 请求就调一次 Handler(w, r)」，
// 不做任何业务路由 / 鉴权 / 派发——那些由上层（如 event 的 OnHTTP 事件）决定。
//
// 实现即本包（无 pkg 门面）：使用方是 internal 侧的 transport/event（逻辑服 HTTP 控制面）
// 与 app 的 AdminServer，业务不直接依赖本包。
//
// 与 ws 的关键区别：ws 是「升级后常驻二进制连接 + 心跳」，本包是
// 「一次性请求-响应式」——没有会话、没有玩家对象、没有上行推送，
// 提供无状态 HTTP 控制面入口。
package http

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/safe"
)

// Handler 每来一个 HTTP 请求回调一次（标准库签名，便于直接复用 http.HandlerFunc / mux）。
// 上层（如 event）在此内做路由匹配与派发，本包不关心。
type Handler func(w http.ResponseWriter, r *http.Request)

// Server HTTP 服务器。
type Server struct {
	cfg     Config
	handler Handler
	mu      sync.Mutex // 保护 ln / httpSrv（Start 写、Addr/Stop 读，可能跨 goroutine）
	ln      net.Listener
	httpSrv *http.Server
	// started Start 重入保护：重复调用会再次监听并覆盖 s.ln/s.httpSrv，
	// 旧 listener 与 Serve goroutine 随之泄漏。
	started atomic.Bool
	// closed Stop 后拒绝再启动（服务器为单生命周期）。
	closed atomic.Bool
}

// NewServer 创建 HTTP 服务器（尚未启动监听）。
func NewServer(cfg Config, handler Handler) *Server {
	return &Server{cfg: cfg.normalize(), handler: handler}
}

// Start 启动监听（后台运行，不阻塞）。
// 重入保护：重复调用返回错误（不再监听/覆盖），已关闭的服务器拒绝再启动。
func (s *Server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("http: server already started")
	}
	if s.closed.Load() {
		return errors.New("http: server closed")
	}
	// TLS 只配一半（有证书无私钥 / 反之）必须启动即失败：静默回落成明文
	// 是「运维以为已加密、实际裸奔」的最坏失败方式。
	if err := s.cfg.TLS.Validate(); err != nil {
		s.started.Store(false)
		return err
	}
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		// 监听失败不算「已启动」：允许调用方修正后重试。
		s.started.Store(false)
		return err
	}
	// handler 为 nil 时 http.HandlerFunc(nil) 会在每个请求上 panic，
	// 兜底回 404，保持「未配置处理器 = 无可用路由」的语义。
	h := s.handler
	if h == nil {
		h = func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }
	}
	httpSrv := &http.Server{
		Handler:           http.HandlerFunc(h),
		ReadTimeout:       s.cfg.ReadTimeout,
		WriteTimeout:      s.cfg.WriteTimeout,
		IdleTimeout:       s.cfg.IdleTimeout,
		MaxHeaderBytes:    s.cfg.MaxHeaderBytes,
		ReadHeaderTimeout: 10 * time.Second, // 缓解 Slowloris 攻击
	}
	s.mu.Lock()
	s.ln = ln
	s.httpSrv = httpSrv
	s.mu.Unlock()
	tlsEnabled := s.cfg.TLS.Enabled()
	logger.Infof("http server listening on %s (tls=%t)", ln.Addr().String(), tlsEnabled)
	safe.GoSafe(func() {
		var err error
		if tlsEnabled {
			// 最低版本显式收敛到 1.2：Go 默认已是 1.2，但显式写出来才不会被
			// 「将来默认值变化」或「有人传了自定义 TLSConfig」悄悄降级。
			httpSrv.TLSConfig = &tls.Config{MinVersion: DefaultTLSMinVersion} // #nosec G402 -- MinVersion 已显式指定
			err = httpSrv.ServeTLS(ln, s.cfg.TLS.CertFile, s.cfg.TLS.KeyFile)
		} else {
			err = httpSrv.Serve(ln)
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Errorf("http server: %v", err)
		}
	})
	return nil
}

// Addr 返回实际监听地址（含系统分配端口）。
func (s *Server) Addr() string {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		return ln.Addr().String()
	}
	return s.cfg.Addr
}

// DefaultShutdownTimeout 通用 HTTP 优雅关闭的默认等待上限。
//
// 与 pkg/app/types 的 AdminConfig 默认值是两个不同用途的常量：那个是**配置默认值**
// （可被 yaml 覆盖），这里是**传输层的兜底值**（调用方不传超时或传 <=0 时用）。
// pkg 不能反向依赖 internal，两者无法共用同一个常量——改默认值时需同步。
const DefaultShutdownTimeout = 5 * time.Second

// Shutdown 优雅关闭一个 *http.Server：先 Shutdown（停止接受新连接并等待在途请求
// 处理完毕，避免 Close() 把正在响应的请求切断成半截 body / 连接重置），
// 超时或失败再强制 Close 兜底，确保端口释放。label 仅用于日志前缀。
//
// 本函数把此前**写了两份**的关闭流程收敛为一处：本包的 Server 与 app 的 AdminServer
// 各自实现过一遍，且各自硬编码了 5s。
func Shutdown(srv *http.Server, timeout time.Duration, label string) error {
	if srv == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Warnf("%s: http shutdown failed (%v), forcing close", label, err)
		return srv.Close()
	}
	return nil
}

// Stop 停止服务器并关闭监听（优雅关闭 + 超时强制兜底，见 Shutdown）。
func (s *Server) Stop() error {
	s.closed.Store(true)
	s.mu.Lock()
	httpSrv := s.httpSrv
	s.mu.Unlock()
	return Shutdown(httpSrv, DefaultShutdownTimeout, "http server")
}
