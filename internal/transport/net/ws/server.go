package ws

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/safe"
)

// wsRejectLogCount 连接数超限拒绝的日志降频计数（首次 + 每 1000 次）。
// 拒绝随连接尝试发生，可被高频触发，不允许逐条刷屏。
var wsRejectLogCount atomic.Uint64

// Server WebSocket 服务器：基于 HTTP 升级，维护在线会话与二进制通信。
type Server struct {
	cfg        ServerConfig
	upgrader   websocket.Upgrader
	mu         sync.Mutex // 保护 ln / httpServer（Start 写、Addr/Stop 读，可能跨 goroutine），并串行化连接数检查与预占
	ln         net.Listener
	httpServer *http.Server
	mgr        *session.Manager
	handler    Handler
	closed     atomic.Bool
	// connCount 当前已升级连接数（含尚未开始发帧的连接）：连接数上限判定的依据。
	connCount atomic.Int32
	// started Start 重入保护：重复 Start 会再次监听并覆盖 s.ln/s.httpServer，
	// 旧 listener 与 Serve goroutine 随之泄漏。
	started atomic.Bool
	wg      sync.WaitGroup // 跟踪 Serve goroutine 退出
}

// NewServer 构造 WebSocket 服务器。
func NewServer(cfg ServerConfig, handler Handler) *Server {
	c := cfg.normalize()
	up := websocket.Upgrader{
		ReadBufferSize:   c.ReadBufferSize,
		WriteBufferSize:  c.WriteBufferSize,
		HandshakeTimeout: c.HandshakeTimeout,
		CheckOrigin:      c.CheckOrigin,
	}
	return &Server{
		cfg:      c,
		upgrader: up,
		mgr:      session.NewManager(),
		handler:  handler,
	}
}

// Start 启动 HTTP 服务并开始接受 WebSocket 升级（后台运行，不阻塞）。
// 重入保护：重复调用返回错误（不再监听/覆盖），已关闭的服务器拒绝再启动。
func (s *Server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("ws: server already started")
	}
	if s.closed.Load() {
		return errors.New("ws: server closed")
	}
	var ln net.Listener
	var err error
	if s.cfg.TLSConfig != nil {
		ln, err = tls.Listen("tcp", s.cfg.Addr, s.cfg.TLSConfig)
	} else {
		ln, err = net.Listen("tcp", s.cfg.Addr)
	}
	if err != nil {
		// 监听失败不算「已启动」：允许调用方修正后重试。
		s.started.Store(false)
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.Path, s.serveWS)
	// WebTransport 证书固定哈希下发端点：浏览器先经本 wss 通道取得哈希，
	// 再以 serverCertificateHashes 建 WebTransport，绕开 Chromium 对自建根的拒绝。
	if s.cfg.WTCertHash != "" || s.cfg.WTCertHashFunc != nil {
		mux.HandleFunc("/wt-cert-hash", s.serveWTCertHash)
	}
	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // 缓解 Slowloris 攻击风险
	}
	s.mu.Lock()
	s.ln = ln
	s.httpServer = httpServer
	s.mu.Unlock()
	scheme := "ws"
	if s.cfg.TLSConfig != nil {
		scheme = "wss"
	}
	logger.Infof("ws server listening on %s://%s%s", scheme, ln.Addr().String(), s.cfg.Path)
	s.wg.Add(1)
	safe.GoSafe(func() {
		defer s.wg.Done()
		if err := httpServer.Serve(ln); err != nil && !s.closed.Load() {
			logger.Errorf("ws http server: %v", err)
		}
	})
	return nil
}

// serveWTCertHash 以 JSON 返回服务器证书 DER 的 SHA-256，供浏览器做 WebTransport 证书固定。
func (s *Server) serveWTCertHash(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-store")
	// 优先取动态哈希：证书运行期间自动轮换后必须返回最新值。
	hash := s.cfg.WTCertHash
	if s.cfg.WTCertHashFunc != nil {
		hash = s.cfg.WTCertHashFunc()
	}
	_, _ = w.Write([]byte(`{"hash":"` + hash + `"}`))
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	// 连接数上限（DoS 防护）：在 Upgrade 之前判定并预占额度。
	// 检查与预占在同一临界区：只建连不发帧的连接不会进入网关会话计数，
	// 若不封顶，攻击者可无限建连耗尽 goroutine/内存。
	s.mu.Lock()
	over := s.cfg.MaxConns > 0 && int(s.connCount.Load()) >= s.cfg.MaxConns
	if !over {
		s.connCount.Add(1)
	}
	s.mu.Unlock()
	if over {
		if n := wsRejectLogCount.Add(1); n == 1 || n%1000 == 0 {
			logger.Warnf("ws: max connections %d reached, rejecting %s（同类累计 %d 次，已降频输出）", s.cfg.MaxConns, r.RemoteAddr, n)
		}
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	wsConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.connCount.Add(-1) // 升级失败：归还预占额度
		logger.Errorf("ws upgrade: %v", err)
		return
	}
	c := newConn(wsConn, session.NewConnID(), r.RemoteAddr, s.cfg.MaxMsgSize, s.cfg.HeartbeatInterval, s.handler)
	// 先启动读写协程再注册到 manager，避免 manager 含未就绪连接导致广播时 Send 到
	// 尚未启动 sendCh 消费者的连接。
	c.start()
	s.mgr.Add(c)
	// 连接关闭后从会话管理器移除（与 TCP 服务端一致、对齐 UDP），避免已断开连接长期驻留；
	// 同步归还连接数额度（每条连接恰好归还一次：本 goroutine 随连接关闭执行一次）。
	safe.GoSafe(func() {
		<-c.ClosedCh()
		s.connCount.Add(-1)
		s.mgr.Remove(c.ConnID())
	})
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

// Path 返回升级路径。
func (s *Server) Path() string { return s.cfg.Path }

// Manager 返回会话管理器。
func (s *Server) Manager() *session.Manager { return s.mgr }

// Stop 停止服务并关闭所有连接。等待 Serve goroutine 退出后才关闭会话。
func (s *Server) Stop() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.mu.Lock()
	ln := s.ln
	httpServer := s.httpServer
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	if httpServer != nil {
		_ = httpServer.Close()
	}
	// 等待 Serve goroutine 退出，然后再关闭所有会话，避免竞态。
	s.wg.Wait()
	s.mgr.CloseAll()
	return nil
}
