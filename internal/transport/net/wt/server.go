package wt

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/safe"
	"github.com/quic-go/quic-go/http3"
	iwt "github.com/quic-go/webtransport-go"
)

// Handler WebTransport 流回调。data 为消息体拷贝。
type Handler func(c *Conn, data []byte)

// Server WebTransport 服务器：按连接维护会话。
type Server struct {
	cfg   ServerConfig
	mgr   *session.Manager
	mu    sync.Mutex
	conns map[string]*Conn
	// pending 正在 Upgrade / AcceptStream 中、尚未登记的连接数。
	// MaxConns 的额度必须把「在途」也算进去：检查与登记之间隔着网络往返，
	// 只数 conns 会让并发升级一起挤进来把上限顶穿（见 handleWebTransport）。
	pending  int
	handler  Handler
	server   *iwt.Server
	h3Server *http3.Server
	closed   atomic.Bool
	// started Start 重入保护：重复启动会覆盖 s.h3Server/s.server，
	// 旧监听与 cleanLoop 随之泄漏。
	started  atomic.Bool
	doneCh   chan struct{}
	doneOnce sync.Once
	wg       sync.WaitGroup // 追踪 ListenAndServe goroutine 退出
}

// NewServer 构造 WebTransport 服务器。
func NewServer(cfg ServerConfig, handler Handler) *Server {
	return &Server{
		cfg:     cfg.normalize(),
		mgr:     session.NewManager(),
		conns:   make(map[string]*Conn),
		handler: handler,
		doneCh:  make(chan struct{}),
	}
}

// Start 启动监听（后台读循环 + 清理，不阻塞）。
// 重入保护：重复调用返回错误（不再启动/覆盖），已关闭的服务器拒绝再启动。
func (s *Server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return fmt.Errorf("webtransport server: already started")
	}
	if s.closed.Load() {
		return fmt.Errorf("webtransport server: closed")
	}
	// 浏览器 WebTransport 必须使用受信任证书：TLSConfig 为 nil 时直接失败（本模块不生成自签名证书）。
	if s.cfg.TLSConfig == nil {
		s.started.Store(false) // 启动失败不算「已启动」，允许修正后重试
		return fmt.Errorf("webtransport server: TLSConfig required (browser only trusts real certs)")
	}
	tlsConf := s.cfg.TLSConfig

	mux := http.NewServeMux()
	mux.HandleFunc("/wt", s.handleWebTransport)

	s.h3Server = &http3.Server{
		Addr:      s.cfg.ListenAddr,
		Handler:   mux,
		TLSConfig: tlsConf,
	}

	s.server = &iwt.Server{
		H3: s.h3Server,
	}
	// 注入 Origin 校验：默认走 webtransport-go 内置同源校验（拒绝跨源）。
	// 网页端通常从独立端口连接网关（Origin 与 Host 不同源），必须显式放开才能完成升级。
	if s.cfg.CheckOrigin != nil {
		s.server.CheckOrigin = s.cfg.CheckOrigin
	}

	logger.Infof("webtransport server listening on %s", s.cfg.ListenAddr)
	safe.GoSafe(s.cleanLoop)

	// 通过 webtransport.Server.ListenAndServe 启动（而非直接 h3Server.ListenAndServe）：
	// 库内部会 quicConf.EnableDatagrams=true 并在 ServeQUICConn 里注册 sessionManager，
	// Upgrade 才能找到会话并把不可靠推送走 Datagram 下发；直接裸起 http3.Server 会
	// 缺这两者导致所有 WebTransport 升级失败。
	s.wg.Add(1)
	safe.GoSafe(func() {
		defer s.wg.Done()
		if err := s.server.ListenAndServe(); err != nil && !s.closed.Load() {
			logger.Errorf("webtransport http3 server: %v", err)
		}
	})
	return nil
}

// handleWebTransport 处理 WebTransport 升级请求。
func (s *Server) handleWebTransport(w http.ResponseWriter, r *http.Request) {
	// Origin 校验交由 webtransport-go 内置的 CheckOrigin（默认同源校验，基于 r.Host 比对，
	// 与 HTTP/3 一致；手动用 r.Header["Host"] 校验在 HTTP/3 中恒为空会误拒所有浏览器请求）。

	// 额度预占：Upgrade + AcceptStream 是网络往返（最坏可达秒级），
	// 「检查 map 大小」与「登记」之间必须先把额度占住，否则并发升级会一起通过检查、
	// 一起登记，MaxConns 直接失效。
	if !s.reserve() {
		logger.Warnf("webtransport server: conns limit %d reached, drop connection from %s", s.cfg.MaxConns, r.RemoteAddr)
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	// 未走到登记就返回（升级失败 / 开流失败）时归还额度；登记成功则由登记处转成活跃连接。
	registered := false
	defer func() {
		if !registered {
			s.release()
		}
	}()

	wtSession, err := s.server.Upgrade(w, r)
	if err != nil {
		logger.Errorf("webtransport upgrade: %v", err)
		return
	}

	stream, err := wtSession.AcceptStream(r.Context())
	if err != nil {
		logger.Errorf("webtransport accept stream: %v", err)
		_ = wtSession.CloseWithError(0, "")
		return
	}

	c := newConn(s, wtSession, stream)
	s.mu.Lock()
	s.conns[c.ConnID()] = c
	// 同一把锁内换账：预占额度转成活跃连接，总量不变，中途不会有别的连接挤进来。
	s.pending--
	registered = true
	s.mu.Unlock()
	s.mgr.Add(c)

	safe.GoSafe(func() { s.readLoop(c) })
}

// reserve 预占一个连接额度：把「检查上限」与「占位」放进同一把锁（原子）。
// 超过上限返回 false。
func (s *Server) reserve() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.MaxConns > 0 && len(s.conns)+s.pending >= s.cfg.MaxConns {
		return false
	}
	s.pending++
	return true
}

// release 归还一个未被登记就放弃的预占额度。
func (s *Server) release() {
	s.mu.Lock()
	if s.pending > 0 {
		s.pending--
	}
	s.mu.Unlock()
}

func (s *Server) readLoop(c *Conn) {
	buf := make([]byte, 65536) // 64KB 缓冲区
	for {
		n, err := c.stream.Read(buf)
		if err != nil {
			if s.closed.Load() {
				return
			}
			logger.Errorf("webtransport read: %v", err)
			_ = c.Close()
			return
		}
		c.touch()
		data := append([]byte(nil), buf[:n]...)
		if s.handler != nil {
			d := data
			safe.SafeRun(func() { s.handler(c, d) })
		}
	}
}

func (s *Server) cleanLoop() {
	// 扫描节奏与退出语义统一在 session.IdleScanner（quic / udp / wt 共用一份）。
	session.IdleScanner[*Conn]{
		Timeout: s.cfg.IdleTimeout,
		Done:    s.doneCh,
		Stopped: s.closed.Load,
		Collect: func(now time.Time, timeout time.Duration) []*Conn {
			var expired []*Conn
			s.mu.Lock()
			for key, c := range s.conns {
				// 以最后一次收或发的时间判定空闲：只下发不收包的连接同样是活跃连接（与 quic 对齐）。
				if now.Sub(c.lastActiveTime()) > timeout {
					expired = append(expired, c)
					delete(s.conns, key)
				}
			}
			s.mu.Unlock()
			return expired
		},
		Close: func(c *Conn) { c.closeWithoutLock() },
	}.Run()
}

// Addr 返回配置的监听地址（未解析系统实际分配的端口）。
func (s *Server) Addr() string {
	s.mu.Lock()
	h3Server := s.h3Server
	s.mu.Unlock()
	if h3Server != nil {
		return s.cfg.ListenAddr
	}
	return s.cfg.ListenAddr
}

// Manager 返回会话管理器。
func (s *Server) Manager() *session.Manager { return s.mgr }

// Stop 停止监听并关闭所有会话。
func (s *Server) Stop() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.doneOnce.Do(func() { close(s.doneCh) })
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()
	if server != nil {
		// iwt.Server.Close 会关闭其 H3 http3.Server 并关闭全部已建立的 QUIC 连接。
		_ = server.Close()
	}
	// 等待 ListenAndServe goroutine 退出（server.Close 后即返回）。
	s.wg.Wait()
	s.mgr.CloseAll()
	return nil
}
