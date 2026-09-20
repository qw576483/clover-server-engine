package udp

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/internal/shared/retry"
	"clover-server-engine/internal/transport/net/demux"
	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/safe"
)

// Handler 数据报回调。data 为消息体拷贝。
type Handler func(c *Conn, data []byte)

// Server UDP 服务器：按来源地址维护会话（连接无关）。
type Server struct {
	cfg     ServerConfig
	pc      net.PacketConn
	mgr     *session.Manager
	mu      sync.Mutex
	conns   map[string]*Conn
	handler Handler
	closed  atomic.Bool
	// started Start / StartFrom 重入保护：重复启动会再次监听并覆盖 s.pc，
	// 旧 socket 与 readLoop/cleanLoop 随之泄漏。
	started  atomic.Bool
	doneCh   chan struct{} // cleanLoop 退出信号
	doneOnce sync.Once     // 保护 doneCh 只关闭一次
}

// NewServer 构造 UDP 服务器。
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
// 重入保护：重复调用返回错误（不再监听/覆盖），已关闭的服务器拒绝再启动。
func (s *Server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("udp: server already started")
	}
	if s.closed.Load() {
		return errors.New("udp: server closed")
	}
	pc, err := net.ListenPacket("udp", s.cfg.ListenAddr)
	if err != nil {
		// 监听失败不算「已启动」：允许调用方修正后重试。
		s.started.Store(false)
		return err
	}
	s.mu.Lock()
	s.pc = pc
	s.mu.Unlock()
	logger.Infof("udp server listening on %s", pc.LocalAddr().String())
	// 直接把本次 PacketConn 传入读循环，避免循环内无锁读 s.pc 与 Start/Stop 竞争。
	safe.GoSafe(func() { s.readLoop(pc) })
	safe.GoSafe(s.cleanLoop)
	return nil
}

func (s *Server) cleanLoop() {
	// 扫描节奏与「锁内收集、锁外关闭」的约定统一在 session.IdleScanner
	//（quic / udp / wt 共用一份；Conn.Close 会回调 s.mu.Lock，锁内关闭会重入死锁）。
	session.IdleScanner[*Conn]{
		Timeout: s.cfg.IdleTimeout,
		Done:    s.doneCh,
		Stopped: s.closed.Load,
		Collect: func(now time.Time, timeout time.Duration) []*Conn {
			var expired []*Conn
			s.mu.Lock()
			for key, c := range s.conns {
				if now.Sub(c.lastReadTime()) > timeout {
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

// StartFrom 从共享 UDP 端口消费裸 UDP 数据报（与 QUIC 共用端口场景）。
// 不自行创建 socket，ch 由 demux.PacketConn.RawChan() 提供。
func (s *Server) StartFrom(ch <-chan *demux.Datagram) error {
	// 与 Start 共用同一重入/生命周期门：两种启动方式互斥，重复启动返回错误。
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("udp: server already started")
	}
	if s.closed.Load() {
		return errors.New("udp: server closed")
	}
	logger.Infof("udp server consuming raw datagrams from shared port %s", s.cfg.ListenAddr)
	safe.GoSafe(func() { s.readLoopFrom(ch) })
	safe.GoSafe(s.cleanLoop)
	return nil
}

// readLoopFrom 消费共享端口分发来的裸 UDP 数据报，逻辑与 readLoop 一致。
func (s *Server) readLoopFrom(ch <-chan *demux.Datagram) {
	for dg := range ch {
		// handler 为 nil 时不必建会话（防伪造源地址洪水内存放大）。
		if s.handler == nil {
			continue
		}
		c := s.getOrCreateConn(dg.Addr)
		if c == nil {
			// 会话数已达上限，丢弃该来源的包。
			continue
		}
		d := dg.Data
		safe.SafeRun(func() { s.handler(c, d) })
	}
}

func (s *Server) readLoop(pc net.PacketConn) {
	buf := make([]byte, s.cfg.MaxPacketSize)
	// 退避曲线统一由 retry.Backoff 提供（1ms → 1s，成功即重置）。
	// 原先这里、demux 与 quic accept 各写一份同款 `*= 2` 逻辑。
	bo := retry.NewBackoff(retry.Policy{
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Second,
		Multiplier: 2,
	})
	for {
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			if s.closed.Load() {
				return
			}
			// 临时性错误（如缓冲区暂时不可用）不应永久退出读循环。
			logger.Errorf("udp read: %v", err)
			time.Sleep(bo.Next())
			continue
		}
		bo.Reset()
		// 读满整个缓冲区：包可能超过缓冲区大小被内核丢弃，记告警。
		if n == len(buf) {
			logger.Warnf("udp read: datagram filled buffer (%d bytes); packet may have been dropped by kernel from %s", n, addr)
		}
		// handler 为 nil 时不必建会话：否则伪造源地址的洪水包会为每个地址
		// 各建一个 Conn 并常驻 conns/mgr，直到 IdleTimeout 才回收，形成内存放大。
		if s.handler == nil {
			continue
		}
		data := append([]byte(nil), buf[:n]...)
		c := s.getOrCreateConn(addr)
		if c == nil {
			// 会话数已达上限，丢弃该来源的包（已在 getOrCreateConn 内告警）。
			continue
		}
		// 同步派发：与 TCP/WS 读循环一致，保证同连接（同 addr）消息按到达顺序处理。
		d := data
		safe.SafeRun(func() { s.handler(c, d) })
	}
}

// getOrCreateConn 取回或新建来源地址对应的会话。
// 会话数达 MaxConns 时返回 nil，调用方应丢弃该包——防止伪造源地址洪水撑爆内存。
func (s *Server) getOrCreateConn(addr net.Addr) *Conn {
	key := addr.String()
	s.mu.Lock()
	c, ok := s.conns[key]
	if ok {
		s.mu.Unlock()
		c.touch()
		return c
	}
	if s.cfg.MaxConns > 0 && len(s.conns) >= s.cfg.MaxConns {
		s.mu.Unlock()
		logger.Warnf("udp server: conns limit %d reached, drop packet from %s", s.cfg.MaxConns, key)
		return nil
	}
	// 不在锁内调用 mgr.Add，避免 mgr.Add→old.Close()→srv.mu.Lock() 锁顺序反转死锁。
	c = newConn(s, addr)
	s.conns[key] = c
	s.mu.Unlock()
	s.mgr.Add(c)
	c.touch()
	return c
}

// packetConn 加锁快照当前 PacketConn，供 Conn.Send 等跨协程路径读取，
// 避免与 Start 中的 s.pc 赋值构成 data race。
func (s *Server) packetConn() net.PacketConn {
	s.mu.Lock()
	pc := s.pc
	s.mu.Unlock()
	return pc
}

// Addr 返回实际监听地址（含系统分配端口）。
func (s *Server) Addr() string {
	s.mu.Lock()
	pc := s.pc
	s.mu.Unlock()
	if pc != nil {
		return pc.LocalAddr().String()
	}
	return s.cfg.ListenAddr
}

// Manager 返回会话管理器。
func (s *Server) Manager() *session.Manager { return s.mgr }

// Stop 停止监听并关闭所有会话。关闭 doneCh 通知 cleanLoop 立即退出。
func (s *Server) Stop() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// 通知 cleanLoop 退出（避免仅靠 ticker + closed 检查存在 goroutine 延迟退出窗口）。
	s.doneOnce.Do(func() { close(s.doneCh) })
	s.mu.Lock()
	pc := s.pc
	s.mu.Unlock()
	if pc != nil {
		_ = pc.Close()
	}
	s.mgr.CloseAll()
	return nil
}
