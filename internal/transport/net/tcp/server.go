package tcp

import (
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/shared/retry"
	"github.com/qw576483/clover-server-engine/internal/transport/net/session"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// Server TCP 服务器：监听并接受连接，维护在线会话。
type Server struct {
	cfg       ServerConfig
	mu        sync.Mutex
	listener  net.Listener
	mgr       *session.Manager
	handler   Handler
	closed    atomic.Bool
	connCount atomic.Int32 // 当前活跃连接数（瞬时值，避免 mgr.Len() 延迟误拒）
}

// NewServer 构造 TCP 服务器。handler 为 nil 时仅保活连接。
func NewServer(cfg ServerConfig, handler Handler) *Server {
	return &Server{
		cfg:     cfg.normalize(),
		mgr:     session.NewManager(),
		handler: handler,
	}
}

// Start 启动监听（后台 accept 循环，不阻塞）。
func (s *Server) Start() error {
	var ln net.Listener
	var err error
	if s.cfg.TLSConfig != nil {
		ln, err = tls.Listen("tcp", s.cfg.ListenAddr, s.cfg.TLSConfig)
	} else {
		ln, err = net.Listen("tcp", s.cfg.ListenAddr)
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	logger.Infof("tcp server listening on %s", ln.Addr().String())
	// 直接把本次的 listener 传入 accept 循环，避免循环内无锁读 s.listener
	// 与 Start/Stop 的写入构成数据竞争。
	safe.GoSafe(func() { s.acceptLoop(ln) })
	return nil
}

func (s *Server) acceptLoop(ln net.Listener) {
	// 错误退避曲线统一由 retry.Backoff 提供（与 udp / quic / demux 读循环同一条策略）。
	bo := retry.NewBackoff(retry.Policy{
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Second,
		Multiplier: 2,
	})
	for {
		netConn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			// listener 已被关闭（Stop 之外的路径）：不可能再恢复，退出循环。
			if errors.Is(err, net.ErrClosed) {
				logger.Warnf("tcp accept: listener closed: %v", err)
				return
			}
			// 其余一律按临时错误处理：退避后继续（超时 / EMFILE 等均可自愈）。
			logger.Errorf("tcp accept: %v", err)
			time.Sleep(bo.Next())
			continue
		}
		bo.Reset()
		// 使用原子计数器替代 mgr.Len()，避免 Remove 延迟（goroutine 异步移除）导致误拒。
		if s.cfg.MaxConns > 0 && int(s.connCount.Load()) >= s.cfg.MaxConns {
			logger.Warnf("tcp: max connections %d reached, rejecting %s", s.cfg.MaxConns, netConn.RemoteAddr().String())
			_ = netConn.Close()
			continue
		}
		// 应用 TCP 调优参数（Nagle、KeepAlive、读写缓冲）。
		s.applyTuning(netConn)
		s.connCount.Add(1)
		c := newConn(netConn, session.NewConnID(), netConn.RemoteAddr().String(),
			s.cfg.MaxMsgSize, s.cfg.HeartbeatInterval, s.handler)
		s.mgr.Add(c)
		// 连接关闭后从会话管理器移除并递减计数。
		safe.GoSafe(func() {
			<-c.ClosedCh()
			s.connCount.Add(-1)
			s.mgr.Remove(c.ConnID())
		})
		c.start()
	}
}

// Addr 返回实际监听地址（含系统分配端口）。
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.ListenAddr
}

// Manager 返回会话管理器。
func (s *Server) Manager() *session.Manager { return s.mgr }

// applyTuning 在新建立的 net.Conn 上应用 TCP 调优参数。
func (s *Server) applyTuning(netConn net.Conn) {
	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(s.cfg.NoDelay)
		if s.cfg.KeepAlivePeriod > 0 {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(s.cfg.KeepAlivePeriod)
		}
		if s.cfg.ReadBufferSize > 0 {
			_ = tcpConn.SetReadBuffer(s.cfg.ReadBufferSize)
		}
		if s.cfg.WriteBufferSize > 0 {
			_ = tcpConn.SetWriteBuffer(s.cfg.WriteBufferSize)
		}
	}
}

// Stop 停止监听并关闭所有连接。
func (s *Server) Stop() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	s.mgr.CloseAll()
	return nil
}
