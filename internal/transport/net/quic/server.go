package quic

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/internal/shared/retry"
	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/safe"
	iquic "github.com/quic-go/quic-go"
)

const (
	// accept 失败后的退避区间：从 1ms 起指数增长到 1s。
	// 底层 PacketConn 由外部注入时（QUIC 与裸 UDP 共享端口），被外部关闭后
	// Accept 会立即返回错误，无退避会形成忙循环并刷屏日志。
	acceptErrBackoffMin = time.Millisecond
	acceptErrBackoffMax = time.Second
	// acceptErrLogBurst 连续错误中逐条记日志的上限，超出后按 acceptErrLogInterval 汇总，
	// 避免持续故障期间日志被同一条错误淹没。
	acceptErrLogBurst    = 3
	acceptErrLogInterval = 30 * time.Second
)

// Handler QUIC 流回调。data 为消息体拷贝。
type Handler func(c *Conn, data []byte)

// Server QUIC 服务器：按连接维护会话。
type Server struct {
	cfg      ServerConfig
	listener *iquic.Listener
	mgr      *session.Manager
	mu       sync.Mutex
	conns    map[string]*Conn
	handler  Handler
	closed   atomic.Bool
	// started Start / StartWithPacketConn 重入保护：重复启动会覆盖 s.listener，
	// 旧 listener 与 acceptLoop/cleanLoop 随之泄漏。
	started  atomic.Bool
	doneCh   chan struct{}
	doneOnce sync.Once
}

// NewServer 构造 QUIC 服务器。
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
		return errors.New("quic: server already started")
	}
	if s.closed.Load() {
		return errors.New("quic: server closed")
	}
	tlsConf, err := s.buildTLSConfig()
	if err != nil {
		s.started.Store(false) // 启动失败不算「已启动」，允许修正后重试
		return err
	}
	listener, err := iquic.ListenAddr(s.cfg.ListenAddr, tlsConf, s.buildQUICConfig())
	if err != nil {
		s.started.Store(false)
		return err
	}
	return s.initListener(listener)
}

// StartWithPacketConn 从外部注入的 PacketConn 启动 QUIC 监听（与裸 UDP 共享端口场景）。
// 数据报经 demux.PacketConn 按首字节分发，仅 QUIC 包到达本监听器。
func (s *Server) StartWithPacketConn(pc net.PacketConn) error {
	// 与 Start 共用同一重入/生命周期门：两种启动方式互斥，重复启动返回错误。
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("quic: server already started")
	}
	if s.closed.Load() {
		return errors.New("quic: server closed")
	}
	tlsConf, err := s.buildTLSConfig()
	if err != nil {
		s.started.Store(false)
		return err
	}
	tr := &iquic.Transport{Conn: pc}
	listener, err := tr.Listen(tlsConf, s.buildQUICConfig())
	if err != nil {
		s.started.Store(false)
		return err
	}
	return s.initListener(listener)
}

// buildTLSConfig 返回 TLS 配置：TLSConfig 必填，为 nil 时返回错误（本模块不生成自签名证书）。
func (s *Server) buildTLSConfig() (*tls.Config, error) {
	if s.cfg.TLSConfig != nil {
		return s.cfg.TLSConfig, nil
	}
	return nil, fmt.Errorf("quic server: TLSConfig required")
}

// buildQUICConfig 构造 quic-go 服务器配置。
func (s *Server) buildQUICConfig() *iquic.Config {
	return &iquic.Config{
		MaxIdleTimeout:     s.cfg.IdleTimeout,
		MaxIncomingStreams: int64(s.cfg.MaxStreams),
		EnableDatagrams:    true,
	}
}

// initListener 保存监听器并启动后台 accept/clean 循环。
func (s *Server) initListener(listener *iquic.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	logger.Infof("quic server listening on %s", listener.Addr().String())
	safe.GoSafe(s.acceptLoop)
	safe.GoSafe(s.cleanLoop)
	return nil
}

func (s *Server) acceptLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 守护协程双监听：正常路径由 Stop 关闭 doneCh 触发 cancel；
	// acceptLoop 因其它原因退出时（defer cancel 先执行，ctx.Done 关闭）也随即退出，不残留。
	go func() {
		select {
		case <-s.doneCh:
		case <-ctx.Done():
		}
		cancel()
	}()
	// 退避曲线统一由 retry.Backoff 提供（与 udp / demux 的读循环同一条策略）；
	// 失败计数兼作日志节流依据，成功后一并清零。
	bo := retry.NewBackoff(retry.Policy{
		BaseDelay:  acceptErrBackoffMin,
		MaxDelay:   acceptErrBackoffMax,
		Multiplier: 2,
	})
	var lastLog time.Time
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil {
			if s.closed.Load() || ctx.Err() != nil {
				return
			}
			delay := bo.Next()
			if failures := bo.Failures(); failures <= acceptErrLogBurst || time.Since(lastLog) >= acceptErrLogInterval {
				lastLog = time.Now()
				if failures > acceptErrLogBurst {
					logger.Errorf("quic accept: %v (repeated %d times)", err, failures)
				} else {
					logger.Errorf("quic accept: %v", err)
				}
			}
			select {
			case <-s.doneCh:
				return
			case <-time.After(delay):
			}
			continue
		}
		bo.Reset()
		// 每条连接一个 goroutine：serveConn 里要等对端开首条流（最长 AcceptStreamTimeout），
		// 同步调用会把**整个 accept 循环**一起挂住 —— 一个只建连不开流的客户端就能让
		// 后续所有连接排队等到超时（比占用一个 MaxConns 名额严重得多）。
		safe.GoSafe(func() { s.serveConn(conn) })
	}
}

// serveConn 接纳一条新连接：先登记再等流。
// 连接必须在 AcceptStream 之前进入 conns/mgr，否则等待首条流期间该连接
// 不受 MaxConns 约束，也无法被 cleanLoop 与 Stop 回收（这是刻意的：
// 一条已建立的 QUIC 连接本来就占资源，理应算在 MaxConns 里，
// 只是等流超时后会被回收）。
func (s *Server) serveConn(conn *iquic.Conn) {
	c := newConn(s, conn)
	s.mu.Lock()
	if s.cfg.MaxConns > 0 && len(s.conns) >= s.cfg.MaxConns {
		s.mu.Unlock()
		logger.Warnf("quic server: conns limit %d reached, drop connection from %s", s.cfg.MaxConns, conn.RemoteAddr())
		_ = conn.CloseWithError(0, "")
		return
	}
	s.conns[c.ConnID()] = c
	s.mu.Unlock()
	// mgr.Add 在 s.mu 之外调用：Add 覆盖同 ID 旧会话时会 Close 旧连接，
	// 而 Conn.Close 内部要反向取 s.mu，锁内调用会形成锁顺序反转。
	s.mgr.Add(c)

	// 等待对端打开首条流；超时未开流的连接直接关闭，避免它长期占住 accept 循环。
	stream, err := c.acceptStream(s.cfg.AcceptStreamTimeout)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			logger.Warnf("quic accept stream: no stream within %s from %s", s.cfg.AcceptStreamTimeout, conn.RemoteAddr())
		case isRemoteNormalClose(err):
			// Error code 0 是远端正常关闭（如客户端 quit），按常规断开处理，不记 ERROR。
			logger.Debugf("quic accept stream: remote closed with code 0 from %s", conn.RemoteAddr())
		default:
			logger.Errorf("quic accept stream: %v", err)
		}
		_ = c.Close()
		return
	}
	// 等待期间可能已被 cleanLoop 或 Stop 关闭，此时不必再起读循环。
	if c.IsClosed() {
		_ = stream.Close()
		return
	}
	c.setStream(stream)

	// 启动读循环：可靠流 + 不可靠数据报（位置同步等）两条并行读。
	safe.GoSafe(func() { s.readLoop(c) })
	safe.GoSafe(func() { s.datagramLoop(c) })
}

// isRemoteNormalClose 判断是否为远端以 code 0 发起的正常关闭。
func isRemoteNormalClose(err error) bool {
	var appErr *iquic.ApplicationError
	return errors.As(err, &appErr) && appErr.ErrorCode == 0
}

func (s *Server) readLoop(c *Conn) {
	var header [quicFrameLenSize]byte
	for {
		if _, err := io.ReadFull(c.stream, header[:]); err != nil {
			if s.closed.Load() {
				return
			}
			if isRemoteNormalClose(err) {
				logger.Debugf("quic read: remote closed normally from %s", c.RemoteAddr())
			} else if errors.Is(err, io.EOF) {
				logger.Debugf("quic read: stream closed normally from %s", c.RemoteAddr())
			} else {
				logger.Errorf("quic read frame header: %v", err)
			}
			_ = c.Close()
			return
		}
		frameLen := binary.BigEndian.Uint32(header[:])
		if frameLen > maxQUICFrameSize {
			logger.Errorf("quic read: frame too large: %d", frameLen)
			_ = c.Close()
			return
		}
		if frameLen == 0 {
			// 空帧（如对端保活）：只算活跃信号，不派发给业务 handler
			//（与 tcp「空业务帧忽略」对齐；此前会把空 body 当有效消息下发）。
			c.touch()
			continue
		}
		data := make([]byte, frameLen)
		if _, err := io.ReadFull(c.stream, data); err != nil {
			logger.Errorf("quic read frame body: %v", err)
			_ = c.Close()
			return
		}
		c.touch()
		if s.handler != nil {
			safe.SafeRun(func() { s.handler(c, data) })
		}
	}
}

// datagramLoop 消费不可靠 QUIC 数据报（QUIC Datagram，用于位置同步等高频数据）。
// 与 readLoop（可靠流）并行，两者互不阻塞；连接关闭时退出。
func (s *Server) datagramLoop(c *Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 双监听：连接关闭时由 ClosedCh 触发；循环因 ReceiveDatagram 出错先行退出时
	//（defer cancel 先执行）也随 ctx 退出，不为「非连接关闭退出」的连接残留守护协程。
	go func() {
		select {
		case <-c.ClosedCh():
		case <-ctx.Done():
		}
		cancel()
	}()
	for {
		data, err := c.conn.ReceiveDatagram(ctx)
		if err != nil {
			// 连接关闭或上下文取消导致的正常退出；非关闭时的错误也按断开处理。
			return
		}
		c.touch()
		if s.handler != nil {
			d := append([]byte(nil), data...)
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
				// 以最后一次收或发的时间判定空闲：只下发不收包的连接同样是活跃连接。
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

// Addr 返回实际监听地址。
func (s *Server) Addr() string {
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener != nil {
		return listener.Addr().String()
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
	listener := s.listener
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	s.mgr.CloseAll()
	return nil
}
