// #nosec G115 -- 转换前已判 frameLen <= maxQUICFrameSize（10MiB），远小于 int 范围。

package quic

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/net/session"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
	iquic "github.com/quic-go/quic-go"
)

// Conn QUIC 连接，实现 session.Session。
type Conn struct {
	session.BaseConn
	conn      *iquic.Conn
	stream    *iquic.Stream
	srv       *Server
	lastRead  atomic.Int64
	lastWrite atomic.Int64
	mu        sync.Mutex
}

// newConn 构造服务端连接。流尚未就绪（对端还没有打开首条流），
// 由 setStream 在流到达后绑定，在此之前 Send 一律返回 ErrClosed。
func newConn(srv *Server, conn *iquic.Conn) *Conn {
	now := time.Now().UnixNano()
	c := &Conn{
		BaseConn: session.NewBaseConn(session.NewConnID(), conn.RemoteAddr().String()),
		conn:     conn,
		srv:      srv,
	}
	c.lastRead.Store(now)
	c.lastWrite.Store(now)
	return c
}

// setStream 绑定对端打开的首条流。Send / Close 在 c.mu 内读 stream，故写入同样受 c.mu 保护。
func (c *Conn) setStream(stream *iquic.Stream) {
	c.mu.Lock()
	c.stream = stream
	c.mu.Unlock()
}

// acceptStream 等待对端打开首条流，超时返回 context.DeadlineExceeded。
// 必须带超时：accept 循环是单协程，一条只建连不开流的连接会把后续连接全部挡住。
func (c *Conn) acceptStream(timeout time.Duration) (*iquic.Stream, error) {
	if timeout <= 0 {
		timeout = defaultAcceptStreamTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.conn.AcceptStream(ctx)
}

// lastActiveTime 返回最近一次收或发的时间。
// 读写任一侧活跃即视为连接活跃：只下发不收包的连接（如纯推送）也要靠发送保活，
// 否则空闲清理会把它当作死连接关掉。
func (c *Conn) lastActiveTime() time.Time {
	last := c.lastRead.Load()
	if w := c.lastWrite.Load(); w > last {
		last = w
	}
	return time.Unix(0, last)
}

func (c *Conn) touch() { c.lastRead.Store(time.Now().UnixNano()) }

// touchWrite 记录一次成功发送，用于空闲判定。
func (c *Conn) touchWrite() { c.lastWrite.Store(time.Now().UnixNano()) }

// Send 向对端发送可靠数据（通过 QUIC Stream）。
func (c *Conn) Send(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream == nil {
		return session.ErrClosed
	}
	err := c.sendStreamLocked(data)
	if err == nil {
		c.touchWrite()
	}
	return err
}

func (c *Conn) sendStreamLocked(data []byte) error {
	// 首条流就绪前 stream 为 nil（对端已建连但尚未开流）。
	if c.stream == nil {
		return session.ErrClosed
	}
	if len(data) > maxQUICFrameSize {
		return fmt.Errorf("quic: frame too large: %d", len(data))
	}
	var header [quicFrameLenSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if _, err := c.stream.Write(header[:]); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := c.stream.Write(data)
	return err
}

// SendUnreliable 向对端发送不可靠数据（通过 QUIC Datagram）。
// 如果 QUIC 连接不支持 Datagram，则降级为 Send（可靠传输）。
func (c *Conn) SendUnreliable(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return session.ErrClosed
	}
	// 尝试通过 QUIC Datagram 发送；失败时复用带长度前缀的可靠发送路径。
	var err error
	if err = c.conn.SendDatagram(data); err != nil {
		err = c.sendStreamLocked(data)
	}
	if err == nil {
		c.touchWrite()
	}
	return err
}

// Capabilities 返回 QUIC 连接的传输能力。
func (c *Conn) Capabilities() session.ConnCapabilities {
	return session.ConnCapabilities{
		Reliable:              true,
		Unreliable:            true,
		UnreliableViaDatagram: true,
		UnreliableViaRawUDP:   false, // QUIC-ONLY 模式不支持裸 UDP
	}
}

// Close 关闭连接。
func (c *Conn) Close() error {
	if !c.MarkClosed() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream != nil {
		_ = c.stream.Close()
	}
	if c.conn != nil {
		_ = c.conn.CloseWithError(0, "")
	}
	if c.srv != nil {
		c.srv.mu.Lock()
		delete(c.srv.conns, c.ConnID())
		c.srv.mu.Unlock()
		c.srv.mgr.Remove(c.ConnID())
	}
	return nil
}

// closeWithoutLock 供 cleanLoop 在已删除 conns 条目后调用，不取 srv.mu 以避免死锁。
func (c *Conn) closeWithoutLock() {
	if !c.MarkClosed() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream != nil {
		_ = c.stream.Close()
	}
	if c.conn != nil {
		_ = c.conn.CloseWithError(0, "")
	}
	if c.srv != nil {
		c.srv.mgr.Remove(c.ConnID())
	}
}

// Client QUIC 客户端：独立连接收发。
type Client struct {
	session.BaseConn
	conn     *iquic.Conn
	stream   *iquic.Stream
	recvCh   chan []byte
	done     chan struct{}
	once     sync.Once
	doneOnce sync.Once
	mu       sync.Mutex
}

// Dial 拨号建立 QUIC 客户端连接。
func Dial(cfg ClientConfig) (*Client, error) {
	c := cfg.normalize()

	tlsConf := c.TLSConfig
	if tlsConf == nil {
		// 说明（gosec G402 InsecureSkipVerify，**有意保留、不加 #nosec**）：
		// 这里读的是**配置开关**（ClientConfig.InsecureSkipVerify，见 config.go 的字段说明），
		// 默认 false = 正常校验证书；只有测试 / 内网自签证书场景才由调用方显式打开。
		// 传了 TLSConfig 的调用方走上面的分支，本字段完全不参与 —— 即「显式配置优先」。
		tlsConf = &tls.Config{
			InsecureSkipVerify: c.InsecureSkipVerify,
			NextProtos:         []string{"clover-quic"},
		}
	}

	quicConf := &iquic.Config{
		MaxIdleTimeout:     c.IdleTimeout,
		MaxIncomingStreams: int64(c.MaxStreams),
	}

	// 建立连接（设置 dial timeout，防止网络不可达时永久阻塞）
	dialTimeout := 10 * time.Second
	if c.ConnectTimeout > 0 {
		dialTimeout = c.ConnectTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	conn, err := iquic.DialAddr(ctx, c.Address, tlsConf, quicConf)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStream()
	if err != nil {
		_ = conn.CloseWithError(0, "")
		return nil, err
	}

	cli := &Client{
		BaseConn: session.NewBaseConn(session.NewConnID(), conn.RemoteAddr().String()),
		conn:     conn,
		stream:   stream,
		recvCh:   make(chan []byte, 64),
		done:     make(chan struct{}),
	}

	safe.GoSafe(cli.readLoop)
	logger.Infof("quic client to %s", c.Address)
	return cli, nil
}

func (c *Client) readLoop() {
	var header [quicFrameLenSize]byte
	for {
		if _, err := io.ReadFull(c.stream, header[:]); err != nil {
			// 读循环退出即回收底层连接：否则对端中止流后 QUIC 连接一直悬挂到调用方显式 Close。
			// Close 幂等（once），同时唤醒 Receive（done 关闭）。
			_ = c.Close()
			return
		}
		frameLen := binary.BigEndian.Uint32(header[:])
		if frameLen > maxQUICFrameSize {
			logger.Errorf("quic client read: frame too large: %d", frameLen)
			_ = c.Close()
			return
		}
		data := make([]byte, frameLen)
		if _, err := io.ReadFull(c.stream, data); err != nil {
			_ = c.Close()
			return
		}
		select {
		case c.recvCh <- data:
		default:
			logger.Warnf("quic client recvCh full, drop packet len=%d", len(data))
		}
	}
}

// Send 向对端发送数据。
func (c *Client) Send(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream == nil {
		return session.ErrClosed
	}
	if len(data) > maxQUICFrameSize {
		return fmt.Errorf("quic: frame too large: %d", len(data))
	}
	var header [quicFrameLenSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if _, err := c.stream.Write(header[:]); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := c.stream.Write(data)
	return err
}

// Receive 接收一条数据（阻塞直到收到、连接关闭或出错）。
func (c *Client) Receive() ([]byte, error) {
	select {
	case data, ok := <-c.recvCh:
		if !ok {
			return nil, session.ErrClosed
		}
		return data, nil
	case <-c.done:
		return nil, session.ErrClosed
	case <-c.ClosedCh():
		return nil, session.ErrClosed
	}
}

// Close 关闭客户端（幂等）。
//
// 返回首个关闭错误：关闭失败不等于「连接还能用」，调用方需要能看见它。
// 此前声明了 err 却把所有错误都赋给 `_`，等于永远返回 nil —— 关闭异常被静默吞掉。
func (c *Client) Close() error {
	var err error
	c.once.Do(func() {
		if c.stream != nil {
			if cerr := c.stream.Close(); cerr != nil {
				err = cerr
			}
		}
		if c.conn != nil {
			if cerr := c.conn.CloseWithError(0, ""); cerr != nil && err == nil {
				err = cerr
			}
		}
		c.MarkClosed()
		c.doneOnce.Do(func() { close(c.done) })
	})
	return err
}
