package wt

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	iwt "github.com/quic-go/webtransport-go"
	"github.com/qw576483/clover-server-engine/internal/transport/net/session"
)

// Conn WebTransport 连接，实现 session.Session。
//
// 关闭状态只有一份：BaseConn 的 closed / closeCh（经 MarkClosed 置位），
// 不再另设一个 Conn.closed —— 两套标志必然被下一个人当成「可能不一致」来读。
type Conn struct {
	session.BaseConn
	session   *iwt.Session
	stream    *iwt.Stream
	srv       *Server
	lastRead  atomic.Int64
	lastWrite atomic.Int64
	mu        sync.Mutex
}

func newConn(srv *Server, s *iwt.Session, stream *iwt.Stream) *Conn {
	now := time.Now().UnixNano()
	c := &Conn{
		BaseConn: session.NewBaseConn(session.NewConnID(), s.RemoteAddr().String()),
		session:  s,
		stream:   stream,
		srv:      srv,
	}
	c.lastRead.Store(now)
	c.lastWrite.Store(now)
	return c
}

// lastActiveTime 返回最近一次收或发的时间。
// 读写任一侧活跃即视为连接活跃：只下发不收包的连接（如纯推送）也要靠发送保活，
// 否则空闲清理会把它当作死连接关掉（与 quic 同款双时间判定）。
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

// Send 向对端发送可靠数据（通过 WT Stream）。
func (c *Conn) Send(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream == nil {
		return session.ErrClosed
	}
	if err := writeStreamFrame(c.stream, data); err != nil {
		return err
	}
	// 发送成功也是活跃信号：不更新会让「仅被推送」的连接被 IdleScanner 误杀。
	c.touchWrite()
	return nil
}

// streamWriter 是 WT 可靠流的写接口（*iwt.Stream 满足），便于帧编解码复用与单测。
type streamWriter interface {
	Write(p []byte) (int, error)
}

// writeStreamFrame 按 [4B 大端长度][body] 帧格式写一条可靠消息。
// 与 quic / ws 的线格式一致：长度前缀让对端能按帧切分流，
// 而不是把每次 Read 的返回当成一条完整消息（后者在 TCP 语义下会被任意拆分/合并）。
func writeStreamFrame(w streamWriter, data []byte) error {
	if len(data) > maxWTFrameSize {
		return fmt.Errorf("wt: frame too large: %d", len(data))
	}
	var header [wtFrameLenSize]byte
	// #nosec G115 -- 转换前已判 len(data) <= maxWTFrameSize（10MiB），远小于 uint32 范围。
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

// SendUnreliable 向对端发送不可靠数据（通过 WT Datagram）。
// 如果 WebTransport 不支持 Datagram，则降级为 Send（可靠传输）。
func (c *Conn) SendUnreliable(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session == nil {
		return session.ErrClosed
	}
	// 尝试通过 WT Datagram 发送
	err := c.session.SendDatagram(data)
	if err != nil {
		// Datagram 不支持或发送失败，降级为 Stream（可靠传输，同样带长度前缀）。
		if c.stream == nil {
			return session.ErrClosed
		}
		if werr := writeStreamFrame(c.stream, data); werr != nil {
			return werr
		}
	}
	// 与 Send 一致：成功发送刷新活跃时间，避免仅被推送的连接被 IdleScanner 误杀。
	c.touchWrite()
	return nil
}

// Capabilities 返回 WebTransport 连接的传输能力。
func (c *Conn) Capabilities() session.ConnCapabilities {
	return session.ConnCapabilities{
		Reliable:              true,
		Unreliable:            true,
		UnreliableViaDatagram: true,
		UnreliableViaRawUDP:   false, // WT 不支持裸 UDP
	}
}

// Close 关闭连接（幂等）。
//
// 只保留 BaseConn 一套关闭标志：`MarkClosed()` 返回「本次是否首次关闭」，
// 与原先自维护的 `closed` 语义完全等价 —— 两套标志并存只会让下一个人怀疑它们不一致。
func (c *Conn) Close() error {
	if !c.MarkClosed() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream != nil {
		// 先关闭 stream，通知对端流结束
		_ = c.stream.Close()
	}
	if c.session != nil {
		// 发送关闭帧，携带正常关闭错误码和消息
		_ = c.session.CloseWithError(0, "normal close")
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
	if c.session != nil {
		_ = c.session.CloseWithError(0, "")
	}
	if c.srv != nil {
		c.srv.mgr.Remove(c.ConnID())
	}
}
