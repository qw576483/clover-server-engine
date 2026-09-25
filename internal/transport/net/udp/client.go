package udp

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/net/session"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// Conn 以对端地址为键的 UDP 会话，实现 session.Session。
// 多条来自同一地址的数据报复用同一 Conn。
type Conn struct {
	session.BaseConn
	srv  *Server
	addr net.Addr
	// lastRead 最后一次收包时间（UnixNano）。
	// 必须原子：readLoop 的 touch() 写、cleanLoop 的 lastReadTime() 读属跨协程访问，
	// time.Time 是多字长结构，裸读写既是 data race 也可能撕裂读导致误踢活跃会话。
	lastRead atomic.Int64
}

func newConn(srv *Server, addr net.Addr) *Conn {
	c := &Conn{
		BaseConn: session.NewBaseConn(session.NewConnID(), addr.String()),
		srv:      srv,
		addr:     addr,
	}
	c.lastRead.Store(time.Now().UnixNano())
	return c
}

func (c *Conn) lastReadTime() time.Time { return time.Unix(0, c.lastRead.Load()) }

func (c *Conn) touch() { c.lastRead.Store(time.Now().UnixNano()) }

// Addr 返回对端 UDP 地址（网关登记该端点用于不可靠推送时使用）。
func (c *Conn) Addr() net.Addr { return c.addr }

// Send 将报写回该来源地址（可靠语义：UDP 本身无可靠保证，但此处作为默认发送通道）。
func (c *Conn) Send(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	if c.srv == nil {
		return session.ErrClosed
	}
	// 经加锁快照读取，避免与 Server.Start 写 s.pc 构成 data race。
	pc := c.srv.packetConn()
	if pc == nil {
		return session.ErrClosed
	}
	_, err := pc.WriteTo(data, c.addr)
	return err
}

// SendUnreliable UDP 天然不可靠，直接走裸 UDP 发送。
func (c *Conn) SendUnreliable(data []byte) error {
	return c.Send(data)
}

// Capabilities 返回 UDP 连接的传输能力。
func (c *Conn) Capabilities() session.ConnCapabilities {
	return session.ConnCapabilities{
		Reliable:              false, // UDP 本身不可靠
		Unreliable:            true,
		UnreliableViaDatagram: false,
		UnreliableViaRawUDP:   true, // 裸 UDP
	}
}

// Close 从服务器会话表中移除并标记关闭（UDP 无独立底层连接可关）。
func (c *Conn) Close() error {
	if !c.MarkClosed() {
		return nil
	}
	c.srv.mu.Lock()
	delete(c.srv.conns, c.addr.String())
	c.srv.mu.Unlock()
	c.srv.mgr.Remove(c.ConnID())
	return nil
}

// closeWithoutLock 供 cleanLoop 在已删除 conns 条目后调用，不取 srv.mu 以避免死锁。
func (c *Conn) closeWithoutLock() {
	if !c.MarkClosed() {
		return
	}
	c.srv.mgr.Remove(c.ConnID())
}

// recvPushTimeout recvCh 满时的反压等待上限：超时仍无法投递才丢包。
const recvPushTimeout = 50 * time.Millisecond

// Client UDP 客户端：独立数据报收发。
type Client struct {
	session.BaseConn
	pc       net.PacketConn
	addr     net.Addr
	recvCh   chan []byte
	done     chan struct{}
	once     sync.Once
	doneOnce sync.Once // 保护 done 只关闭一次
	// maxPacketSize 接收缓冲区大小，来自 ClientConfig.MaxPacketSize（normalize 后必 >0）。
	maxPacketSize int
}

// Dial 拨号建立 UDP 客户端连接。
func Dial(cfg ClientConfig) (*Client, error) {
	c := cfg.normalize()
	raddr, err := net.ResolveUDPAddr("udp", c.Address)
	if err != nil {
		return nil, err
	}
	// 未配置 LocalAddr 时用 ":0" 让 OS 按路由表选择到目标地址的正确出口网卡。
	localAddr := c.LocalAddr
	if localAddr == "" {
		localAddr = ":0"
	}
	pc, err := net.ListenPacket("udp", localAddr)
	if err != nil {
		return nil, err
	}
	cli := &Client{
		BaseConn: session.NewBaseConn(session.NewConnID(), raddr.String()),
		pc:       pc,
		addr:     raddr,
		recvCh:   make(chan []byte, 64),
		done:     make(chan struct{}),

		maxPacketSize: c.MaxPacketSize,
	}
	safe.GoSafe(cli.readLoop)
	logger.Infof("udp client to %s", c.Address)
	return cli, nil
}

func (c *Client) readLoop() {
	// 接收缓冲区按 ClientConfig.MaxPacketSize 分配（normalize 已保证 >0）。
	size := c.maxPacketSize
	if size <= 0 {
		size = defaultMaxPacketSize
	}
	buf := make([]byte, size)
	for {
		n, _, err := c.pc.ReadFrom(buf)
		if err != nil {
			c.doneOnce.Do(func() { close(c.done) })
			return
		}
		// 读满整个缓冲区：数据报可能超长被内核截断（与 server.readLoop 同一告警口径）。
		if n == len(buf) {
			logger.Warnf("udp client read: datagram filled buffer (%d bytes); packet may have been truncated from %s", n, c.addr)
		}
		data := append([]byte(nil), buf[:n]...)
		// 先尝试非阻塞投递；满时带超时短暂反压，
		// 给消费者留出追赶窗口，降低位置同步等高频场景的丢包。
		// 超时仍未投递才丢弃并告警；期间连接关闭则立即退出。
		select {
		case c.recvCh <- data:
		default:
			timer := time.NewTimer(recvPushTimeout)
			select {
			case c.recvCh <- data:
				timer.Stop()
			case <-c.done:
				timer.Stop()
				return
			case <-c.ClosedCh():
				timer.Stop()
				return
			case <-timer.C:
				// 反压超时仍无法投递：消费者严重滞后，丢弃并告警以便诊断。
				logger.Warnf("udp client recvCh full, drop packet len=%d addr=%s", n, c.addr)
			}
		}
	}
}

// Send 向目标地址发送数据报。
func (c *Client) Send(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	_, err := c.pc.WriteTo(data, c.addr)
	return err
}

// Receive 接收一条数据报（阻塞直到收到、连接关闭或出错）。
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

// Close 关闭客户端。
func (c *Client) Close() error {
	var err error
	c.once.Do(func() {
		err = c.pc.Close()
		c.MarkClosed()
		c.doneOnce.Do(func() { close(c.done) })
	})
	return err
}
