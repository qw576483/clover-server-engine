// Package demux 提供按首字节分发 UDP 数据报的 PacketConn 包装，
// 使 QUIC 与裸 UDP 共享同一个 UDP 端口（最小化端口方案的核心组件）。
//
// 分发规则：
//   - 数据报首字节 == 0x55（RawUDPMagic）→ 裸 UDP 处理器（经 RawChan 消费）
//   - 数据报首字节最高两位非 00（0x40-0xFF，覆盖 QUIC 短头/长头）→ QUIC 协议栈
//   - 其余首字节 → 丢弃并告警
package demux

import (
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"clover-server-engine/internal/shared/retry"
	"clover-server-engine/pkg/foundation/logger"
)

// RawUDPMagic 裸 UDP 通道魔数：数据报首字节为该值时由裸 UDP 处理器消费。
const RawUDPMagic = 0x55

// IsRawUDP 判断数据报首字节是否属于裸 UDP 通道（用户约定魔数）。
func IsRawUDP(b byte) bool { return b == RawUDPMagic }

// IsQUIC 判断数据报首字节是否属于 QUIC 包头。
// QUIC 长包头首字节 0xC0-0xFF（最高两位 11），短包头 0x40-0x7F（bit7=0、bit6=1），
// 因此只要最高两位非 00 即为 QUIC 包（裸 UDP 魔数 0x55 需优先于本判断）。
func IsQUIC(b byte) bool { return b&0xC0 != 0 }

// Datagram 一条 UDP 数据报（含来源地址）。
type Datagram struct {
	Data []byte
	Addr net.Addr
}

// PacketConn 按首字节分发的 UDP PacketConn 包装：
//   - 实现 net.PacketConn，作为 QUIC 协议栈的读端（ReadFrom 只返回 QUIC 数据报）；
//   - 提供 RawChan() 通道，供裸 UDP 处理器消费；
//   - WriteTo/LocalAddr 直接透传底层连接。
type PacketConn struct {
	pc     net.PacketConn
	quicCh chan *Datagram // QUIC 数据报队列（经 ReadFrom 供 quic-go 消费）
	rawCh  chan *Datagram // 裸 UDP 数据报队列（经 RawChan 供裸 UDP 处理器消费）
	quit   chan struct{}
	closed atomic.Bool
	once   sync.Once
	wg     sync.WaitGroup

	deadlineMu   sync.RWMutex
	readDeadline time.Time
	// deadlineCh 当前读 deadline 的到期广播通道（到期即关闭）。
	// 每个 deadline 只创建一个 timer（SetReadDeadline 时），ReadFrom 热路径因此零分配；
	// 关闭是广播语义，多个并发 ReadFrom 一起超时。deadline 为零值时该通道为 nil
	//（select 对 nil channel 永久阻塞，等价于无超时）。
	deadlineCh    chan struct{}
	deadlineTimer *time.Timer
}

// New 构造按首字节分发的 PacketConn。quicBuf/rawBuf 为两条分发队列容量，<=0 取默认 1024。
func New(pc net.PacketConn, quicBuf, rawBuf int) *PacketConn {
	if quicBuf <= 0 {
		quicBuf = 1024
	}
	if rawBuf <= 0 {
		rawBuf = 1024
	}
	return &PacketConn{
		pc:     pc,
		quicCh: make(chan *Datagram, quicBuf),
		rawCh:  make(chan *Datagram, rawBuf),
		quit:   make(chan struct{}),
	}
}

// Start 启动后台分发读循环。
func (d *PacketConn) Start() {
	d.wg.Add(1)
	go d.dispatchLoop()
}

// dispatchLoop 从底层 socket 读取数据报并按首字节分发。
func (d *PacketConn) dispatchLoop() {
	defer d.wg.Done()
	buf := make([]byte, 65535)
	// 退避曲线统一由 retry.Backoff 提供（1ms → 1s，成功即重置）；
	// 与 udp readLoop / quic acceptLoop 同一条策略，此前三处各写一份。
	bo := retry.NewBackoff(retry.Policy{
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Second,
		Multiplier: 2,
	})
	for {
		n, addr, err := d.pc.ReadFrom(buf)
		if err != nil {
			if d.closed.Load() {
				return
			}
			logger.Errorf("demux: read: %v", err)
			time.Sleep(bo.Next())
			continue
		}
		bo.Reset()
		if n == 0 {
			continue
		}
		data := append([]byte(nil), buf[:n]...)
		dg := &Datagram{Data: data, Addr: addr}
		switch {
		case IsRawUDP(data[0]):
			// 0x55 只是共享端口上的路由魔数，用完后在此剥掉：
			// 下游（udp.Server → 网关 handleClient）按 TCP/WS 同款帧
			// [4B requestID][4B msgID][body] 解码，留着 0x55 会读坏 requestID。
			dg.Data = data[1:]
			select {
			case d.rawCh <- dg:
			case <-d.quit:
				return
			}
		case IsQUIC(data[0]):
			select {
			case d.quicCh <- dg:
			case <-d.quit:
				return
			}
		default:
			logger.Warnf("demux: drop datagram with unknown first byte 0x%02x from %s", data[0], addr)
		}
	}
}

// ReadFrom 实现 net.PacketConn：仅返回 QUIC 数据报（供 quic-go 消费）。
// deadline 到期信号取自 SetReadDeadline 时创建的共享广播通道，本热路径零分配。
func (d *PacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	d.deadlineMu.RLock()
	dl := d.readDeadline
	deadlineCh := d.deadlineCh
	d.deadlineMu.RUnlock()
	// 读取前已过期即返回超时（与标准库 net 语义一致）。
	if !dl.IsZero() && time.Until(dl) <= 0 {
		return 0, nil, os.ErrDeadlineExceeded
	}
	select {
	case dg := <-d.quicCh:
		if dg == nil {
			return 0, nil, io.EOF
		}
		n := copy(p, dg.Data)
		if n < len(dg.Data) {
			// p 小于数据报：静默截断会让消费方（quic-go）拿到损坏报文且无从定位，补一条告警。
			logger.Warnf("demux: quic datagram truncated (%d -> %d bytes) from %s", len(dg.Data), n, dg.Addr)
		}
		return n, dg.Addr, nil
	case <-d.quit:
		return 0, nil, io.EOF
	case <-deadlineCh:
		return 0, nil, os.ErrDeadlineExceeded
	}
}

// WriteTo 透传写到底层连接。
func (d *PacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	return d.pc.WriteTo(p, addr)
}

// LocalAddr 返回底层连接本地地址。
func (d *PacketConn) LocalAddr() net.Addr { return d.pc.LocalAddr() }

// SetDeadline 同时设置读写 deadline（写侧暂不生效，仅记录读侧）。
func (d *PacketConn) SetDeadline(t time.Time) error {
	d.deadlineMu.Lock()
	d.setReadDeadlineLocked(t)
	d.deadlineMu.Unlock()
	return nil
}

// SetReadDeadline 设置读 deadline，ReadFrom 超时返回 os.ErrDeadlineExceeded。
func (d *PacketConn) SetReadDeadline(t time.Time) error {
	d.deadlineMu.Lock()
	d.setReadDeadlineLocked(t)
	d.deadlineMu.Unlock()
	return nil
}

// setReadDeadlineLocked 更新读 deadline 与到期广播通道（调用方须持 deadlineMu）。
// 到期信号按 deadline 创建（而非每次 ReadFrom 创建）：
//   - 读热路径不再每次分配 timer；
//   - 通道关闭是广播语义，多个并发 ReadFrom 一起超时。
func (d *PacketConn) setReadDeadlineLocked(t time.Time) {
	if d.deadlineTimer != nil {
		d.deadlineTimer.Stop()
		d.deadlineTimer = nil
	}
	d.readDeadline = t
	d.deadlineCh = nil
	if t.IsZero() {
		return
	}
	ch := make(chan struct{})
	d.deadlineCh = ch
	d.deadlineTimer = time.AfterFunc(time.Until(t), func() { close(ch) })
}

// SetWriteDeadline 写 deadline 由底层连接透传。
func (d *PacketConn) SetWriteDeadline(t time.Time) error { return d.pc.SetWriteDeadline(t) }

// SetReadBuffer 透传设置底层 UDP socket 读缓冲区（供 quic-go 调优）。
func (d *PacketConn) SetReadBuffer(size int) error {
	if sb, ok := d.pc.(interface{ SetReadBuffer(int) error }); ok {
		return sb.SetReadBuffer(size)
	}
	return nil
}

// SetWriteBuffer 透传设置底层 UDP socket 写缓冲区（供 quic-go 调优）。
func (d *PacketConn) SetWriteBuffer(size int) error {
	if sb, ok := d.pc.(interface{ SetWriteBuffer(int) error }); ok {
		return sb.SetWriteBuffer(size)
	}
	return nil
}

// SyscallConn 透传底层 UDP socket 的 RawConn（供 quic-go 读取/设置 socket 选项）。
func (d *PacketConn) SyscallConn() (syscall.RawConn, error) {
	if sc, ok := d.pc.(interface {
		SyscallConn() (syscall.RawConn, error)
	}); ok {
		return sc.SyscallConn()
	}
	return nil, os.ErrInvalid
}

// RawChan 返回裸 UDP 数据报消费通道（供 udp.Server 消费）。
func (d *PacketConn) RawChan() <-chan *Datagram { return d.rawCh }

// Close 关闭底层连接并停止分发。
// 幂等：quic-go 的 Transport.Listener.Close() 也会触发本方法。
func (d *PacketConn) Close() error {
	d.once.Do(func() {
		d.closed.Store(true)
		close(d.quit)
		_ = d.pc.Close() // 唤醒可能阻塞在 pc.ReadFrom 的 dispatchLoop
		d.wg.Wait()      // 等待 dispatchLoop 退出后再安全关闭队列
		close(d.quicCh)
		close(d.rawCh)
	})
	return nil
}
