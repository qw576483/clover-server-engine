package tcp

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/net/session"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// ErrSendTimeout 发送队列满且等待超时返回。
//
// 真身在 session（与 ws 共用一个 sentinel）：调用方对两种传输一次 errors.Is 即可判断，
// 不必按类型分支。与 session.ErrClosed 区分，避免把「对端太慢」误判成断线而触发清理/重连（假断线）。
var ErrSendTimeout = session.ErrSendTimeout

// unknownFrameTypeLogCount 未知帧类型日志降频计数（首次 + 每 1000 次）。
// 未知类型往往来自协议退化 / 对端垃圾帧，可被持续发送方刷屏，故降频。
var unknownFrameTypeLogCount atomic.Uint64

// Handler 业务帧回调。data 为 payload 拷贝，可安全持有。
type Handler func(c *Conn, data []byte)

// outFrame 写队列元素：带类型的帧（data/ping/pong）。
type outFrame struct {
	typ     byte
	payload []byte
}

// Conn 一条 TCP 连接，实现 session.Session。
type Conn struct {
	session.BaseConn
	netConn   net.Conn
	reader    *bufio.Reader
	sendCh    chan outFrame
	maxMsg    int
	heartbeat time.Duration
	onMessage Handler

	once sync.Once
	wg   sync.WaitGroup
	// wrMu 串行化对 netConn 的物理写：writeLoop 与 pongDirect（readLoop 内直写）
	// 并发写同一连接时，writeTypedFrame 的「写头+写体」两次 Write 可被对方插入，
	// 造成帧字节交错、协议流永久错位。
	wrMu sync.Mutex
	// userData 供上层挂载任意业务数据（如 master 层绑定 nodeID），线程安全。
	userData sync.Map
	// monitored 标记是否已启动断开监控 goroutine，避免重复启动。
	monitored atomic.Bool
	// closing 标记是否已安排延迟关闭（与 monitored 同源的去重开关，见 MarkClosing）。
	closing atomic.Bool
}

// DefaultCloseGrace 「回复帧落地后关连接」的等待窗口。
// 取值只要覆盖一次本地写队列的落地时间即可（毫秒级）；过长会让被拒连接多存活一会儿，
// 但不会影响并发上限（每个连接最多安排一次，见 MarkClosing）。
const DefaultCloseGrace = 100 * time.Millisecond

// CloseGracefully 先让已在发送队列里的帧落地，再关闭连接（幂等、每连接只安排一次）。
// 返回 false 表示本连接此前已安排过（本次不再重复安排）。
func (c *Conn) CloseGracefully() bool {
	if c == nil || !c.MarkClosing() {
		return false
	}
	safe.GoSafe(func() {
		time.Sleep(DefaultCloseGrace)
		if err := c.Close(); err != nil {
			logger.Warnf("tcp: graceful close conn=%s failed: %v", c.ConnID(), err)
		}
	})
	return true
}

func newConn(netConn net.Conn, id, remote string, maxMsg int, heartbeat time.Duration,
	onMessage Handler) *Conn {
	return &Conn{
		BaseConn:  session.NewBaseConn(id, remote),
		netConn:   netConn,
		reader:    bufio.NewReader(netConn),
		sendCh:    make(chan outFrame, defaultSendBufferSize),
		maxMsg:    maxMsg,
		heartbeat: heartbeat,
		onMessage: onMessage,
	}
}

// writeTimeout 单次写操作的超时（无心跳时兜底，避免对端不读导致写永久阻塞）。
// 取自 session 的共用默认值（与 ws 同源，避免两边各写一个 30s）。
const writeTimeout = session.DefaultWriteTimeout

// readIdleTimeout 读空闲兜底超时：心跳关闭（heartbeat==0）时仍设置绝对读超时，
// 避免对端不响应导致连接长期挂起。
const readIdleTimeout = 120 * time.Second

// writeDeadline 计算单次写操作的截止时间。
// 心跳开启时取 2×heartbeat 而非 heartbeat 本身——写超时若等于心跳间隔，
// 大帧 / RTT 抖动下正常连接会被误关；留 2 倍余量避免心跳误杀。
func (c *Conn) writeDeadline() time.Time {
	if c.heartbeat > 0 {
		return time.Now().Add(2 * c.heartbeat)
	}
	return time.Now().Add(writeTimeout)
}

// writeFrame 带写超时的类型帧写出；心跳开启时以 2×心跳间隔为限时，否则用默认兜底超时。
func (c *Conn) writeFrame(typ byte, payload []byte) error {
	c.wrMu.Lock()
	defer c.wrMu.Unlock()
	_ = c.netConn.SetWriteDeadline(c.writeDeadline())
	return writeTypedFrame(c.netConn, typ, payload)
}

// start 启动读 / 写协程。
func (c *Conn) start() {
	// 存活连接数在此 +1，与 Close 中的 -1 严格配对：
	// 放在 start 而非 newConn，保证「未启动就失败」的连接不会污染存活数。
	metricConnOpened(connRole(c))
	c.wg.Add(2)
	safe.GoSafe(c.readLoop)
	safe.GoSafe(c.writeLoop)
}

// readLoop 读侧：按类型 + 长度头拆帧；心跳开启时以 2×心跳间隔为读超时。
func (c *Conn) readLoop() {
	defer c.wg.Done()
	for {
		if c.heartbeat > 0 {
			_ = c.netConn.SetReadDeadline(time.Now().Add(2 * c.heartbeat))
		} else {
			// 心跳关闭时仍以绝对读超时兜底，避免空闲连接无限挂起。
			_ = c.netConn.SetReadDeadline(time.Now().Add(readIdleTimeout))
		}
		typ, data, err := readFrame(c.reader, c.maxMsg)
		if err != nil {
			metricReadError(connRole(c))
			if cerr := c.Close(); cerr != nil {
				logger.Warnf("tcp readLoop close on read error: %v (read err: %v)", cerr, err)
			}
			return
		}
		switch typ {
		case frameTypePing:
			// 收到 ping：直接写 pong（绕开发送队列），避免 sendCh 积压导致 pong 延迟、
			// 对端误判超时断线。
			c.pongDirect()
		case frameTypePong:
			// pong：仅确认对端存活，不回复。
		case frameTypeData:
			if len(data) == 0 {
				continue // 空业务帧忽略
			}
			metricRecvBytes(connRole(c), len(data))
			if c.onMessage != nil {
				// 同步派发：在 readLoop 内顺序调用，保证同连接上的消息按到达顺序处理；
				// 异步 GoSafe 会破坏帧顺序（并发 handler 可能乱序回包）。
				d := append([]byte(nil), data...)
				safe.SafeRun(func() { c.onMessage(c, d) })
			}
		default:
			// 未知类型：忽略（协议退化 / 垃圾帧的信号，降频记录便于定位：首次 + 每 1000 次）。
			if n := unknownFrameTypeLogCount.Add(1); n == 1 || n%1000 == 0 {
				logger.Warnf("tcp: unknown frame type 0x%02x from %s（同类累计 %d 次，已降频输出）", typ, c.RemoteAddr(), n)
			}
		}
	}
}

// writeLoop 单写协程：串行写 net.Conn，避免并发写竞争；心跳开启时周期发 ping。
// 退出条件：对端/写失败触发 Close，或 Close 已关闭连接（ClosedCh 关闭）。
// 注意：sendCh 永不主动关闭（避免并发 Send 向已关闭 channel 发送而 panic），
// 连接关闭一律通过 ClosedCh 通知写协程退出。
func (c *Conn) writeLoop() {
	defer c.wg.Done()
	// 角色在连接生命周期内不变，循环外取一次，避免热路径重复查 sync.Map。
	role := connRole(c)
	var tickerC <-chan time.Time
	if c.heartbeat > 0 {
		ticker := time.NewTicker(c.heartbeat / 2)
		defer ticker.Stop()
		tickerC = ticker.C
	}
	for {
		select {
		case f := <-c.sendCh:
			// 出队后上报剩余积压，这是观测写侧反压最直接的信号。
			metricSendQueue(role, len(c.sendCh))
			if err := c.writeFrame(f.typ, f.payload); err != nil {
				metricWriteError(role)
				if cerr := c.Close(); cerr != nil {
					logger.Warnf("tcp writeLoop close on write error: %v (write err: %v)", cerr, err)
				}
				return
			}
			if f.typ == frameTypeData {
				metricSentBytes(role, len(f.payload))
			}
		case <-tickerC:
			// 写侧周期发 ping（控制帧）。
			if err := c.writeFrame(frameTypePing, nil); err != nil {
				metricWriteError(role)
				if cerr := c.Close(); cerr != nil {
					logger.Warnf("tcp heartbeatLoop close on write error: %v (write err: %v)", cerr, err)
				}
				return
			}
		case <-c.ClosedCh():
			return
		}
	}
}

// sendTimeout Send 写等待超时（避免对端不读时永久阻塞调用方 goroutine）。
// 取自 session 的共用默认值（与 ws 同源）。
const sendTimeout = session.DefaultSendTimeout

// Send 向对端发送业务数据（线程安全）。
//
// 两段式：先无阻塞试写（成功路径不申请定时器），队列满才走带超时的慢路径并显式 Stop。
// 原实现无条件 `time.After`，每次发送都造一个 runtime timer。
func (c *Conn) Send(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	frame := outFrame{typ: frameTypeData, payload: append([]byte(nil), data...)}
	select {
	case c.sendCh <- frame:
		return nil
	default:
	}

	t := time.NewTimer(sendTimeout)
	defer t.Stop()
	select {
	case c.sendCh <- frame:
		return nil
	case <-c.ClosedCh():
		return session.ErrClosed
	case <-t.C:
		// 写缓冲区满且超时，避免永久阻塞调用方（如同步 handler 反压读循环导致整连接卡死）。
		// 连接实际未关闭，返回独立 ErrSendTimeout，避免调用方误判断线触发清理/重连。
		metricSendTimeout(connRole(c))
		logger.Warnf("tcp send timeout, id=%s", c.ConnID())
		return ErrSendTimeout
	}
}

// SendUnreliable TCP 无真正不可靠语义，降级为 Send（可靠传输）。
func (c *Conn) SendUnreliable(data []byte) error {
	return c.Send(data)
}

// Close 关闭连接（幂等）。
// 不关闭 sendCh：Send 通过 select 监听 ClosedCh，写协程也通过 ClosedCh 退出，
// 避免「并发 Send + Close」向已关闭 channel 发送而 panic。
func (c *Conn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.netConn.Close()
		c.MarkClosed()
		// 放在 once 内：Close 幂等，存活数只能减一次，否则 Gauge 会被减成负数。
		metricConnClosed(connRole(c))
	})
	return err
}

// SetValue 在连接上挂载任意业务键值（线程安全）。
// 典型用途：master 层绑定 nodeID 以便断开时触发 RemoveNode。
func (c *Conn) SetValue(key, value any) {
	c.userData.Store(key, value)
}

// MarkMonitored 原子地标记本连接已启动断开监控，首次调用返回 true（用于去重）。
func (c *Conn) MarkMonitored() bool {
	return c.monitored.CompareAndSwap(false, true)
}

// MarkClosing 原子地标记「本连接已安排好延迟关闭」，首次调用返回 true（用于去重）。
//
// 用途：需要「先把响应帧发出去、再关连接」的拒绝路径（如鉴权失败回 unauthorized 后断连）。
// Close 会立刻关闭 socket，而 Send 只把帧放进发送队列 —— 二者紧邻执行时帧可能还没落地
// 就被丢弃，对端只看到「断连」而读不到拒绝原因（排查成本差一个数量级）。
// 因此调用方先 Send，再用本方法确保只安排**一次**延迟关闭（否则每个被拒帧都会起一个
// goroutine，攻击者刷帧即可放大 goroutine 数量）。
func (c *Conn) MarkClosing() bool {
	return c.closing.CompareAndSwap(false, true)
}

// Value 读取 SetValue 存入的键值（线程安全）。
func (c *Conn) Value(key any) (any, bool) {
	return c.userData.Load(key)
}

// pongDirect 直接在 netConn 写 pong 控制帧（绕过 sendCh，但与 writeLoop 共用 wrMu 串行化物理写）。
// 在 readLoop 收到 ping 时同步调用，避免 sendCh 积压导致 pong 延迟。
func (c *Conn) pongDirect() {
	if err := c.writeFrame(frameTypePong, nil); err != nil {
		logger.Warnf("tcp pong direct write failed: %v", err)
	}
}
