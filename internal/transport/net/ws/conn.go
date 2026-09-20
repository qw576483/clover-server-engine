package ws

import (
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/safe"
)

// ErrSendTimeout 发送队列满且等待超时返回。
//
// 真身在 session（与 tcp 共用一个 sentinel）：调用方对两种传输一次 errors.Is 即可判断。
// 连接实际未关闭，与 session.ErrClosed 区分，避免误判断线触发清理/重连（假断线）。
var ErrSendTimeout = session.ErrSendTimeout

// errMessageTooLarge 发送的消息超过本连接约定的单条消息上限（c.maxMsg），拒绝本地写出。
var errMessageTooLarge = errors.New("ws: message too large")

// Handler 业务消息回调。data 为消息体拷贝，可安全持有。
type Handler func(c *Conn, data []byte)

// Conn 一条 WebSocket 连接，实现 session.Session。
type Conn struct {
	session.BaseConn
	ws        *websocket.Conn
	sendCh    chan []byte
	maxMsg    int64
	heartbeat time.Duration
	onMessage Handler

	once sync.Once
	wg   sync.WaitGroup
}

func newConn(wsConn *websocket.Conn, id, remote string, maxMsg int64, heartbeat time.Duration, onMessage Handler) *Conn {
	return &Conn{
		BaseConn:  session.NewBaseConn(id, remote),
		ws:        wsConn,
		sendCh:    make(chan []byte, defaultSendBufferSize),
		maxMsg:    maxMsg,
		heartbeat: heartbeat,
		onMessage: onMessage,
	}
}

// start 启动读 / 写协程。
func (c *Conn) start() {
	c.wg.Add(2)
	safe.GoSafe(c.readLoop)
	safe.GoSafe(c.writeLoop)
}

// readLoop 读侧：仅处理二进制消息；心跳开启时以 2×心跳间隔为读超时，用于检测对端断开。
// 通过 SetPongHandler 在每次收到 pong 时刷新读超时，避免因一次 pong 迟到就误判超时。
// 心跳关闭时也设置默认读超时（5 分钟），防止对端不发包时永久阻塞无法退出。
func (c *Conn) readLoop() {
	defer c.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			// 带堆栈便于定位；并关闭连接——readLoop 退出后连接对象不得继续悬空。
			logger.Errorf("ws readLoop panic: %v\n%s", r, debug.Stack())
			_ = c.Close()
		}
	}()
	c.ws.SetReadLimit(c.maxMsg)
	// 设置 PongHandler：收到 pong 时重置 ReadDeadline，避免因一次 pong 迟到就误判超时。
	// gorilla 对 ping 默认自动回复 pong，本 handler 确保读出 pong 时延长等待窗口。
	if c.heartbeat > 0 {
		c.ws.SetPongHandler(func(string) error {
			_ = c.ws.SetReadDeadline(time.Now().Add(2 * c.heartbeat))
			return nil
		})
	}
	// 默认读超时：心跳关闭时防止对端不发包导致永久阻塞
	const defaultReadTimeout = 5 * time.Minute
	for {
		if c.heartbeat > 0 {
			_ = c.ws.SetReadDeadline(time.Now().Add(2 * c.heartbeat))
		} else {
			_ = c.ws.SetReadDeadline(time.Now().Add(defaultReadTimeout))
		}
		mt, data, err := c.ws.ReadMessage()
		if err != nil {
			_ = c.Close()
			return
		}
		if mt != websocket.BinaryMessage {
			// 跨实现互操作时文本帧/控制帧被静默丢弃无提示，记录日志辅助排障。
			if mt == websocket.TextMessage {
				logger.Warnf("ws: received text message, only binary supported, dropping")
			}
			continue
		}
		if c.onMessage != nil {
			// 同步派发：在 readLoop 内顺序调用，保证同连接消息按到达顺序处理；
			// 异步 GoSafe 会破坏帧顺序（并发 handler 可能乱序回包）。
			d := append([]byte(nil), data...)
			safe.SafeRun(func() { c.onMessage(c, d) })
		}
	}
}

// writeTimeout 单次写操作的超时（无心跳时兜底，避免对端不读导致写永久阻塞）。
// 取自 session 的共用默认值（与 tcp 同源）。
const writeTimeout = session.DefaultWriteTimeout

// writeMsg 带写超时的消息写出；心跳开启时以 2×心跳间隔为限时（与读超时同款余量），
// 否则用默认兜底超时。
func (c *Conn) writeMsg(data []byte) error {
	if c.heartbeat > 0 {
		_ = c.ws.SetWriteDeadline(time.Now().Add(2 * c.heartbeat))
	} else {
		_ = c.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
	}
	return c.ws.WriteMessage(websocket.BinaryMessage, data)
}

// writePing 带写超时的 ping 控制帧写出。
func (c *Conn) writePing() error {
	if c.heartbeat > 0 {
		_ = c.ws.SetWriteDeadline(time.Now().Add(2 * c.heartbeat))
	} else {
		_ = c.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
	}
	return c.ws.WriteMessage(websocket.PingMessage, nil)
}

// writeLoop 单写协程：串行写，避免并发写竞争；心跳开启时周期发 ping。
// 退出条件：写失败触发 Close，或 Close 已关闭连接（ClosedCh 关闭）。
// sendCh 永不主动关闭（避免并发 Send 向已关闭 channel 发送而 panic）。
// ping 间隔为 heartbeat/2，与 TCP 对齐，避免一次 pong 迟到即触发断线（读写超时为 2*heartbeat）。
func (c *Conn) writeLoop() {
	defer c.wg.Done()
	var tickerC <-chan time.Time
	if c.heartbeat > 0 {
		ticker := time.NewTicker(c.heartbeat / 2)
		defer ticker.Stop()
		tickerC = ticker.C
	}
	for {
		select {
		case data := <-c.sendCh:
			if err := c.writeMsg(data); err != nil {
				_ = c.Close()
				return
			}
		case <-tickerC:
			if err := c.writePing(); err != nil {
				_ = c.Close()
				return
			}
		case <-c.ClosedCh():
			return
		}
	}
}

// sendTimeout Send 写等待超时（避免对端不读时永久阻塞调用方 goroutine）。
// 取自 session 的共用默认值（与 tcp 同源）。
const sendTimeout = session.DefaultSendTimeout

// Send 向对端发送二进制数据（线程安全）。
//
// 两段式：先无阻塞试写 —— 绝大多数发送都能立刻入队，这一段不碰任何定时器；
// 只有队列确实满了才走带超时的慢路径，并在返回前显式 Stop。
// （原实现无条件 `time.After`，等于每次发送都造一个 runtime timer，
// 而绝大多数的成功路径根本不需要等待。）
func (c *Conn) Send(data []byte) error {
	if c.IsClosed() {
		return session.ErrClosed
	}
	// 写侧对称守卫：与读侧 SetReadLimit 同一上限（c.maxMsg，normalize 后恒 >0）。
	// 超限消息本地能写出，但对端会按自己的读上限关闭连接，错误现场落在连接层之外。
	if c.maxMsg > 0 && int64(len(data)) > c.maxMsg {
		logger.Warnf("ws: send message too large, id=%s, size=%d, limit=%d", c.ConnID(), len(data), c.maxMsg)
		return errMessageTooLarge
	}
	frame := append([]byte(nil), data...)
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
		// 连接实际未关闭，返回独立 ErrSendTimeout（与 tcp 一致），避免误判断线。
		logger.Warnf("ws send timeout, id=%s", c.ConnID())
		return ErrSendTimeout
	}
}

// SendUnreliable WebSocket 无真正不可靠语义（底层 TCP 可靠传输），降级为 Send。
func (c *Conn) SendUnreliable(data []byte) error {
	return c.Send(data)
}

// Close 关闭连接（幂等）。
// 不关闭 sendCh：Send 通过 select 监听 ClosedCh，写协程也通过 ClosedCh 退出，
// 避免「并发 Send + Close」向已关闭 channel 发送而 panic。
func (c *Conn) Close() error {
	var err error
	c.once.Do(func() {
		// 发送 CloseMessage 通知对端关闭连接。
		// 必须用 WriteControl：gorilla 允许它与 writeLoop 的 WriteMessage 并发调用，
		// 而 WriteMessage + SetWriteDeadline 与 writeLoop 并发写会造成帧字节交错、协议流损坏
		// （ws 没有 tcp 的 wrMu 串行化物理写）。
		closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
		deadline := time.Now().Add(3 * time.Second)
		if c.heartbeat > 0 {
			deadline = time.Now().Add(c.heartbeat)
		}
		_ = c.ws.WriteControl(websocket.CloseMessage, closeMsg, deadline)
		err = c.ws.Close()
		c.MarkClosed()
	})
	return err
}
