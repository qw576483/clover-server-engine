// Package tcpmsg 的 client.go：TCP 客户端，封装连接管理 + 请求-响应匹配。
package tcpmsg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	netpkg "clover-server-engine/internal/transport/net/tcp"
	"clover-server-engine/pkg/foundation/logger"
)

const defaultTimeout = 10 * time.Second

var (
	ErrClosed       = errors.New("tcpmsg: client closed")
	ErrTimeout      = errors.New("tcpmsg: request timeout")
	ErrDisconnected = errors.New("tcpmsg: disconnected")
)

// Client TCP 消息客户端，支持并发请求和响应匹配。
type Client struct {
	conn *netpkg.Conn
	addr string

	mu      sync.Mutex
	pending map[uint32]chan *responseFrame // requestID → response channel
	reqSeq  atomic.Uint32
	closeCh chan struct{}
	closed  atomic.Bool
	// disconnected 底层连接已断开且尚未 Close 主动关闭：
	// 此时响应不可能再到达，新请求直接失败，不必再等满超时。
	disconnected atomic.Bool

	// 逐帧异常日志降频计数（decode 失败 / 迟到或未知响应帧），步长见 logThrottleEvery。
	decodeErrLogCount  atomic.Uint64
	orphanRespLogCount atomic.Uint64
}

type responseFrame struct {
	msgID uint32
	body  []byte
}

// dialOptions Dial 的可选参数。
type dialOptions struct {
	authMsgID uint32
	authToken string
}

// DialOption Dial 的可选项。
type DialOption func(*dialOptions)

// WithAuth 让连接在建立后**立即**发送鉴权握手帧（authMsgID + token）：
// 握手失败则 Dial 直接失败（fail-fast，不返回一条从未鉴权成功的连接）。
//
// 用「首帧握手」而不是把 token 塞进每个请求体：token 只需比较一次，
// 不进入业务报文（业务 handler 完全无感），也不会因为某个 handler 忘校验而漏掉；
// 消息号由调用方（领域层）给出，本包不约定具体数值——那是领域契约。
func WithAuth(authMsgID uint32, token string) DialOption {
	return func(o *dialOptions) {
		o.authMsgID = authMsgID
		o.authToken = token
	}
}

// Dial 连接到 TCP 消息服务地址，返回就绪的 Client。
// 未传 WithAuth（或 token 为空）时不握手，用于只绑回环、无需鉴权的通道。
func Dial(addr string, opts ...DialOption) (*Client, error) {
	o := dialOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	c := &Client{
		addr:    addr,
		pending: make(map[uint32]chan *responseFrame),
		closeCh: make(chan struct{}),
	}

	conn, err := netpkg.Dial(netpkg.ClientConfig{
		Address:           addr,
		HeartbeatInterval: 30 * time.Second,
	}, c.onFrame)
	if err != nil {
		return nil, fmt.Errorf("tcpmsg: dial %s: %w", addr, err)
	}

	c.conn = conn
	go c.watchDisconnect()
	if o.authToken != "" {
		if err := c.handshake(o.authMsgID, o.authToken); err != nil {
			// 握手失败必须关连接：否则这条未鉴权连接会一直挂在那里，
			// 服务端侧的拒绝日志与连接数都会被它持续污染。
			_ = c.Close()
			return nil, fmt.Errorf("tcpmsg: auth handshake with %s failed: %w", addr, err)
		}
	}
	return c, nil
}

// authFrame 握手请求体（字段名由领域契约决定，这里与 master state.AuthReq 一致）。
type authFrame struct {
	Token string `json:"token"`
}

// authReply 握手响应体（与领域层的 ok/error 回包结构一致）。
type authReply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

// handshake 发送握手帧并校验服务端回包。
func (c *Client) handshake(msgID uint32, token string) error {
	var reply authReply
	if err := c.CallContext(context.Background(), msgID, authFrame{Token: token}, &reply); err != nil {
		return err
	}
	if !reply.OK {
		return fmt.Errorf("unauthorized: %s", reply.Error)
	}
	return nil
}

// watchDisconnect 监听底层连接关闭，并在断开时唤醒所有在途调用。
//
// 连接一断，对端响应永远不会到达；若不唤醒，pending 的调用方只能等到
// defaultTimeout 超时才拿到失败，故障感知被白白拉长一个超时周期。
func (c *Client) watchDisconnect() {
	select {
	case <-c.conn.ClosedCh():
	case <-c.closeCh:
		return
	}
	// 自身 Close 会先关 closeCh 再关连接，两个 case 同时就绪时不算异常断连。
	if c.closed.Load() {
		return
	}
	c.disconnected.Store(true)
	c.wakePending()
}

// wakePending 用 nil 帧唤醒所有等待中的请求：CallContext 收到 nil 即返回 ErrDisconnected。
// 非阻塞发送——channel 带缓冲且不重复投递，避免与已到达的响应互相阻塞。
func (c *Client) wakePending() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pending {
		select {
		case ch <- nil:
		default:
		}
		delete(c.pending, id)
	}
}

// onFrame 收到一个完整的 TCP 帧（已由 net/tcp 拆帧）。解码帧并路由到等待的调用者。
func (c *Client) onFrame(conn *netpkg.Conn, data []byte) {
	msgID, requestID, body, err := Decode(data)
	if err != nil {
		// 坏帧不能静默丢弃：协议错位时这行日志是唯一线索（与 server 侧同款降频）。
		if n := c.decodeErrLogCount.Add(1); n == 1 || n%logThrottleEvery == 0 {
			logger.Errorf("tcpmsg client: decode error: %v（同类累计 %d 次，已降频输出）", err, n)
		}
		return
	}

	c.mu.Lock()
	ch, ok := c.pending[requestID]
	if ok {
		delete(c.pending, requestID)
	}
	c.mu.Unlock()

	if ok {
		ch <- &responseFrame{msgID: msgID, body: body}
		return
	}
	// requestID 不在 pending：迟到的响应 / 未知帧（协议错位），记录以便定位（降频防刷屏）。
	if n := c.orphanRespLogCount.Add(1); n == 1 || n%logThrottleEvery == 0 {
		logger.Warnf("tcpmsg client: response for unknown requestID=%d msgID=%d（同类累计 %d 次，已降频输出）", requestID, msgID, n)
	}
}

// Call 同步调用。自动序列化 req、发送、等待响应并反序列化到 resp。
// 使用默认超时，等价于 CallContext(context.Background(), ...)。
func (c *Client) Call(msgID uint32, req any, resp any) error {
	return c.CallContext(context.Background(), msgID, req, resp)
}

// CallContext 同步调用，并遵守 ctx 的取消/超时（仍受 defaultTimeout 兜底）。
func (c *Client) CallContext(ctx context.Context, msgID uint32, req any, resp any) error {
	if c.closed.Load() {
		return ErrClosed
	}
	// 连接已断开：响应不可能到达，立即失败而不是让调用方空等超时。
	if c.disconnected.Load() {
		return ErrDisconnected
	}

	requestID := c.reqSeq.Add(1)
	frame, err := MarshalCall(requestID, msgID, req)
	if err != nil {
		return err
	}

	ch := make(chan *responseFrame, 1)
	c.mu.Lock()
	c.pending[requestID] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
	}()

	if err := c.conn.Send(frame); err != nil {
		return fmt.Errorf("tcpmsg: send: %w", err)
	}

	// 用 Timer 而非 time.After，避免超时前返回时把定时器泄漏到 defaultTimeout 结束。
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()

	select {
	case rf := <-ch:
		if rf == nil {
			return ErrDisconnected
		}
		if len(rf.body) == 0 {
			// 无 body（服务端 handler 返回 nil, nil）：没有可反序列化的内容，
			// 直接按成功返回，避免把「无 body」误判成坏包。
			return nil
		}
		if err := json.Unmarshal(rf.body, resp); err != nil {
			return fmt.Errorf("tcpmsg: unmarshal response: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		return ErrClosed
	case <-timer.C:
		return ErrTimeout
	}
}

// Close 关闭客户端连接。
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	close(c.closeCh)
	return c.conn.Close()
}

// IsClosed 返回客户端是否已关闭。
func (c *Client) IsClosed() bool {
	return c.closed.Load()
}
