package nats

import (
	"context"
	"time"

	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/nats-io/nats.go"
)

// minRequestTimeout NATS 同步请求的最小超时，防止调用方传入过小的无效超时。
const minRequestTimeout = 50 * time.Millisecond

// Request 同步发送消息，等待指定超时返回应答（请求-应答 / 轻量跨服同步查询）。
// 适用于跨服玩家信息查询、竞技场匹配同步请求。
// 应答方在 Subscribe/QueueSubscribe 回调中调用 msg.Respond(data) 回复。
// timeout 低于最小阈值（50ms）时自动提升，避免无效超时。
func (c *Client) Request(subject string, data any, timeout time.Duration) (*Msg, error) {
	if timeout <= 0 {
		timeout = orDuration(c.conf.MsgTimeout, 500*time.Millisecond)
	}
	if timeout < minRequestTimeout {
		timeout = minRequestTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.RequestCtx(ctx, subject, data)
}

// RequestCtx 是 Request 的 ctx 感知版本。
// 通过 RequestMsgWithContext 传入 ctx，使调用方可主动取消/传递超时与链路上下文，
// 避免仅靠固定 timeout 导致请求无法随上游取消而及时释放。
func (c *Client) RequestCtx(ctx context.Context, subject string, data any) (*Msg, error) {
	conn := c.conn.Load()
	if conn == nil {
		return nil, ErrNotConnected
	}
	body, err := ujson.Marshal(data)
	if err != nil {
		return nil, err
	}
	// 若 ctx 无 deadline，补一个默认超时防止永久阻塞。
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, orDuration(c.conf.MsgTimeout, 500*time.Millisecond))
		defer cancel()
	}
	req := nats.NewMsg(subject)
	c.injectHeaders(req)
	req.Data = body

	resp, err := conn.RequestMsgWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.wrapMsg(resp), nil
}
