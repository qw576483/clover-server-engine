package nats

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/qw576483/clover-server-engine/pkg/shared/timeutil"
)

// slowMsgThreshold 消息消费耗时超过该阈值打印 Warn 日志。
const slowMsgThreshold = 200 * time.Millisecond

// Publish 发布结构化消息，自动序列化为 JSON 并注入公共字段。
func (c *Client) Publish(subject string, data any) error {
	conn := c.conn.Load()
	if conn == nil {
		return ErrNotConnected
	}
	body, err := ujson.Marshal(data)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	c.injectHeaders(msg)
	msg.Data = body
	return conn.PublishMsg(msg)
}

// PublishRaw 发布已编码的原始字节，不经 JSON 二次编码（用于 NotifyPush 等二进制信封透传）。
func (c *Client) PublishRaw(subject string, data []byte) error {
	conn := c.conn.Load()
	if conn == nil {
		return ErrNotConnected
	}
	msg := nats.NewMsg(subject)
	c.injectHeaders(msg)
	msg.Data = data
	return conn.PublishMsg(msg)
}

// Subscribe 普通订阅，同一 subject 多实例同时收到完整消息（广播）。
// 回调内部自动 recover，单条消息 panic 不中断订阅协程。
// 检测重复订阅，相同 subject 已注册时返回错误防止资源泄漏。
func (c *Client) Subscribe(subject string, callback func(msg *Msg)) error {
	conn := c.conn.Load()
	if conn == nil {
		return ErrNotConnected
	}
	c.mu.Lock()
	if c.subTopics == nil {
		c.subTopics = make(map[string]bool)
	}
	if c.subTopics[subject] {
		c.mu.Unlock()
		return fmt.Errorf("nats: subject %q already subscribed", subject)
	}
	c.subTopics[subject] = true
	c.mu.Unlock()

	sub, err := conn.Subscribe(subject, func(m *nats.Msg) {
		c.dispatch(m, callback)
	})
	if err != nil {
		c.mu.Lock()
		delete(c.subTopics, subject)
		c.mu.Unlock()
		return err
	}
	c.track(subject, sub)
	return nil
}

// dispatch 包裹订阅回调，捕获 panic、记录耗时、慢消息告警。
func (c *Client) dispatch(m *nats.Msg, cb func(msg *Msg)) {
	start := timeutil.NowMS()
	msg := c.wrapMsg(m)
	func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("nats consume panic subject=%s trace_id=%s: %v\n%s",
					m.Subject, msg.TraceID, r, debug.Stack())
			}
		}()
		cb(msg)
	}()
	c.slowLog(m.Subject, timeutil.NowMS()-start, msg.TraceID)
}

func (c *Client) slowLog(subject string, cost int64, traceID string) {
	if cost > int64(slowMsgThreshold/time.Millisecond) {
		logger.Warnf("nats slow consume subject=%s cost=%dms trace_id=%s", subject, cost, traceID)
	}
}
