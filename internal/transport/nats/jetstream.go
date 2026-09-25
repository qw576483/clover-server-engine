package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// 说明：JetStream 的**流**由部署侧预建（nats CLI / 服务端配置）；
// 引擎只保留**持久发布**这一侧，唯一入口是 `JetPublishRaw`。

// JetPublishRaw 以原始字节持久发布（实现 remotexfer.JetPublisher 接口）。
// 用于已编码好的二进制指令（如跨机迁移）：普通 Publish 是 at-most-once，
// 抖动 / 瞬断会静默丢消息，故此路径带有限重试 + 退避，保证「至少一次」写入。
func (c *Client) JetPublishRaw(ctx context.Context, subject string, data []byte) error {
	js, err := c.JS()
	if err != nil {
		return err
	}
	// 与普通 Publish 同理注入公共头，保证消费端能拿到 trace_id 串联链路。
	msg := nats.NewMsg(subject)
	c.injectHeaders(msg)
	msg.Data = data
	// 重试口径：退避 + 总尝试次数有界，
	// 且每次尝试前先看 ctx（调用方取消/超时后不再白重试）。
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return fmt.Errorf("nats jet publish %s canceled: %w", subject, ctx.Err())
		}
		if _, err = js.PublishMsg(msg); err == nil {
			return nil
		}
		lastErr = err
		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return fmt.Errorf("nats jet publish %s canceled: %w", subject, ctx.Err())
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			}
		}
	}
	return fmt.Errorf("nats jet publish %s (after %d attempts): %w", subject, maxAttempts, lastErr)
}
