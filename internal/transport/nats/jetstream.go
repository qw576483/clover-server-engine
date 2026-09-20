package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// 说明：本包原先还有一批 JetStream 入口，已按「死代码删净」逐个删除（删前逐个确认全仓零引用）：
//
//   - `StreamConfig` / `CreateStream` / `CreateConsumer` / `streamStorage` —— 预建流与消费者。
//     订阅路径无需预建（流由部署侧用 nats CLI 建），引擎侧没有调用点。
//   - `JetPublish` / `JetPubOpt` / `WithMsgID` —— 结构化持久发布（内部做 JSON 编码）。
//     引擎与业务都只经 `JetPublishRaw` 发**已编码字节**（remotexfer 的 JetPublisher 接口），
//     没有任何调用点。
//   - `JetSubscribe` / `JetHandler` / `ConsumerConfig` / `jetDispatch` —— 持久消费者订阅 +
//     ack/nak/死信转发/慢消息告警。全仓零调用点（引擎不消费 JetStream 主题，
//     跨服事件走普通 pub/sub + 死信队列，见 deadletter.go）。
//   - 配套的私有辅助 `subKeyJetPrefix`（订阅登记键前缀）、`orZeroDefault`（仅这两处用过）
//     随它们的唯一调用方一并删除。
//
// 现状（需知悉的副作用，不是缺陷）：
//
//   - JetStream 的**流**改由部署侧预建（nats CLI / 服务端配置）；
//   - 引擎只保留**持久发布**这一侧，唯一入口是 `JetPublishRaw`；
//   - 由此 `JetStream.StorageType` / `MaxMsgAge` 两个配置项**不再有任何读取点**（见 config.go）。

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
	// 与结构化持久发布（已删除）相同的重试口径：退避 + 总尝试次数有界，
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
