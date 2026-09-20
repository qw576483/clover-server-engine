package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// QueueSubscribe 队列消费，同一分组内消息只分给一台实例（逻辑服负载均衡）。
// 适用于异步任务分发（邮件、离线奖励、付费订单），多条实例均分消息、避免重复消费。
//
// 与 Subscribe 一致做重复订阅保护：同一 (subject, queueGroup) 重复注册会让本实例
// 在组内占据多个消费位，同一条消息被本进程重复消费（重复发奖/重复扣费）。
func (c *Client) QueueSubscribe(subject, queueGroup string, callback func(msg *Msg)) error {
	conn := c.conn.Load()
	if conn == nil {
		return ErrNotConnected
	}
	// 与普通订阅共用登记表，但用独立 key 空间避免与 Subscribe 的同名 subject 互相冲突。
	regKey := subKeyQueuePrefix + queueGroup + ":" + subject
	c.mu.Lock()
	if c.subTopics == nil {
		c.subTopics = make(map[string]bool)
	}
	if c.subTopics[regKey] {
		c.mu.Unlock()
		return fmt.Errorf("nats: subject %q queue %q already subscribed", subject, queueGroup)
	}
	c.subTopics[regKey] = true
	c.mu.Unlock()

	sub, err := conn.QueueSubscribe(subject, queueGroup, func(m *nats.Msg) {
		c.dispatch(m, callback)
	})
	if err != nil {
		c.mu.Lock()
		delete(c.subTopics, regKey)
		c.mu.Unlock()
		return err
	}
	c.track(regKey, sub)
	return nil
}
