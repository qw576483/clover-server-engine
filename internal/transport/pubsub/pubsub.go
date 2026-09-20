// Package pubsub 统一引擎内发布订阅接口定义，消除各包的重复定义。
package pubsub

// Publisher 发布接口（与 data.Publisher 语义一致，均解耦具体消息总线）。
type Publisher interface {
	Publish(subject string, data []byte) error
}

// Subscriber 订阅接口：Subscribe 注册回调，handler 参数为 (subject, payload)。
// 各模块（gateway/mmo/entity）的具体实现可能支持通配符订阅。
type Subscriber interface {
	Subscribe(subject string, handler func(subject string, payload []byte)) error
}

// Unsubscriber 是**可选**的订阅反注册能力：实现方能在 Stop/Close 时只退自己那几条订阅。
//
// 为什么做成可选接口而不是塞进 Subscriber：Subscriber 有多个实现（NATS 适配器、
// 测试替身、网关侧适配器），把方法加进主接口会逼所有实现都写一个空方法；
// 调用方用类型断言探测，不支持时降级为"只能等客户端整体 Close"并留一条 Warn。
//
// 语义要求（实现方须满足）：
//   - 幂等：subject 未订阅 / 重复调用时返回 nil；
//   - 只退该 subject 的订阅，不影响其它订阅。
//
// 注：此前 transport/nats.Client 只保存 []*nats.Subscription、不保存句柄与 subject 的
// 对应关系，上层模块 Stop 时**根本无法**只退自己那几条（订阅生命周期只能止于客户端
// Close）——本接口即为此补齐的出口。
type Unsubscriber interface {
	Unsubscribe(subject string) error
}
