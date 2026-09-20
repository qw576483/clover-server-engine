// Package proto 定义 clover 对业务公开的客户端协议契约。
//
// 本包是**自包含的类型真身**：客户端帧边界常量、消息号区间上界、C2S / 推送 opcode、
// 弹窗载体与传输模式，全部定义于此，业务 import 本包即可点到，F12 不会跳进 internal。
//
// 不含线编解码实现：帧的 encode / decode、网关 ↔ 逻辑服信封、网关控制指令、
// 鉴权 / 会话结构体、全量同步载体等引擎内部协议一律留在 internal/shared/proto，
// 业务不可见。
//
// 消息号区间（全局统一，不可改动）：
//   - 引擎占 [1, InternalMsgMax]（C2S 从 1 起、推送从 4001 起、game↔master 6001–6004）；
//     引擎未来扩展的内部消息也只会落在该区间内，不会侵占业务段。
//   - 业务消息（C2S / 回包 / 推送）统一从 InternalMsgMax+1 起（即 >= 10001），
//     由业务 def 包自行编排；该约束由 app.Game.On 在绑定时统一校验。
//   - 回包按 requestID 配对，不占用独立消息号区间。
package proto

// 客户端帧字段长度（均为 uint32 大端）。
const (
	// RequestIDLen 请求关联 ID 字段长度。
	RequestIDLen = 4
	// MsgIDLen 消息号字段长度。
	MsgIDLen = 4
)

// ClientFrameHeaderLen 客户端帧头总长度（requestID + msgID）。
const ClientFrameHeaderLen = RequestIDLen + MsgIDLen

// InternalMsgMax 引擎内部保留消息号上界：<= 此值的 opcode 为引擎内部消息，
// 业务消息必须从 InternalMsgMax+1 起（即 >= 10001）。
// 该约束由 app.Game.On 在绑定时统一校验，业务层无需自行检查。
const InternalMsgMax uint32 = 10000
