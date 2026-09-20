// Package event 定义网关/逻辑层通用的事件模型与信封。
//
// 事件按"方向 + 消息号"建模，与游戏服务器典型协议对齐：
//
//   - 客户端请求 → ClientRequestEvent（上行，Type=client.request）
//     玩家操作从网络/网关进入逻辑层，MsgID 为协议消息号（opcode）。
//
//   - 服务器通知客户端 → ServerEvent（下行，Type=server.notify）
//     逻辑层/数据层主动推送，按 Target 投递到 group / gate / player。
//
//   - 服务器通知服务器 → InternalServerEvent（跨服，Type=server.internal）
//     跨服内部事件，可能带 player 信息；MMO 模式下 Object 即对象事件载体。
//
// 所有事件统一以 Envelope 为载体，MsgID 标记消息号，Payload 携带类型化载荷。
// 当前提供进程内事件总线（Bus），未来可在不改动业务代码的前提下桥接 NATS。
package event

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
)

// 事件方向/类型常量。
const (
	// EventClientRequest 客户端→服务器上行请求（网关/网络 → 逻辑层）。
	EventClientRequest = "client.request"
	// EventServerNotify 服务器→客户端下行通知（逻辑层/数据层 → 网关 → group/gate/player）。
	EventServerNotify = "server.notify"
	// EventServerInternal 服务器→服务器跨服内部事件（可能带 player 信息；MMO 下为对象事件）。
	EventServerInternal = "server.internal"
)

// Target 下行通知的投递目标（ServerEvent 的投递范围）。
type TargetKind string

const (
	// TargetPlayer 单播给某个玩家。TargetID = 玩家 UID。
	TargetPlayer TargetKind = "player"
	// TargetGroup 发给某个 group / room（如房间内广播）。TargetID = 组 ID。
	TargetGroup TargetKind = "group"
	// TargetGate 经网关投递/广播（如全服公告）。TargetID = 网关 ID；空表示全部网关。
	TargetGate TargetKind = "gate"
)

// Envelope 通用事件信封。所有事件统一以此为载体。
//
// ⚠️ 本类型是**进程内**载体，不可整体 JSON 往返：Ctx 是运行时上下文（序列化后为 null）、
// Payload 是 any（json 往返退化为 map[string]any，具体类型丢失）、字段本身无 json tag
// （编码后字段名是 Go 原名）。需要跨进程传输请走专门载荷（如 crossnode 的 CrossNodePayload），
// 需要打日志请用专用 DTO —— 不要对 Envelope 直接 json.Marshal 后指望能还原。
type Envelope struct {
	ID        string          // 事件唯一 ID
	Type      string          // 事件类型，见 Event* 常量
	MsgID     uint32          // 消息号 / opcode：标记事件种类的协议号
	Source    string          // 来源标识：gateway / logic / data
	UID       string          // 关联玩家 UID（可选）
	ConnID    string          // 关联连接 ID（可选）
	TraceID   string          // 全链路追踪 ID（可选）
	Timestamp time.Time       // 产生时间
	Payload   any             // 类型化载荷（*ClientRequestPayload / *ServerNotifyPayload / *InternalServerPayload）
	Ctx       context.Context // 原始请求上下文（含超时/trace/Owner），bus handler 通过它感知链路信息
}

// ClientRequestPayload 客户端上行请求载荷。
type ClientRequestPayload struct {
	Body []byte // 原始请求体（协议层按 MsgID 解码前的字节流）
}

// ServerNotifyPayload 服务器→客户端下行通知载荷。
type ServerNotifyPayload struct {
	Target   TargetKind // 投递目标：player / group / gate
	TargetID string     // 目标 ID：玩家 UID / 组 ID / 网关 ID（gate 可空=全部网关）
	Body     []byte     // 下行编码体
}

// InternalServerPayload 服务器→服务器内部事件载荷。
type InternalServerPayload struct {
	PlayerUID string // 关联玩家 UID（可选，跨服定位用）
	Object    any    // MMO 对象事件载体（实体快照/变更等）；无对象语义时可空
	Body      []byte // 内部协议体
}

// EventOption 信封可选修饰（设置 TraceID / Source / ConnID / MsgID / UID 等）。
// 注意：逻辑服内核的 Option（func(*Logic)，见 internal/transport/event）与本类型分属不同包，注意区分。
type EventOption func(*Envelope)

// WithTraceID 设置全链路追踪 ID。
func WithTraceID(id string) EventOption { return func(e *Envelope) { e.TraceID = id } }

// WithSource 设置来源标识。
func WithSource(s string) EventOption { return func(e *Envelope) { e.Source = s } }

// WithConnID 设置连接 ID。
func WithConnID(c string) EventOption { return func(e *Envelope) { e.ConnID = c } }

// WithMsgID 覆盖消息号（构造时也可直接传，此选项用于延后/批量设置）。
func WithMsgID(m uint32) EventOption { return func(e *Envelope) { e.MsgID = m } }

// WithUID 设置关联玩家 UID。
func WithUID(u string) EventOption { return func(e *Envelope) { e.UID = u } }

// WithCtx 设置原始请求上下文（含超时/trace/Owner），bus handler 通过它感知链路信息。
func WithCtx(ctx context.Context) EventOption { return func(e *Envelope) { e.Ctx = ctx } }

var seq atomic.Int64

// GenID 生成进程内唯一事件 ID（时间纳秒 + 自增序号，无外部依赖）。
func GenID() string {
	n := seq.Add(1)
	// 进制为内部固定常量 36，必然落在 [2, 36] 内，错误分支不可达。
	ts, _ := conv.FormatIntBase(time.Now().UnixNano(), 36)
	seqPart, _ := conv.FormatIntBase(n, 36)
	return ts + "-" + seqPart
}

// NewClientRequestEvent 构造客户端上行请求事件（Type=EventClientRequest）。
//   - uid / connID：关联玩家与连接（可选）
//   - msgID：请求消息号 / opcode
//   - body：原始请求体
func NewClientRequestEvent(uid, connID string, msgID uint32, body []byte, opts ...EventOption) Envelope {
	e := Envelope{
		ID:        GenID(),
		Type:      EventClientRequest,
		MsgID:     msgID,
		UID:       uid,
		ConnID:    connID,
		Timestamp: time.Now(),
		Payload:   &ClientRequestPayload{Body: body},
	}
	for _, o := range opts {
		o(&e)
	}
	return e
}

// NewServerNotifyEvent 构造服务器→客户端下行通知事件（Type=EventServerNotify）。
//   - msgID：下行消息号 / opcode
//   - target：投递目标（player / group / gate）
//   - targetID：目标 ID（player=UID / group=组ID / gate=网关ID，gate 可空）
//   - body：下行编码体
func NewServerNotifyEvent(msgID uint32, target TargetKind, targetID string, body []byte, opts ...EventOption) Envelope {
	e := Envelope{
		ID:        GenID(),
		Type:      EventServerNotify,
		MsgID:     msgID,
		Timestamp: time.Now(),
		Payload:   &ServerNotifyPayload{Target: target, TargetID: targetID, Body: body},
	}
	for _, o := range opts {
		o(&e)
	}
	// 单播玩家时，目标即 UID，便于下游直接定位连接。
	if target == TargetPlayer && e.UID == "" {
		e.UID = targetID
	}
	return e
}

// NewInternalServerEvent 构造服务器→服务器跨服内部事件（Type=EventServerInternal）。
//   - msgID：内部消息号 / opcode
//   - playerUID：关联玩家（可选，跨服定位用）
//   - object：MMO 对象事件载体（可选；无对象语义时可空）
//   - body：内部协议体
func NewInternalServerEvent(msgID uint32, playerUID string, object any, body []byte, opts ...EventOption) Envelope {
	// 深拷贝 body，避免调用方后续修改影响已发布的事件。
	var bodyCopy []byte
	if body != nil {
		bodyCopy = append([]byte(nil), body...)
	}
	e := Envelope{
		ID:        GenID(),
		Type:      EventServerInternal,
		MsgID:     msgID,
		UID:       playerUID,
		Timestamp: time.Now(),
		Payload:   &InternalServerPayload{Object: object, Body: bodyCopy},
	}
	for _, o := range opts {
		o(&e)
	}
	// 保持 Object.PlayerUID 与信封 UID 一致（便于跨服按玩家定位）。
	if p, ok := e.Payload.(*InternalServerPayload); ok {
		p.PlayerUID = e.UID
	}
	return e
}

// NewEvent 构造一个自定义类型事件（用于业务领域事件派发，如 "player.OnCreateRole"）。
//   - typ：事件类型 / 订阅键（业务自定义字符串，建议使用 "对象.事件" 命名，如 "player.OnCreateRole"）
//   - payload：类型化载荷（任意结构体；订阅者通过 Envelope.Payload 取回）
//
// ⚠️ typ 必须是**静态、有限**的事件名：总线发布计数把 typ 用作指标 label
// （clover_event_publish_total{kind=...}），拼入 playerID / uid / connID 等动态值会让
// 时间序列与指标注册表无界膨胀（metrics 的基数纪律见 pkg/foundation/metrics/naming.go）。
func NewEvent(typ string, payload any, opts ...EventOption) Envelope {
	e := Envelope{
		ID:        GenID(),
		Type:      typ,
		Timestamp: time.Now(),
		Payload:   payload,
	}
	for _, o := range opts {
		o(&e)
	}
	return e
}

// ClientPayload 类型安全地取出客户端请求载荷。
func (e Envelope) ClientPayload() (*ClientRequestPayload, bool) {
	p, ok := e.Payload.(*ClientRequestPayload)
	return p, ok
}

// ServerNotifyPayload 类型安全地取出服务器下行通知载荷。
func (e Envelope) ServerNotifyPayload() (*ServerNotifyPayload, bool) {
	p, ok := e.Payload.(*ServerNotifyPayload)
	return p, ok
}

// InternalPayload 类型安全地取出跨服内部事件载荷。
func (e Envelope) InternalPayload() (*InternalServerPayload, bool) {
	p, ok := e.Payload.(*InternalServerPayload)
	return p, ok
}
