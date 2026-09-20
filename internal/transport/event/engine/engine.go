// package engine 定义 Clover 引擎的「标准事件词汇表」：引擎内核在关键生命周期点
// （连接断开、实体进入 / 离开场景、进入 / 离开视野、移动、玩家断线）会 emit 的标准事件。
//
// 业务通过 app.Game.OnEvent(type, handler) 订阅（与领域事件同一机制）；type 直接使用本包
// 导出的常量（如 engine.），payload 使用本包导出的 *Payload 结构体。
//
// 设计要点：
//   - 本包只定义词汇（类型常量 + 类型化载荷）与一个轻量 Dispatcher，不依赖事件总线具体实现；
//   - 真正的 emit 通过 Emitter 接口完成。AsEmitter 把 *event.Logic 适配为
//     Emitter，使 emit 走 event.Logic.EmitEvent，从而业务经 g.OnEvent 可直接订阅内核事件；
//   - 场景 / 移动等内核组件通过 WithEmitter / SetEmitter 注入 Emitter，即可在对应生命周期点
//     自动 emit 引擎标准事件（object.create/destroy/entry/leave/move、scene.player_disconnect、conn.disconnect）。
package engine

import (
	"clover-server-engine/internal/transport/event"
	apptypes "clover-server-engine/pkg/app/types"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/geom"
	"clover-server-engine/pkg/shared/safe"
)

// Type 事件类型（底层为 string，与 event 机制一致）。
type Type = string

// 引擎标准事件词汇（共 8 个）。
const (
	ObjectCreate          Type = "object.create"           // 实体进入场景
	ObjectDestroy         Type = "object.destroy"          // 实体离开场景
	ObjectEntry           Type = "object.entry"            // 实体进入另一实体视野（AOI）
	ObjectLeave           Type = "object.leave"            // 实体离开视野
	ObjectMove            Type = "object.move"             // 实体移动（mover）
	ScenePlayerDisconnect Type = "scene.player_disconnect" // 玩家断线（gateway→logic）
	PlayerSessionResumed  Type = "player.session_resumed"  // 玩家重连恢复会话
)

// 事件载荷坐标统一用 geom.Vec3（不再自定义同构类型）：
// 仓库里已有 geom.Vec3 这一套三维原语，此处再声明一个字段完全相同的 Vec3 只会多出一种
// 坐标类型，让「aoi.Position / geom.Vec3 / 载荷坐标」之间需要来回转换。
//
// 为什么载荷必须是三维：mover 判定位移变化时**含 Y**（起跳、落地这类纯垂直位移同样要 emit
// object.move），订阅方若只拿到 (X,Z)，就无法还原高度——视野同步、客户端插值、跨节点转发
// 会一起丢掉楼层信息。
type Vec3 = geom.Vec3

// ObjectRef 实体 / 对象标识。OID 为场景内对象序号（可选），Kind/ID 为实体种类与业务 id。
type ObjectRef struct {
	OID  uint64
	Kind string
	ID   string
}

// CreatePayload object.create 载荷：实体进入场景。
type CreatePayload struct {
	Scene  uint64
	Entity ObjectRef
	At     Vec3 // 出生坐标（含高度：多层地图的出生点可能在二楼/空中）
}

// DestroyPayload object.destroy 载荷：实体离开场景。
type DestroyPayload struct {
	Scene  uint64
	Entity ObjectRef
	Reason string
}

// EntryPayload object.entry 载荷：target 进入 observer 视野。
type EntryPayload struct {
	Observer ObjectRef
	Target   ObjectRef
}

// LeavePayload object.leave 载荷：target 离开 observer 视野。
type LeavePayload struct {
	Observer ObjectRef
	Target   ObjectRef
}

// MovePayload object.move 载荷：实体移动。From/To 为三维坐标——跳跃、飞行、上下楼的
// 纯垂直位移也必须能表达，否则订阅方按 (X,Z) 复原位置时会把人放回地面。
type MovePayload struct {
	Entity ObjectRef
	From   Vec3
	To     Vec3
}

// PlayerDisconnectPayload scene.player_disconnect 载荷：玩家断线。
type PlayerDisconnectPayload struct {
	Owner     string
	AccountID string
	Reason    string
}

// ConnDisconnectType 连接断开事件类型（conn.disconnect）：网关/连接层在连接真正关闭时 emit。
const ConnDisconnectType = "conn.disconnect"

// ConnDisconnectEvent 连接断开事件载荷。
type ConnDisconnectEvent = apptypes.ConnDisconnectEvent

// Emitter 引擎事件发射器接口（解耦事件总线实现，便于测试替换）。
type Emitter interface {
	// Emit 同步派发一个引擎事件给订阅者。
	Emit(typ string, payload any)
}

// Dispatcher 引擎标准事件发射器：封装 Emitter，提供按词汇的命名发射方法。
type Dispatcher struct{ e Emitter }

// NewDispatcher 用给定 Emitter 构造 Dispatcher。
func NewDispatcher(e Emitter) *Dispatcher { return &Dispatcher{e: e} }

// Emit 实现 Emitter 接口：透传到底层 Emitter，使 *Dispatcher 可直接作为引擎事件发射器使用。
// 加 recover 防止 subscriber panic 传播。
func (d *Dispatcher) Emit(typ string, payload any) {
	safe.SafeRun(func() {
		d.e.Emit(typ, payload)
	})
}

// AsEmitter 把 *event.Logic 适配为 Emitter：emit 走 Logic.EmitEvent，
// 因此业务经 app.Game.OnEvent(type, handler) 可直接订阅本包事件。
func AsEmitter(l *event.Logic) Emitter { return logicEmitter{l} }

type logicEmitter struct{ l *event.Logic }

func (x logicEmitter) Emit(typ string, payload any) {
	if x.l == nil {
		logger.Warnf("engine: emit %s skipped: logic not wired", typ)
		return
	}
	// 返回值必须处理：EmitEvent 聚合所有订阅者的错误，
	// 丢弃会让「订阅者全部失败」与「没有订阅者」在调用方看来一模一样（静默黑洞）。
	if err := x.l.EmitEvent(typ, nil, payload); err != nil {
		logger.Errorf("engine: emit %s: %v", typ, err)
	}
}

// 以下命名方法必须走 d.Emit 而非直连 d.e.Emit——否则绕过 recover 保护
// ，subscriber panic 会沿调用栈传播击穿场景/移动等内核组件。
//
// EmitCreate 实体进入场景（object.create）。
func (d *Dispatcher) EmitCreate(p CreatePayload) { d.Emit(ObjectCreate, p) }

// EmitDestroy 实体离开场景（object.destroy）。
func (d *Dispatcher) EmitDestroy(p DestroyPayload) { d.Emit(ObjectDestroy, p) }

// EmitEntry 实体进入另一实体视野（object.entry）。
func (d *Dispatcher) EmitEntry(p EntryPayload) { d.Emit(ObjectEntry, p) }

// EmitLeave 实体离开视野（object.leave）。
func (d *Dispatcher) EmitLeave(p LeavePayload) { d.Emit(ObjectLeave, p) }

// EmitMove 实体移动（object.move）。
func (d *Dispatcher) EmitMove(p MovePayload) { d.Emit(ObjectMove, p) }

// EmitPlayerDisconnect 玩家断线（scene.player_disconnect）。
func (d *Dispatcher) EmitPlayerDisconnect(p PlayerDisconnectPayload) {
	d.Emit(ScenePlayerDisconnect, p)
}

// SessionResumedPayload 玩家重连会话恢复载荷（player.session_resumed）。
type SessionResumedPayload struct {
	PlayerID string
	Account  string
}

// EmitPlayerSessionResumed 玩家重连恢复会话（player.session_resumed）。
func (d *Dispatcher) EmitPlayerSessionResumed(p SessionResumedPayload) {
	d.Emit(PlayerSessionResumed, p)
}
