// Package aoi 兴趣区域（AOI）内核的公开 API 定义层。
//
// 本包定义 AOI 公共接口与类型，不含任何实现细节。
// 具体实现位于 internal/domain/mmo/spatial/aoi。
// 业务 / 框架层统一从本包引用 AOI 能力：
//
//	import (
//	    "github.com/qw576483/clover-server-engine/pkg/domain/mmo"
//	    "github.com/qw576483/clover-server-engine/pkg/domain/mmo/aoi"
//	    "github.com/qw576483/clover-server-engine/pkg/domain/object"
//	)
//
//	// 构造入口在 mmo 包（本包只提供接口与类型）。
//	g := mmo.NewGrid(64) // 64 米一格
//	g.SetObserver(func(watcher, target object.ObjectID, ev aoi.Event) {
//	    // 视野变化：把「进入 / 离开」推送给 watcher 对应的客户端
//	})
//	player := object.NewObjectID(object.TypePlayer, 1001)
//	g.Enter(player, aoi.Position{X: 100, Y: 0, Z: 200}) // Y 是高度
//	g.Watch(player, 96) // 玩家视野半径 96 米
//	// 之后 g.Move / g.Enter / g.Leave 会自动算出视野增量并回调 Observer。
package aoi

import (
	"github.com/qw576483/clover-server-engine/pkg/domain/object"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// Position 三维坐标（x=东西, y=高度, z=南北）。
//
// 它是 geom.Vec3 的**别名**：`mmo.Vec3` 与本类型是同一个类型，可以直接互传，无需转换。
type Position = geom.Vec3

// Event 视野事件类型。
type Event int

const (
	// EnterView 目标进入观察者视野。
	EnterView Event = iota + 1
	// LeaveView 目标离开观察者视野。
	LeaveView
	// LeaveAll 群体离开（RemoveAll 广播用）。
	LeaveAll
)

// Observer 视野变化回调：在 watcher 的视野中，target 发生了 ev（进入 / 离开）。
// 回调总是在释放内部锁之后调用，业务可在回调里安全地再调用本包方法或推送网络消息，无死锁风险。
type Observer func(watcher, target object.ObjectID, ev Event)

// VisualObserver 视觉系统的视野增量回调：viewer 对 target 发生了 ev（EnterView/LeaveView）。
type VisualObserver func(viewer, target object.ObjectID, ev Event)

// FilterFn 自定义可见性过滤（分组 / 阵营 / 距离二次校验等）。
// 参数为 viewer、target 的 uint64 线化身份（object.ObjectID.MarshalUint64）；
// 返回 true 表示允许可见。设 nil 关闭过滤（全部放行）。
type FilterFn func(viewer, target uint64) bool

// Quota 单个观察者可保留的最大可见目标数；0 表示不限。
type Quota int

// Grid 基于格子的 AOI 管理器，按世界区域分片，并发安全。
type Grid interface {
	SetObserver(o Observer)
	SetPermChecker(check func(watcher, target object.ObjectID) bool)
	Stop()
	RemoveAll() []object.ObjectID
	CellSize() float64
	Count() int
	Position(id object.ObjectID) (Position, bool)
	Enter(id object.ObjectID, p Position)
	Move(id object.ObjectID, p Position)
	Leave(id object.ObjectID)
	Watch(id object.ObjectID, radius float64) []object.ObjectID
	Unwatch(id object.ObjectID)
	BeginBatch()
	EndBatch()
	Visible(id object.ObjectID) []object.ObjectID
	Around(center Position, radius float64) []object.ObjectID
	Neighbors(id object.ObjectID, radius float64) []object.ObjectID
}

// VisualSystem 在 Grid 之上提供「双向可见性 + 过滤 + 配额」的视野关系系统。
type VisualSystem interface {
	SetObserver(o VisualObserver)
	Stop()
	Close()
	Enter(id object.ObjectID, p Position)
	Move(id object.ObjectID, p Position)
	Leave(id object.ObjectID)
	SetVisual(id object.ObjectID, radius float64)
	ClearVisual(id object.ObjectID)
	SetFilter(fn FilterFn)
	SetQuota(q Quota)
	Visuals(id object.ObjectID) []object.ObjectID
	Observers(id object.ObjectID) []object.ObjectID
}
