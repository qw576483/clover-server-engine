// facade.go 是 AOI 内核的**门面包装真身**：把 internal 的具体 *Grid / *VisualSystem
// 适配成 `pkg/domain/mmo/aoi` 的公开接口（`aoi.Grid` / `aoi.VisualSystem`）。
//
// 为什么包装必须放在 internal：`pkg/**` 只允许做门面（别名 / 转发 / 极薄适配），
// 见 `结构规则.md` §5.1；这里做的是「协变包装 + 事件枚举与函数类型的适配」，
// 属实现体，所以真身在 internal，`pkg/domain/mmo/mmo.go` 只做变量转发。
//
// 适配面只有两类（其余方法逐字同形，可零转换直通）：
//   - 事件枚举：本包的 `Event` 与 `pkg/domain/mmo/aoi.Event` 是两个独立命名类型，
//     回调里的 ev 需要显式转换；
//   - 函数类型：`FilterFn` / `Quota` 在两侧各自命名，需要转换（底层类型相同，转换无损）。
//
// 坐标 `Position` 两侧都是 `geom.Vec3` 的别名 —— 同一个类型，原样透传、零转换。
package aoi

import (
	"time"

	"clover-server-engine/internal/domain/object"
	pkaoi "clover-server-engine/pkg/domain/mmo/aoi"
)

// NewGridFacade 创建 AOI 网格并包装为公开接口 `aoi.Grid`。
// cellSize 为格子边长；底层与 `New` 同一套三维球体视野模型。
func NewGridFacade(cellSize float64) pkaoi.Grid {
	return &gridFacade{inner: New(cellSize)}
}

// gridFacade 门面 aoi.Grid 接口的实现：包装 internal 的 *Grid。
type gridFacade struct{ inner *Grid }

// SetObserver 设置视野变化回调（把内部事件枚举适配为门面 aoi.Event）。
func (w *gridFacade) SetObserver(o pkaoi.Observer) {
	w.inner.SetObserver(func(watcher, target object.ObjectID, ev Event) {
		o(watcher, target, pkaoi.Event(ev))
	})
}

// SetPermChecker 设置观察者权限校验回调（函数类型逐字同形，直通）。
func (w *gridFacade) SetPermChecker(check func(watcher, target object.ObjectID) bool) {
	w.inner.SetPermChecker(check)
}

func (w *gridFacade) SetRefreshRate(d time.Duration)             { w.inner.SetRefreshRate(d) }
func (w *gridFacade) Stop()                                      { w.inner.Stop() }
func (w *gridFacade) RemoveAll() []object.ObjectID               { return w.inner.RemoveAll() }
func (w *gridFacade) CellSize() float64                          { return w.inner.CellSize() }
func (w *gridFacade) Count() int                                 { return w.inner.Count() }
func (w *gridFacade) Enter(id object.ObjectID, p pkaoi.Position) { w.inner.Enter(id, p) }
func (w *gridFacade) Move(id object.ObjectID, p pkaoi.Position)  { w.inner.Move(id, p) }
func (w *gridFacade) Leave(id object.ObjectID)                   { w.inner.Leave(id) }
func (w *gridFacade) Unwatch(id object.ObjectID)                 { w.inner.Unwatch(id) }
func (w *gridFacade) BeginBatch()                                { w.inner.BeginBatch() }
func (w *gridFacade) EndBatch()                                  { w.inner.EndBatch() }
func (w *gridFacade) Visible(id object.ObjectID) []object.ObjectID {
	return w.inner.Visible(id)
}

func (w *gridFacade) Position(id object.ObjectID) (pkaoi.Position, bool) {
	// aoi.Position 与 internal Position 同为 geom.Vec3 的别名 → 直接返回，零转换。
	return w.inner.Position(id)
}

func (w *gridFacade) Watch(id object.ObjectID, radius float64) []object.ObjectID {
	return w.inner.Watch(id, radius)
}

func (w *gridFacade) Around(center pkaoi.Position, radius float64) []object.ObjectID {
	return w.inner.Around(center, radius)
}

func (w *gridFacade) Neighbors(id object.ObjectID, radius float64) []object.ObjectID {
	return w.inner.Neighbors(id, radius)
}

// 编译期断言：包装满足门面接口。
var _ pkaoi.Grid = (*gridFacade)(nil)

// NewVisualSystemFacade 以格子边长与默认视觉半径构造双向可见性系统，并包装为 `aoi.VisualSystem`。
// 底层网格与 NewGridFacade 同一套空间模型（三维球体判定）。
func NewVisualSystemFacade(cellSize, defaultR float64) pkaoi.VisualSystem {
	return &visualSystemFacade{inner: NewVisualSystem(cellSize, defaultR)}
}

// visualSystemFacade 门面 aoi.VisualSystem 接口的实现：包装 internal 的 *VisualSystem。
type visualSystemFacade struct{ inner *VisualSystem }

// SetObserver 设置视野增量回调（内部事件枚举 → 门面 aoi.Event）。
func (w *visualSystemFacade) SetObserver(o pkaoi.VisualObserver) {
	w.inner.SetObserver(func(viewer, target object.ObjectID, ev Event) {
		o(viewer, target, pkaoi.Event(ev))
	})
}

func (w *visualSystemFacade) Stop()                    { w.inner.Stop() }
func (w *visualSystemFacade) Close()                   { w.inner.Close() }
func (w *visualSystemFacade) Leave(id object.ObjectID) { w.inner.Leave(id) }
func (w *visualSystemFacade) SetVisual(id object.ObjectID, r float64) {
	w.inner.SetVisual(id, r)
}
func (w *visualSystemFacade) ClearVisual(id object.ObjectID) { w.inner.ClearVisual(id) }
func (w *visualSystemFacade) Enter(id object.ObjectID, p pkaoi.Position) {
	w.inner.Enter(id, p)
}
func (w *visualSystemFacade) Move(id object.ObjectID, p pkaoi.Position) {
	w.inner.Move(id, p)
}
func (w *visualSystemFacade) Visuals(id object.ObjectID) []object.ObjectID {
	return w.inner.Visuals(id)
}
func (w *visualSystemFacade) Observers(id object.ObjectID) []object.ObjectID {
	return w.inner.Observers(id)
}

// SetFilter 设置自定义可见性过滤（两侧 FilterFn 各自命名，底层类型相同，转换无损）。
func (w *visualSystemFacade) SetFilter(fn pkaoi.FilterFn) {
	w.inner.SetFilter(FilterFn(fn))
}

// SetQuota 设置单个观察者可保留的最大可见目标数（同上，命名类型转换）。
func (w *visualSystemFacade) SetQuota(q pkaoi.Quota) {
	w.inner.SetQuota(Quota(q))
}

// 编译期断言：包装满足门面接口。
var _ pkaoi.VisualSystem = (*visualSystemFacade)(nil)
