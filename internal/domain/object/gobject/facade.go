// facade.go 是统一游戏对象内核的**门面真身**：把 internal 的具体 *GameObject
// 协变包装成业务可用的接口（`GameObjectFacade`，业务侧可见名 `gobject.GameObject`）。
//
// 为什么真身必须在 internal：`pkg/**` 只允许做门面（类型别名 / 变量转发 / 极薄适配），
// 见 `结构规则.md` §5.1；本文件里的包装与工厂是行为实现体，故落在 internal。
// `pkg/domain/object/gobject`（门面包）只持有 `type X = internal.X` 与 `var F = internal.F`。
//
// 接口名加 `Facade` 后缀以避开本包既有 struct `GameObject`。
package gobject

import (
	"context"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	object "github.com/qw576483/clover-server-engine/internal/domain/object"
	pkgdata "github.com/qw576483/clover-server-engine/pkg/domain/data"
	"github.com/qw576483/clover-server-engine/pkg/runtime/timer"
)

// GameObjectFacade 统一游戏对象（身份 + 版本 + 乐观并发写 + 增量同步 + 运行时组件槽）。
// 业务侧可见名 = `pkg/domain/object/gobject.GameObject`。
//
// 说明：为保持单向依赖与最小暴露，本接口只收编业务可达的方法。以下「引擎装配型」方法未编入接口，
// 它们返回/接收引擎内部装配类型（*object.Bag / *object.Schema / *fsm.Machine / 子对象树），仅 internal
// 装配与仓储层使用，业务无句柄：
//   - Props / AddRecord / Record / RecordNames（返回内部 Bag / Record）
//   - SetSchema / Schema / ValidateProps / MarshalPropsIndexed / ApplyPropsIndexed / SyncPropsPatch /
//     SavePropsSnapshot（返回内部 *object.Schema）
//   - AttachFSM / FSM（返回内部 *fsm.Machine）
//   - AddChild / Child / Children / ChildSlots / ChildIDs / RemoveChild（子对象树装配）
//   - SyncPropsPatch / ApplyPropsCompact / SavePropsSnapshot 等序列化内部格式
type GameObjectFacade interface {
	// ObjectID 返回对象号（满足 object.Object，可被 object.Manager 按号注册与派发）。
	ObjectID() object.ObjectID
	// Load 从 Store 载入 props 与所有已定义 records；不存在的键视为空。
	Load(ctx context.Context) error
	// Save 落库 props 与所有 records，并 bump 乐观并发版本号。
	Save(ctx context.Context) error
	// Delete 从 Store 彻底删除该对象数据（props + 全部 records + 级联子对象）。
	Delete(ctx context.Context) error
	// Version 返回当前乐观并发版本号。
	Version() int64
	// SaveIfVersion 仅在本地版本仍等于 expected 时保存；否则返回 ErrVersionConflict。
	SaveIfVersion(ctx context.Context, expected int64) error

	// AttachTimer 绑定定期器组（业务凭其注册 Every/After 等定时任务）。
	AttachTimer(grp *timer.Group)
	// Timer 返回已绑定的定期器组。
	Timer() *timer.Group
	// Attach 挂载一个运行时组件（限流器/状态机/定时器/AI 行为树等）。
	Attach(c Component)
	// Detach 按名称卸载并停止某个运行时组件。
	Detach(name string)
	// Component 取已挂载的运行时组件；未挂则返回 nil。
	Component(name string) Component
	// ComponentNames 返回全部已挂载组件名（便于枚举/调试）。
	ComponentNames() []string
	// DumpComponents 导出全部运行时组件的二进制快照（迁移/序列化用）。
	DumpComponents() map[string][]byte
	// ImportComponents 从快照批量恢复运行时组件。
	ImportComponents(list map[string][]byte)
	// StopComponents 停止全部已挂载运行时组件，便于对象下线/销毁。
	StopComponents()

	// SetSyncContext 绑定「写即自动同步」广播的生命周期上下文：
	// 传入的 ctx 被取消后，之后的自动同步广播会被丢弃（不再下发）。
	// 常用于「持有者结束」（会话断开 / 房间销毁 / 服务停机）——避免对象已下线、广播还在跑。
	// 传 nil 恢复为 context.Background()（此时仅靠单次超时兜底）。
	SetSyncContext(ctx context.Context)

	// MarshalJSON 完整线化对象（props + records + children 归属），便于快照/调试。
	MarshalJSON() ([]byte, error)
	// UnmarshalJSON 从完整线化格式覆盖式载入并恢复子对象归属索引。
	UnmarshalJSON(b []byte) error
}

// gameObjectFacade 门面 GameObjectFacade 接口的实现：包装 internal 的 *GameObject。
type gameObjectFacade struct{ inner *GameObject }

// ObjectID 返回对象号（满足 object.Object，可被 object.Manager 按号注册与派发）。
func (g *gameObjectFacade) ObjectID() object.ObjectID { return g.inner.ObjectID() }

// Load 从 Store 载入 props 与所有已定义 records；不存在的键视为空，载入后清除脏标记。
func (g *gameObjectFacade) Load(ctx context.Context) error { return g.inner.Load(ctx) }

// Save 落库 props 与所有 records，级联保存子对象并 bump 乐观并发版本号；已注入发布器时同步广播变动。
func (g *gameObjectFacade) Save(ctx context.Context) error { return g.inner.Save(ctx) }

// Delete 从 Store 彻底删除该对象数据（props + 全部 records + 级联子对象）。
func (g *gameObjectFacade) Delete(ctx context.Context) error { return g.inner.Delete(ctx) }

// Version 返回当前乐观并发版本号（自加载后本地递增；平衡点见 SaveIfVersion）。
func (g *gameObjectFacade) Version() int64 { return g.inner.Version() }

// SaveIfVersion 仅在本地版本仍等于 expected 时保存并 bump；否则返回 ErrVersionConflict。
func (g *gameObjectFacade) SaveIfVersion(ctx context.Context, expected int64) error {
	return g.inner.SaveIfVersion(ctx, expected)
}

// AttachTimer 绑定定期器组（业务凭其注册 Every/After 等定时任务）。
func (g *gameObjectFacade) AttachTimer(grp *timer.Group) {
	if grp == nil {
		return
	}
	g.inner.AttachTimer(grp)
}

// Timer 返回已绑定的定期器组。
func (g *gameObjectFacade) Timer() *timer.Group { return g.inner.Timer() }

// Attach 挂载一个运行时组件（限流器/状态机/定时器/AI 行为树等）。
func (g *gameObjectFacade) Attach(c Component) { g.inner.Attach(c) }

// Detach 按名称卸载并停止某个运行时组件。
func (g *gameObjectFacade) Detach(name string) { g.inner.Detach(name) }

// Component 取已挂载的运行时组件；未挂则返回 nil。
func (g *gameObjectFacade) Component(name string) Component { return g.inner.Component(name) }

// ComponentNames 返回全部已挂载组件名（便于枚举/调试）。
func (g *gameObjectFacade) ComponentNames() []string { return g.inner.ComponentNames() }

// DumpComponents 导出全部运行时组件的二进制快照（迁移/序列化用）。
func (g *gameObjectFacade) DumpComponents() map[string][]byte { return g.inner.DumpComponents() }

// ImportComponents 从快照批量恢复运行时组件。
func (g *gameObjectFacade) ImportComponents(list map[string][]byte) {
	g.inner.ImportComponents(list)
}

// StopComponents 停止全部已挂载运行时组件，便于对象下线/销毁。
func (g *gameObjectFacade) StopComponents() { g.inner.StopComponents() }

// SetSyncContext 绑定「写即自动同步」广播的生命周期上下文（取消后不再广播）。
func (g *gameObjectFacade) SetSyncContext(ctx context.Context) { g.inner.SetSyncContext(ctx) }

// MarshalJSON 完整线化对象（props + records + children 归属），便于快照/调试。
func (g *gameObjectFacade) MarshalJSON() ([]byte, error) { return g.inner.MarshalJSON() }

// UnmarshalJSON 从完整线化格式覆盖式载入并恢复子对象归属索引。
func (g *gameObjectFacade) UnmarshalJSON(b []byte) error { return g.inner.UnmarshalJSON(b) }

// 编译期断言：门面包装满足门面接口。
var _ GameObjectFacade = (*gameObjectFacade)(nil)

// NewGameObjectFacade 构造一个空 GameObject（仅身份 + 空属性袋），并以门面接口返回。
// store 为数据存储抽象接口（门面 data.Store）；id 为带类型的对象号（object.ObjectID）。
// store 必须是 data.Store 的 internal 底层实现，否则返回 nil（typed-nil 安全）。
// 业务侧可见名 = `pkg/domain/object/gobject.New`。
func NewGameObjectFacade(store pkgdata.Store, id object.ObjectID) GameObjectFacade {
	is, ok := data.InternalStore(store)
	if !ok || is == nil {
		return nil
	}
	return &gameObjectFacade{inner: New(is, id)}
}

// FromGameObjectFacade 将 internal 的 *GameObject 包装为门面 GameObjectFacade 接口。
// 业务侧可见名 = `pkg/domain/object/gobject.FromGameObject`。
func FromGameObjectFacade(g *GameObject) GameObjectFacade {
	if g == nil {
		return nil
	}
	return &gameObjectFacade{inner: g}
}

// InternalGameObjectFacade 将门面 GameObjectFacade 接口还原为 internal 的 *GameObject；非底层则 ok=false。
// 业务侧可见名 = `pkg/domain/object/gobject.InternalGameObject`。
func InternalGameObjectFacade(g GameObjectFacade) (*GameObject, bool) {
	if v, ok := g.(*gameObjectFacade); ok {
		return v.inner, true
	}
	return nil, false
}
