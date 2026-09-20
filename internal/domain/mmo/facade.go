// facade.go 是 MMO 场景系统的**门面真身**：把 internal 的具体类型
// （*SceneManager / *Scene / *Instance）协变包装成业务可用的接口形态，
// 并提供「包级操作函数 + 工厂」这一层公开 API。
//
// 为什么真身必须在 internal：`pkg/**` 只允许做门面（类型别名 / 变量转发 / 极薄适配），
// 见 `结构规则.md` §5.1；本文件里的包装与工厂是行为实现体，故落在 internal。
// `pkg/domain/mmo`（门面包）只持有 `type X = internal.X` 与 `var F = internal.F`。
//
// 接口命名：门面接口加 `Facade` 后缀（`SceneManagerFacade` / `SceneFacade` /
// `InstanceFacade` / `SceneEventHandlerFacade`），以避开本包既有的同名 struct
// （`SceneManager` / `Scene` / `Instance` / `SceneEventHandler`）。
//
// 门面接口在业务侧的可见名是**不带后缀**的：`pkg/domain/mmo.Scene` 等（别名到本文件）。
package mmo

import (
	"context"
	"time"

	"clover-server-engine/internal/domain/data"
	ibtree "clover-server-engine/internal/domain/mmo/gameplay/ai/btree"
	icombat "clover-server-engine/internal/domain/mmo/gameplay/combat"
	imov "clover-server-engine/internal/domain/mmo/mover"
	isync "clover-server-engine/internal/domain/mmo/sync/core"
	"clover-server-engine/internal/domain/object"
	pkgdata "clover-server-engine/pkg/domain/data"
	pkgbtree "clover-server-engine/pkg/domain/mmo/ai/btree"
	pkgcollide "clover-server-engine/pkg/domain/mmo/collide"
	combatpkg "clover-server-engine/pkg/domain/mmo/combat"
	movpkg "clover-server-engine/pkg/domain/mmo/mover"
	syncpkg "clover-server-engine/pkg/domain/mmo/sync"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/geom"
)

// 场景管理器
// SceneManagerFacade 全局场景管理器：管理所有 Scene 的生命周期与跨 Scene 传输。
// 业务侧可见名 = `pkg/domain/mmo.SceneManager`。
type SceneManagerFacade interface {
	// CreateScene 创建并注册一个场景。
	CreateScene(id uint64, name string) SceneFacade
	// GetScene 按场景 id 获取场景。
	GetScene(id uint64) (SceneFacade, bool)
	// SceneOf 按对象 id 反查它当前所在的场景（引擎内部维护 objID→Scene 索引，O(1)）。
	//
	// 为什么要有它：业务常见写法是"按玩家记账 sceneID"、再自己维护一张 objID→场景 的表 ——
	// 那份表与引擎索引是**两份真值**，漏写一处就会出现"移动/离场被静默忽略"
	// （handler 查不到场景直接 return，客户端表现为按键不动）。直接问引擎，只剩一份真值。
	SceneOf(objID uint64) (SceneFacade, bool)
	// DestroyScene 销毁场景并踢出全部成员。
	DestroyScene(id uint64)
	// TransferRemote 跨机器迁移对象到目标场景坐标（三维落点，含高度）；异机经 NATS 路由、同机走本地迁移。
	TransferRemote(dstSceneID, objID uint64, pos Vec3) error
	// Run 阻塞启动场景管理器，运行各场景心跳直到 ctx 取消。
	Run(ctx context.Context) error
	// ObjectManager 返回场景内对象所在的对象管理器。
	// 场景内实体（玩家 / 怪物）注册在此，对它们发事件（SendEvent / SendQueueEvent）必须用它，
	// 而不是 Game 内置的那个（两者是不同实例，互不可见）。
	ObjectManager() *object.Manager
}

// sceneManagerFacade 门面 SceneManagerFacade 接口的实现：包装 internal 的 *SceneManager，
// 并把返回的 *Scene 协变包装为门面 SceneFacade 接口（协变 wrapper）。
type sceneManagerFacade struct{ inner *SceneManager }

// CreateScene 创建并注册一个场景。
func (m *sceneManagerFacade) CreateScene(id uint64, name string) SceneFacade {
	return fromScene(m.inner.CreateScene(id, name))
}

// GetScene 按场景 id 获取场景。
func (m *sceneManagerFacade) GetScene(id uint64) (SceneFacade, bool) {
	s, ok := m.inner.GetScene(id)
	if !ok {
		return nil, false
	}
	return fromScene(s), true
}

// SceneOf 按对象 id 反查它当前所在的场景。
func (m *sceneManagerFacade) SceneOf(objID uint64) (SceneFacade, bool) {
	s, ok := m.inner.SceneOf(objID)
	if !ok {
		return nil, false
	}
	return fromScene(s), true
}

// DestroyScene 销毁场景并踢出全部成员。
func (m *sceneManagerFacade) DestroyScene(id uint64) { m.inner.DestroyScene(id) }

// TransferRemote 跨机器迁移对象到目标场景坐标（三维落点，含高度）。
func (m *sceneManagerFacade) TransferRemote(dstSceneID, objID uint64, pos Vec3) error {
	return m.inner.TransferRemote(dstSceneID, objID, pos)
}

// Run 阻塞启动场景管理器，运行各场景心跳直到 ctx 取消。
func (m *sceneManagerFacade) Run(ctx context.Context) error { return m.inner.Run(ctx) }

// ObjectManager 返回场景内对象所在的对象管理器。
func (m *sceneManagerFacade) ObjectManager() *object.Manager { return m.inner.ObjectManager() }

// 编译期断言：包装满足门面接口。
var _ SceneManagerFacade = (*sceneManagerFacade)(nil)

// 场景
// SceneFacade SceneManagerFacade 内的一张地图：多个隔离 Instance + 共享物理碰撞 + 视野同步。
// 业务侧可见名 = `pkg/domain/mmo.Scene`。
type SceneFacade interface {
	// ID 返回场景 id。
	ID() uint64
	// CreateInstance 创建新实例。
	CreateInstance(id uint32) (InstanceFacade, error)
	// RemoveInstance 删除实例（默认 instance 0 不允许删除）。
	RemoveInstance(id uint32) error
	// Enter 让玩家对象以坐标 pos 进入场景。
	Enter(objID uint64, pos Vec3) error
	// EnterOwnerType 让对象以指定实体类型进入场景默认 instance 0。
	EnterOwnerType(objID uint64, ownerType data.OwnerType, pos Vec3) error
	// EnterOwnerTypeInstance 让对象以指定实体类型进入指定 instance。
	EnterOwnerTypeInstance(objID uint64, ownerType data.OwnerType, pos Vec3, instanceID uint32) error
	// Leave 让对象离开场景。
	Leave(objID uint64)
	// Move 移动对象到指定坐标（会触发视野同步）。
	Move(objID uint64, pos Vec3)
	// MoveBatch 批量移动多个对象，整帧只刷新一次 AOI 与推送一次视野事件。
	MoveBatch(moves []MoveOp)
	// BeginBatch 开始手动批量模式；之后移动/进出/设置视野操作累积到同一 batch。
	BeginBatch()
	// EndBatch 结束手动批量模式，统一刷新 AOI 并推送视野事件。
	EndBatch()
	// SetViewRadius 设置对象视野半径；r<=0 取消观察。
	SetViewRadius(objID uint64, radius float64)
	// Stop 停止场景心跳。
	Stop()
	// Tick 手动驱动一次心跳（dt 为距上次间隔）。
	Tick(dt time.Duration)
	// EnablePhysics 开启场景物理步进。
	EnablePhysics()
	// DisablePhysics 关闭场景物理步进。
	DisablePhysics()
	// AddBody 在场景内注册一个物理体。
	AddBody(objID uint64, b *Body)
	// RemoveBody 移除场景内的物理体。
	RemoveBody(objID uint64)
	// ApplyForce 给物理体施加力。
	ApplyForce(objID uint64, f Vec3)
	// SetVelocity 直接设置物理体速度（三分量，带 Y 分量即可做垂直运动）。
	SetVelocity(objID uint64, v Vec3)
	// Collider 返回场景的二维碰撞宽相（水平投影，Y 不参与）：
	//
	//	col := scene.Collider()
	//	// 注意 2D 的 AABB{MinY,MaxY} 承载的是**世界 Z**（见 mmo.bodyAABB）
	//	col.Insert("wall:1", collide.AABB{MinX: 0, MinY: 0, MaxX: 10, MaxY: 2})
	//	col.SetMask("wall:1", collide.GroupWall)
	//
	// 要按**真实高度**注册楼板 / 三层墙体、做立体弹道与视线判定，用 Collider3。
	Collider() *pkgcollide.Grid
	// Collider3 返回场景的三维碰撞宽相（含高度），用于**立体**空间判定：
	//
	//	col := scene.Collider3()
	//	col.Insert("floor2", collide.NewAABB3(center, half)) // center/half 为 geom.Vec3
	//	col.SetMask("floor2", collide.GroupWall)             // Mask 含 GroupWall 才参与遮挡判定
	//	hit, t := col.SweepCCD("bullet:1", from, to)         // 立体弹道避障
	//	wall, t := col.Raycast(eyePos, targetPos)            // 视线判定
	//	ids := col.QuerySphere(collide.Sphere{Center: p, R: 8}) // 三维范围查询（AOE / 拾取）
	Collider3() *pkgcollide.Grid3
	// Neighbors 返回指定半径内的其他对象 id。
	Neighbors(objID uint64, radius float64) []uint64
	// Around 返回指定半径内的全部对象（含自身）。
	Around(objID uint64, radius float64) []uint64
	// Position 返回对象在场景中的坐标。
	Position(objID uint64) (Vec3, bool)
	// Members 返回场景内全部成员 id。
	Members() []uint64
	// Broadcast 向场景全部成员广播一条消息。
	Broadcast(msgID uint32, body []byte)
	// SendTo 向单个对象发送消息。
	SendTo(objID uint64, msgID uint32, body []byte) error
	// OnEvent 注册场景事件处理器（eventType → handler）。
	OnEvent(eventType string, h SceneEventHandlerFacade)
	// SendEvent 同步派发场景事件（并发通道：handler 可能并发执行）。
	SendEvent(ctx context.Context, eventType string, payload any) error
	// SendQueueEvent 串行派发场景事件（同一场景上的事件互斥执行）。
	SendQueueEvent(ctx context.Context, eventType string, payload any) error
	// Sync 在场景串行锁内执行 fn（与 SendQueueEvent 共用同一把锁），
	// 用于 Tick 等驱动协程读写场景共享状态，避免与 handler 并发覆盖。
	Sync(fn func())
	// AttachBeat 挂载多级扫描定时器，驱动场景心跳。
	AttachBeat(b *SceneBeat)
	// MemberKind 返回对象在本场景登记的实体类型；ok=false 表示它不在本场景。
	//
	// 用途：按类型过滤接收者（典型是"推送只给观看者/玩家"，不给怪物与已离场对象）。
	// 不要用"查不到就当玩家"的写法 —— 那会把已离场对象也算进接收者。
	MemberKind(objID uint64) (data.OwnerType, bool)
}

// sceneFacade 门面 SceneFacade 接口的实现：包装 internal 的 *Scene，
// 并把返回的 *Instance 协变包装为门面 InstanceFacade 接口。
type sceneFacade struct{ inner *Scene }

// ID 返回场景 id。
func (s *sceneFacade) ID() uint64 { return s.inner.ID() }

// CreateInstance 创建新实例。
func (s *sceneFacade) CreateInstance(id uint32) (InstanceFacade, error) {
	l, err := s.inner.CreateInstance(id)
	if err != nil {
		return nil, err
	}
	return fromInstance(l), nil
}

// RemoveInstance 删除实例（默认 instance 0 不允许删除）。
func (s *sceneFacade) RemoveInstance(id uint32) error { return s.inner.RemoveInstance(id) }

// Enter 让玩家对象以坐标 pos 进入场景。
func (s *sceneFacade) Enter(objID uint64, pos Vec3) error {
	return s.inner.Enter(objID, pos)
}

// EnterOwnerType 让对象以指定实体类型进入场景默认 instance 0。
func (s *sceneFacade) EnterOwnerType(objID uint64, ownerType data.OwnerType, pos Vec3) error {
	return s.inner.EnterOwnerType(objID, ownerType, pos)
}

// EnterOwnerTypeInstance 让对象以指定实体类型进入指定 instance。
func (s *sceneFacade) EnterOwnerTypeInstance(objID uint64, ownerType data.OwnerType, pos Vec3, instanceID uint32) error {
	return s.inner.EnterOwnerTypeInstance(objID, ownerType, pos, instanceID)
}

// Leave 让对象离开场景。
func (s *sceneFacade) Leave(objID uint64) { s.inner.Leave(objID) }

// Move 移动对象到指定坐标（会触发视野同步）。
func (s *sceneFacade) Move(objID uint64, pos Vec3) { s.inner.Move(objID, pos) }

// MoveBatch 批量移动多个对象，整帧只刷新一次 AOI 与推送一次视野事件。
func (s *sceneFacade) MoveBatch(moves []MoveOp) { s.inner.MoveBatch(moves) }

// BeginBatch 开始手动批量模式。
func (s *sceneFacade) BeginBatch() { s.inner.BeginBatch() }

// EndBatch 结束手动批量模式，统一刷新 AOI 并推送视野事件。
func (s *sceneFacade) EndBatch() { s.inner.EndBatch() }

// SetViewRadius 设置对象视野半径；r<=0 取消观察。
func (s *sceneFacade) SetViewRadius(objID uint64, radius float64) {
	s.inner.SetViewRadius(objID, radius)
}

// Stop 停止场景心跳。
func (s *sceneFacade) Stop() { s.inner.Stop() }

// Tick 手动驱动一次心跳（dt 为距上次间隔）。
func (s *sceneFacade) Tick(dt time.Duration) { s.inner.Tick(dt) }

// EnablePhysics 开启场景物理步进。
func (s *sceneFacade) EnablePhysics() { s.inner.EnablePhysics() }

// DisablePhysics 关闭场景物理步进。
func (s *sceneFacade) DisablePhysics() { s.inner.DisablePhysics() }

// AddBody 在场景内注册一个物理体。
func (s *sceneFacade) AddBody(objID uint64, b *Body) { s.inner.AddBody(objID, b) }

// RemoveBody 移除场景内的物理体。
func (s *sceneFacade) RemoveBody(objID uint64) { s.inner.RemoveBody(objID) }

// ApplyForce 给物理体施加力。
func (s *sceneFacade) ApplyForce(objID uint64, f Vec3) { s.inner.ApplyForce(objID, f) }

// SetVelocity 直接设置物理体速度。
func (s *sceneFacade) SetVelocity(objID uint64, v Vec3) { s.inner.SetVelocity(objID, v) }

// Collider 返回场景的二维碰撞宽相（水平投影）。通过 SceneFacade 接口获取，示例见接口注释。
func (s *sceneFacade) Collider() *pkgcollide.Grid { return s.inner.Collider() }

// Collider3 返回场景的三维碰撞宽相（Y 为高度）。通过 SceneFacade 接口获取，示例见接口注释。
func (s *sceneFacade) Collider3() *pkgcollide.Grid3 { return s.inner.Collider3() }

// Neighbors 返回指定半径内的其他对象 id。
func (s *sceneFacade) Neighbors(objID uint64, radius float64) []uint64 {
	return s.inner.Neighbors(objID, radius)
}

// Around 返回指定半径内的全部对象（含自身）。
func (s *sceneFacade) Around(objID uint64, radius float64) []uint64 {
	return s.inner.Around(objID, radius)
}

// Position 返回对象在场景中的坐标。
func (s *sceneFacade) Position(objID uint64) (Vec3, bool) { return s.inner.Position(objID) }

// MemberKind 返回对象在本场景登记的实体类型；ok=false 表示它不在本场景。
func (s *sceneFacade) MemberKind(objID uint64) (data.OwnerType, bool) {
	return s.inner.MemberKind(objID)
}

// Members 返回场景内全部成员 id。
func (s *sceneFacade) Members() []uint64 { return s.inner.Members() }

// Broadcast 向场景全部成员广播一条消息。
func (s *sceneFacade) Broadcast(msgID uint32, body []byte) { s.inner.Broadcast(msgID, body) }

// SendTo 向单个对象发送消息。
func (s *sceneFacade) SendTo(objID uint64, msgID uint32, body []byte) error {
	return s.inner.SendTo(objID, msgID, body)
}

// OnEvent 注册场景事件处理器（eventType → handler）。
// 门面 SceneEventHandlerFacade 的 s 是 SceneFacade 接口，而 internal 需要具体 *Scene，
// 这里做一次适配：把 internal 回调里的 *Scene 用 fromScene 包成门面接口再交给业务 handler。
func (s *sceneFacade) OnEvent(eventType string, h SceneEventHandlerFacade) {
	if h == nil {
		return
	}
	s.inner.OnEvent(eventType, func(ctx context.Context, inner *Scene, et string, p any) error {
		return h(ctx, fromScene(inner), et, p)
	})
}

// SendEvent 同步派发场景事件（并发通道：handler 可能并发执行）。
func (s *sceneFacade) SendEvent(ctx context.Context, eventType string, payload any) error {
	return s.inner.SendEvent(ctx, eventType, payload)
}

// SendQueueEvent 串行派发场景事件（同一场景上的事件互斥执行）。
func (s *sceneFacade) SendQueueEvent(ctx context.Context, eventType string, payload any) error {
	return s.inner.SendQueueEvent(ctx, eventType, payload)
}

// Sync 在场景串行锁内执行 fn（与 SendQueueEvent 共用同一把锁）。
func (s *sceneFacade) Sync(fn func()) { s.inner.Sync(fn) }

// AttachBeat 挂载多级扫描定时器，驱动场景心跳。
func (s *sceneFacade) AttachBeat(b *SceneBeat) { s.inner.AttachBeat(b) }

// 编译期断言：包装满足门面接口。
var _ SceneFacade = (*sceneFacade)(nil)

// 实例
// InstanceFacade SceneFacade 内的一个隔离实例：独立 AOI 网格 + 物理体 + 实体类型记录。
// 业务侧可见名 = `pkg/domain/mmo.Instance`。
type InstanceFacade interface {
	// ID 返回实例 id。
	ID() uint32
	// Members 返回本实例全部成员 id。
	Members() []uint64
}

// instanceFacade 门面 InstanceFacade 接口的实现：包装 internal 的 *Instance。
type instanceFacade struct{ inner *Instance }

// ID 返回实例 id。
func (i *instanceFacade) ID() uint32 { return i.inner.ID() }

// Members 返回本实例全部成员 id。
func (i *instanceFacade) Members() []uint64 { return i.inner.Members() }

// 编译期断言：包装满足门面接口。
var _ InstanceFacade = (*instanceFacade)(nil)

// 场景事件处理器
// SceneEventHandlerFacade 场景事件处理器：处理 (场景, 事件名) 这一组合。
// s 是门面 SceneFacade 接口（不是 internal 的具体 *Scene），业务可直接书写闭包字面量；
// sceneFacade.OnEvent 内部负责适配到 internal 版本。
// 业务侧可见名 = `pkg/domain/mmo.SceneEventHandler`。
type SceneEventHandlerFacade func(ctx context.Context, s SceneFacade, eventType string, payload any) error

// internal ↔ 门面转换
// FromSceneManager 将 internal 的 *SceneManager 包装为门面 SceneManagerFacade 接口。
func FromSceneManager(m *SceneManager) SceneManagerFacade { return &sceneManagerFacade{inner: m} }

// InternalSceneManager 将门面 SceneManagerFacade 还原为 internal 的 *SceneManager；非底层则 ok=false。
func InternalSceneManager(sm SceneManagerFacade) (*SceneManager, bool) {
	if v, ok := sm.(*sceneManagerFacade); ok {
		return v.inner, true
	}
	return nil, false
}

// FromScene 将 internal 的 *Scene 包装为门面 SceneFacade 接口。
func FromScene(s *Scene) SceneFacade { return &sceneFacade{inner: s} }

// InternalScene 将门面 SceneFacade 还原为 internal 的 *Scene；非底层则 ok=false。
func InternalScene(s SceneFacade) (*Scene, bool) {
	if v, ok := s.(*sceneFacade); ok {
		return v.inner, true
	}
	return nil, false
}

// FromInstance 将 internal 的 *Instance 包装为门面 InstanceFacade 接口。
func FromInstance(l *Instance) InstanceFacade { return &instanceFacade{inner: l} }

// InternalInstance 将门面 InstanceFacade 还原为 internal 的 *Instance；非底层则 ok=false。
func InternalInstance(l InstanceFacade) (*Instance, bool) {
	if v, ok := l.(*instanceFacade); ok {
		return v.inner, true
	}
	return nil, false
}

// fromScene 从 internal 的具体 *Scene 构造门面接口（空指针返回 nil 接口，避免 typed-nil 装箱）。
func fromScene(s *Scene) SceneFacade {
	if s == nil {
		return nil
	}
	return &sceneFacade{inner: s}
}

// fromInstance 从 internal 的具体 *Instance 构造门面接口（空指针返回 nil 接口）。
func fromInstance(l *Instance) InstanceFacade {
	if l == nil {
		return nil
	}
	return &instanceFacade{inner: l}
}

// fromManager 从 internal 的具体 *SceneManager 构造门面接口（空指针返回 nil 接口）。
func fromManager(m *SceneManager) SceneManagerFacade {
	if m == nil {
		return nil
	}
	return &sceneManagerFacade{inner: m}
}

// 门面接口与内部类型的能力面对齐（防止两侧方法集漂移）。
var (
	// 门面 SceneFacade 满足「观看者计算」所需的最小能力面。
	_ ViewerScene = SceneFacade(nil)
	// 内部 *Scene 同样满足（门面与真身都能直接喂给 PlayerViewers）。
	_ ViewerScene = (*Scene)(nil)
)

// 工厂与构造器
// WithStoreFacade 注入数据存储（用于玩家对象加载/保存）。
// 非引擎 data.Store 实现（含装箱 nil）会被拒收并留痕：直接透传会让内部拿到 nil store，
// 后续加载/落库静默失效或空指针，且无任何线索。
//
// 业务侧可见名 = `pkg/domain/mmo.WithStore`（门面转发；签名仍是门面 data.Store）。
func WithStoreFacade(store pkgdata.Store) Option {
	is, ok := data.InternalStore(store)
	if !ok || is == nil {
		logger.Warnf("mmo.WithStore: store is not an engine data.Store impl; ignored (persistence disabled)")
		return WithStore(nil)
	}
	return WithStore(is)
}

// NewSceneManagerFacade 创建全局场景管理器，并以门面接口返回。
// 业务侧可见名 = `pkg/domain/mmo.NewSceneManager`。
func NewSceneManagerFacade(opts ...Option) SceneManagerFacade {
	return fromManager(NewSceneManager(opts...))
}

// NewMoverFacade 以初始位置与基础移动速度构造运动体（七态运动：走/跑/跳/落/爬/飞/游）。
// 业务侧可见名 = `pkg/domain/mmo.NewMover`。
func NewMoverFacade(pos geom.Vec3, speed float64) movpkg.Mover {
	return imov.NewMover(pos, speed)
}

// NewZone 以名称与多边形顶点构造触发区（进入/离开事件 + 减速/每秒伤害数据）。
// 业务侧可见名 = `pkg/domain/mmo.NewZone`。
func NewZone(name string, poly []Vec2) *pkgcollide.Zone {
	p := make([]pkgcollide.Vec2, len(poly))
	for i, v := range poly {
		// 直接转换而非重写字面量：Vec2 与 pkgcollide.Vec2 是同一底层类型
		//（门面的 Vec2 就是 collide.Vec2 的别名），重写字面量属静态检查
		// 命中的冗余写法（staticcheck S1016）。
		p[i] = pkgcollide.Vec2(v)
	}
	return pkgcollide.NewZone(name, p)
}

// NewCalculator 创建伤害计算器（可插拔公式，Apply 自动扣血并返回结果）。
// 业务侧可见名 = `pkg/domain/mmo.NewCalculator`。
func NewCalculator() combatpkg.Calculator {
	return icombat.New()
}

// WireEntitySyncFacade 把视野同步桥接到实体变更广播：订阅 viewSubject 维护反向索引，
// 并把实体变更（已剔除 ServerOnly）只推给视野内的玩家。sub 注入订阅能力（NATS Client 或测试替身）。
//
// 业务侧可见名 = `pkg/domain/mmo.WireEntitySync`；`pub` 仍是门面 `data.Publisher`
// （内部实现要的就是这个接口面，转换只是命名空间差异）。
func WireEntitySyncFacade(viewSubject string, entityAcc EntityAccessor, sub Subscriber, pub pkgdata.Publisher) error {
	return WireEntitySync(viewSubject, entityAcc, sub, pub)
}

// WireEntitySyncHandleFacade 与 WireEntitySyncFacade 等价，但返回 *EntitySync 句柄。
// 调用方停止时必须 Close() 它（幂等），否则两条订阅会一直挂着、回调在持有者停止后继续被派发。
// 业务侧可见名 = `pkg/domain/mmo.WireEntitySyncHandle`。
func WireEntitySyncHandleFacade(viewSubject string, entityAcc EntityAccessor, sub Subscriber, pub pkgdata.Publisher) (*EntitySync, error) {
	return WireEntitySyncHandle(viewSubject, entityAcc, sub, pub)
}

// 同步（sync）
// NewSyncManagerFacade 根据模式创建同步管理器（以门面接口返回）。
// 业务侧可见名 = `pkg/domain/mmo.NewSyncManager`。
func NewSyncManagerFacade(mode syncpkg.SyncMode) syncpkg.SyncManager {
	return isync.NewSyncManager(mode)
}

// 场景管理器操作
// CreateScene 创建并注册一个场景。
func CreateScene(sm SceneManagerFacade, id uint64, name string) SceneFacade {
	return sm.CreateScene(id, name)
}

// GetScene 按场景 id 获取场景。
func GetScene(sm SceneManagerFacade, id uint64) (SceneFacade, bool) { return sm.GetScene(id) }

// TransferRemote 跨机器迁移：同机走本地 Transfer，异机发 NATS 指令对端 EnterOwnerType + 本机 Leave。
// 玩家三元键持久数据落在共享 Store，随指令无需搬运。
// pos 为三维落点：多层地形 / 飞行玩法必须带高度，否则对象跨图后会落到 y=0 的地面。
func TransferRemote(sm SceneManagerFacade, dstSceneID, objID uint64, pos Vec3) error {
	return sm.TransferRemote(dstSceneID, objID, pos)
}

// DestroyScene 销毁场景并踢出全部成员。
func DestroyScene(sm SceneManagerFacade, id uint64) { sm.DestroyScene(id) }

// 场景操作
// Enter 让玩家对象以坐标 pos 进入场景。
func Enter(s SceneFacade, objID uint64, pos Vec3) error { return s.Enter(objID, pos) }

// EnterOwnerType 让对象以指定 data 实体类型进入场景默认 instance 0。
func EnterOwnerType(s SceneFacade, objID uint64, ownerType data.OwnerType, pos Vec3) error {
	return s.EnterOwnerType(objID, ownerType, pos)
}

// EnterOwnerTypeInstance 让对象进入指定 instance。
func EnterOwnerTypeInstance(s SceneFacade, objID uint64, ownerType data.OwnerType, pos Vec3, instanceID uint32) error {
	return s.EnterOwnerTypeInstance(objID, ownerType, pos, instanceID)
}

// CreateInstance 创建新实例。
func CreateInstance(s SceneFacade, id uint32) (InstanceFacade, error) { return s.CreateInstance(id) }

// RemoveInstance 删除实例。
func RemoveInstance(s SceneFacade, id uint32) error { return s.RemoveInstance(id) }

// Leave 让对象离开场景。
func Leave(s SceneFacade, objID uint64) { s.Leave(objID) }

// Move 移动对象到指定坐标（会触发视野同步）。
func Move(s SceneFacade, objID uint64, pos Vec3) { s.Move(objID, pos) }

// MoveBatch 批量移动多个对象，整帧只刷新一次 AOI 与推送一次视野事件。
func MoveBatch(s SceneFacade, moves []MoveOp) { s.MoveBatch(moves) }

// BeginBatch 开始手动批量模式；之后 Move/EnterOwnerType/Leave/SetViewRadius 会累积到同一 batch，
// 必须配对调用 EndBatch 统一刷新。
func BeginBatch(s SceneFacade) { s.BeginBatch() }

// EndBatch 结束手动批量模式，统一刷新 AOI 并推送视野事件。
func EndBatch(s SceneFacade) { s.EndBatch() }

// SetViewRadius 设置对象视野半径；r<=0 取消观察。
func SetViewRadius(s SceneFacade, objID uint64, radius float64) { s.SetViewRadius(objID, radius) }

// Stop 停止场景心跳。
func Stop(s SceneFacade) { s.Stop() }

// Tick 手动驱动一次心跳（dt 为距上次间隔）。
func Tick(s SceneFacade, dt time.Duration) { s.Tick(dt) }

// EnablePhysics 开启场景物理步进。
func EnablePhysics(s SceneFacade) { s.EnablePhysics() }

// DisablePhysics 关闭场景物理步进。
func DisablePhysics(s SceneFacade) { s.DisablePhysics() }

// AddBody 在场景内注册一个物理体。
func AddBody(s SceneFacade, id uint64, body *Body) { s.AddBody(id, body) }

// RemoveBody 移除场景内的物理体。
func RemoveBody(s SceneFacade, id uint64) { s.RemoveBody(id) }

// ApplyForce 给物理体施加力。
func ApplyForce(s SceneFacade, id uint64, f Vec3) { s.ApplyForce(id, f) }

// SetVelocity 直接设置物理体速度。
func SetVelocity(s SceneFacade, id uint64, v Vec3) { s.SetVelocity(id, v) }

// Neighbors 返回指定半径内的其他对象 id。
func Neighbors(s SceneFacade, objID uint64, radius float64) []uint64 {
	return s.Neighbors(objID, radius)
}

// Around 返回指定半径内的全部对象（含自身）。
func Around(s SceneFacade, objID uint64, radius float64) []uint64 {
	return s.Around(objID, radius)
}

// Position 返回对象在场景中的坐标。
func Position(s SceneFacade, objID uint64) (Vec3, bool) { return s.Position(objID) }

// Members 返回场景内全部成员 id。
func Members(s SceneFacade) []uint64 { return s.Members() }

// Broadcast 向场景全部成员广播一条消息。
func Broadcast(s SceneFacade, msgID uint32, body []byte) { s.Broadcast(msgID, body) }

// SendTo 向单个对象发送消息。
func SendTo(s SceneFacade, objID uint64, msgID uint32, body []byte) error {
	return s.SendTo(objID, msgID, body)
}

// Run 阻塞启动 MMO 场景管理器（启动所有场景心跳），直到 ctx 取消。
func Run(ctx context.Context, sm SceneManagerFacade) error { return sm.Run(ctx) }

// 行为树（btree）工厂
// NewBlackboard 创建共享黑板实例。
func NewBlackboard() pkgbtree.Blackboard { return ibtree.NewBlackboard() }

// NewSequence 构造序列节点。
func NewSequence(children ...pkgbtree.Node) pkgbtree.Sequence {
	return ibtree.NewSequence(children...)
}

// NewSelector 构造选择器节点。
func NewSelector(children ...pkgbtree.Node) pkgbtree.Selector {
	return ibtree.NewSelector(children...)
}

// NewParallel 构造并行节点。
func NewParallel(policy pkgbtree.ParallelPolicy, children ...pkgbtree.Node) pkgbtree.Parallel {
	return ibtree.NewParallel(policy, children...)
}

// NewInverter 构造取反装饰节点。
func NewInverter(child pkgbtree.Node) pkgbtree.Inverter { return ibtree.NewInverter(child) }

// NewRepeater 构造重复装饰节点。
func NewRepeater(max int, child pkgbtree.Node) pkgbtree.Repeater {
	return ibtree.NewRepeater(max, child)
}

// NewUntilFailure 构造"直到失败"装饰节点。
func NewUntilFailure(child pkgbtree.Node) pkgbtree.UntilFailure {
	return ibtree.NewUntilFailure(child)
}

// NewLimiter 构造频次限制装饰节点。
//
// ⚠️ 时间源：本节点读黑板键 "now"（逻辑时刻），该键由 `Tree.Tick` 保证每帧存在且推进，
// 归属按**首帧**判定：首次 Tick 前已注入（b.Set("now", mmo.LogicalTime(逻辑秒))，
// 或直接写逻辑秒数值 float64/int64）⇒ 归驱动方（Tree 只读）；未注入 ⇒ 归 Tree，
// 由它按 dt 自累加推进 —— 与 Timeout 同口径。
// 只有直接 tick 本节点（不经 Tree.Tick）才回落墙钟 time.Now()，并留一条降频 Warn。
//
// ⚠️ 节点状态在节点上（计数 + 窗口）：**一棵树只服务一个 Agent**，多 Agent 共用会互相串扰。
func NewLimiter(limit int, window time.Duration, child pkgbtree.Node) pkgbtree.Limiter {
	return ibtree.NewLimiter(limit, window, child)
}

// NewCooldown 构造冷却装饰节点。时间源与「一 Agent 一棵树」的要求同 NewLimiter。
func NewCooldown(d time.Duration, child pkgbtree.Node) pkgbtree.Cooldown {
	return ibtree.NewCooldown(d, child)
}

// NewTimeout 构造超时装饰节点。
func NewTimeout(d time.Duration, child pkgbtree.Node) pkgbtree.Timeout {
	return ibtree.NewTimeout(d, child)
}

// NewCondition 构造条件叶子节点。
func NewCondition(fn func(b pkgbtree.Blackboard) bool) pkgbtree.Condition {
	return ibtree.NewCondition(fn)
}

// NewAction 构造行为叶子节点。
func NewAction(fn func(b pkgbtree.Blackboard) pkgbtree.Status) pkgbtree.Action {
	return ibtree.NewAction(fn)
}

// NewActionFn 构造执行即成功的行为叶子节点。
func NewActionFn(fn func(b pkgbtree.Blackboard)) pkgbtree.ActionFn {
	return ibtree.NewActionFn(fn)
}

// NewTree 以根节点构造一棵行为树。
func NewTree(root pkgbtree.Node) pkgbtree.Tree { return ibtree.NewTree(root) }

// LogicalTime 把「逻辑秒」（dt 累加值，如 MobManager 的内部 clock）转成可注入黑板的逻辑时刻：
//
//	b.Set("now", mmo.LogicalTime(逻辑秒))
//
// 基准取 Unix 纪元，**只用于同一逻辑时钟域内的先后比较**，不要与墙钟混用。
// 之所以必须由门面导出：内部实现位于 internal/（业务不可 import），
// 业务要用同一套逻辑时钟就只能走门面。
var LogicalTime = ibtree.LogicalTime
