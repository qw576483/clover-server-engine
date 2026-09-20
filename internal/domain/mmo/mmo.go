// Package mmo 是 MMO 场景的统一组合层实现。
//
// 架构：
//
//	SceneManager          全局场景管理器
//	 ├─ Scene("map")      一张地图（共享物理碰撞）
//	 │    ├─ Instance 0    默认实例（公共区，所有玩家互相可见）
//	 │    ├─ Instance 1    个人任务实例（solo，独立 AOI）
//	 │    └─ Instance N    副本实例（组队可见）
//	 └─ Scene("dungeon")
//
// Instance 是隔离的最小单位：同一 Instance 内的实体通过 AOI 互相可见；
// 跨 Instance 互不可见，但共享 Scene 的物理碰撞（墙壁/障碍物）。
package mmo

import (
	"errors"
	"time"

	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/domain/data/accessor"
	"clover-server-engine/internal/domain/object"
	"clover-server-engine/internal/transport/pubsub"
	"clover-server-engine/pkg/domain/mmo/collide"
	"clover-server-engine/pkg/shared/geom"
)

// 错误定义。
var (
	ErrSceneNotFound    = errors.New("mmo: scene not found")
	ErrNoPublisher      = errors.New("mmo: publisher not configured")
	ErrNoRoute          = errors.New("mmo: route store not configured")
	ErrInstanceNotFound = errors.New("mmo: instance not found")
	ErrExists           = errors.New("mmo: object already in scene")
)

// DefaultInstance 是不关心分实例时使用的默认实例 id（EnterOwnerType 不带实例参数时落到该实例）。
const DefaultInstance uint32 = 0

// 类型透传：从 pkg 层桥接内部类型。
type (
	Vec2 = collide.Vec2
	AABB = collide.AABB
)

// Vec3 三维坐标（X=东西, Y=高度, Z=南北）。
type Vec3 = geom.Vec3

// Publisher 发布接口（统一定义在 transport/pubsub）。
type Publisher = pubsub.Publisher

// options 构造选项。
type options struct {
	store       *data.Store
	pub         Publisher
	entityAcc   *accessor.Accessor
	objMgr      *object.Manager
	tickRate    time.Duration
	cellSize    float64
	viewSubject string
	nodeID      uint64
	route       SceneRoute
	remoteSub   Subscriber
	maxBodies   int
}

// Option 场景管理器构造选项。
type Option func(*options)

func WithStore(s *data.Store) Option                 { return func(o *options) { o.store = s } }
func WithPublisher(p Publisher) Option               { return func(o *options) { o.pub = p } }
func WithEntityAccessor(e *accessor.Accessor) Option { return func(o *options) { o.entityAcc = e } }

// WithObjectManager 注入对象管理器。强烈建议传 Game 的那个（g.ObjectManager()），
// 使场景内对象与 Game 门面共用同一张对象表；不传则 SceneManager 自建一个，
// 两张表互不可见（对场景内对象调用 g.SendQueueEventToGObject 会拿到 ErrObjectNotFound）。
func WithObjectManager(m *object.Manager) Option {
	return func(o *options) { o.objMgr = m }
}

// WithTickRate 配置场景 tick 间隔：主循环应以此为驱动周期（Scene.Tick(dt) 的 dt 基准）。
// 此前该值写进 options 后全包无任何读取点（选项静默无效），现由 SceneManager.TickRate 暴露。
func WithTickRate(d time.Duration) Option { return func(o *options) { o.tickRate = d } }
func WithCellSize(cell float64) Option    { return func(o *options) { o.cellSize = cell } }
func WithViewSubject(subject string) Option {
	return func(o *options) { o.viewSubject = subject }
}
func WithNodeID(node uint64) Option { return func(o *options) { o.nodeID = node } }

// WithMaxBodies 配置「单个 Instance 内物理体数量上限」（0 = 不限）。
// 超出上限时 AddBody 拒绝挂载并打日志——没有这个闸门，物理体表会被无上限地写大。
func WithMaxBodies(n int) Option { return func(o *options) { o.maxBodies = n } }

// TickRate 返回配置的 tick 间隔（见 WithTickRate）。
func (sm *SceneManager) TickRate() time.Duration { return sm.opts.tickRate }

// MaxBodies 返回配置的单实例物理体上限（见 WithMaxBodies；0 表示不限）。
func (sm *SceneManager) MaxBodies() int { return sm.opts.maxBodies }
