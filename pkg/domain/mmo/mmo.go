// Package mmo 是 MMO 场景的统一组合层（公开门面）。
//
// 业务 / 框架层统一从本包引用 MMO 能力：
//
//	import "github.com/qw576483/clover-server-engine/pkg/domain/mmo"
//
//	sm := mmo.NewSceneManager(mmo.WithTickRate(50*time.Millisecond), mmo.WithPublisher(pub))
//	r := mmo.CreateScene(sm, 1, "map-1")
//	mmo.SetViewRadius(r, 1001, 96)
//
//	mmo.Enter(r, 1001, mmo.Vec3{X: 100, Z: 200})
//	mmo.Move(r, 1001, mmo.Vec3{X: 105, Z: 205})
//	mmo.Broadcast(r, 2001, []byte("hello scene"))
//
// 本包是**门面**（见 `结构规则.md` §5.1）：真身全部在 `internal/domain/mmo`
// —— 主门面 `facade.go`（`SceneManagerFacade` / `SceneFacade` / `InstanceFacade` /
// `SceneEventHandlerFacade` + 包装实现 + 包级操作函数 + 工厂），
// 以及按能力拆分的 `{spatial/aoi,spatial/pathfinding,gameplay/buff,gameplay/skill,gameplay/mob}/facade.go`
// （aoi / 寻路 / buff / skill 的对口包装）。
//
// 这里只有三类东西：**类型别名**（`type X = internal.X`）、**声明转发**
// （`var F = internal.F` / `const K = internal.K`）、以及**接口与常量的直接来源**
// （`pkg/domain/mmo/<子包>`，它们自身不 import internal，是自包含的类型真身包）。
// 不含任何实现体，改真身只改 internal。
//
// 门面接口在 business 侧就是不后缀的名字：`mmo.SceneManager` / `mmo.Scene` / `mmo.Instance` /
// `mmo.SceneEventHandler`（F12 一跳进 internal 即可看到完整方法集与注释）。
package mmo

import (
	im "github.com/qw576483/clover-server-engine/internal/domain/mmo"
	ibuff "github.com/qw576483/clover-server-engine/internal/domain/mmo/gameplay/buff"
	imob "github.com/qw576483/clover-server-engine/internal/domain/mmo/gameplay/mob"
	iskill "github.com/qw576483/clover-server-engine/internal/domain/mmo/gameplay/skill"
	ispataoi "github.com/qw576483/clover-server-engine/internal/domain/mmo/spatial/aoi"
	ispf "github.com/qw576483/clover-server-engine/internal/domain/mmo/spatial/pathfinding"
	isync "github.com/qw576483/clover-server-engine/internal/domain/mmo/sync/core"
	buffpkg "github.com/qw576483/clover-server-engine/pkg/domain/mmo/buff"
	pkgcollide "github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	combatpkg "github.com/qw576483/clover-server-engine/pkg/domain/mmo/combat"
	mobpkg "github.com/qw576483/clover-server-engine/pkg/domain/mmo/mob"
	movpkg "github.com/qw576483/clover-server-engine/pkg/domain/mmo/mover"
	skillpkg "github.com/qw576483/clover-server-engine/pkg/domain/mmo/skill"
)

// 错误透传。
var (
	ErrSceneNotFound = im.ErrSceneNotFound
	ErrNoPublisher   = im.ErrNoPublisher
	ErrNoRoute       = im.ErrNoRoute

	// 场景事件相关错误。
	ErrNoSceneEventHandler = im.ErrNoSceneEventHandler
	ErrEmptySceneEventType = im.ErrEmptySceneEventType
)

// 场景三级结构（接口真身见 internal/domain/mmo/facade.go）。
type (
	// SceneManager 全局场景管理器：管理所有 Scene 的生命周期与跨 Scene 传输。
	SceneManager = im.SceneManagerFacade
	// Scene SceneManager 内的一张地图：多个隔离 Instance + 共享物理碰撞 + 视野同步。
	Scene = im.SceneFacade
	// Instance Scene 内的一个隔离实例：独立 AOI 网格 + 物理体 + 实体类型记录。
	Instance = im.InstanceFacade
	// SceneEventHandler 场景事件处理器：处理 (场景, 事件名) 这一组合。
	// s 是门面 Scene 接口（不是 internal 的具体 *Scene），业务可直接书写闭包字面量。
	SceneEventHandler = im.SceneEventHandlerFacade
	// EntityAccessor 提供实体快照与变更通知 subject（与 accessor.Accessor 对齐）。
	EntityAccessor = im.EntityAccessor
)

// 类型透传（纯值 / 枚举 / 已公开别名）。
type (
	// Body 轻量物理体：Position/Velocity/Force 均为三维坐标（Y 为高度）。
	//
	// 与 mover 的分工：Body 是无重力的纯积分体（施力/设速），适合投射物、击退；
	// 需要重力、跳跃与地形落地的角色用 mover。
	Body = im.Body
	// MoveOp 对外暴露的移动操作（对象 id + 目标坐标）。
	MoveOp = im.MoveOp
	// Vec2 2D 平面上的点 / 向量。
	Vec2 = im.Vec2
	// Vec3 三维坐标（X=东西, Y=高度, Z=南北）。
	Vec3 = im.Vec3
	// AABB 轴对齐包围盒。
	AABB = im.AABB
	// Option 场景管理器构造选项。
	Option = im.Option
	// Publisher 发布接口（统一定义在 transport/pubsub）。
	Publisher = im.Publisher
	// Subscriber 是 WireEntitySync 注入的最小订阅能力（与 NATS / 测试替身解耦）。
	Subscriber = im.Subscriber
	// Unsubscriber 是 Subscriber 的可选反注册能力（实现它才能单独退回自己的订阅）。
	Unsubscriber = im.Unsubscriber
	// EntitySync 是实体同步管线句柄（Close 反注册其订阅）。
	EntitySync = im.EntitySync
	// SceneRoute 场景→节点路由表的能力面（跨机对象迁移寻址用；真身见 internal/domain/mmo）。
	SceneRoute = im.SceneRoute

	// MobManager 怪物/NPC 管理器接口（挂到任意 *Scene，行为树 AI 骨架）。
	MobManager = mobpkg.MobManager
	// Mob 单只怪物运行时实例（数据载体）。
	Mob = mobpkg.Mob
	// SpawnConfig 刷怪点配置。
	SpawnConfig = mobpkg.SpawnConfig
	// MobScene mob 管理所需的最小场景接口。
	MobScene = mobpkg.MobScene

	// Mover 运动 / 转向七态体（对应 CGameMotion）。
	Mover = movpkg.Mover
	// Zone 多边形触发区（对应 CSceneArea）：Boss 领域 / 减速区 / 持续伤害区。
	Zone = pkgcollide.Zone
	// SceneBeat 场景多级扫描定时器（对应 SceneBeat）。
	SceneBeat = im.SceneBeat
	// BuffContainer 状态效果容器（对应 Buff 运行时）。
	BuffContainer = buffpkg.Container
	// BuffDef Buff 定义（来自配置 buff_*.json 的运行时表达）。
	BuffDef = buffpkg.Def
	// BuffModifier Buff 对单个属性的修改。
	BuffModifier = buffpkg.Modifier
	// BuffFlag 状态标志位（可叠加 OR）。
	BuffFlag = buffpkg.Flag
	// BuffInstance 运行中的 Buff 实例。
	BuffInstance = buffpkg.Instance
	// BuffRemoveCond Buff 移除条件。
	BuffRemoveCond = buffpkg.RemoveCond
	// CooldownManager 技能体系冷却管理器（对应 CoolDownModule）。
	CooldownManager = skillpkg.CooldownManager
	// SkillSet 技能集（施放流程入口）。
	SkillSet = skillpkg.Set
	// SkillDef 技能定义。
	SkillDef = skillpkg.Def
	// SkillEffectRef 技能生效引用。
	SkillEffectRef = skillpkg.EffectRef
	// SkillTargetType 技能目标类型。
	SkillTargetType = skillpkg.TargetType
	// Calculator 伤害计算器接口（对应战斗公式运行时）。
	Calculator = combatpkg.Calculator
	// CombatResult 一次伤害结算结果快照。
	CombatResult = combatpkg.Result
	// Formula 基础伤害公式。
	Formula = combatpkg.Formula
)

// 运动八态状态（含 Idle）。
const (
	// StateIdle 静止。
	StateIdle = movpkg.StateIdle
	// StateWalk 步行。
	StateWalk = movpkg.StateWalk
	// StateRun 奔跑。
	StateRun = movpkg.StateRun
	// StateJump 起跳上升。
	StateJump = movpkg.StateJump
	// StateFall 下落。
	StateFall = movpkg.StateFall
	// StateClimb 攀爬。
	StateClimb = movpkg.StateClimb
	// StateFly 飞行。
	StateFly = movpkg.StateFly
	// StateSwim 游泳。
	StateSwim = movpkg.StateSwim
)

// 场景扫描档位（实时/快/慢三档）。
const (
	// TierRealtime 实时档，每帧扫描。
	TierRealtime = im.TierRealtime
	// TierFast 快档，按 fast 周期扫描。
	TierFast = im.TierFast
	// TierSlow 慢档，按 slow 周期扫描。
	TierSlow = im.TierSlow
)

// Buff 状态标志位。
const (
	// FlagStun 眩晕。
	FlagStun = buffpkg.FlagStun
	// FlagSilence 沉默。
	FlagSilence = buffpkg.FlagSilence
	// FlagRoot 定身。
	FlagRoot = buffpkg.FlagRoot
	// FlagInvincible 无敌。
	FlagInvincible = buffpkg.FlagInvincible
	// FlagCantBeTarget 不可被选中。
	FlagCantBeTarget = buffpkg.FlagCantBeTarget
)

// Buff 移除条件。
const (
	// RemoveNone 不自动移除。
	RemoveNone = buffpkg.RemoveNone
	// RemoveOnExpire 到期移除。
	RemoveOnExpire = buffpkg.RemoveOnExpire
	// RemoveOnDeath 死亡移除。
	RemoveOnDeath = buffpkg.RemoveOnDeath
	// RemoveDispel 被驱散移除。
	RemoveDispel = buffpkg.RemoveDispel
)

// 技能目标类型。
const (
	// TargetEnemy 敌方目标。
	TargetEnemy = skillpkg.TargetEnemy
	// TargetAlly 友方目标。
	TargetAlly = skillpkg.TargetAlly
	// TargetSelf 自身。
	TargetSelf = skillpkg.TargetSelf
	// TargetPoint 指定点位。
	TargetPoint = skillpkg.TargetPoint
)

// 公共属性 ID 常量（供 combat / buff / skill 复用）。
var (
	// AttrHP 当前生命。
	AttrHP = combatpkg.AttrHP
	// AttrMaxHP 最大生命。
	AttrMaxHP = combatpkg.AttrMaxHP
	// AttrATK 攻击。
	AttrATK = combatpkg.AttrATK
	// AttrDEF 防御。
	AttrDEF = combatpkg.AttrDEF
	// AttrSpeed 速度/移动。
	AttrSpeed = combatpkg.AttrSpeed
	// AttrCrit 暴击率。
	AttrCrit = combatpkg.AttrCrit
	// AttrDistance 施法者到目标的距离（世界单位），供技能范围校验读取。
	AttrDistance = combatpkg.AttrDistance
	// AttrCamp 阵营标识（同值 = 同阵营），供技能目标合法性校验读取。
	AttrCamp = combatpkg.AttrCamp
)

// 构造选项（真身见 internal/domain/mmo/mmo.go 与 cluster.go）。
var (
	// WithStore 注入数据存储（用于玩家对象加载/保存）。
	// 非引擎 data.Store 实现（含装箱 nil）会被拒收并留痕。
	WithStore = im.WithStoreFacade
	// WithPublisher 注入消息发布器。
	WithPublisher = im.WithPublisher
	// WithObjectManager 注入对象管理器。强烈建议传 g.ObjectManager()，使场景内对象与
	// Game 门面共用同一张对象表；不传则 SceneManager 自建一个，两张表互不可见。
	WithObjectManager = im.WithObjectManager
	// WithTickRate 设置默认心跳频率（如 20fps=50ms, 60fps≈16ms）。
	WithTickRate = im.WithTickRate
	// WithCellSize 设置 AOI 格子边长。
	WithCellSize = im.WithCellSize
	// WithViewSubject 设置视野同步使用的 NATS subject。
	WithViewSubject = im.WithViewSubject
	// WithClusterRoute 开启「跨机对象迁移」：注入 scene→node 路由表与本节点 ID。
	//
	//	sm := mmo.NewSceneManager(
	//	    mmo.WithStore(store), mmo.WithPublisher(pub),
	//	    mmo.WithClusterRoute(g.SceneRoute(), g.NodeID()),
	//	    mmo.WithRemoteTransferSubscriber(g.SceneSubscriber()),
	//	)
	//
	// 注入后：CreateScene / DestroyScene 自动登记 / 注销路由，Run 期间自动续期，
	// TransferRemote 会先查路由定位目标节点再定向投递。
	// 不注入时 TransferRemote 返回 ErrNoRoute —— 单机部署不需要它。
	WithClusterRoute = im.WithClusterRoute
	// WithRemoteTransferSubscriber 注入订阅能力，用于接收跨机迁移指令（接收端接线）。
	// 必须与 WithClusterRoute 成对使用，否则不开启接收端。
	WithRemoteTransferSubscriber = im.WithRemoteTransferSubscriber
)

// 工厂与构造器。
var (
	// NewSceneManager 创建全局场景管理器。
	NewSceneManager = im.NewSceneManagerFacade
	// NewMobManager 以给定场景（通常为 Scene）构造怪物/NPC 管理器。
	// 内部用 btree 行为树驱动每只怪的「追击 / 攻击 / 巡逻」决策。
	NewMobManager = imob.NewMobManagerFacade
	// NewMover 以初始位置与基础移动速度构造运动体（七态运动：走/跑/跳/落/爬/飞/游）。
	NewMover = im.NewMoverFacade
	// NewZone 以名称与多边形顶点构造触发区（进入/离开事件 + 减速/每秒伤害数据）。
	NewZone = im.NewZone
	// NewSceneBeat 构造场景多级扫描定时器（realtime 每帧，fast/slow 按周期触发）。
	NewSceneBeat = im.NewSceneBeat
	// NewBuffContainer 以属性集构造 Buff 容器（Apply/Remove/Tick/标志位/叠加）。
	NewBuffContainer = ibuff.NewContainerFacade
	// NewSkillCooldown 创建技能冷却管理器。
	NewSkillCooldown = iskill.NewCooldownFacade
	// NewSkillSet 以冷却管理器构造技能集（CanCast/Cast 串联 combat 与 buff）。
	NewSkillSet = iskill.NewSkillSetFacade
	// NewCalculator 创建伤害计算器（可插拔公式，Apply 自动扣血并返回结果）。
	NewCalculator = im.NewCalculator
	// NewGrid 创建 AOI 网格，cellSize 为格子边长。
	//
	// 视野按**三维球体**判定（X/Y/Z 全参与距离）；2D 俯视玩法把 Y 固定为 0 即可。
	NewGrid = ispataoi.NewGridFacade
	// NewVisualSystem 以格子边长与默认视觉半径构造双向可见性系统。
	// 底层网格与 NewGrid 同一套空间模型（三维球体判定）。
	NewVisualSystem = ispataoi.NewVisualSystemFacade
	// NewSyncManager 根据模式创建同步管理器。
	NewSyncManager = im.NewSyncManagerFacade
	// NewInterpolation 创建插值策略。
	NewInterpolation = isync.NewInterpolation
	// NewExtrapolation 创建外推策略。
	NewExtrapolation = isync.NewExtrapolation
	// NewPrediction 创建客户端预测策略。
	NewPrediction = isync.NewPrediction
)

// internal ↔ 门面转换。
var (
	// FromSceneManager 将 internal 的 *SceneManager 包装为门面 SceneManager 接口。
	FromSceneManager = im.FromSceneManager
	// InternalSceneManager 将门面 SceneManager 接口还原为 internal 的 *SceneManager；非底层则 ok=false。
	InternalSceneManager = im.InternalSceneManager
	// FromScene 将 internal 的 *Scene 包装为门面 Scene 接口。
	FromScene = im.FromScene
	// InternalScene 将门面 Scene 接口还原为 internal 的 *Scene；非底层则 ok=false。
	InternalScene = im.InternalScene
	// FromInstance 将 internal 的 *Instance 包装为门面 Instance 接口。
	FromInstance = im.FromInstance
	// InternalInstance 将门面 Instance 接口还原为 internal 的 *Instance；非底层则 ok=false。
	InternalInstance = im.InternalInstance
	// FromMobManager 将 internal 的 *MobManager 包装为门面 MobManager。
	FromMobManager = imob.FromMobManagerFacade
	// InternalMobManager 将门面 MobManager 还原为 internal 的 *MobManager；非底层则 ok=false。
	InternalMobManager = imob.InternalMobManagerFacade
	// FromBuffContainer 将 internal 的 *Container 包装为门面 Container；nil 返回 nil。
	FromBuffContainer = ibuff.FromContainerFacade
	// InternalBuffContainer 将门面 Container 还原为 internal 的 *Container；非底层则 ok=false。
	InternalBuffContainer = ibuff.InternalContainerFacade
	// InternalCooldownManager 将门面 CooldownManager 还原为 internal 的 *CooldownManager；非底层则 ok=false。
	InternalCooldownManager = iskill.InternalCooldownManagerFacade
)

// 场景管理器操作。
var (
	// CreateScene 创建并注册一个场景。
	CreateScene = im.CreateScene
	// GetScene 按场景 id 获取场景。
	GetScene = im.GetScene
	// TransferRemote 跨机器迁移：同机走本地 Transfer，异机发 NATS 指令对端 EnterOwnerType + 本机 Leave。
	// 玩家三元键持久数据落在共享 Store，随指令无需搬运。
	// pos 为三维落点：多层地形 / 飞行玩法必须带高度，否则对象跨图后会落到 y=0 的地面。
	TransferRemote = im.TransferRemote
	// DestroyScene 销毁场景并踢出全部成员。
	DestroyScene = im.DestroyScene
	// Run 阻塞启动 MMO 场景管理器（启动所有场景心跳），直到 ctx 取消。
	Run = im.Run
	// WireEntitySync 把视野同步桥接到实体变更广播：订阅 viewSubject 维护反向索引，
	// 并把实体变更（已剔除 ServerOnly）只推给视野内的玩家。sub 注入订阅能力（NATS Client 或测试替身）。
	WireEntitySync = im.WireEntitySyncFacade
	// WireEntitySyncHandle 与 WireEntitySync 等价，但返回 *EntitySync 句柄。
	// 调用方停止时必须 Close() 它（幂等），否则两条订阅会一直挂着、回调在持有者停止后继续被派发。
	WireEntitySyncHandle = im.WireEntitySyncHandleFacade
)

// 场景操作。
var (
	// Enter 让玩家对象以坐标 pos 进入场景。
	Enter = im.Enter
	// EnterOwnerType 让对象以指定 data 实体类型进入场景默认 instance 0。
	EnterOwnerType = im.EnterOwnerType
	// EnterOwnerTypeInstance 让对象进入指定 instance。
	EnterOwnerTypeInstance = im.EnterOwnerTypeInstance
	// CreateInstance 创建新实例。
	CreateInstance = im.CreateInstance
	// RemoveInstance 删除实例。
	RemoveInstance = im.RemoveInstance
	// Leave 让对象离开场景。
	Leave = im.Leave
	// Move 移动对象到指定坐标（会触发视野同步）。
	Move = im.Move
	// MoveBatch 批量移动多个对象，整帧只刷新一次 AOI 与推送一次视野事件。
	MoveBatch = im.MoveBatch
	// BeginBatch 开始手动批量模式；之后 Move/EnterOwnerType/Leave/SetViewRadius 会累积到同一 batch，
	// 必须配对调用 EndBatch 统一刷新。
	BeginBatch = im.BeginBatch
	// EndBatch 结束手动批量模式，统一刷新 AOI 并推送视野事件。
	EndBatch = im.EndBatch
	// SetViewRadius 设置对象视野半径；r<=0 取消观察。
	SetViewRadius = im.SetViewRadius
	// Stop 停止场景心跳。
	Stop = im.Stop
	// Tick 手动驱动一次心跳（dt 为距上次间隔）。
	Tick = im.Tick
	// EnablePhysics 开启场景物理步进。
	EnablePhysics = im.EnablePhysics
	// DisablePhysics 关闭场景物理步进。
	DisablePhysics = im.DisablePhysics
	// AddBody 在场景内注册一个物理体。
	AddBody = im.AddBody
	// RemoveBody 移除场景内的物理体。
	RemoveBody = im.RemoveBody
	// ApplyForce 给物理体施加力。
	ApplyForce = im.ApplyForce
	// SetVelocity 直接设置物理体速度。
	SetVelocity = im.SetVelocity
	// Neighbors 返回指定半径内的其他对象 id。
	Neighbors = im.Neighbors
	// Around 返回指定半径内的全部对象（含自身）。
	Around = im.Around
	// Position 返回对象在场景中的坐标。
	Position = im.Position
	// Members 返回场景内全部成员 id。
	Members = im.Members
	// Broadcast 向场景全部成员广播一条消息。
	Broadcast = im.Broadcast
	// SendTo 向单个对象发送消息。
	SendTo = im.SendTo
)

// pathfinding（寻路；真身见 internal/domain/mmo/spatial/pathfinding/facade.go）。
var (
	// FindPath 计算从 start 到 end 的路径。
	FindPath = ispf.FindPathFacade
	// FindPathWithCost 同 FindPath，但允许自定义移动代价函数。
	FindPathWithCost = ispf.FindPathWithCostFacade
	// NewWalkAction 构造「走到目标点」的行为树 Action。
	NewWalkAction = ispf.NewWalkActionFacade
	// NewRunAction 构造「跑到目标点」的行为树 Action。
	NewRunAction = ispf.NewRunActionFacade
	// SimplifyPath 对路径做拉直简化。
	SimplifyPath = ispf.SimplifyPathFacade
	// DistanceSq 两点距离的平方。
	DistanceSq = ispf.DistanceSqFacade
	// Distance 两点欧氏距离。
	Distance = ispf.DistanceFacade
	// ManhattanDistance 两点曼哈顿距离。
	ManhattanDistance = ispf.ManhattanDistanceFacade
)

// 行为树（btree）工厂（真身见 internal/domain/mmo/facade.go）。
var (
	// NewBlackboard 创建共享黑板实例。
	NewBlackboard = im.NewBlackboard
	// NewSequence 构造序列节点。
	NewSequence = im.NewSequence
	// NewSelector 构造选择器节点。
	NewSelector = im.NewSelector
	// NewParallel 构造并行节点。
	NewParallel = im.NewParallel
	// NewInverter 构造取反装饰节点。
	NewInverter = im.NewInverter
	// NewRepeater 构造重复装饰节点。
	NewRepeater = im.NewRepeater
	// NewUntilFailure 构造"直到失败"装饰节点。
	NewUntilFailure = im.NewUntilFailure
	// NewLimiter 构造频次限制装饰节点。
	//
	// ⚠️ 时间源：本节点读黑板键 "now"（逻辑时刻），该键由 Tree.Tick 保证每帧存在且推进
	// （首次 Tick **之前**注入 ⇒ 归驱动方、Tree 只读；否则归 Tree 按 dt 自累加），
	// 故与 Timeout 同口径；只有直接 tick 本节点（不经 Tree.Tick）才回落墙钟 time.Now()。
	// ⚠️ 节点状态在节点上（计数 + 窗口）：**一棵树只服务一个 Agent**，多 Agent 共用会互相串扰。
	NewLimiter = im.NewLimiter
	// NewCooldown 构造冷却装饰节点。时间源与「一 Agent 一棵树」的要求同 NewLimiter。
	NewCooldown = im.NewCooldown
	// NewTimeout 构造超时装饰节点。
	NewTimeout = im.NewTimeout
	// NewCondition 构造条件叶子节点。
	NewCondition = im.NewCondition
	// NewAction 构造行为叶子节点。
	NewAction = im.NewAction
	// NewActionFn 构造执行即成功的行为叶子节点。
	NewActionFn = im.NewActionFn
	// NewTree 以根节点构造一棵行为树。
	NewTree = im.NewTree
	// LogicalTime 把「逻辑秒」（dt 累加值，如 MobManager 的内部 clock）转成可注入黑板的逻辑时刻。
	LogicalTime = im.LogicalTime
)

// 编译期断言：门面 Scene 实现 MobScene（真身侧 facade.go 已断言内部 *Scene 也满足，
// 两侧同时成立才能保证「业务传门面 Scene、引擎内部收 *Scene」这条链不断）。
var _ MobScene = Scene(nil)
