// Package mob 是 MMO 怪物/NPC 管理骨架（引擎级，不含具体数值业务）。

// 提供 MMO 怪物/NPC 的「刷新/仇恨/巡逻」语义，行为树节点词表与 behaviac 对齐
// （Action_Chase / Action_PurePatrol / Action_ChangeTarget / Action_DelTargetHatred /
// HasFindTarget / InChaseRange），可挂到任意 MobScene（*mmo.Scene 天然满足）。
package mob

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/mmo"
	"github.com/qw576483/clover-server-engine/internal/domain/mmo/gameplay/ai/btree"
	pkgbtree "github.com/qw576483/clover-server-engine/pkg/domain/mmo/ai/btree"
	"github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	pkgmob "github.com/qw576483/clover-server-engine/pkg/domain/mmo/mob"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// mobFailf mob 包的异常降频日志（首次全量 + 之后每 1000 条一条）。
// 行为树节点每帧都会跑，异常分支直接打日志会刷屏。
var mobFailCount atomic.Uint64

func mobFailf(format string, args ...any) {
	n := mobFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// Mob 是一只怪物的运行时实例。
type Mob struct {
	ID             uint64
	OwnerType      data.OwnerType
	Name           string
	Speed          float64 // 单位/秒
	HP             float64
	MaxHP          float64
	ChaseRange     float64
	AttackRange    float64
	AggroRange     float64
	Patrol         []collide.Vec2
	RespawnSec     float64
	SpawnX         float64
	SpawnZ         float64
	AttackCooldown float64 // 攻击冷却（秒），0=默认1s

	// 以下两项是 Spawn 时写定的**只读配置**（构造后不再被写入，可无锁读，
	// 与 ID / OwnerType / SpawnX / SpawnZ / Patrol / RespawnSec 同一条约定）。
	Passive         bool    // 被动（纯靶子）：不索敌、不还手
	AggroTimeoutSec float64 // 仇恨时效（秒），<=0 = 永不忘

	nowFn func() time.Time // 真实时间源（与 MobManager 同一个函数，Spawn 时写入）

	mu           sync.Mutex
	lastAttack   float64   // 上次攻击时间（逻辑时钟秒）
	lastAggroAt  time.Time // 最后一次「被激怒」（挨打 / 仇恨变化）的真实时刻；零值 = 从未
	target       uint64
	hate         map[uint64]float64
	patrolIdx    int
	dead         bool
	respawnAt    float64
	respawnRetry int // 重试次数（退避式重生）
	bb           *btree.Blackboard

	// brain 是**这只怪自己的**行为树。
	//
	// 为什么必须每怪一棵：行为树节点上有可变状态 —— Selector 的续跑位置
	// （lastRunning / running）、Limiter 的窗口与计数、Cooldown 的冷却时刻、
	// Repeater 的计数、Timeout 的已耗时。共用一棵树时这些状态跨怪串扰：
	// 一只怪把 Selector 状态留在「追击」分支后，下一只怪本帧直接从索引 1 起跑、
	// 永久跳过索引 0 的「攻击」分支 —— 表现为**有目标却站着不攻击**。
	//
	// 每怪一棵的开销见 buildBrain 的注释（约 0.5KB / 怪，仅 Spawn 时构建一次）。
	brain *btree.Tree
}

func (m *Mob) Target() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.target
}

func (m *Mob) SetTarget(t uint64) {
	m.mu.Lock()
	m.target = t
	m.mu.Unlock()
}

func (m *Mob) Hate(t uint64) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hate[t]
}

// maxHateEntries 单只怪仇恨表的容量上限。
// AggroTimeoutSec<=0（默认"永不忘"）时仇恨条目没有任何时效清理时机，
// 离场/死亡玩家留下的历史条目会随攻击者数量无上限增长（内存泄漏）。
// 超限时优先剔除「非当前目标且仇恨最低」的条目——只做内存保护，不改变时效语义。
const maxHateEntries = 1024

func (m *Mob) AddHate(t uint64, v float64) {
	m.mu.Lock()
	if _, exists := m.hate[t]; !exists && len(m.hate) >= maxHateEntries {
		var evict uint64
		evictHate := math.MaxFloat64
		for id, h := range m.hate {
			if id == m.target {
				continue // 当前目标不剔除
			}
			if h < evictHate {
				evictHate = h
				evict = id
			}
		}
		if evict != 0 {
			delete(m.hate, evict)
			mobFailf("mob: 仇恨表已达上限 %d，剔除最低仇恨条目 obj=%d（怪 obj=%d）", maxHateEntries, evict, m.ID)
		}
	}
	m.hate[t] += v
	m.mu.Unlock()
	// 仇恨变化 = 被激怒，刷新时效（否则"业务主动拉仇恨"这种路径会被时效提前忘掉）。
	m.touchAggro()
}

func (m *Mob) DelHate(t uint64) {
	m.mu.Lock()
	delete(m.hate, t)
	m.mu.Unlock()
}

func (m *Mob) IsDead() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dead
}

// Revive 复活：恢复血量、清空目标/仇恨/巡逻进度，并**重建决策树**。
//
// 为什么要重建：行为树节点上的续跑位置（Selector 的 lastRunning/running）是"上一世"的状态。
// 一只怪死在追击途中时，续跑位置停在索引 1（追击分支）；复活后若沿用，
// 它本帧就从追击分支起跑、跳过索引 0 的攻击分支 —— 表现为「刚复活、明明贴着人却只追不打」。
// Limiter 窗口 / Cooldown 时刻同理，属于上一世的时间账，复活后应当从头算。
//
// 并发：本函数持 m.mu 写 brain，Update 在**同一把锁**里读它（见 Update 的临界区），
// 因此与外部 goroutine 调 Revive 并发也安全。
func (m *Mob) Revive() {
	m.mu.Lock()
	m.dead = false
	m.HP = m.MaxHP
	m.hate = make(map[uint64]float64)
	m.target = 0
	m.lastAggroAt = time.Time{}
	m.patrolIdx = 0
	m.brain = buildBrain()
	m.mu.Unlock()
}

// touchAggro 记录"刚刚被激怒"（挨打 / 仇恨变化）的真实时刻，供仇恨时效判定。
func (m *Mob) touchAggro() {
	if m.nowFn == nil {
		return
	}
	now := m.nowFn()
	m.mu.Lock()
	m.lastAggroAt = now
	m.mu.Unlock()
}

// tryForgetAggro 仇恨时效判定：超时则清空目标与仇恨并返回 true。
//
// 未启用（AggroTimeoutSec<=0）、从未被激怒（零值）、或本来就没有仇恨时恒为 false
// —— 后者是为了不产生"什么都没忘"的日志噪音。
//
// ★ 语义边界：它管的是**仇恨记忆**，不是"巡逻锁定"。清掉之后同一帧行为树照常运转，
// 目标若仍在索敌范围内会立刻被重新锁定（怪就在你旁边，当然继续打你）；
// 真正的收益是"目标已经离场 / 玩家挂机不动"时不再无限记仇。
func (m *Mob) tryForgetAggro(now time.Time) bool {
	if m.AggroTimeoutSec <= 0 {
		return false
	}
	// AggroTimeoutSec 是 Spawn 后不再写入的只读配置，可在锁外读并做夹紧：
	// 直接乘 1e9 转 Duration 时，量级 >= 约 9.2e9 秒会溢出 int64 变负
	// → 时效判定反转（刚被激怒即判超时）。先夹紧到 1e9 秒（约 31 年，
	// 语义上等同"永不忘"）再转换；日志也在锁外打，不拖长临界区。
	sec := m.AggroTimeoutSec
	const maxAggroTimeoutSec = 1e9
	if sec > maxAggroTimeoutSec {
		mobFailf("mob: AggroTimeoutSec=%.1f 超出可转换上限，已夹紧为 %.0f 秒（怪 obj=%d）", sec, maxAggroTimeoutSec, m.ID)
		sec = maxAggroTimeoutSec
	}
	timeout := time.Duration(sec * float64(time.Second))
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.target == 0 && len(m.hate) == 0 {
		return false
	}
	if m.lastAggroAt.IsZero() {
		return false
	}
	if now.Sub(m.lastAggroAt) < timeout {
		return false
	}
	m.target = 0
	m.hate = make(map[uint64]float64)
	return true
}

// MobManager 管理一组怪物及其刷新/心跳。
//
// ★ 行为树不再由管理器持有：每只怪在 Spawn 时构建**自己的** brain（见 Mob.brain）。
// 原实现把单个 *btree.Tree 挂在管理器上供所有怪共用，节点上的可变状态
// （Selector 续跑位置 / Limiter 窗口 / Cooldown 时刻）会跨怪串扰。
type MobManager struct {
	scene    pkgmob.MobScene
	mu       sync.Mutex
	mobs     map[uint64]*Mob
	clock    float64          // 逻辑秒（攻击冷却/重生退避/AI 逻辑时刻用）
	nowFn    func() time.Time // 真实时间源（仇恨时效用；与 ratelimit.Clock 同一思路）
	onAttack func(attacker, target uint64)
}

func NewMobManager(scene pkgmob.MobScene) *MobManager {
	return &MobManager{
		scene: scene,
		mobs:  make(map[uint64]*Mob),
		nowFn: time.Now,
	}
}

// setClock 注入真实时间源（测试用：固定时钟即可精确断言仇恨时效，不必真的 sleep）。
func (mgr *MobManager) setClock(fn func() time.Time) {
	if fn == nil {
		fn = time.Now
	}
	mgr.mu.Lock()
	mgr.nowFn = fn
	mgr.mu.Unlock()
}

// nowTime 读真实时间源；只加锁取函数指针，**不在锁内调用户回调**（避免与场景锁串链）。
func (mgr *MobManager) nowTime() time.Time { return mgr.nowTimeSource()() }

// nowTimeSource 返回时间源函数本身（Spawn 时交给 Mob，保证刷新与判定用的是同一把尺子）。
func (mgr *MobManager) nowTimeSource() func() time.Time {
	mgr.mu.Lock()
	fn := mgr.nowFn
	mgr.mu.Unlock()
	if fn == nil {
		return time.Now
	}
	return fn
}

// SetOnAttack 设置攻击回调。onAttack 会被行为树 tick 线程读（actAttack），
// 与这里的外部写并发即 data race，统一由 mgr.mu 保护。
// 注意读取方只取函数指针、在锁外调用——不在持锁期间执行用户回调。
func (mgr *MobManager) SetOnAttack(fn func(attacker, target uint64)) {
	mgr.mu.Lock()
	mgr.onAttack = fn
	mgr.mu.Unlock()
}

// onAttackOf 取当前攻击回调指针（锁内取、锁外调，不在锁内执行用户回调）。
func (mgr *MobManager) onAttackOf() func(attacker, target uint64) {
	mgr.mu.Lock()
	fn := mgr.onAttack
	mgr.mu.Unlock()
	return fn
}

func (mgr *MobManager) Register(m pkgmob.Mob) {
	im, ok := m.(*Mob)
	if !ok || im == nil {
		// 直接 m.(*Mob) 断言：传入非 *Mob 的 Mob 实现即 panic，改为留痕并拒绝。
		mobFailf("mob: Register 收到非 *Mob 实现 (%T)，已忽略", m)
		return
	}
	mgr.mu.Lock()
	// 兜底：Mob 的字段是导出的，业务可能自行拼装 *Mob 再 Register（未经 Spawn），
	// 此时 brain / bb 为空，Update 里直接解引用会 panic。在**管理器锁内**补建，
	// 使 Update 取列表快照时对这两个字段有 happens-before（避免数据竞争）。
	if im.brain == nil {
		im.brain = buildBrain()
		mobFailf("mob: obj=%d 未经 Spawn 即 Register，已补建行为树/黑板", im.ID)
	}
	if im.bb == nil {
		// 此处已持 mgr.mu，不能再走 clockLocked()（sync.Mutex 不可重入）；直读字段。
		im.bb = mgr.makeBB(im, mgr.clock)
	}
	mgr.mobs[im.ID] = im
	mgr.mu.Unlock()
}

func (mgr *MobManager) Spawn(cfg pkgmob.SpawnConfig) (pkgmob.Mob, error) {
	if err := mgr.scene.EnterOwnerType(cfg.ID, cfg.OwnerType, mmo.Vec3{X: cfg.X, Z: cfg.Z}); err != nil {
		return nil, err
	}
	m := &Mob{
		ID:          cfg.ID,
		OwnerType:   cfg.OwnerType,
		Name:        cfg.Name,
		Speed:       cfg.Speed,
		HP:          cfg.HP,
		MaxHP:       cfg.HP,
		ChaseRange:  cfg.ChaseRange,
		AttackRange: cfg.AttackRange,
		AggroRange:  cfg.AggroRange,
		Patrol:      cfg.Patrol,
		RespawnSec:  cfg.RespawnSec,
		SpawnX:      cfg.X,
		SpawnZ:      cfg.Z,

		Passive:         cfg.Passive,
		AggroTimeoutSec: cfg.AggroTimeoutSec,
		nowFn:           mgr.nowTimeSource(), // 与 manager 同一个时间源：时效与刷新必须同一把尺子

		hate: make(map[uint64]float64),

		// 每怪一棵行为树：节点上的可变状态（Selector 续跑位置 / Limiter 窗口 /
		// Cooldown 时刻）必须归这只怪自己，不能与别的怪共用（见 Mob.brain 注释）。
		brain: buildBrain(),
	}
	m.bb = mgr.makeBB(m, mgr.clockLocked())
	mgr.Register(m)
	return m, nil
}

func (mgr *MobManager) Update(dt time.Duration) {
	mgr.mu.Lock()
	mgr.clock += dt.Seconds()
	now := mgr.clock
	list := make([]*Mob, 0, len(mgr.mobs))
	for _, m := range mgr.mobs {
		list = append(list, m)
	}
	mgr.mu.Unlock()

	// AI 统一时间源：把本帧的**逻辑时刻**经黑板书注入（KeyNow），
	// 让行为树里的 Limiter / Cooldown 与 dt 驱动的 Timeout 走同一把尺子。
	// 这里注入的是 mgr.clock（与仇恨时效 / 复活同一把尺子）。
	// makeBB 在 Spawn 时就预置了该键 ⇒ 按 btree 的「首帧归属」规则，KeyNow 归本管理器所有，
	// Tree.Tick 全程只读、不会用自累加钟覆盖它。
	logicalNow := btree.LogicalTime(now)

	for _, m := range list {
		m.mu.Lock()
		dead := m.dead
		respawn := m.respawnAt
		// brain 与 dead/respawnAt 在同一次持锁里取：
		// Revive（可能来自业务 goroutine）会在此锁下**重建** brain，
		// 锁外直接读字段就构成数据竞争。
		brain := m.brain
		bb := m.bb
		m.mu.Unlock()
		if dead {
			if now >= respawn {
				if err := mgr.respawn(m, now); err != nil {
					logger.Errorf("mob: respawn failed: %v", err)
				}
			}
			continue
		}
		// 仇恨时效：被激怒之后 AggroTimeoutSec 秒内没人再激它 → 忘掉目标与仇恨。
		// 放在 brain.Tick 之前：本 tick 就回到巡逻（不必等下一帧）。
		if m.tryForgetAggro(mgr.nowTime()) {
			logger.Infof("mob: 仇恨时效到期，已忘记目标 obj=%d timeout=%.1fs", m.ID, m.AggroTimeoutSec)
		}
		// 每怪一棵树：这里用 m.brain，不再是管理器共享的那一棵。
		// brain 的补建发生在 Register（见彼处），此处只做防御性判空：
		// 真的为空时跳过本帧而不是 panic —— 一只怪 tick 不了不该打崩整个场景。
		if brain == nil || bb == nil {
			mobFailf("mob: obj=%d 无行为树/黑板，本帧跳过（未经 Register/Spawn）", m.ID)
			continue
		}
		bb.Set(btree.KeyNow, logicalNow)
		brain.Tick(bb, dt)
	}
}

func (mgr *MobManager) OnDamaged(m pkgmob.Mob, dmg float64) {
	im, ok := m.(*Mob)
	if !ok || im == nil {
		// 直接 m.(*Mob) 断言：传入非 *Mob 的 Mob 实现即 panic，改为留痕并拒绝。
		mobFailf("mob: OnDamaged 收到非 *Mob 实现 (%T)，已忽略", m)
		return
	}
	// 先取 manager 时钟（锁外），再进怪锁：避免「持 im.mu 时取 mgr.mu」的嵌套锁序
	//（与"不把怪锁带到慢路径"的纪律一致；嵌套一旦有反向路径即成死锁）。
	clockNow := mgr.clockLocked()
	im.mu.Lock()
	if im.dead {
		im.mu.Unlock()
		return
	}
	im.HP -= dmg
	if im.HP < 0 {
		im.HP = 0
	}
	if im.MaxHP > 0 && im.HP > im.MaxHP {
		im.HP = im.MaxHP
	}
	if im.HP <= 0 {
		im.HP = 0
		im.dead = true
		im.hate = make(map[uint64]float64)
		im.target = 0
		im.respawnAt = clockNow + im.RespawnSec
		im.mu.Unlock()
		mgr.scene.Leave(im.ID)
		return
	}
	im.mu.Unlock()
	// 挨打即刷新仇恨时效：被动单位也刷新（它虽然不还手，但"这条仇恨该什么时候过期"照旧）。
	im.touchAggro()
}

// 内部辅助
func (mgr *MobManager) clockLocked() float64 {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.clock
}

// makeBB 构造一只怪的私有黑板；now 为当前逻辑秒（由调用方在**不自持 mgr.mu** 时取）。
// 预置逻辑时刻：Spawn 后到第一次 Update 之间若有人 tick（测试 / 业务直接驱动），
// Limiter / Cooldown 也拿到同一口径的时间源，不会静默回落墙钟。
func (mgr *MobManager) makeBB(m *Mob, now float64) *btree.Blackboard {
	b := btree.NewBlackboard()
	b.Set("mob", m)
	b.Set("scene", mgr.scene)
	b.Set("mgr", mgr)
	b.Set(btree.KeyNow, btree.LogicalTime(now))
	return b
}

const maxRespawnRetries = 5

// maxRespawnBackoffSec 重试次数耗尽后下一次尝试的间隔（秒）。
// 重试耗尽后必须把 respawnAt 推到远期：否则 Update 每帧都满足 now>=respawnAt、
// 每帧重进 respawn 分支并打一条 Errorf（怪永久重试 + 每帧一条日志风暴）。
const maxRespawnBackoffSec = 3600

// respawn 让一只死掉的怪重新入场，失败按指数退避重试。
//
// 并发约束：`respawnRetry` / `respawnAt` 由本函数写、由 Update 与 Revive 读或写，
// 三个入口都在 `m.mu` 下访问 —— 这里同样必须持锁（且**不能**把锁带到
// `scene.EnterOwnerType` 上：那是会回调场景锁的慢路径，持怪锁调它会把
// 怪锁与场景锁串成一条链）。
// 只读字段（ID / OwnerType / SpawnX / SpawnZ / Patrol / RespawnSec）在 Spawn 构造后
// 不再被写入，可无锁读。
func (mgr *MobManager) respawn(m *Mob, now float64) error {
	m.mu.Lock()
	retry := m.respawnRetry
	m.mu.Unlock()
	if retry >= maxRespawnRetries {
		// 把下次尝试推到远期再报错：否则 Update 每帧重进本分支、每帧刷一条错误日志。
		m.mu.Lock()
		m.respawnAt = now + maxRespawnBackoffSec
		m.mu.Unlock()
		return fmt.Errorf("mob: exceeded max respawn retries (%d)，%ds 后再试", maxRespawnRetries, maxRespawnBackoffSec)
	}
	x, z := m.SpawnX, m.SpawnZ
	if len(m.Patrol) > 0 {
		x, z = m.Patrol[0].X, m.Patrol[0].Y
	}
	if err := mgr.scene.EnterOwnerType(m.ID, m.OwnerType, mmo.Vec3{X: x, Z: z}); err != nil {
		base := m.RespawnSec
		if base < 1 {
			base = 1
		}
		m.mu.Lock()
		backoff := base * float64(int64(1)<<retry)
		m.respawnRetry = retry + 1
		m.respawnAt = now + backoff
		if m.respawnAt > now+3600 {
			m.respawnAt = now + 3600
		}
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.respawnRetry = 0
	m.mu.Unlock()
	m.Revive()
	return nil
}

// mobOf 安全读取黑板中的 *Mob（行为树每个节点的第一件事）。
// 直接 b.Get("mob").(*Mob) 断言：黑板内容异常（缺键 / 类型不符）时 panic，
// 而行为树节点位于每帧路径上——一个坏黑板不该把整个场景 tick 打崩。
func mobOf(b pkgbtree.Blackboard) (*Mob, bool) {
	m, ok := b.Get("mob").(*Mob)
	if !ok || m == nil {
		mobFailf("mob: 黑板键 mob 缺失或类型不符 (%T)，行为树节点失败返回", b.Get("mob"))
		return nil, false
	}
	return m, true
}

// mgrScene 安全读取黑板中的场景（与 actAttack 对 "mgr" 判 ok 的口径一致）。
func mgrScene(b pkgbtree.Blackboard) (pkgmob.MobScene, bool) {
	s, ok := b.Get("scene").(pkgmob.MobScene)
	if !ok || s == nil {
		mobFailf("mob: 黑板键 scene 缺失或类型不符 (%T)，行为树节点失败返回", b.Get("scene"))
		return nil, false
	}
	return s, true
}

// buildBrain 构建一棵完整的行为树（**每次调用都是新实例**）。
//
// # 为什么每怪一棵（而不是共用一棵）
//
// 行为树节点持有可变状态：Selector 的 lastRunning/running、Limiter 的窗口与计数、
// Cooldown 的冷却时刻、Repeater 计数、Timeout 已耗时。共用一棵树 = 共用一个 Agent 的
// 决策状态，串扰发生在**同一帧内**（Update 按 map 顺序逐怪 tick），
// 表现为「上一只怪停在哪，下一只就从哪续跑」——例如永久跳过「攻击」分支。
//
// # 开销（口径）
//
// 每次调用新建 11 个节点对象 + 3 个 children 切片：
//   - 节点结构体合计约 170 字节（Selector 48 / 2×Sequence 各 24 / 4×Condition 各 8 /
//     3×Action 各 8 / Tree 16）；
//   - 切片头与底层数组约 200 字节；
//   - Go 分配头（11 次小对象分配）约 180 字节。
//
// 合计 **约 0.5KB / 怪**，且只在 Spawn 时分配一次（不是每帧）。1 万只怪 ≈ 5MB 常驻，
// 与每只怪自带的 hate map / blackboard map 同量级；相对共用一棵树多做的是
// 「每次 Spawn 11 次小分配」（纳秒级），不是每帧开销。
//
// 若业务把怪量推到十万级且在意这 5MB：可自行把节点状态搬进黑板（按节点路径作 key），
// 复用同一棵不可变结构树 —— 引擎默认实现选择「每怪一棵」，因为它是唯一
// 无需业务配合、且不会因漏写 key 而静默串扰的做法。
func buildBrain() *btree.Tree {
	return btree.NewTree(btree.NewSelector(
		btree.NewSequence(
			btree.NewCondition(hasTargetOrFind),
			btree.NewCondition(inAttackRange),
			btree.NewAction(actAttack),
		),
		btree.NewSequence(
			btree.NewCondition(hasTargetOrFind),
			btree.NewCondition(inChaseRange),
			btree.NewAction(actChase),
		),
		btree.NewAction(actPatrol),
	))
}

const arriveThreshold = 1e-3

// 行为树节点（对应 behaviac 词表）
func hasTargetOrFind(b pkgbtree.Blackboard) bool {
	m, ok := mobOf(b)
	if !ok {
		return false
	}
	// 被动单位（纯靶子 / 展示 NPC）：不索敌、不还手。
	// 这里恒返回 false，行为树自然落到 actPatrol（没有巡逻点时原地站着）——
	// 用"决策层不索敌"表达"打不还手"，而不是把数值调低到打不死你（后者是数值问题）。
	if m.Passive {
		return false
	}
	sp, ok := mgrScene(b)
	if !ok {
		return false
	}
	if t := m.Target(); t != 0 {
		if _, ok := sp.Position(t); ok {
			return true
		}
		// 仇恨表里的目标已离场：清目标是对的，但这是线上最难查的状态之一
		//（怪"莫名断仇恨/卡住"），必须降频留痕。
		mobFailf("mob: 目标 obj=%d 已不在场景，清理目标（怪 obj=%d）", t, m.ID)
		m.SetTarget(0)
	}
	var bestTarget uint64
	var bestHate float64
	for _, n := range sp.Neighbors(m.ID, m.AggroRange) {
		if n == m.ID {
			continue
		}
		h := m.Hate(n)
		if h > bestHate || (h == bestHate && bestTarget == 0) {
			bestHate = h
			bestTarget = n
		}
	}
	if bestTarget != 0 {
		if m.Hate(bestTarget) == 0 {
			m.AddHate(bestTarget, 1)
		}
		m.SetTarget(bestTarget)
		return true
	}
	return false
}

func inChaseRange(b pkgbtree.Blackboard) bool {
	m, ok := mobOf(b)
	if !ok {
		return false
	}
	t := m.Target()
	if t == 0 {
		return false
	}
	sp, ok := mgrScene(b)
	if !ok {
		return false
	}
	selfPos, ok := sp.Position(m.ID)
	if !ok {
		// 自身不在场景：怪没进好场或已被离场，属非预期状态，留痕。
		mobFailf("mob: obj=%d 自身不在场景，追击范围判定失败", m.ID)
		return false
	}
	targetPos, ok2 := sp.Position(t)
	if !ok2 {
		// 「仇恨表里的目标已离场」属非预期状态却无痕迹时，怪中途卡死无法定位。
		mobFailf("mob: 追击目标 obj=%d 已不在场景（仇恨残留），本帧判定失败（怪 obj=%d）", t, m.ID)
		return false
	}
	return math.Hypot(targetPos.X-selfPos.X, targetPos.Z-selfPos.Z) <= m.ChaseRange
}

func inAttackRange(b pkgbtree.Blackboard) bool {
	m, ok := mobOf(b)
	if !ok {
		return false
	}
	t := m.Target()
	if t == 0 {
		return false
	}
	sp, ok := mgrScene(b)
	if !ok {
		return false
	}
	selfPos, ok := sp.Position(m.ID)
	if !ok {
		mobFailf("mob: obj=%d 自身不在场景，攻击范围判定失败", m.ID)
		return false
	}
	targetPos, ok2 := sp.Position(t)
	if !ok2 {
		mobFailf("mob: 攻击目标 obj=%d 已不在场景（仇恨残留），本帧判定失败（怪 obj=%d）", t, m.ID)
		return false
	}
	return math.Hypot(targetPos.X-selfPos.X, targetPos.Z-selfPos.Z) <= m.AttackRange
}

func actChase(b pkgbtree.Blackboard) pkgbtree.Status {
	m, ok := mobOf(b)
	if !ok {
		return pkgbtree.StatusFailure
	}
	sp, ok := mgrScene(b)
	if !ok {
		return pkgbtree.StatusFailure
	}
	t := m.Target()
	if t == 0 {
		return pkgbtree.StatusFailure
	}
	selfPos, ok := sp.Position(m.ID)
	if !ok {
		mobFailf("mob: obj=%d 自身不在场景，追击失败", m.ID)
		return pkgbtree.StatusFailure
	}
	targetPos, ok2 := sp.Position(t)
	if !ok2 {
		mobFailf("mob: 追击目标 obj=%d 已不在场景（仇恨残留），追击失败（怪 obj=%d）", t, m.ID)
		return pkgbtree.StatusFailure
	}
	dx, dz := targetPos.X-selfPos.X, targetPos.Z-selfPos.Z
	d := math.Hypot(dx, dz)
	if d <= arriveThreshold {
		return pkgbtree.StatusSuccess
	}
	step := m.Speed * b.GetFloat64("dt")
	if step > d {
		step = d
	}
	sp.Move(m.ID, mmo.Vec3{X: selfPos.X + dx/d*step, Z: selfPos.Z + dz/d*step})
	return pkgbtree.StatusRunning
}

func actAttack(b pkgbtree.Blackboard) pkgbtree.Status {
	m, ok := mobOf(b)
	if !ok {
		return pkgbtree.StatusFailure
	}
	t := m.Target()
	if t == 0 {
		return pkgbtree.StatusFailure
	}
	cd := m.AttackCooldown
	if cd <= 0 {
		cd = 1.0
	}
	const epsilon = 1e-9
	mgr, ok := b.Get("mgr").(*MobManager)
	if !ok || mgr == nil {
		mobFailf("mob: 黑板键 mgr 缺失或类型不符 (%T)，攻击失败", b.Get("mgr"))
		return pkgbtree.StatusFailure
	}
	now := mgr.clockLocked()
	m.mu.Lock()
	if now-m.lastAttack < cd-epsilon {
		m.mu.Unlock()
		return pkgbtree.StatusRunning
	}
	m.lastAttack = now
	m.mu.Unlock()
	// 锁外取回调指针、锁外调用：不在持锁期间跑用户代码。
	if fn := mgr.onAttackOf(); fn != nil {
		fn(m.ID, t)
	}
	return pkgbtree.StatusSuccess
}

func actPatrol(b pkgbtree.Blackboard) pkgbtree.Status {
	m, ok := mobOf(b)
	if !ok {
		return pkgbtree.StatusFailure
	}
	sp, ok := mgrScene(b)
	if !ok {
		return pkgbtree.StatusFailure
	}
	if len(m.Patrol) == 0 {
		return pkgbtree.StatusSuccess
	}
	// patrolIdx 必须持锁读：Revive 与本函数末尾的推进都持 m.mu 写它。
	// 越界兜底仅为防御 —— 正常路径下索引恒在界内，但一旦 Patrol 被换短，
	// 无锁索引会变成 panic 而不是「走错一个巡逻点」。
	m.mu.Lock()
	idx := m.patrolIdx
	m.mu.Unlock()
	if idx < 0 || idx >= len(m.Patrol) {
		idx = 0
	}
	wp := m.Patrol[idx]
	selfPos, ok := sp.Position(m.ID)
	if !ok {
		mobFailf("mob: obj=%d 自身不在场景，巡逻失败", m.ID)
		return pkgbtree.StatusFailure
	}
	dx, dz := wp.X-selfPos.X, wp.Y-selfPos.Z
	d := math.Hypot(dx, dz)
	if d <= arriveThreshold {
		m.mu.Lock()
		m.patrolIdx = (m.patrolIdx + 1) % len(m.Patrol)
		m.mu.Unlock()
		return pkgbtree.StatusSuccess
	}
	step := m.Speed * b.GetFloat64("dt")
	if step > d {
		step = d
	}
	sp.Move(m.ID, mmo.Vec3{X: selfPos.X + dx/d*step, Z: selfPos.Z + dz/d*step})
	return pkgbtree.StatusRunning
}
