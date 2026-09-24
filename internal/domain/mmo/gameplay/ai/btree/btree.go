// Package btree 轻量行为树框架（引擎级、与具体游戏业务解耦）的内部实现。

// 具体实现本包；公开 API 与类型定义位于 pkg/domain/mmo/ai/btree。
package btree

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	pkgbtree "github.com/qw576483/clover-server-engine/pkg/domain/mmo/ai/btree"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// 黑板上的公共键：由驱动方（Tree.Tick / MobManager.Update）写入，节点只读。
// 字面量真身定义在 pkg 侧（业务也要引用），这里只做别名，避免两处各写一次字符串。
const (
	// KeyDT 是「本帧步长（秒）」，浮点。由 Tree.Tick 每帧写入，Timeout / 移动类行为读它。
	KeyDT = pkgbtree.KeyDT
	// KeyNow 是「当前逻辑时刻」，供 Limiter / Cooldown 判窗口与冷却。
	// 由驱动方按自己的逻辑时钟写入（见 LogicalTime）；**缺省时节点回落墙钟**。
	KeyNow = pkgbtree.KeyNow
)

// Blackboard 是行为树的共享黑板：Agent 级状态 store（线程安全）。
type Blackboard struct {
	mu    sync.RWMutex
	store map[string]any
}

// NewBlackboard 构造空黑板。
func NewBlackboard() *Blackboard { return &Blackboard{store: make(map[string]any)} }

// Set 写入键值。
func (b *Blackboard) Set(key string, v any) {
	b.mu.Lock()
	b.store[key] = v
	b.mu.Unlock()
}

// Get 读取键值（不存在返回 nil）。
func (b *Blackboard) Get(key string) any {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.store[key]
}

// Del 删除键。
func (b *Blackboard) Del(key string) {
	b.mu.Lock()
	delete(b.store, key)
	b.mu.Unlock()
}

// LogicalTime 把「逻辑秒」（dt 累加值，如 MobManager.clock）转成可供 Limiter / Cooldown
// 比较的 time.Time。
//
// 为什么需要它：本框架里 Timeout 用 dt 累加、Limiter / Cooldown 用时刻比较，两者必须
// 共用同一个时间源，否则「服务器暂停 / 变速」时冷却与限流窗口仍按墙钟走，行为脱节。
// 基准取 Unix 纪元（0 秒起算）：**只用于同一逻辑时钟域内的先后比较**，
// 不得与墙钟 time.Now() 混用（两者差值无意义）。
//
// 非有限值（NaN / ±Inf）会让同一帧内的所有比较恒为 false（静默失效），
// 这里夹紧为 0 并留痕 —— 调用方读到 0 时刻等价于「全部窗口已过期」。
func LogicalTime(sec float64) time.Time {
	if math.IsNaN(sec) || math.IsInf(sec, 0) {
		logger.Warnf("btree: 逻辑时钟值非法 (%v)，本次按 0 处理（限流/冷却窗全部重置）", sec)
		return time.Unix(0, 0)
	}
	ns := sec * float64(time.Second)
	// 夹紧到 int64 可表示范围，避免溢出成负值导致「时间倒流」。
	const maxSec = 9.2e9
	if ns > maxSec*float64(time.Second) {
		ns = maxSec * float64(time.Second)
	}
	if ns < -maxSec*float64(time.Second) {
		ns = -maxSec * float64(time.Second)
	}
	return time.Unix(0, int64(ns))
}

// bbNowRaw 解析黑板上的逻辑时刻（**不写日志**）：缺失 / 类型不符都返回 ok=false。
// 载体两种：time.Time（推荐，配 LogicalTime(逻辑秒)）；float64 / int64 / float32 / int（逻辑秒）。
// 单独拆出不带日志的版本，是给 Tree.Tick 做「首帧归属判定」用的 ——
// 那里「黑板本来就没有这个键」是正常路径（本树将接管该键），不该打 Warn。
func bbNowRaw(b pkgbtree.Blackboard) (time.Time, bool) {
	switch v := b.Get(KeyNow).(type) {
	case nil:
		return time.Time{}, false
	case time.Time:
		return v, true
	case float64:
		return LogicalTime(v), true
	case int64:
		return LogicalTime(float64(v)), true
	case float32:
		return LogicalTime(float64(v)), true
	case int:
		return LogicalTime(float64(v)), true
	default:
		return time.Time{}, false
	}
}

// bbNow 读取黑板上的逻辑时刻，并对「缺失 / 类型不符」两种故障各自留一条降频 Warn
// （历史上这条注入路径没有生产端、又完全静默，表现为「限流窗口永远基于真实时钟」，
// 却看不出是"没人注入"还是"注入类型不对"）。
// 两者都不在时返回 ok=false —— 由调用方回落墙钟。
// 注意：正常路径（经 Tree.Tick 驱动）不会有这两种故障；只有**直接 tick 节点**才会走到回落分支。
func bbNow(b pkgbtree.Blackboard) (time.Time, bool) {
	now, ok := bbNowRaw(b)
	if ok {
		return now, true
	}
	if v := b.Get(KeyNow); v == nil {
		bbNowMissingFailf("btree: 黑板无 %q 键（逻辑时刻），本帧回落墙钟 time.Now()；节点应经 Tree.Tick 驱动", KeyNow)
	} else {
		bbNowTypeFailf("btree: 黑板键 %q 类型不符 (%T)，本帧回落墙钟", KeyNow, v)
	}
	return time.Time{}, false
}

// bbNowTypeFailf 类型不符的降频日志：行为树每帧都跑，逐帧打印会刷屏。
var bbNowTypeFailCount atomic.Uint64

func bbNowTypeFailf(format string, args ...any) {
	n := bbNowTypeFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// bbNowMissingFailf 缺键回落的降频日志（与类型不符分开计数：两种故障各自首次可见）。
var bbNowMissingFailCount atomic.Uint64

func bbNowMissingFailf(format string, args ...any) {
	n := bbNowMissingFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// GetFloat64 / GetInt64 / GetString / GetBool 是类型化读取，缺失或类型不符返回零值。
func (b *Blackboard) GetFloat64(key string) float64 {
	if v, ok := b.Get(key).(float64); ok {
		return v
	}
	return 0
}
func (b *Blackboard) GetInt64(key string) int64 {
	if v, ok := b.Get(key).(int64); ok {
		return v
	}
	return 0
}
func (b *Blackboard) GetString(key string) string {
	if v, ok := b.Get(key).(string); ok {
		return v
	}
	return ""
}
func (b *Blackboard) GetBool(key string) bool {
	if v, ok := b.Get(key).(bool); ok {
		return v
	}
	return false
}

// 控制节点
// Sequence 顺序执行子节点：任一 Failure→整体 Failure；任一 Running→Running；全 Success→Success。
type Sequence struct{ children []pkgbtree.Node }

// NewSequence 构造序列节点。
func NewSequence(children ...pkgbtree.Node) *Sequence { return &Sequence{children: children} }

// Tick 实现 Node。
func (n *Sequence) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	for _, c := range n.children {
		switch c.Tick(b) {
		case pkgbtree.StatusFailure:
			return pkgbtree.StatusFailure
		case pkgbtree.StatusRunning:
			return pkgbtree.StatusRunning
		}
	}
	return pkgbtree.StatusSuccess
}

// Selector 优先级选择：依次执行子节点，返回第一个非 Failure 的状态；全 Failure→Failure。
type Selector struct {
	mu          sync.Mutex // 保护下面两个可变续跑状态
	children    []pkgbtree.Node
	lastRunning int
	running     bool
}

// NewSelector 构造选择器（优先级）节点。
func NewSelector(children ...pkgbtree.Node) *Selector {
	return &Selector{children: children, lastRunning: -1}
}

// Tick 实现 Node。
//
// lastRunning / running 是**节点自己的可变状态**：多个 Agent 共享同一棵树时
// （或多线程并发 tick 时）不加锁就是赤裸的数据竞争，且会让「这只怪续跑的位置」
// 串到另一只怪身上。这里加锁保护。
func (n *Selector) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	start := 0
	if n.running && n.lastRunning >= 0 && n.lastRunning < len(n.children) {
		start = n.lastRunning
	}
	for i := start; i < len(n.children); i++ {
		switch s := n.children[i].Tick(b); s {
		case pkgbtree.StatusSuccess:
			n.running = false
			n.lastRunning = -1
			return pkgbtree.StatusSuccess
		case pkgbtree.StatusRunning:
			n.running = true
			n.lastRunning = i
			return pkgbtree.StatusRunning
		case pkgbtree.StatusFailure:
			if i <= n.lastRunning {
				n.running = false
				n.lastRunning = -1
			}
		}
	}
	n.running = false
	n.lastRunning = -1
	return pkgbtree.StatusFailure
}

// Parallel 并行执行全部子节点；策略决定整体结果。
type Parallel struct {
	policy   pkgbtree.ParallelPolicy
	children []pkgbtree.Node
}

// NewParallel 构造并行节点；policy 决定成功条件。
func NewParallel(policy pkgbtree.ParallelPolicy, children ...pkgbtree.Node) *Parallel {
	return &Parallel{policy: policy, children: children}
}

// Tick 实现 Node。
func (n *Parallel) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	succ, fail := 0, 0
	for _, c := range n.children {
		switch c.Tick(b) {
		case pkgbtree.StatusSuccess:
			succ++
		case pkgbtree.StatusFailure:
			fail++
		}
	}
	switch n.policy {
	case pkgbtree.ParallelOneSuccess:
		if succ > 0 {
			return pkgbtree.StatusSuccess
		}
		if fail == len(n.children) {
			return pkgbtree.StatusFailure
		}
		return pkgbtree.StatusRunning
	default:
		if fail > 0 {
			return pkgbtree.StatusFailure
		}
		if succ == len(n.children) {
			return pkgbtree.StatusSuccess
		}
		return pkgbtree.StatusRunning
	}
}

// 装饰节点
// Inverter 取反子节点结果（Running 透传）。
type Inverter struct{ child pkgbtree.Node }

// NewInverter 构造取反装饰。
func NewInverter(child pkgbtree.Node) *Inverter { return &Inverter{child: child} }

// Tick 实现 Node。
func (n *Inverter) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	switch n.child.Tick(b) {
	case pkgbtree.StatusSuccess:
		return pkgbtree.StatusFailure
	case pkgbtree.StatusFailure:
		return pkgbtree.StatusSuccess
	default:
		return pkgbtree.StatusRunning
	}
}

// Repeater 重复执行子节点最多 max 次，直到子节点 Failure 或达到上限。
type Repeater struct {
	child   pkgbtree.Node
	max     int
	current int
}

// NewRepeater 构造重复装饰（max=0 表示无限重复，max>0 执行最多 max 轮）。
func NewRepeater(max int, child pkgbtree.Node) *Repeater {
	return &Repeater{child: child, max: max}
}

// Tick 实现 Node。
func (n *Repeater) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	s := n.child.Tick(b)
	switch s {
	case pkgbtree.StatusRunning:
		return pkgbtree.StatusRunning
	case pkgbtree.StatusFailure:
		n.current = 0
		return pkgbtree.StatusFailure
	case pkgbtree.StatusSuccess:
		n.current++
		if n.max > 0 && n.current >= n.max {
			n.current = 0
			return pkgbtree.StatusSuccess
		}
		return pkgbtree.StatusRunning
	}
	return pkgbtree.StatusRunning
}

// UntilFailure 重复执行子节点直到子节点失败。
type UntilFailure struct{ child pkgbtree.Node }

// NewUntilFailure 构造"直到失败"装饰。
func NewUntilFailure(child pkgbtree.Node) *UntilFailure {
	return &UntilFailure{child: child}
}

// Tick 实现 Node。
func (n *UntilFailure) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	switch n.child.Tick(b) {
	case pkgbtree.StatusRunning:
		return pkgbtree.StatusRunning
	case pkgbtree.StatusFailure:
		return pkgbtree.StatusSuccess
	default:
		return pkgbtree.StatusRunning
	}
}

// Limiter 限制子节点在 window 时间段内最多执行 limit 次。
type Limiter struct {
	child     pkgbtree.Node
	limit     int
	window    time.Duration
	count     int
	windowEnd time.Time
}

// NewLimiter 构造频次限制装饰。
func NewLimiter(limit int, window time.Duration, child pkgbtree.Node) *Limiter {
	return &Limiter{child: child, limit: limit, window: window}
}

// Tick 实现 Node。
//
// ⚠️ 状态在**节点上**（count / windowEnd）：一棵树被多个 Agent 共用时，
// 限流窗口会跨 Agent 串扰。引擎侧已改为「每怪一棵树」（见 mob.MobManager.Spawn）；
// 业务自建树也必须一 Agent 一棵，或自行保证单 Agent 独占。
//
// 时间源：读黑板 KeyNow（逻辑时刻，支持 time.Time / 逻辑秒 float64）—— 由 Tree.Tick
// 每帧写入（驱动方注入优先，未注入时按 dt 自累加），故正常路径与 dt 驱动的 Timeout 同口径；
// 只有**直接 tick 本节点**（未经 Tree.Tick）才会回落墙钟 time.Now()（并留一条降频 Warn）。
func (n *Limiter) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	now, ok := bbNow(b)
	if !ok {
		now = time.Now()
	}
	if n.windowEnd.IsZero() || now.After(n.windowEnd) {
		n.windowEnd = now.Add(n.window)
		n.count = 0
	}
	if n.count >= n.limit {
		return pkgbtree.StatusFailure
	}
	n.count++
	return n.child.Tick(b)
}

// Cooldown 冷却装饰：子节点成功执行后进入 cooldown。
type Cooldown struct {
	child    pkgbtree.Node
	d        time.Duration
	cooldown time.Time
}

// NewCooldown 构造冷却装饰。
func NewCooldown(d time.Duration, child pkgbtree.Node) *Cooldown {
	return &Cooldown{child: child, d: d}
}

// Tick 实现 Node。
//
// ⚠️ 同 Limiter：冷却时刻是**节点上的可变状态**，多 Agent 共用一棵树会互相串扰
// （A 的冷却会把 B 挡住）；引擎侧已改为每怪一棵树。
//
// 时间源同 Limiter：黑板 KeyNow（逻辑时刻）优先，且该键现在由 Tree.Tick 每帧保证存在
// ⇒ 全树的 Timeout / Limiter / Cooldown 共用同一个逻辑时刻（只有直接 tick 本节点才回落墙钟）。
func (n *Cooldown) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	now, ok := bbNow(b)
	if !ok {
		now = time.Now()
	}
	if now.Before(n.cooldown) {
		return pkgbtree.StatusFailure
	}
	s := n.child.Tick(b)
	if s == pkgbtree.StatusSuccess {
		n.cooldown = now.Add(n.d)
	}
	return s
}

// Timeout 装饰：子节点在 d 内未完成则强制 Failure。
type Timeout struct {
	child   pkgbtree.Node
	d       time.Duration
	elapsed time.Duration
}

// NewTimeout 构造超时装饰。
func NewTimeout(d time.Duration, child pkgbtree.Node) *Timeout {
	return &Timeout{child: child, d: d}
}

// Tick 实现 Node。
//
// 累加口径：只在**本帧的 dt** 上做一次「浮点秒 → Duration」换算，再累加进 Duration 形态的
// n.elapsed。此前是 `float64(n.elapsed)/1e9 + dt` 再 `time.Duration(sec*1e9)`：每 tick 都把
// **已累加的 elapsed** 过一次 float64，舍入误差随 tick 数累积（实测 1/30s 步长下 200k tick
// 偏短 14.6µs、1e6 tick 约 25µs）⇒ 超时/节流的时间口径长跑偏短。
// 现在误差只来自本帧 dt 的这一次换算（常数级，不随已累加时长放大）。
// 见 btree_logicalclock_test.go 的 TestTimeoutElapsedDoesNotAccumulateRounding。
func (n *Timeout) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	s := n.child.Tick(b)
	if s != pkgbtree.StatusRunning {
		n.elapsed = 0
		return s
	}
	n.elapsed += time.Duration(b.GetFloat64(KeyDT) * float64(time.Second))
	if n.elapsed >= n.d {
		n.elapsed = 0
		return pkgbtree.StatusFailure
	}
	return pkgbtree.StatusRunning
}

// 叶子节点
// Condition 条件叶子：fn 返回 true→Success，false→Failure。
type Condition struct {
	fn func(b pkgbtree.Blackboard) bool
}

// NewCondition 构造条件节点。
func NewCondition(fn func(b pkgbtree.Blackboard) bool) *Condition {
	return &Condition{fn: fn}
}

// Tick 实现 Node。
func (n *Condition) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	if n.fn(b) {
		return pkgbtree.StatusSuccess
	}
	return pkgbtree.StatusFailure
}

// Action 行为叶子：fn 决定返回状态（可 Running）。
type Action struct {
	fn func(b pkgbtree.Blackboard) pkgbtree.Status
}

// NewAction 构造行为节点。
func NewAction(fn func(b pkgbtree.Blackboard) pkgbtree.Status) *Action {
	return &Action{fn: fn}
}

// Tick 实现 Node。
func (n *Action) Tick(b pkgbtree.Blackboard) pkgbtree.Status { return n.fn(b) }

// ActionFn 行为叶子：执行 fn 后恒返回 Success。
type ActionFn struct{ fn func(b pkgbtree.Blackboard) }

// NewActionFn 构造「执行即成功」行为节点。
func NewActionFn(fn func(b pkgbtree.Blackboard)) *ActionFn { return &ActionFn{fn: fn} }

// Tick 实现 Node。
func (n *ActionFn) Tick(b pkgbtree.Blackboard) pkgbtree.Status {
	n.fn(b)
	return pkgbtree.StatusSuccess
}

// Tree 是一棵行为树（持有根节点），可绑定 dt 供 Timeout 等使用。
type Tree struct {
	root pkgbtree.Node
	// elapsed 是本树自累加的 dt 逻辑钟（仅在 ownNow 为 true 时推进）。
	// 它连同下面的 ownNow/decided 都是**节点级可变状态**，与根节点同生命期
	// ⇒ 同受「一棵树只服务一个 Agent」约束。
	elapsed time.Duration
	// ownNow 表示「黑板 KeyNow 键归本树所有」——由**首帧**黑板有没有该键决定：
	// 有 ⇒ 驱动方负责写，本树全程只读；没有 ⇒ 本树接管，此后每帧自己推进。
	ownNow bool
	// decided 表示首帧归属判定已做过（只判一次，避免每帧猜测谁在写）。
	decided bool
}

// NewTree 以根节点构造树。
func NewTree(root pkgbtree.Node) *Tree { return &Tree{root: root} }

// Tick 驱动一帧：注入 dt 与逻辑时刻，然后 tick 根节点。
//
// 时间源（统一口径，全树一套）：
//   - KeyDT（本帧步长，秒）：每帧写入，Timeout / 移动类节点读它；
//   - KeyNow（当前逻辑时刻）：**首帧黑板有该键 ⇒ 归驱动方**（如 mob.MobManager.Update
//     每帧 b.Set(KeyNow, LogicalTime(mgr.clock))，Spawn 时 makeBB 已预置），本树全程只读；
//     **首帧没有该键 ⇒ 归本树**，此后每帧用**自累加的 dt 逻辑钟**写入
//     ⇒ Limiter / Cooldown / Timeout 在同一棵树里口径一致。
//     ⛔ 两者都**不许**用 time.Now()：墙钟在服务器暂停 / 变速时仍前进，
//     会让限流窗口与冷却与 dt 脱节（这正是历史上三套时间口径的成因）。
//     只有「直接 tick 节点、不经本方法」时，节点才各自回落墙钟（并留一条降频 Warn）。
//
// ⚠️ 归属只判一次（首帧）：想用驱动方的时钟，**必须在首次 Tick 之前**写入 KeyNow，
// 否则本树接管该键、后续由本树推进（不会被中途出现的驱动方值打断）。
//
// ⚠️ 本 Tree 的节点持有可变状态（Selector 续跑位置、Limiter 窗口、Cooldown 时刻、
// Repeater 计数、Timeout 已耗时，以及上面自累加的 elapsed），因此**一棵树只服务一个 Agent**；
// 多 Agent 共用会把状态串在一起（引擎侧已改为每怪一棵树）。
func (t *Tree) Tick(b pkgbtree.Blackboard, dt time.Duration) pkgbtree.Status {
	b.Set(KeyDT, dt.Seconds())
	if !t.decided {
		_, has := bbNowRaw(b)
		t.ownNow = !has
		t.decided = true
	}
	if t.ownNow {
		t.elapsed += dt
		b.Set(KeyNow, LogicalTime(t.elapsed.Seconds()))
	}
	return t.root.Tick(b)
}
