// Package combat 实现引擎级伤害/治疗计算内核，按通用战斗伤害公式骨架自实现。
//
// 设计要点：
// - 公式（Formula）可插拔：业务可自定义基础伤害算法；未配置时使用内置默认公式。
// - 计算器（Calculator）负责「计算公式 → 判定暴击 → 扣血并夹紧下限 0」的完整流程，
// 但绝不自动触发死亡逻辑，仅通过 Result.Dead 返回标记，由上层（如 mob/entity）决定后续行为。
// - 公共属性 ID 常量（AttrHP/AttrMaxHP/...）集中定义，供 buff/skill/业务复用，避免 magic number。
// - 纯标准库，只依赖 internal/domain/object。
package combat

import (
	"math"
	"sync"
	"sync/atomic"

	"clover-server-engine/internal/domain/object"
	pkgcombat "clover-server-engine/pkg/domain/mmo/combat"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/rand"
)

// combatRand 包级加密安全随机源，用于战斗方差与暴击判定。
var combatRand = rand.NewSource()

// combatFailf 是战斗内核的异常降频日志（首次全量 + 之后每 1000 条一条）。
// Apply 可能被高频调用（乱斗/持续伤害），逐条打日志会刷屏。
var combatFailCount atomic.Uint64

func combatFailf(format string, args ...any) {
	n := combatFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// defaultFormula 内置确定性默认公式（无随机方差；方差只在 Apply 中叠加）。
// 独立成包级函数：SetFormula(nil) 与零值 Calculator 都需要回落到它。
func defaultFormula(src, dst *object.AttrSet, base float64) float64 {
	if base <= 0 {
		return 0
	}
	srcATK := src.Get(AttrATK)
	dstDEF := dst.Get(AttrDEF)
	// 减伤系数夹紧到 [0,1]，避免 dstDEF>=200 时系数变负；
	// 高防至多减到 0（即免伤，仍受下限 1 兜底），不会出现负伤害。
	mitigation := math.Max(0, math.Min(1, 1-dstDEF*0.005))
	return math.Max(1, base*(1+srcATK*0.01)*mitigation)
}

// 公共属性 ID 常量（供全引擎复用）。
// 注意：这些 ID 是引擎共识，业务与 buff/skill 包应统一引用此处定义。
var (
	AttrHP    object.AttrID = 1 // 当前生命
	AttrMaxHP object.AttrID = 2 // 最大生命
	AttrATK   object.AttrID = 3 // 攻击（默认公式按 +1% 每点）
	AttrDEF   object.AttrID = 4 // 防御（默认公式按 -0.5 每点）
	AttrSpeed object.AttrID = 5 // 速度/移动
	AttrCrit  object.AttrID = 6 // 暴击率（概率，0.2 表示 20%）
	// AttrDistance 施法者到目标的距离（世界单位，与 mob 的 AttackRange / ChaseRange 同一单位体系）。
	// 技能施放校验（skill.CanCastTarget）用它比对 def.Range；
	// **目标 AttrSet 里没定义这个属性时该维度不参与校验**（没有几何信息，不猜）。
	AttrDistance object.AttrID = 7
	// AttrCamp 阵营标识：同名值 = 同阵营，供技能 TargetEnemy / TargetAlly 判定目标合法性。
	// 施法者与目标**任一方**没定义这个属性时阵营维度不参与校验。
	AttrCamp object.AttrID = 8
)

// VarianceStrategy、CritStrategy、Result 和 Formula 类型现在定义在 pkg/domain/mmo/combat 包中
//
// DefaultVariance 内置默认方差策略：[0.9, 1.1] 均匀随机。
func DefaultVariance(base float64) float64 {
	return base * (0.9 + combatRand.Float64()*0.2)
}

// DefaultCrit 内置默认暴击策略：roll 概率判定，暴击 ×1.5。
func DefaultCrit(chance, damage float64) (bool, float64) {
	if combatRand.Float64() < chance {
		return true, damage * 1.5
	}
	return false, damage
}

// Calculator 伤害计算器，持有当前生效公式与暴击/方差策略。
// mu 保护 formula 与策略字段的读/写。
//
// 随机源说明：随机方差与暴击判定使用 util/rand 加密安全随机源（crypto/rand），
// 天然 goroutine 安全，伤害 roll 无需串行化。
//
// 确定性说明：内置默认公式（formula）本身是纯确定性的——不含随机方差，
// 方差只在 Apply 中叠加。这样 Compute（预览）返回的正是"无方差期望伤害"，
// 与 Apply 实际结果的中值一致，避免"预览≠实际"。
type Calculator struct {
	mu       sync.Mutex
	formula  pkgcombat.Formula
	variance pkgcombat.VarianceStrategy
	crit     pkgcombat.CritStrategy
	hit      pkgcombat.HitStrategy

	// applyMu 串行化「HP 读-算-写」结算段。
	// AttrSet 的 Get/Set 各自加锁，但两者之间没有任何互斥：两次并发 Apply
	// 会读到同一 HP 再各自写回（后写覆盖先写 → 丢伤害）。公式/方差/暴击
	// （业务可插拔代码）都在本锁之外执行，持锁区间只包含纯数值结算。
	applyMu sync.Mutex
}

// New 创建计算器，使用内置确定性默认公式、默认方差策略与默认暴击策略。
func New() *Calculator {
	return &Calculator{
		formula:  defaultFormula,
		variance: DefaultVariance,
		crit:     DefaultCrit,
	}
}

// SetFormula 替换默认公式（可插拔）；传 nil 回落到内置默认公式。
// 此前不做 nil 校验也不回落：传入 nil 后 Compute/Apply 调用 c.formula 直接 panic，
// 与 SetCritStrategy(nil) 回落默认实现的口径也不一致。
// 注意：若自定义公式内部使用随机数，Compute/Apply 的一致性由业务自行保证。
func (c *Calculator) SetFormula(f pkgcombat.Formula) {
	if f == nil {
		f = defaultFormula
	}
	c.mu.Lock()
	c.formula = f
	c.mu.Unlock()
}

// SetVarianceStrategy 替换方差策略。
// nil 表示关闭方差（Apply 不再叠加随机波动）。
func (c *Calculator) SetVarianceStrategy(v pkgcombat.VarianceStrategy) {
	c.mu.Lock()
	c.variance = v
	c.mu.Unlock()
}

// SetCritStrategy 替换暴击策略（nil 恢复为默认 DefaultCrit）。
func (c *Calculator) SetCritStrategy(cs pkgcombat.CritStrategy) {
	c.mu.Lock()
	if cs == nil {
		c.crit = DefaultCrit
	} else {
		c.crit = cs
	}
	c.mu.Unlock()
}

// SetHitStrategy 替换命中判定策略（nil = 不做命中判定，一律命中）。
//
// 未配置时引擎不 roll 命中，Result.Hit 恒为 true；配置后由业务策略决定是否结算。
func (c *Calculator) SetHitStrategy(hs pkgcombat.HitStrategy) {
	c.mu.Lock()
	c.hit = hs
	c.mu.Unlock()
}

// Compute 仅用公式算基础伤害（不含暴击/方差/扣血，确定性预览）。
func (c *Calculator) Compute(src, dst *object.AttrSet, base float64) float64 {
	c.mu.Lock()
	f := c.formula
	c.mu.Unlock()
	if f == nil {
		// 零值 Calculator{}（未经 New）没有公式：回落默认，避免直接 panic。
		f = defaultFormula
	}
	return f(src, dst, base)
}

// Apply 计算并应用一次伤害：命中判定 → 基础伤害 = 公式结果 → 叠加方差策略 →
// 暴击策略（按来源 AttrCrit 概率）→ 目标 HP -= 伤害（夹紧至 0~MaxHP）→ 返回 Result。
// 不触发任何死亡逻辑，仅以 Dead 标记致死。目标已死亡（HP<=0）时跳过结算。
//
// 命中判定排在最前（未配置 HitStrategy 时跳过）：未命中则**完全不进入后续结算** ——
// 不叠加方差、不 roll 暴击、不扣血，只回报 Hit=false 供上层做表现（miss 飘字等）。
func (c *Calculator) Apply(src, dst *object.AttrSet, base float64) pkgcombat.Result {
	if hp := dst.Get(AttrHP); hp <= 0 {
		// 两种情况都会命中这里：目标真已死亡，或目标的属性集根本没初始化 HP
		// （Get 对未定义属性返回 0）——后者会被误判为"已死亡"、本次伤害被静默丢弃。
		// 无法从数值上区分两者，至少降频留痕，避免"伤害没生效"在线上无从追查。
		combatFailf("combat: 目标 AttrHP=%.1f<=0（已死亡或属性集未初始化 HP），本次伤害跳过", hp)
		return pkgcombat.Result{Hit: false, Dead: true, TargetHP: 0}
	}

	c.mu.Lock()
	formula := c.formula
	variance := c.variance
	crit := c.crit
	hit := c.hit
	c.mu.Unlock()
	if formula == nil {
		formula = defaultFormula // 零值 Calculator{}
	}
	if crit == nil {
		// crit 此前只在 New() 中赋值：零值 Calculator{} 直接调用会 panic
		//（同处 variance/hit 都有判空，唯独 crit 没有）。零值回落默认暴击策略。
		crit = DefaultCrit
	}
	dmg := formula(src, dst, base)

	// 先命中后暴击：打不中就不该再 roll 暴击。
	// TargetHP 回填当前值——未命中不是"打到 0 血"，上层据此区分未命中与致死。
	if hit != nil && !hit(src, dst) {
		return pkgcombat.Result{Hit: false, TargetHP: dst.Get(AttrHP)}
	}

	if variance != nil {
		dmg = variance(dmg)
	}

	// 暴击率夹紧至 [0,1]，避免配置误填百分比导致恒暴击。
	critChance := math.Max(0, math.Min(1, src.Get(AttrCrit)))
	critHit, dmg := crit(critChance, dmg)
	if math.IsNaN(dmg) || math.IsInf(dmg, 0) {
		// 非有限伤害绝不写回：NaN 同时通不过 hp<0 与 hp>maxhp 两个比较，
		// 会直接把 AttrHP 污染成 NaN，之后所有数值比较全部静默失效。
		combatFailf("combat: 伤害计算产出非有限值 dmg=%v (base=%v)，本次结算跳过", dmg, base)
		return pkgcombat.Result{Hit: false, TargetHP: dst.Get(AttrHP)}
	}

	// 原子读-算-写（applyMu 见结构体注释）：两次并发 Apply 不得读到同一 HP 各写各的。
	c.applyMu.Lock()
	hp := dst.Get(AttrHP) - dmg
	if hp < 0 {
		hp = 0
	}
	if maxhp := dst.Get(AttrMaxHP); maxhp > 0 && hp > maxhp {
		hp = maxhp
	}
	dst.Set(AttrHP, hp)
	c.applyMu.Unlock()
	return pkgcombat.Result{
		Hit:      true,
		Damage:   dmg,
		Crit:     critHit,
		Dead:     hp <= 0,
		TargetHP: hp,
	}
}
