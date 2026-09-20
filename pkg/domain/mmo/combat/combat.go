// Package combat 战斗伤害公式内核的公开门面层（类型定义 + 薄透传）。
//
// 本包定义核心类型（Result, Formula, VarianceStrategy, CritStrategy），
// 业务/框架层统一从本包引用战斗能力：
//
//	c := combat.New()
//	res := c.Apply(src, dst, 100) // src/dst 为 *object.AttrSet
//	if res.Dead { /* 由上层处理死亡 */ }
package combat

import (
	object "github.com/qw576483/clover-server-engine/pkg/domain/object"
)

// Result 单次 Apply 的结果快照。
type Result struct {
	Hit      bool    // 是否命中（未配置 HitStrategy 时恒为 true）
	Damage   float64 // 实际造成伤害（已含暴击与方差）
	Crit     bool    // 是否暴击
	Dead     bool    // 目标是否因此次伤害致死（HP<=0）
	TargetHP float64 // 结算后目标剩余 HP
}

// Formula 基础伤害公式：输入来源/目标属性集与基础值，返回基础伤害。
// 业务可自定义（如不同职业/技能用不同系数），通过 Calculator.SetFormula 注入。
type Formula func(src, dst *object.AttrSet, base float64) float64

// VarianceStrategy 伤害方差策略：输入公式计算后的基础伤害，返回叠加方差后的伤害值。
// 默认实现为 [0.9, 1.1] 均匀随机，业务可替换为固定值/高斯分布等。
type VarianceStrategy func(base float64) float64

// CritStrategy 暴击策略：输入暴击率（已从 src AttrCrit 读取并夹紧至 [0,1]）
// 与当前伤害值，返回 (是否暴击, 暴击后伤害)。默认实现为 roll 概率 ×1.5。
type CritStrategy func(chance, damage float64) (crit bool, final float64)

// HitStrategy 命中判定策略：返回本次攻击是否命中目标。
//
// 未配置（nil）时引擎视为「必定命中」—— 与历史行为一致，不影响现有玩法。
// 业务用它实现命中率 / 闪避：返回 false 时本次**完全不结算**
// （Result.Hit=false、Damage=0、不叠加方差、不 roll 暴击、HP 不变）。
//
// 与 CritStrategy 的顺序是「**先命中、后暴击**」：打不中就不该再 roll 暴击。
//
// 帧同步注意：命中判定在服务端执行；若玩法是客户端确定性重演，
// 策略内部必须使用确定性随机源（pkg/shared/rand 的 NewSeededSource），
// 否则各端结果会发散。
type HitStrategy func(src, dst *object.AttrSet) bool

// Calculator 伤害计算器（接口。方法承载型，由 internal/gameplay/combat 的 *Calculator 直接满足）。
//
// F12 停在接口上即可看到完整方法集；浏览内部实现字段（formula/variance/crit）需进 internal。
type Calculator interface {
	// SetFormula 替换基础伤害公式（可插拔）。
	SetFormula(f Formula)
	// SetVarianceStrategy 替换伤害方差策略（nil 表示关闭方差）。
	SetVarianceStrategy(v VarianceStrategy)
	// SetCritStrategy 替换暴击策略（nil 恢复为默认）。
	SetCritStrategy(cs CritStrategy)
	// SetHitStrategy 替换命中判定策略（nil = 必定命中，与默认一致）。
	SetHitStrategy(hs HitStrategy)
	// Compute 仅用公式算基础伤害（不含暴击/方差/扣血，确定性预览）。
	Compute(src, dst *object.AttrSet, base float64) float64
	// Apply 计算并应用一次伤害，直接扣血并返回结果快照。
	Apply(src, dst *object.AttrSet, base float64) Result
}

// 公共属性 ID 常量（供全引擎复用）。
var (
	// AttrHP 当前生命。
	AttrHP object.AttrID = 1
	// AttrMaxHP 最大生命。
	AttrMaxHP object.AttrID = 2
	// AttrATK 攻击。
	AttrATK object.AttrID = 3
	// AttrDEF 防御。
	AttrDEF object.AttrID = 4
	// AttrSpeed 速度/移动。
	AttrSpeed object.AttrID = 5
	// AttrCrit 暴击率。
	AttrCrit object.AttrID = 6
	// AttrDistance 施法者到目标的距离（世界单位）。技能施放校验（CanCastTarget）用它比对 Def.Range；
	// 目标 AttrSet 未定义该属性时距离维度不参与校验。真身见 internal/domain/mmo/gameplay/combat。
	AttrDistance object.AttrID = 7
	// AttrCamp 阵营标识：同值 = 同阵营，供技能 TargetEnemy / TargetAlly 判定目标合法性。
	// 任一方未定义该属性时阵营维度不参与校验。
	AttrCamp object.AttrID = 8
)
