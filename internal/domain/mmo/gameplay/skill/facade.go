// facade.go 是技能体系的**门面包装真身**：把 internal 的具体 *CooldownManager / *Set
// 适配成 `pkg/domain/mmo/skill` 的公开契约（`skill.CooldownManager` / `skill.Set`）。
//
// 为什么包装必须放在 internal：`pkg/**` 只允许做门面（别名 / 转发 / 极薄适配），
// 见 `结构规则.md` §5.1；这里的包装是行为实现体，所以真身在 internal，
// `pkg/domain/mmo/mmo.go` 只做变量转发。
package skill

import (
	object "github.com/qw576483/clover-server-engine/internal/domain/object"
	pkbuff "github.com/qw576483/clover-server-engine/pkg/domain/mmo/buff"
	pkcombat "github.com/qw576483/clover-server-engine/pkg/domain/mmo/combat"
	pkskill "github.com/qw576483/clover-server-engine/pkg/domain/mmo/skill"
)

// 编译期断言：内部 *CooldownManager 直接满足门面接口（方法集逐字同形）。
var _ pkskill.CooldownManager = (*CooldownManager)(nil)

// NewCooldownFacade 创建技能冷却管理器并以门面接口返回。
func NewCooldownFacade() pkskill.CooldownManager { return NewCooldown() }

// SetFacade 门面 skill.Set 接口的实现：包装 internal 的 *Set。
type SetFacade struct{ inner *Set }

// RegisterBuffDef 登记技能可能施加的 Buff 定义（供 Cast 内部查找）。
func (w *SetFacade) RegisterBuffDef(def pkbuff.Def) { w.inner.RegisterBuffDef(def) }

// Calculator 返回伤害计算器（门面接口）。
func (w *SetFacade) Calculator() pkcombat.Calculator { return w.inner.Calculator() }

// CanCast 判断技能是否可施放（冷却 / 目标合法性前置检查）。
func (w *SetFacade) CanCast(def pkskill.Def) bool { return w.inner.CanCast(def) }

// CanCastTarget 判断技能能否作用于给定目标：冷却 + 距离（AttrDistance vs Def.Range）+
// 目标类型合法性（AttrCamp / 自身同一性；属性缺失时该维度不参与校验，见内部实现注释）。
func (w *SetFacade) CanCastTarget(def pkskill.Def, src, dst *object.AttrSet) bool {
	return w.inner.CanCastTarget(def, src, dst)
}

// Cast 施放技能：串联冷却、伤害计算与 Buff 施加，返回逐目标结算结果。
func (w *SetFacade) Cast(def pkskill.Def, src, dst *object.AttrSet, buffs pkbuff.Container) ([]pkcombat.Result, error) {
	return w.inner.Cast(def, src, dst, buffs)
}

// 编译期断言：包装满足门面接口。
var _ pkskill.Set = (*SetFacade)(nil)

// InternalCooldownManagerFacade 把门面 CooldownManager 还原为 internal 的 *CooldownManager；
// 非底层则 ok=false。业务侧可见名 = `pkg/domain/mmo.InternalCooldownManager`。
func InternalCooldownManagerFacade(c pkskill.CooldownManager) (*CooldownManager, bool) {
	v, ok := c.(*CooldownManager)
	return v, ok
}

// NewSkillSetFacade 以冷却管理器构造技能集；cd 不是引擎实现（或为 nil）时返回 nil。
//
// 非引擎实现会被内部 `NewSkillSet(*CooldownManager)` 解引用 panic，此处显式拦下，
// 与 `pkg/domain/mmo` 原有门面行为逐字一致（曾返回 nil 而非半成品）。
func NewSkillSetFacade(cd pkskill.CooldownManager) pkskill.Set {
	ic, ok := cd.(*CooldownManager)
	if !ok || ic == nil {
		return nil
	}
	return &SetFacade{inner: NewSkillSet(ic)}
}
