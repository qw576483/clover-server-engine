// Package skill 技能定义/冷却/施放流程的公开门面层。
//
// 本包定义公开类型（TargetType/EffectRef/Def/CooldownManager/Set 接口）与构造函数，
// 不含实现也不 import internal；setImpl 桥接器位于 pkg/domain/mmo 包。
//
// 业务/框架层统一从本包引用技能能力：
//
//	cd := skill.NewCooldown()
//	ss := skill.NewSkillSet(cd)
//	if ss.CanCast(skillDef) {
//	    ss.Cast(skillDef, src, dst, buffs)
//	}
package skill

import (
	buffpkg "github.com/qw576483/clover-server-engine/pkg/domain/mmo/buff"
	combatpkg "github.com/qw576483/clover-server-engine/pkg/domain/mmo/combat"
	object "github.com/qw576483/clover-server-engine/pkg/domain/object"
)

// TargetType 技能目标类型。
type TargetType int

const (
	TargetEnemy TargetType = iota
	TargetAlly
	TargetSelf
	TargetPoint
)

// EffectRef 施放产生的效果描述。
type EffectRef struct {
	Kind  string
	Ref   uint32
	Value float64
}

// Def 技能定义（来自配置）。
type Def struct {
	ID         uint32
	Name       string
	CDGroup    uint32
	CD         float64
	CastTime   float64
	Range      float64
	TargetType TargetType
	Effects    []EffectRef
}

// CooldownManager 冷却管理器（接口。方法承载型，由 internal/gameplay/skill 的 *CooldownManager 直接满足）。
type CooldownManager interface {
	Begin(group uint32, cd float64)
	Ready(group uint32) bool
	Remaining(group uint32) float64
	Tick(dt float64)
}

// Set 技能集（接口。由 pkg/domain/mmo 的 setImpl 桥接实现）。
type Set interface {
	RegisterBuffDef(def buffpkg.Def)
	Calculator() combatpkg.Calculator
	// CanCast 只判冷却 / 沉默一类的施法者侧前置条件。
	CanCast(def Def) bool
	// CanCastTarget 在 CanCast 的基础上再校验「目标侧」条件：
	//   - 距离：读目标的 combatpkg.AttrDistance 与 Def.Range 比较（属性未定义 = 不校验该维度）；
	//   - 目标类型：TargetSelf 要求 src == dst（同一个 AttrSet）；
	//     TargetEnemy / TargetAlly 读双方 combatpkg.AttrCamp 比较阵营（任一方未定义 = 不校验该维度）；
	//     TargetPoint 不做阵营约束。
	CanCastTarget(def Def, src, dst *object.AttrSet) bool
	Cast(def Def, src, dst *object.AttrSet, buffs buffpkg.Container) ([]combatpkg.Result, error)
}
