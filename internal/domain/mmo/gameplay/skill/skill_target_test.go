package skill

import (
	"testing"

	"github.com/qw576483/clover-server-engine/internal/domain/mmo/gameplay/combat"
	object "github.com/qw576483/clover-server-engine/internal/domain/object"
	pkgskill "github.com/qw576483/clover-server-engine/pkg/domain/mmo/skill"
)

// mkAttr 用「属性 → 值」构造 AttrSet：**未列出的属性不定义**（Has=false）。
// 这个区别是本组用例的关键 —— 引擎用「属性是否定义」来区分
// 「没有几何/阵营信息（不校验该维度）」与「值是 0（参与校验）」。
func mkAttr(kv map[object.AttrID]float64) *object.AttrSet {
	defs := make([]object.AttrDef, 0, len(kv))
	for id := range kv {
		defs = append(defs, object.AttrDef{ID: id, Kind: object.AttrKindFloat})
	}
	s := object.NewAttrSet(defs...)
	for id, v := range kv {
		s.Set(id, v)
	}
	return s
}

// TestCanCastTargetValidatesRange：超距必须拒绝，且在范围内必须放行。
func TestCanCastTargetValidatesRange(t *testing.T) {
	ss := NewSkillSet(NewCooldown())
	def := pkgskill.Def{ID: 1, Range: 5, TargetType: pkgskill.TargetEnemy}
	src := mkAttr(map[object.AttrID]float64{combat.AttrCamp: 1})
	near := mkAttr(map[object.AttrID]float64{combat.AttrDistance: 4, combat.AttrCamp: 2})
	far := mkAttr(map[object.AttrID]float64{combat.AttrDistance: 6, combat.AttrCamp: 2})
	if !ss.CanCastTarget(def, src, near) {
		t.Fatal("距离 4 ≤ Range 5、目标为敌阵营：应可施放")
	}
	if ss.CanCastTarget(def, src, far) {
		t.Fatal("距离 6 > Range 5：超距必须拒绝施放")
	}
}

// TestCanCastTargetRangeSkippedWithoutAttribute：目标没有 AttrDistance 时该维度跳过校验
// （无几何信息不猜），保持既有调用方（不写距离属性）的行为不变。
func TestCanCastTargetRangeSkippedWithoutAttribute(t *testing.T) {
	ss := NewSkillSet(NewCooldown())
	def := pkgskill.Def{ID: 1, Range: 0.1, TargetType: pkgskill.TargetPoint}
	src := mkAttr(map[object.AttrID]float64{})
	dst := mkAttr(map[object.AttrID]float64{})
	if !ss.CanCastTarget(def, src, dst) {
		t.Fatal("无距离属性时距离维度应跳过校验（既有调用方行为不变）")
	}
}

// TestCanCastTargetValidatesTargetType：目标类型与阵营/自身同一性必须被校验。
func TestCanCastTargetValidatesTargetType(t *testing.T) {
	ss := NewSkillSet(NewCooldown())
	me := mkAttr(map[object.AttrID]float64{combat.AttrCamp: 1})
	ally := mkAttr(map[object.AttrID]float64{combat.AttrCamp: 1})
	enemy := mkAttr(map[object.AttrID]float64{combat.AttrCamp: 2})

	atEnemy := pkgskill.Def{ID: 10, TargetType: pkgskill.TargetEnemy}
	if !ss.CanCastTarget(atEnemy, me, enemy) {
		t.Fatal("TargetEnemy 打异阵营：应可施放")
	}
	if ss.CanCastTarget(atEnemy, me, ally) {
		t.Fatal("TargetEnemy 打同阵营：必须拒绝（非法目标）")
	}

	atAlly := pkgskill.Def{ID: 11, TargetType: pkgskill.TargetAlly}
	if !ss.CanCastTarget(atAlly, me, ally) {
		t.Fatal("TargetAlly 对同阵营：应可施放")
	}
	if ss.CanCastTarget(atAlly, me, enemy) {
		t.Fatal("TargetAlly 对异阵营：必须拒绝（非法目标）")
	}

	atSelf := pkgskill.Def{ID: 12, TargetType: pkgskill.TargetSelf}
	if !ss.CanCastTarget(atSelf, me, me) {
		t.Fatal("TargetSelf 指向自身：应可施放")
	}
	if ss.CanCastTarget(atSelf, me, ally) {
		t.Fatal("TargetSelf 指向他人：必须拒绝（非法目标）")
	}
}

// TestCanCastTargetCampSkippedWithoutAttribute：任一方缺 AttrCamp 时阵营维度跳过校验
// （无法判定阵营时不擅自拒绝，保持既有调用方行为）。
func TestCanCastTargetCampSkippedWithoutAttribute(t *testing.T) {
	ss := NewSkillSet(NewCooldown())
	noCamp := mkAttr(map[object.AttrID]float64{})
	withCamp := mkAttr(map[object.AttrID]float64{combat.AttrCamp: 1})
	def := pkgskill.Def{ID: 20, TargetType: pkgskill.TargetEnemy}
	if !ss.CanCastTarget(def, withCamp, noCamp) {
		t.Fatal("目标无阵营属性：阵营维度应跳过校验，不得擅自拒绝")
	}
	if !ss.CanCastTarget(def, noCamp, withCamp) {
		t.Fatal("施法者无阵营属性：阵营维度应跳过校验，不得擅自拒绝")
	}
}

// TestCanCastTargetRejectsUnknownTargetType：未知 TargetType（配置写错）不许默认放行。
func TestCanCastTargetRejectsUnknownTargetType(t *testing.T) {
	ss := NewSkillSet(NewCooldown())
	def := pkgskill.Def{ID: 30, TargetType: pkgskill.TargetType(99)}
	a := mkAttr(map[object.AttrID]float64{})
	if ss.CanCastTarget(def, a, a) {
		t.Fatal("未知 TargetType 必须拒绝（配置写错不许静默放行）")
	}
}

// TestCanCastTargetKeepsNilAndCooldownChecks：既有的 nil 判空与冷却检查不许被改动。
func TestCanCastTargetKeepsNilAndCooldownChecks(t *testing.T) {
	cd := NewCooldown()
	ss := NewSkillSet(cd)
	def := pkgskill.Def{ID: 40, CDGroup: 7, CD: 5, TargetType: pkgskill.TargetSelf}
	a := mkAttr(map[object.AttrID]float64{})
	if ss.CanCastTarget(def, nil, a) || ss.CanCastTarget(def, a, nil) {
		t.Fatal("src/dst 为 nil：必须返回 false")
	}
	if !ss.CanCastTarget(def, a, a) {
		t.Fatal("冷却就绪 + 自身目标：应可施放")
	}
	cd.Begin(7, 5) // 未 Tick 过的冷却组处于冷却中
	if ss.CanCastTarget(def, a, a) {
		t.Fatal("冷却中：必须返回 false")
	}
}
