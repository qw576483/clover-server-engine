// Package skill 实现引擎级技能定义/冷却/目标选择/施放流程（按通用骨架自实现）。
//
// 设计要点：
//   - 冷却由 CooldownManager 统一管理（按 CDGroup 分组，支持多技能共享冷却组），
//     内部状态由互斥锁保护：Tick 与 Begin/Ready 可能来自不同线程，裸 map 读写
//     是 data race，并可能触发 runtime "concurrent map writes" 直接崩溃。
//   - 技能集 Set 绑定一个 CooldownManager，施放前检查冷却，施放后扣冷却；
//     检查与扣减原子完成（BeginIfReady），避免并发施放绕过冷却。
//   - 施放效果（Effects）通过 Kind 路由：damage→combat 计算扣血；buff→buff.Container 施加；
//     heal→直接加血（夹到 MaxHP）。不自动触发死亡逻辑，仅由 combat.Result 返回。
//   - 目标选择保持引擎级通用：InRange 按距离函数筛选，不耦合具体几何库。
package skill

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/qw576483/clover-server-engine/internal/domain/mmo/gameplay/combat"
	"github.com/qw576483/clover-server-engine/internal/domain/object"
	pkgbuff "github.com/qw576483/clover-server-engine/pkg/domain/mmo/buff"
	pkgcombat "github.com/qw576483/clover-server-engine/pkg/domain/mmo/combat"
	pkgskill "github.com/qw576483/clover-server-engine/pkg/domain/mmo/skill"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

var ErrSilenced = errors.New("skill: caster silenced")

// skillFailf 技能包异常降频日志（首次全量 + 之后每 1000 条一条），防止频道刷屏。
var skillFailCount atomic.Uint64

func skillFailf(format string, args ...any) {
	n := skillFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// CooldownManager 技能冷却管理器（并发安全）。
type CooldownManager struct {
	mu        sync.Mutex
	remaining map[uint32]float64
}

func NewCooldown() *CooldownManager {
	return &CooldownManager{remaining: make(map[uint32]float64)}
}

func (c *CooldownManager) Begin(group uint32, cd float64) {
	if cd < 0 {
		cd = 0
	}
	c.mu.Lock()
	if c.remaining == nil {
		c.remaining = make(map[uint32]float64)
	}
	c.remaining[group] = cd
	c.mu.Unlock()
}

// beginIfReady 原子地「检查冷却就绪并立即扣减」，返回 false 表示仍在冷却中（未扣减）。
// 分开调 Ready 与 Begin 时，两次并发施放可同时通过检查（check-then-act），冷却被绕过。
func (c *CooldownManager) beginIfReady(group uint32, cd float64) bool {
	if cd < 0 {
		cd = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining == nil {
		c.remaining = make(map[uint32]float64)
	}
	if c.remaining[group] > 0 {
		return false
	}
	c.remaining[group] = cd
	return true
}

func (c *CooldownManager) Ready(group uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining == nil {
		return true
	}
	return c.remaining[group] <= 0
}

func (c *CooldownManager) Remaining(group uint32) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining == nil {
		return 0
	}
	r := c.remaining[group]
	if r < 0 {
		r = 0
	}
	return r
}

func (c *CooldownManager) Tick(dt float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining == nil {
		c.remaining = make(map[uint32]float64)
		return
	}
	for g, r := range c.remaining {
		if r <= 0 {
			continue
		}
		r -= dt
		if r < 0 {
			r = 0
		}
		c.remaining[g] = r
	}
}

// Set 技能集。buffDefs / calc 会被 Register / 惰性赋值与 Cast 并发访问，由 mu 统一保护
// （无同步保护时 RegisterBuffDef 写、Cast 读、Calculator 惰性赋值并发访问均为 data race）。
type Set struct {
	mu       sync.RWMutex
	cd       *CooldownManager
	calc     *combat.Calculator
	buffDefs map[uint32]pkgbuff.Def
}

func NewSkillSet(cd *CooldownManager) *Set {
	if cd == nil {
		// 传 nil 时原样保存会让 CanCast/Cast 对 nil 的 *CooldownManager 解引用 panic。
		cd = NewCooldown()
	}
	return &Set{
		cd:       cd,
		calc:     combat.New(),
		buffDefs: make(map[uint32]pkgbuff.Def),
	}
}

func (s *Set) RegisterBuffDef(def pkgbuff.Def) {
	s.mu.Lock()
	if s.buffDefs == nil {
		s.buffDefs = make(map[uint32]pkgbuff.Def)
	}
	s.buffDefs[def.ID] = def
	s.mu.Unlock()
}

func (s *Set) Calculator() *combat.Calculator {
	s.mu.RLock()
	c := s.calc
	s.mu.RUnlock()
	if c != nil {
		return c
	}
	s.mu.Lock()
	if s.calc == nil {
		s.calc = combat.New()
	}
	c = s.calc
	s.mu.Unlock()
	return c
}

// cdOrNew 返回冷却管理器；零值 Set（未经 NewSkillSet）惰性补一个，
// 避免对 nil 的 *CooldownManager 解引用 panic。
func (s *Set) cdOrNew() *CooldownManager {
	s.mu.RLock()
	c := s.cd
	s.mu.RUnlock()
	if c != nil {
		return c
	}
	s.mu.Lock()
	if s.cd == nil {
		s.cd = NewCooldown()
	}
	c = s.cd
	s.mu.Unlock()
	return c
}

func (s *Set) CanCast(def pkgskill.Def) bool {
	return s.cdOrNew().Ready(def.CDGroup)
}

// CanCastTarget 判断技能能否作用于给定目标：施法者侧条件（冷却）+ 目标侧条件（距离 / 目标类型）。
//
// 距离与阵营的载体是**引擎共识属性**（见 combat.AttrDistance / combat.AttrCamp）：
//   - 距离：目标 AttrSet 的 AttrDistance（世界单位，与 mob.AttackRange / ChaseRange 同单位）。
//     def.Range > 0 且该属性**已定义**时，距离 > def.Range 即超距 ⇒ 不可施放；
//     属性未定义说明调用方没有几何信息 ⇒ 该维度跳过校验（保持既有调用方行为），仅留一条降频日志。
//   - 目标类型：TargetSelf 要求 src 与 dst 是同一个 AttrSet；TargetEnemy / TargetAlly 要求双方
//     AttrCamp 都已定义且「不同 / 相同」；TargetPoint 不做阵营约束。
//     任一方缺 AttrCamp ⇒ 无法判定阵营 ⇒ 该维度跳过（同上，留痕）。
//   - 未知 TargetType（配置写错）：不默认放行，留痕并拒绝。
func (s *Set) CanCastTarget(def pkgskill.Def, src, dst *object.AttrSet) bool {
	if src == nil || dst == nil {
		return false
	}
	if !s.cdOrNew().Ready(def.CDGroup) {
		return false
	}
	if !targetInRange(def, dst) {
		return false
	}
	return targetTypeOK(def, src, dst)
}

// targetInRange 校验施法距离（只读目标侧的 AttrDistance；无几何信息时跳过并留痕）。
func targetInRange(def pkgskill.Def, dst *object.AttrSet) bool {
	if def.Range <= 0 {
		return true // 未配 Range = 无距离限制
	}
	if !dst.Has(combat.AttrDistance) {
		skillFailf("skill: def=%d 配了 Range=%.2f 但目标无 attr %d(AttrDistance)，距离维度跳过校验", def.ID, def.Range, combat.AttrDistance)
		return true
	}
	if dist := dst.Get(combat.AttrDistance); dist > def.Range {
		skillFailf("skill: def=%d 超距 目标距离=%.2f > Range=%.2f，拒绝施放", def.ID, dist, def.Range)
		return false
	}
	return true
}

// targetTypeOK 校验目标类型合法性（阵营缺失时跳过，未知类型一律拒绝）。
func targetTypeOK(def pkgskill.Def, src, dst *object.AttrSet) bool {
	switch def.TargetType {
	case pkgskill.TargetSelf:
		if src != dst {
			skillFailf("skill: def=%d TargetSelf 但目标不是施法者自身，拒绝施放", def.ID)
			return false
		}
	case pkgskill.TargetEnemy, pkgskill.TargetAlly:
		srcCamp, okSrc := campOf(src)
		dstCamp, okDst := campOf(dst)
		if !okSrc || !okDst {
			skillFailf("skill: def=%d 阵营属性 attr %d(AttrCamp) 缺失（src ok=%v dst ok=%v），目标类型维度跳过校验",
				def.ID, combat.AttrCamp, okSrc, okDst)
			return true
		}
		same := srcCamp == dstCamp
		if def.TargetType == pkgskill.TargetEnemy && same {
			skillFailf("skill: def=%d TargetEnemy 但目标是同阵营 camp=%v，拒绝施放", def.ID, dstCamp)
			return false
		}
		if def.TargetType == pkgskill.TargetAlly && !same {
			skillFailf("skill: def=%d TargetAlly 但目标异阵营 src=%v dst=%v，拒绝施放", def.ID, srcCamp, dstCamp)
			return false
		}
	case pkgskill.TargetPoint:
		// 点名施放：不限定阵营，距离仍由 targetInRange 负责。
	default:
		skillFailf("skill: 未知 TargetType %d（def=%d），已拒绝施放", def.TargetType, def.ID)
		return false
	}
	return true
}

// campOf 读阵营属性；未定义（或 as 为 nil）时 ok=false，表示「无法判定阵营」。
func campOf(as *object.AttrSet) (float64, bool) {
	if as == nil || !as.Has(combat.AttrCamp) {
		return 0, false
	}
	return as.Get(combat.AttrCamp), true
}

func (s *Set) Cast(def pkgskill.Def, src, dst *object.AttrSet, buffs pkgbuff.Container) ([]pkgcombat.Result, error) {
	if src == nil || dst == nil {
		return nil, errors.New("skill cast: nil src or dst")
	}
	if buffs != nil && buffs.HasFlag(pkgbuff.FlagSilence) {
		return nil, ErrSilenced
	}
	// 检查与扣减必须原子（见 beginIfReady）：分开两步可被并发施放绕过冷却。
	if !s.cdOrNew().beginIfReady(def.CDGroup, def.CD) {
		return nil, errors.New("skill cooldown not ready")
	}

	invincible := buffs != nil && buffs.HasFlag(pkgbuff.FlagInvincible)

	var results []pkgcombat.Result
	dead := false
	for _, e := range def.Effects {
		if dead || dst.Get(combat.AttrHP) <= 0 {
			break
		}
		switch e.Kind {
		case "damage":
			if invincible {
				results = append(results, pkgcombat.Result{Hit: false, Damage: 0, Crit: false, Dead: false, TargetHP: dst.Get(combat.AttrHP)})
				continue
			}
			r := s.Calculator().Apply(src, dst, e.Value)
			results = append(results, r)
			if r.Dead {
				dead = true
			}
		case "buff":
			if buffs != nil {
				s.mu.RLock()
				bd, ok := s.buffDefs[e.Ref]
				s.mu.RUnlock()
				if !ok {
					// 技能配置缺失：静默跳过后技能完全无效果且无任何提示，必须留痕。
					skillFailf("skill: buff 效果引用的定义不存在（def=%d ref=%d），已跳过", def.ID, e.Ref)
					continue
				}
				buffs.Apply(bd)
			}
		case "heal":
			if e.Value < 0 {
				// 非法配置（负治疗）：静默 continue 会让漏配无从发现。
				skillFailf("skill: heal 效果数值为负（def=%d value=%v），已跳过", def.ID, e.Value)
				continue
			}
			hp := dst.Get(combat.AttrHP) + e.Value
			if hp < 0 {
				hp = 0
			}
			if maxhp := dst.Get(combat.AttrMaxHP); maxhp > 0 && hp > maxhp {
				hp = maxhp
			}
			dst.Set(combat.AttrHP, hp)
		default:
			// 未知 Kind 若被静默忽略，配置写错时技能完全无效果且无任何提示。
			skillFailf("skill: 未知 effect kind %q（def=%d ref=%d），已忽略", e.Kind, def.ID, e.Ref)
		}
	}
	return results, nil
}

func InRange(targets []uint64, dist func(uint64) float64, maxRange float64) []uint64 {
	out := make([]uint64, 0, len(targets))
	for _, t := range targets {
		if d := dist(t); d <= maxRange {
			out = append(out, t)
		}
	}
	return out
}
