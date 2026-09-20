// Package buff 实现引擎级状态效果容器，消费 `buff_*.json` 配置的运行时（按通用骨架自实现）。
//
// 设计要点：
// - 一个 Container 绑定一个 object.AttrSet，是 Buff 对属性的唯一写入方。
// - 施加 Modifiers 时记录「实际施加量」快照，移除时按快照精确回退（不影响其它来源改动）。
// - 百分比 Modifier 按「回退已记录量后的基值」计算（叠加层间不做 compound），
// 移除时按快照精确回退；叠加加层与 Tick 间隔刷新走同一重算口径，
// 使 Buff 的相对贡献随底层属性变化保持正确、数值不会来回跳变，且不会无界增长。
// - 纯标准库，只依赖 internal/domain/object。
package buff

import (
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
	pkgbuff "github.com/qw576483/clover-server-engine/pkg/domain/mmo/buff"
)

// Container Buff 容器，绑定一个 object.AttrSet。
type Container struct {
	mu   sync.Mutex
	set  *object.AttrSet
	byID map[uint32]*pkgbuff.Instance
}

// NewContainer 以属性集构造容器。
func NewContainer(set *object.AttrSet) *Container {
	return &Container{mu: sync.Mutex{}, set: set, byID: make(map[uint32]*pkgbuff.Instance)}
}

// revertAll 回退整实例（所有层数）已记录的影响。
func (c *Container) revertAll(inst *pkgbuff.Instance) {
	for attrID, amt := range inst.Applied {
		c.set.Add(attrID, -amt)
	}
}

// reapplyAll 先回退全部已记录量，再按当前层数重新施加（保持叠加层数正确；
// 百分比按回退后快照的基值统一重算，避免多层层间 compound）。
func (c *Container) reapplyAll(inst *pkgbuff.Instance) {
	// 先回退已施加量，然后快照基值（百分比必须基于回退后原始值统一计算，防止逐层 compound）。
	for attrID, amt := range inst.Applied {
		c.set.Add(attrID, -amt)
	}
	// 快照所有相关属性的基础值。
	bases := make(map[object.AttrID]float64, len(inst.Def.Modifiers))
	for _, m := range inst.Def.Modifiers {
		bases[m.Attr] = c.set.Get(m.Attr)
	}
	inst.Applied = make(map[object.AttrID]float64, len(inst.Def.Modifiers))
	// 统一施加所有层：每层百分比均使用快照基值计算。
	for i := 0; i < inst.Stacks; i++ {
		for _, m := range inst.Def.Modifiers {
			var amt float64
			if m.IsPercent {
				amt = bases[m.Attr] * m.Delta
			} else {
				amt = m.Delta
			}
			c.set.Add(m.Attr, amt)
			inst.Applied[m.Attr] += amt
		}
	}
}

// Apply 应用 Buff：若同 ID 已存在且未超 Stack 则叠加一层并刷新时长；
// 否则新建实例。施加时即对 object.AttrSet 写入 Modifiers（记录快照以便移除回退）。
func (c *Container) Apply(def pkgbuff.Def) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if inst, ok := c.byID[def.ID]; ok {
		// 以已存实例的 Stack 上限为准（防热更/配置变更导致上限跳变）：
		// 新传入 def 的 Stack 不参与，包括「原不可叠加(<=0)→可叠加」的热更路径
		//（inst.Def.Stack<=0 一律视为不可叠加）——注释与行为必须一致。
		maxStack := inst.Def.Stack
		if maxStack <= 0 {
			maxStack = 1
		}
		if maxStack > 1 && inst.Stacks < maxStack {
			inst.Stacks++
		}
		// 统一走 reapplyAll（含新增一层与已达上限两种情况）：百分比按回退后基值
		// 统一重算，与 Tick 周期刷新同一口径。若新增层时按「当前值」直接加一层，
		// 属性会逐层 compound（(1+Δ)^n）、下次刷新又被打回，数值来回跳变。
		c.reapplyAll(inst)
		// 时长同样以已存实例的 Def 为准：混用新传入 def 的 Duration/Interval
		// 会造成「时长按新配置、效果按旧配置」的混合口径。
		inst.Remain = inst.Def.Duration
		inst.TickRemain = inst.Def.Interval
		return
	}
	inst := &pkgbuff.Instance{
		Def:        def,
		Remain:     def.Duration,
		TickRemain: def.Interval,
		Stacks:     1,
		Applied:    make(map[object.AttrID]float64),
	}
	c.byID[def.ID] = inst
	c.reapplyAll(inst)
}

// Remove 移除指定 Buff 并回退其对 object.AttrSet 的影响。
func (c *Container) Remove(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inst, ok := c.byID[id]
	if !ok {
		return
	}
	c.revertAll(inst)
	delete(c.byID, id)
}

// maxReapplyPerTick 单帧最大补偿次数：防止 dt 极大/Interval 极小时瞬时叠加大量周期效果。
const maxReapplyPerTick = 10

// Tick 每帧推进：Remain -= dt；Interval 到点重新施加（保持相对贡献正确）；
// 仅 RemoveOnExpire 的 Buff 在到期时移除。
//
// 对 RemoveNone/RemoveDispel/RemoveOnDeath 这些「不到期自动移除」的 Buff，
// 若同时配置了有限时长（Duration>0）与周期刷新（Interval>0），一旦 Remain<=0
// 就停止周期刷新（不再 reapply），避免其永不移除导致的无限周期结算。
// Buff 本体（含 Flags）仍保留，需由上层按各自语义（手动 Remove / 死亡 Clear / 驱散）清理。
// Duration<=0 视为无时限，周期刷新照常进行。
func (c *Container) Tick(dt float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var expired []uint32
	for id, inst := range c.byID {
		inst.Remain -= dt
		// 有限时长且已到期：非 RemoveOnExpire 的 Buff 停止周期刷新，仅保留其存在。
		expiredNoRefresh := inst.Def.Duration > 0 && inst.Remain <= 0 &&
			inst.Def.Remove != pkgbuff.RemoveOnExpire
		if inst.Def.Interval > 0 && dt > 0 && !expiredNoRefresh {
			inst.TickRemain -= dt
			reapplied := 0
			for inst.TickRemain <= 0 && reapplied < maxReapplyPerTick {
				// 防御：Interval 过小或为 0 时重置为正值避免死循环。
				if inst.Def.Interval <= 0 {
					inst.TickRemain = 0.1
					break
				}
				inst.TickRemain += inst.Def.Interval
				c.reapplyAll(inst)
				reapplied++
			}
			// 超过单帧补偿上限：直接对齐到当前周期，避免无限循环/尖峰。
			if inst.TickRemain <= 0 {
				inst.TickRemain = inst.Def.Interval
			}
		}
		if inst.Remain <= 0 && inst.Def.Remove == pkgbuff.RemoveOnExpire {
			expired = append(expired, id)
		}
	}
	// 直接回退并删除，避免调用 Remove（内部会再次 Lock，此处已持锁 → 递归死锁）。
	for _, id := range expired {
		if inst, ok := c.byID[id]; ok {
			c.revertAll(inst)
			delete(c.byID, id)
		}
	}
}

// HasFlag 判断是否有任意激活 Buff 携带指定标志位。
func (c *Container) HasFlag(f pkgbuff.Flag) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, inst := range c.byID {
		if inst.Def.Flags&f != 0 {
			return true
		}
	}
	return false
}

// Active 返回当前激活的 Buff 定义列表。
// 返回的是深拷贝：Modifiers 切片若浅拷贝，调用方持有的是容器内部正在使用的
// 同一底层数组，一次 append / 改写就会静默污染运行中的 Buff 定义。
func (c *Container) Active() []pkgbuff.Def {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]pkgbuff.Def, 0, len(c.byID))
	for _, inst := range c.byID {
		d := inst.Def
		d.Modifiers = append([]pkgbuff.Modifier(nil), d.Modifiers...)
		out = append(out, d)
	}
	return out
}

// Clear 全部移除并回退（如携带者死亡/离场时调用）。
// 调用方在 Clear 后应自行确保 HP <= MaxHP（若有血量上限变化），本容器无法内省各属性语义。
// Clear 清理所有 Buff，先收集 ID 再逐一卸载，避免遍历 map 时删除导致未定义行为。
func (c *Container) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]uint32, 0, len(c.byID))
	for id := range c.byID {
		ids = append(ids, id)
	}
	for _, id := range ids {
		if inst, ok := c.byID[id]; ok {
			c.revertAll(inst)
			delete(c.byID, id)
		}
	}
}
