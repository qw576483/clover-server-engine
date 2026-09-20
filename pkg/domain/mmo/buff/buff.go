// Package buff 状态效果容器的公开门面层。
//
// 本包定义公开类型（Flag/Modifier/RemoveCond/Def/Instance/Container 接口），
// 不含实现也不 import internal；转换函数位于 pkg/domain/mmo 包。
package buff

import (
	object "github.com/qw576483/clover-server-engine/pkg/domain/object"
)

// Flag 状态标志位（可叠加 OR）。
type Flag uint32

const (
	FlagStun         Flag = 1 << iota // 眩晕
	FlagSilence                       // 沉默（禁技能）
	FlagRoot                          // 定身（禁移动）
	FlagInvincible                    // 无敌（免伤，由上层 HasFlag 判定）
	FlagCantBeTarget                  // 不可被选中（本包不实现，由业务判定）
)

// Modifier Buff 对单个属性的修改。
type Modifier struct {
	Attr      object.AttrID // 目标属性
	Delta     float64       // 绝对值或百分比数值
	IsPercent bool          // true 表示 Delta 为百分比（如 0.2 = +20%）
}

// RemoveCond 移除条件。
type RemoveCond int

const (
	RemoveNone     RemoveCond = iota // 不自动移除（靠手动 Remove / 死亡 / 驱散）
	RemoveOnExpire                   // 持续时长到期自动移除
	RemoveOnDeath                    // 携带者死亡时移除（上层调用 Clear）
	RemoveDispel                     // 被驱散时移除（上层调用 Remove）
)

// Def Buff 定义（来自配置 buff_*.json 的运行时表达）。
type Def struct {
	ID        uint32     // Buff 唯一 ID
	Duration  float64    // 持续时长（秒）；0 表示瞬时/无时限
	Interval  float64    // 持续型刷新间隔（秒）；>0 时每间隔重新施加 Modifiers
	Modifiers []Modifier // 属性修改列表
	Flags     Flag       // 状态标志位
	Remove    RemoveCond // 移除条件
	Stack     int        // 最大叠加层数；<=1 视为不可叠加（仅刷新）；>=2 可叠加至该上限
	Tag       string     // 业务自定义标签
}

// Instance 运行中的 Buff 实例。
type Instance struct {
	Def        Def
	Remain     float64                   // 剩余持续时长
	TickRemain float64                   // 距离下次间隔刷新的剩余时间
	Stacks     int                       // 当前层数
	Applied    map[object.AttrID]float64 // 已施加的属性修改量快照
}

// Container Buff 容器（接口。方法承载型，由 internal/gameplay/buff 的 *Container 直接满足）。
//
// F12 停在接口上即可看到完整方法集；浏览内部实现字段（mu/byID/set）需进 internal。
type Container interface {
	// Apply 应用一个 Buff 定义（含叠加/刷新语义），并写入绑定属性集。
	Apply(def Def)
	// Remove 移除指定 Buff 并回退其对属性集的影响。
	Remove(id uint32)
	// Tick 按帧推进所有 Buff（到期移除、间隔刷新）。
	Tick(dt float64)
	// HasFlag 是否持有任意激活 Buff 携带的标志位。
	HasFlag(f Flag) bool
	// Active 当前生效的 Buff 定义快照。
	Active() []Def
	// Clear 清空所有 Buff（死亡 / 驱散落地）。
	Clear()
}
