// Package mob 是怪物/NPC 管理的公开类型定义。
//
// 实现全部位于 internal/domain/mmo/gameplay/mob；本包只定义公开接口与值类型。
package mob

import (
	"time"

	"github.com/qw576483/clover-server-engine/pkg/domain/data"
	"github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// MobScene 是 mob 管理所需的最小场景接口。
// *mmo.Scene 满足（由使用方编译期断言保证）。
type MobScene interface {
	EnterOwnerType(objID uint64, ownerType data.OwnerType, pos geom.Vec3) error
	Move(objID uint64, pos geom.Vec3)
	Leave(objID uint64)
	Position(objID uint64) (geom.Vec3, bool)
	Neighbors(objID uint64, radius float64) []uint64
	Members() []uint64
}

// Mob 是一只怪物的运行时实例（接口）。
type Mob interface {
	Target() uint64
	SetTarget(t uint64)
	Hate(t uint64) float64
	AddHate(t uint64, v float64)
	DelHate(t uint64)
	IsDead() bool
	Revive()
}

// SpawnConfig 是刷怪点配置。
type SpawnConfig struct {
	ID          uint64
	OwnerType   data.OwnerType
	Name        string
	X, Z        float64
	Speed       float64
	HP          float64
	ChaseRange  float64
	AttackRange float64
	AggroRange  float64
	Patrol      []collide.Vec2
	RespawnSec  float64

	// AggroTimeoutSec 仇恨时效（秒）：距最后一次「被激怒」（挨打 / 仇恨变化）超过该时长，
	// 这只怪就**忘掉当前目标并清空仇恨**，回到巡逻；<=0 = 永不忘记（保持旧行为）。
	//
	// 为什么必须有它：没有时效时仇恨是**永久**的 —— 玩家离图 / 挂机 / 换会话之后，
	// objID 仍然存在，怪会继续追着那个 objID 打（追着空气打，仇恨也不会掉），
	// 玩家一重新进图就被围殴。一进图即"你已阵亡"。
	// 注意它管的是**仇恨记忆**，不是"巡逻锁定"：目标仍在索敌范围内时怪会重新索敌，
	// 时效的意义是"没人再激它，就别一直记着"。
	AggroTimeoutSec float64

	// Passive 被动（纯靶子 / 展示用 NPC）：true = 永不主动索敌、永不还手，只挨打、只巡逻。
	//
	// 用途：训练靶子、剧情 NPC、展示用怪 —— 语义是"打不还手"，
	// 而不是"数值调到打不死你"（后者是数值问题，前者是设计意图）。
	// 实测教训：让训练靶子会还手时，近战玩家"砍靶子"会变成"被靶子磨死"，
	// 玩家感受是"我一挥剑自己就掉血"；死亡/复活演示应另刷一只会还手的怪。
	// 注意 Passive 只影响**决策**（不索敌、不攻击）：被 OnDamaged 扣血与重生照常。
	Passive bool
}

// MobManager 怪物/NPC 管理器接口。
type MobManager interface {
	SetOnAttack(fn func(attacker, target uint64))
	Register(m Mob)
	Spawn(cfg SpawnConfig) (Mob, error)
	Update(dt time.Duration)
	OnDamaged(m Mob, dmg float64)
}
