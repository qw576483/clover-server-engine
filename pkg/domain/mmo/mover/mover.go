// Package mover 运动体（七态运动组件）的公开类型定义。
//
// State 类型与常量定义在本包；internal/domain/mmo/mover 引用本包的 State。
// Mover 接口由 internal 的 *Mover 满足，业务层通过接口操作。
package mover

import (
	pkgcollide "github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// State 运动七态（外加 Idle）。
type State int

const (
	// StateIdle 静止。
	StateIdle State = iota
	// StateWalk 步行。
	StateWalk
	// StateRun 奔跑。
	StateRun
	// StateJump 起跳上升。
	StateJump
	// StateFall 下落。
	StateFall
	// StateClimb 攀爬（引擎中立状态，可由上层地形标记触发）。
	StateClimb
	// StateFly 飞行（引擎中立状态）。
	StateFly
	// StateSwim 游泳（引擎中立状态）。
	StateSwim
)

// Mover 单个对象的七态运动体（接口。方法承载型，由 internal/domain/mmo/mover 的 *Mover 直接满足）。
//
// F12 停在接口上即可看到完整方法集。装配专用方法（SetOnMove/SetTerrain/SetEmitter/SetObjectID，
// 引用 internal 的 collide.HeightField / engine.Emitter 句柄）已从业务接口剔除。
type Mover interface {
	// Pos 返回当前位置。
	Pos() geom.Vec3
	// SetPos 直接设置位置（瞬移/传送用）。
	SetPos(p geom.Vec3)
	// Facing 返回当前朝向（弧度）。
	Facing() float64
	// SetFacing 设置朝向（弧度）。
	SetFacing(rad float64)
	// State 返回当前运动态。
	State() State
	// SetVelocity 直接设置速度（如被击退）：平面速度 + 垂直速度。
	SetVelocity(plane pkgcollide.Vec2, vy float64)
	// MoveTo 设置直线寻路目标；Step 时朝目标移动并自动更新朝向。
	MoveTo(dest geom.Vec3)
	// Stop 清除目标、速度归零，回到 Idle。
	Stop()
	// Jump 给一个向上的初速度，进入 Jump 态。
	Jump(impulse float64)
	// Step 逐帧积分推进 dt 秒。
	Step(dt float64)
}
