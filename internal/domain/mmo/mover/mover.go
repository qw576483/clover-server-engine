// package mover 的七态运动组件。
//
// 设计原则：纯几何/物理积分，不绑定任何业务（无技能、无网络、无数据落地）。
// 对外只暴露一个 *Mover：持有位置/速度/朝向/状态，由 Step(dt) 逐帧积分推进。
// 想落地到真实地面，可注入 *collide.HeightField；不注入则在创建时的 Y 平面运动。
//
// 七态（State）定义：
// Idle/Walk/Run/Jump/Fall/Climb/Fly/Swim。其中 Jump/Fall 由垂直速度自动判定，
// Walk/Run 由平面移动自动判定；Climb/Fly/Swim 作为引擎中立状态保留（可由上层按
// 地形/水体标记直接赋值扩展，本层不强依赖外部信息）。
package mover

import (
	"math"

	"clover-server-engine/internal/transport/event/engine"
	"clover-server-engine/pkg/domain/mmo/collide"
	pkgmover "clover-server-engine/pkg/domain/mmo/mover"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/geom"
)

// 默认物理参数与阈值。
const (
	defaultGravity  = 20.0
	runSpeedThresh  = 5.0 // 平面基础速度大于该值视为 Run，否则 Walk
	groundEps       = 1e-6
	arriveThreshold = 1e-3 // 到目标点的到达判定阈值
)

// Mover 是单个对象的七态运动体（引擎级，不绑业务）。
type Mover struct {
	pos     geom.Vec3    // 位置（X/Z 为水平面，Y 为垂直高度）
	vel     collide.Vec2 // 平面速度（X/Z 对应世界 X/Z）
	vy      float64      // 垂直速度
	facing  float64      // 朝向（弧度，atan2(dz,dx)）
	state   pkgmover.State
	speed   float64 // 基础移动速度
	gravity float64
	groundY float64    // 当前所在格地面高度（用于落地判定）
	dest    *geom.Vec3 // 自动寻路目标（朝目标直线移动）
	onMove  func(pos geom.Vec3, facing float64)
	terrain *collide.HeightField // 可选地面高度场；nil 则在创建 Y 平面运动

	// 引擎事件相关（不传则完全不触发，零开销）。
	emitter engine.Emitter
	disp    *engine.Dispatcher
	oidSeq  uint64 // 本 Mover 对应实体序号（用于 object.move 事件标识）
	lastX   float64
	lastY   float64
	lastZ   float64
}

// NewMover 以初始位置与基础移动速度构造一个静止 Mover。
func NewMover(pos geom.Vec3, speed float64) *Mover {
	return &Mover{
		pos:     pos,
		speed:   speed,
		gravity: defaultGravity,
		groundY: pos.Y, // 无地形时以创建高度作为落地平面
		state:   pkgmover.StateIdle,
		lastX:   pos.X,
		lastY:   pos.Y,
		lastZ:   pos.Z,
	}
}

// Pos 返回当前位置。
func (m *Mover) Pos() geom.Vec3 { return m.pos }

// SetPos 直接设置位置（瞬移/传送用）。
func (m *Mover) SetPos(p geom.Vec3) {
	m.pos = p
	// 传送后同步 groundY，避免旧值导致落地高度错误。
	m.refreshGroundY()
}

// Facing 返回当前朝向（弧度）。
func (m *Mover) Facing() float64 { return m.facing }

// SetFacing 设置朝向（弧度）。
func (m *Mover) SetFacing(rad float64) { m.facing = rad }

// State 返回当前运动态。
func (m *Mover) State() pkgmover.State { return m.state }

// SetVelocity 直接设置速度（如被击退）：平面速度 + 垂直速度。会放弃自动寻路目标。
func (m *Mover) SetVelocity(plane collide.Vec2, vy float64) {
	m.vel = plane
	m.vy = vy
	m.dest = nil
}

// MoveTo 设置直线寻路目标；Step 时朝目标移动并自动更新朝向；到达后自动停止。
func (m *Mover) MoveTo(dest geom.Vec3) {
	d := dest
	m.dest = &d
}

// Stop 清除目标、速度归零，回到 Idle。
func (m *Mover) Stop() {
	m.dest = nil
	m.vel = collide.Vec2{}
	m.vy = 0
	m.state = pkgmover.StateIdle
}

// Jump 给一个向上的初速度，进入 Jump 态；随后由重力积分上升再下落。
func (m *Mover) Jump(impulse float64) {
	m.vy = impulse
	m.state = pkgmover.StateJump
}

// SetOnMove 注册每帧移动后的回调（用于客户端同步等）。
func (m *Mover) SetOnMove(fn func(pos geom.Vec3, facing float64)) { m.onMove = fn }

// SetTerrain 注入地面高度场；落地高度以高度场查询为准。不传则在 y=pos.Y 平面。
func (m *Mover) SetTerrain(hf *collide.HeightField) {
	m.terrain = hf
	// 切换地形后同步 groundY，避免旧值导致落地高度错误。
	m.refreshGroundY()
}

// SetEmitter 注入引擎事件发射器：Mover 在位置变化时 emit object.move 标准事件。
func (m *Mover) SetEmitter(e engine.Emitter) {
	m.emitter = e
	if e != nil {
		m.disp = engine.NewDispatcher(e)
	}
}

// SetObjectID 设置本 Mover 对应的实体序号（用于 object.move 事件标识）。
func (m *Mover) SetObjectID(seq uint64) { m.oidSeq = seq }

// emitMove 在位置确实变化时 emit 引擎标准事件 object.move（供业务订阅实体移动）。
func (m *Mover) emitMove() {
	if m.disp == nil || m.oidSeq == 0 {
		return
	}
	// 位置变化判定须同时考虑 Y：跳跃/落地纯垂直位移也应触发 object.move，
	// 否则 X/Z 不变的起跳与落地帧会被静默丢弃。
	if m.lastX == m.pos.X && m.lastY == m.pos.Y && m.lastZ == m.pos.Z {
		return
	}
	m.disp.EmitMove(engine.MovePayload{
		Entity: engine.ObjectRef{OID: m.oidSeq},
		// 三维载荷：与上面的变化判定保持一致，Y 必须带上——否则「起跳/落地只改 Y」
		// 的事件虽然发出了，订阅方拿到的 From/To 却完全相同，等于空事件。
		From: engine.Vec3{X: m.lastX, Y: m.lastY, Z: m.lastZ},
		To:   engine.Vec3{X: m.pos.X, Y: m.pos.Y, Z: m.pos.Z},
	})
	m.lastX, m.lastY, m.lastZ = m.pos.X, m.pos.Y, m.pos.Z
}

// Step 逐帧积分推进 dt 秒。
func (m *Mover) Step(dt float64) {
	if dt <= 0 {
		return // dt<=0 不做任何积分，也不触发事件
	}
	if m.speed <= 0 {
		// speed<=0 清除自动寻路目标，自由速度积分照旧（击退等独立于 speed）。
		m.dest = nil
	}
	m.stepHorizontal(dt)
	m.stepVertical(dt)
	m.updateState()
	if m.onMove != nil {
		m.onMove(m.pos, m.facing)
	}
	m.emitMove()
}

// saneFinite 把非有限值（NaN / ±Inf）拉回给定兜底值并留痕。
//
// 位置/速度一旦变成 NaN，后续所有比较（>= / <= / ==）恒为 false：
// 落地判定、到达判定、状态机全部静默失效，角色表现为"卡住不动且查不出原因"。
// 极端 SetVelocity / Jump / 超大 dt 都可能算出非有限值，必须在写回前挡掉。
func saneFinite(v, fallback float64, what string) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		logger.Warnf("mover: %s 计算出非有限值 %v，已回退到 %v", what, v, fallback)
		return fallback
	}
	return v
}

// stepHorizontal 处理平面移动：有目标朝目标走，否则按平面速度自由积分。
func (m *Mover) stepHorizontal(dt float64) {
	if m.dest != nil {
		dx := m.dest.X - m.pos.X
		dz := m.dest.Z - m.pos.Z
		d := math.Hypot(dx, dz)
		if d <= arriveThreshold || m.speed*dt >= d {
			// 到达：吸附到目标，清除目标。
			m.pos.X = m.dest.X
			m.pos.Z = m.dest.Z
			m.dest = nil
			m.vel = collide.Vec2{}
			return
		}
		inv := 1.0 / d
		dir := collide.Vec2{X: dx * inv, Y: dz * inv}
		step := m.speed * dt
		m.pos.X += dir.X * step
		m.pos.Z += dir.Y * step
		m.vel = collide.Vec2{X: dir.X * m.speed, Y: dir.Y * m.speed}
		m.facing = math.Atan2(dir.Y, dir.X)
		return
	}
	// 自由速度积分。
	m.pos.X = saneFinite(m.pos.X+m.vel.X*dt, m.pos.X, "pos.X")
	m.pos.Z = saneFinite(m.pos.Z+m.vel.Y*dt, m.pos.Z, "pos.Z")
	if m.vel.X != 0 || m.vel.Y != 0 {
		m.facing = math.Atan2(m.vel.Y, m.vel.X)
	}
}

// stepVertical 处理垂直积分与落地。
//
// 积分走 geom.IntegrateScalar（半隐式欧拉）—— 与 Scene.physicsStep 的 Body 积分是同一套规则，
// 全引擎只有一份实现，避免「无重力体」与「有重力体」各写一遍导致行为漂移。
func (m *Mover) stepVertical(dt float64) {
	ny, nvy := geom.IntegrateScalar(m.pos.Y, m.vy, -m.gravity, dt)
	// 垂直积分同样要挡非有限值：一旦 pos.Y 变成 NaN，
	// 下面的 `m.pos.Y <= m.groundY` 恒为 false → 角色永远落不了地。
	m.pos.Y = saneFinite(ny, m.groundY, "pos.Y")
	m.vy = saneFinite(nvy, 0, "vy")
	// 用高度场刷新地面高度（仅当可行走）。
	m.refreshGroundY()
	// 落地：钳制到地面，垂直速度清零。
	// 须同时要求 vy<=0（正在下降/静止）才算落地；否则在 pos.Y==groundY 的起跳瞬间
	// 会把刚给出的向上初速度立即清零，导致永远无法起跳。
	if m.vy <= 0 && m.pos.Y <= m.groundY {
		m.pos.Y = m.groundY
		m.vy = 0
	}
}

// refreshGroundY 根据当前高度场与实体高度刷新 groundY：
//   - 实体位于覆盖物顶面或之上（pos.Y >= maxH）时，以 maxH（平台/屋顶上表面）为地面；
//   - 实体处于覆盖物下方空间时，以 minH（最低可站立面）为地面，
//     避免身处隧道/屋檐下却被错误抬升到屋顶（恒取 maxH 会穿透/错位）。
func (m *Mover) refreshGroundY() {
	if m.terrain == nil {
		return
	}
	minH, maxH, ok := m.terrain.Height(m.pos.X, m.pos.Z)
	if !ok {
		return
	}
	if m.pos.Y >= maxH {
		m.groundY = maxH
	} else {
		m.groundY = minH
	}
}

// updateState 依据垂直/水平状态切换运动态（启发式）。
// 落地时（从 Jump/Fall 过渡到地面态）Y 状态已由 stepVertical 钳制，此处按 vy/vel 分发。
func (m *Mover) updateState() {
	switch {
	case m.vy > groundEps:
		m.state = pkgmover.StateJump
	case m.vy < -groundEps && m.pos.Y > m.groundY+groundEps:
		m.state = pkgmover.StateFall
	case m.dest != nil || m.vel.X != 0 || m.vel.Y != 0:
		if m.speed > runSpeedThresh {
			m.state = pkgmover.StateRun
		} else {
			m.state = pkgmover.StateWalk
		}
	default:
		m.state = pkgmover.StateIdle
	}
}
