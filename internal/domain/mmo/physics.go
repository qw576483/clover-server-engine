package mmo

import (
	"math"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/object"

	"github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// maxPhysicsStepSec 单帧物理步长的上限（秒）。
//
// dt 来自主循环，进程长时间挂起（GC 停顿 / 调试断点 / 容器被冻结）后补帧时可能给出
// 几十秒的 dt：一次积分就把物体甩出地图、并让宽相网格瞬间失效。这里统一夹紧，
// 保证「掉帧」表现为「慢动作」，而不是「物体瞬移穿墙」。
const maxPhysicsStepSec = 0.1

// Body 是轻量物理体。Position/Velocity/Force 均为**三维坐标**（Y 为高度，单位米）。
//
// 与 mover 的分工：
//   - Body：无重力的纯欧拉积分体（施力 / 设速 / 位置积分），适合投射物、被击退等
//     短生命周期物体；
//   - mover：带重力、跳跃、七态与地形落地的角色运动组件。
//
// 两者都以 Y 为高度、都以相同坐标写入 AOI，可混用。Body 本身不内置重力，
// 需要垂直运动就自己给 Force/Velocity 的 Y 分量（或直接用 mover）。
type Body struct {
	Mass     float64
	Radius   float64
	Position Vec3
	Velocity Vec3
	Force    Vec3
	Static   bool
}

// AddBody 给对象挂载物理体。
func (s *Scene) AddBody(objID uint64, b *Body) {
	if b == nil {
		return
	}
	if b.Radius <= 0 {
		b.Radius = 1
	}
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		// instance 不存在：对象不参与碰撞，也不应插入 cgrid，
		// 否则产生「有 AABB 无 Body」的幽灵碰撞体。
		return
	}
	l.mu.Lock()
	if max := s.sm.opts.maxBodies; max > 0 && len(l.bodies) >= max {
		// 上限闸门（WithMaxBodies）：该选项写进 options 后必须有人读取，否则等于静默无效。
		l.mu.Unlock()
		logger.Warnf("mmo: scene %d instance %d 物理体数量已达上限 %d，拒绝为 obj=%d 挂载",
			s.id, lid, max, objID)
		return
	}
	l.bodies[objID] = b
	l.mu.Unlock()
	// 同时登记到 2D 与 3D 两套宽相：
	//   - cgrid：水平投影，服务俯视玩法与贴地碰撞；
	//   - cgrid3：含高度的球体，服务多层地形 / 飞行 / 立体弹道的区域查询与避障。
	s.cgrid.Insert(strID(objID), bodyAABB(b))
	s.cgrid3.Insert(strID(objID), bodyAABB3(b))
	s.cgrid3.SetSphere(strID(objID), collide.Sphere{Center: b.Position, R: b.Radius})
}

// RemoveBody 摘掉对象的物理体，是 AddBody 的严格逆操作。
//
// ★ 四张表的归属边界：
//   - l.bodies / cgrid / cgrid3 → **物理体生命周期**，由 AddBody / RemoveBody 成对维护；
//   - l.kinds / l.grid（AOI）/ s.instanceOf → **成员生命周期**，由 Enter / Leave 成对维护。
//
// RemoveBody 只摘物理体，对象仍是场景成员（照常被别人看见、照常能 Move）——
// 「拿掉碰撞但保留可见」是合法用法；要让它彻底离场请调 Leave。
func (s *Scene) RemoveBody(objID uint64) {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	had := false
	if l != nil {
		l.mu.Lock()
		_, had = l.bodies[objID]
		delete(l.bodies, objID)
		l.mu.Unlock()
	}
	if !had {
		// 非预期分支：对「没有物理体」的对象调 RemoveBody，说明调用方与 AddBody 不配对。
		// 静默放过会让这类漏配永远查不出来（宽相里有没有东西全靠猜）。
		logger.Warnf("mmo: scene %d RemoveBody obj=%d 但该对象没有物理体（instance=%d 存在=%t）", s.id, objID, lid, l != nil)
	}
	// 与 AddBody 一样在释放 l.mu 之后改宽相：网格自带锁，不参与 l.mu 的锁序。
	s.cgrid.Remove(strID(objID))
	s.cgrid3.Remove(strID(objID))
}

func (s *Scene) Body(objID uint64) *Body {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.bodies[objID]
}

// bodyAABB 把物理体投影到 (X,Z) 水平面，返回二维轴对齐包围盒。
//
// 供 Scene 的 (X,Z) 宽相 cgrid 使用（俯视类玩法 / 角色与墙体的平面碰撞）。
// 三维精确判定请直接用 Body 的球体表示（Position 球心 + Radius），两者同源，
// 不会出现「2D 一套位置、3D 另一套位置」的分裂。
func bodyAABB(b *Body) collide.AABB {
	return collide.AABB{
		MinX: b.Position.X - b.Radius,
		MinY: b.Position.Z - b.Radius,
		MaxX: b.Position.X + b.Radius,
		MaxY: b.Position.Z + b.Radius,
	}
}

// bodyAABB3 返回物理体的三维包围盒（球体按半径沿三轴外扩）。
// 这是 cgrid3 的宽相形状，Y 是真实高度——因此落在不同楼层的两个体不会互相误报。
func bodyAABB3(b *Body) collide.AABB3 {
	r := b.Radius
	return collide.AABB3{
		Min: Vec3{X: b.Position.X - r, Y: b.Position.Y - r, Z: b.Position.Z - r},
		Max: Vec3{X: b.Position.X + r, Y: b.Position.Y + r, Z: b.Position.Z + r},
	}
}

func (s *Scene) ApplyForce(objID uint64, f Vec3) {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return
	}
	l.mu.Lock()
	// Mass<=0 必须一并跳过：physicsStep 对无质量体直接 continue、不再清零 Force，
	// 只在这里放行的话 Force 会只增不减（数值无界），最终溢出成 Inf/NaN。
	if b, ok := l.bodies[objID]; ok && !b.Static && b.Mass > 0 {
		// 三分量全收：只取 X/Z 会吞掉高度分量，飞行/击飞施加不上。
		b.Force = b.Force.Add(f)
	}
	l.mu.Unlock()
}

func (s *Scene) SetVelocity(objID uint64, v Vec3) {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return
	}
	l.mu.Lock()
	if b, ok := l.bodies[objID]; ok && !b.Static {
		b.Velocity = v
	}
	l.mu.Unlock()
}

// tickInstance 驱动单个 Instance 的物理步进（对每帧 tick 事件的回调）。
func (s *Scene) tickInstance(l *Instance, dt time.Duration) {
	if l == nil {
		return
	}
	if !s.physicsOn.Load() {
		return
	}
	sec := dt.Seconds()
	switch {
	case sec == 0:
		return // 零帧（暂停）：正常，不打日志
	case sec < 0 || math.IsNaN(sec) || math.IsInf(sec, 0):
		logger.Warnf("mmo: scene %d 收到非法物理步长 dt=%v，本次步进跳过", s.id, dt)
		return
	case sec > maxPhysicsStepSec:
		// 挂起后补帧：不夹紧的话一次积分就把物体甩出地图。
		logger.Warnf("mmo: scene %d 物理步长 dt=%v 超过上限 %vs，已夹紧", s.id, dt, maxPhysicsStepSec)
		sec = maxPhysicsStepSec
	}
	s.physicsStep(l, sec)
}

// physicsStep 单层物理步进：三维欧拉积分 + 二维宽相更新 + 完整坐标落回 AOI 网格。
//
// 积分是三维的（Y 同样按 v += (F/m)dt、p += v·dt 推进），因此施力/设速带上 Y 分量
// 即可做垂直运动；Body 不内置重力，需要落地/跳跃的用 mover。
//
// 三点并发约束：
//  1. 移动列表用局部切片而非 Scene 级共享字段——多个 Instance 并发 tick 时共享切片会竞态；
//  2. **宽相（cgrid / cgrid3）更新必须在仍持有 l.mu 时完成**：
//     先解锁再 Insert 的话，并发的 Leave / RemoveBody 可能已经把该对象从宽相删掉，
//     我们随后又把它插回去 → 留下「有 AABB 无 Body」的幽灵碰撞体。cgrid 自带锁、
//     且 Insert/Remove 不回调任何业务代码，因此 l.mu → 网格锁 的锁序是安全的；
//  3. grid.Move 触发 AOI 重算与视野推送，属重操作，放在锁外执行，
//     避免持层锁期间阻塞该层全部读写。
func (s *Scene) physicsStep(l *Instance, dt float64) {
	l.mu.Lock()
	if len(l.bodies) == 0 {
		l.mu.Unlock()
		return
	}
	moves := make([]moveOp, 0, len(l.bodies))
	for objID, b := range l.bodies {
		if b.Static || b.Mass <= 0 {
			continue
		}
		// 与 mover.stepVertical 共用同一套积分规则（geom.Integrate，半隐式欧拉）：
		// 全引擎只有这一份实现，避免「无重力体」与「有重力体」各写一遍导致行为漂移。
		np, nv := geom.Integrate(b.Position, b.Velocity, b.Force.Scale(1/b.Mass), dt)
		b.Force = Vec3{}
		if !finiteVec3(np) || !finiteVec3(nv) {
			// 非有限值一旦写回，会顺着 AOI 网格与快照污染整张地图的距离判定
			// （NaN 参与的比较恒为 false，之后所有范围判断都会静默失效）。
			logger.Warnf("mmo: scene %d obj=%d 物理积分产出非有限值 (pos=%v vel=%v dt=%v)，本次跳过",
				s.id, objID, np, nv, dt)
			continue
		}
		b.Position, b.Velocity = np, nv
		key := strID(objID)
		s.cgrid.Insert(key, bodyAABB(b))
		s.cgrid3.Insert(key, bodyAABB3(b))
		s.cgrid3.SetSphere(key, collide.Sphere{Center: b.Position, R: b.Radius})
		moves = append(moves, moveOp{
			id:  object.NewObjectID(object.TypePlayer, objID),
			pos: b.Position,
		})
	}
	l.mu.Unlock()
	for _, m := range moves {
		l.grid.Move(m.id, m.pos)
	}
}

// finiteVec3 三维坐标是否全为有限值（既不是 NaN 也不是 ±Inf）。
func finiteVec3(v Vec3) bool {
	return !math.IsNaN(v.X) && !math.IsInf(v.X, 0) &&
		!math.IsNaN(v.Y) && !math.IsInf(v.Y, 0) &&
		!math.IsNaN(v.Z) && !math.IsInf(v.Z, 0)
}
