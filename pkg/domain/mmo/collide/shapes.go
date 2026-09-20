package collide

import (
	"math"

	"clover-server-engine/pkg/shared/geom"
)

// Circle 是以 (X,Y) 为圆心、R 为半径的圆。
type Circle struct {
	X, Y, R float64
}

// AABB 是轴对齐包围盒（Axis-Aligned Bounding Box）。
type AABB struct {
	MinX, MinY, MaxX, MaxY float64
}

// Segment 是线段（AX,AY）→（BX,BY）。
type Segment struct {
	AX, AY, BX, BY float64
}

// 点到形状
// Contains 判断点是否落在圆内（含边界）。
func (c Circle) Contains(p Vec2) bool {
	dx, dy := p.X-c.X, p.Y-c.Y
	return dx*dx+dy*dy <= c.R*c.R
}

// Contains 判断点是否落在矩形内（含边界）。
func (b AABB) Contains(p Vec2) bool {
	return p.X >= b.MinX && p.X <= b.MaxX && p.Y >= b.MinY && p.Y <= b.MaxY
}

// 形状间相交
// CircleVsCircle 两圆是否相交（含相切）。
func CircleVsCircle(a, b Circle) bool {
	dx, dy := a.X-b.X, a.Y-b.Y
	r := a.R + b.R
	return dx*dx+dy*dy <= r*r
}

// CircleVsAABB 圆与矩形是否相交（含相切）。
func CircleVsAABB(c Circle, b AABB) bool {
	// 圆心到矩形最近点
	nx := clampf(c.X, b.MinX, b.MaxX)
	ny := clampf(c.Y, b.MinY, b.MaxY)
	dx, dy := c.X-nx, c.Y-ny
	return dx*dx+dy*dy <= c.R*c.R
}

// AABBvsAABB 两矩形是否相交（含相切）。
func AABBvsAABB(a, b AABB) bool {
	return a.MinX <= b.MaxX && a.MaxX >= b.MinX &&
		a.MinY <= b.MaxY && a.MaxY >= b.MinY
}

// 线段相交（射线检测）
// SegmentIntersectsCircle 线段是否与圆相交。
func SegmentIntersectsCircle(s Segment, c Circle) bool {
	// 把线段参数化 P(t)=A+t(B-A)，求到圆心距离 <= R 的 t∈[0,1]
	dx, dy := s.BX-s.AX, s.BY-s.AY
	fx, fy := s.AX-c.X, s.AY-c.Y
	a := dx*dx + dy*dy
	if a == 0 { // 退化点
		return c.Contains(Vec2{s.AX, s.AY})
	}
	b := 2 * (fx*dx + fy*dy)
	cc := fx*fx + fy*fy - c.R*c.R
	disc := b*b - 4*a*cc
	if disc < 0 {
		return false
	}
	disc = math.Sqrt(disc)
	t1 := (-b - disc) / (2 * a)
	t2 := (-b + disc) / (2 * a)
	return (t1 >= 0 && t1 <= 1) || (t2 >= 0 && t2 <= 1) ||
		(t1 < 0 && t2 > 1) // 整段都在圆内
}

// SegmentIntersectsAABB 线段是否与矩形相交（slab 法）。
func SegmentIntersectsAABB(s Segment, b AABB) bool {
	dx, dy := s.BX-s.AX, s.BY-s.AY
	tmin, tmax := 0.0, 1.0
	// X slab
	if !slab(s.AX, dx, b.MinX, b.MaxX, &tmin, &tmax) {
		return false
	}
	// Y slab
	if !slab(s.AY, dy, b.MinY, b.MaxY, &tmin, &tmax) {
		return false
	}
	return tmax >= tmin
}

func slab(start, delta, min, max float64, tmin, tmax *float64) bool {
	if delta == 0 {
		// 平行：起点必须在范围内
		return start >= min && start <= max
	}
	t1 := (min - start) / delta
	t2 := (max - start) / delta
	if t1 > t2 {
		t1, t2 = t2, t1
	}
	if t1 > *tmin {
		*tmin = t1
	}
	if t2 < *tmax {
		*tmax = t2
	}
	return *tmax >= *tmin
}

// 碰撞解算（用于滑动）
// ClosestPointOnAABB 返回矩形上离圆心最近的点。
func ClosestPointOnAABB(c Circle, b AABB) Vec2 {
	return Vec2{clampf(c.X, b.MinX, b.MaxX), clampf(c.Y, b.MinY, b.MaxY)}
}

// ResolveCircleAABB 把圆推出矩形（沿最小穿透轴），返回修正后的圆与是否发生碰撞。
// 这是 MMO「沿障碍滑行」的窄相核心：先判定相交，再把圆心沿最小穿透方向移出。
func ResolveCircleAABB(c Circle, b AABB) (Circle, bool) {
	if !CircleVsAABB(c, b) {
		return c, false
	}
	cx := clampf(c.X, b.MinX, b.MaxX)
	cy := clampf(c.Y, b.MinY, b.MaxY)
	// 圆心已在矩形内：沿最近的边推出
	if c.X > b.MinX && c.X < b.MaxX && c.Y > b.MinY && c.Y < b.MaxY {
		dl, dr := c.X-b.MinX, b.MaxX-c.X
		dd, du := c.Y-b.MinY, b.MaxY-c.Y
		m := min4(dl, dr, dd, du)
		switch m {
		case dl:
			c.X = b.MinX - c.R
		case dr:
			c.X = b.MaxX + c.R
		case dd:
			c.Y = b.MinY - c.R
		default:
			c.Y = b.MaxY + c.R
		}
		return c, true
	}
	// 圆心在矩形外：沿「圆心→最近点」方向推出到恰好相切
	nx, ny := c.X-cx, c.Y-cy
	d := math.Hypot(nx, ny)
	if d == 0 {
		return c, true
	}
	nx, ny = nx/d, ny/d
	c.X = cx + nx*c.R
	c.Y = cy + ny*c.R
	return c, true
}

func clampf(v, lo, hi float64) float64 {
	return geom.Clamp(v, lo, hi)
}

func min4(a, b, c, d float64) float64 {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	if d < m {
		m = d
	}
	return m
}
