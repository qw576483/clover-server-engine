package collide

import "math"

// ShapeKind 碰撞外形种类。
type ShapeKind int

const (
	// ShapeCircle 圆形外形：仅用 Radius（以 center 为圆心）。
	ShapeCircle ShapeKind = iota
	// ShapePolygon 多边形外形：用 Points（相对 center、逆时针）。
	ShapePolygon
)

// Shape 是对象的碰撞外形。
//   - 圆：Kind=ShapeCircle、Radius>0，center 为圆心；
//   - 多边形：Kind=ShapePolygon、Points 为相对 center、逆时针排列的顶点。
//
// 多边形顶点相对中心存储，便于整体平移到任意世界坐标（Contains/Intersect 的 center 参数）。
type Shape struct {
	Kind   ShapeKind
	Radius float64
	Points []Vec2
}

// CircleShape 构造圆形外形（半径 r>0）。
// 命名带 Shape 后缀以避开本包已有的 type Circle（圆碰撞基元），避免破坏现有 API。
func CircleShape(r float64) Shape { return Shape{Kind: ShapeCircle, Radius: r} }

// PolygonShape 构造多边形外形（pts 相对中心、逆时针；内部拷贝，避免外部后续修改污染）。
func PolygonShape(pts []Vec2) Shape {
	cp := make([]Vec2, len(pts))
	copy(cp, pts)
	return Shape{Kind: ShapePolygon, Points: cp}
}

// worldPoints 返回形状在 center 处的世界坐标顶点（仅多边形有意义）。
func (s Shape) worldPoints(center Vec2) []Vec2 {
	if s.Kind != ShapePolygon {
		return nil
	}
	out := make([]Vec2, len(s.Points))
	for i, p := range s.Points {
		out[i] = center.Add(p)
	}
	return out
}

// Contains 判断点 p（世界坐标）是否落在形状内（center 为形状中心）。
//   - 圆：欧氏距离 <= Radius（含边界）；
//   - 多边形：射线法（偶数交叉判定），与顶点顺序无关、稳定；恰落在边上的点不保证返回 true（射线法特性）。
func (s Shape) Contains(p, center Vec2) bool {
	switch s.Kind {
	case ShapeCircle:
		dx, dy := p.X-center.X, p.Y-center.Y
		return dx*dx+dy*dy <= s.Radius*s.Radius
	case ShapePolygon:
		return pointInPolygon(p, s.worldPoints(center))
	default:
		// 未知 Kind 不能静默返回 false：外部传入非法外形时表现为「不与任何东西碰撞」
		//（穿模）且无任何线索。此处降频留痕（详见 geomguard.go）。
		dropInvalidGeom("Shape.Contains", s.Kind)
		return false
	}
}

// Intersect 判断形状 s（中心 a）与形状 o（中心 oc）是否相交。
// b 当前实现未使用。
//   - 圆-圆：半径和（精确）；
//   - 圆-多边形：圆心在多边形内，或圆心到多边形任一边最近距离 <= 半径；
//   - 多边形-多边形：任一顶点落在对方内，或任一对边相交（凸/凹均给出合理近似）。
func (s Shape) Intersect(a, b Vec2, o Shape, oc Vec2) bool {
	// 圆 - 圆
	if s.Kind == ShapeCircle && o.Kind == ShapeCircle {
		dx, dy := a.X-oc.X, a.Y-oc.Y
		r := s.Radius + o.Radius
		return dx*dx+dy*dy <= r*r
	}
	// 圆 - 多边形
	if s.Kind == ShapeCircle {
		return circleIntersectsPolygon(a, s.Radius, o.worldPoints(oc))
	}
	if o.Kind == ShapeCircle {
		return circleIntersectsPolygon(oc, o.Radius, s.worldPoints(a))
	}
	// 多边形 - 多边形（合理近似）
	return polygonsIntersect(s.worldPoints(a), o.worldPoints(oc))
}

// 多边形几何辅助
// pointInPolygon 射线法判断点 p 是否在多边形 poly 内（边界不视为内）。
func pointInPolygon(p Vec2, poly []Vec2) bool {
	n := len(poly)
	if n < 3 {
		return false
	}
	inside := false
	j := n - 1
	for i := 0; i < n; i++ {
		pi, pj := poly[i], poly[j]
		// 从 p 向右发射水平射线，仅统计跨过 p.Y 的边
		if (pi.Y > p.Y) != (pj.Y > p.Y) {
			// 边与射线交点的 x 坐标
			xint := (pj.X-pi.X)*(p.Y-pi.Y)/(pj.Y-pi.Y) + pi.X
			if p.X < xint {
				inside = !inside
			}
		}
		j = i
	}
	return inside
}

// distPointSeg2 点到线段最近距离的平方（避免 sqrt）。
func distPointSeg2(p, a, b Vec2) float64 {
	abx, aby := b.X-a.X, b.Y-a.Y
	apx, apy := p.X-a.X, p.Y-a.Y
	len2 := abx*abx + aby*aby
	var t float64
	if len2 > 0 {
		t = (apx*abx + apy*aby) / len2
		if t < 0 {
			t = 0
		} else if t > 1 {
			t = 1
		}
	}
	cx, cy := a.X+abx*t, a.Y+aby*t
	dx, dy := p.X-cx, p.Y-cy
	return dx*dx + dy*dy
}

// circleIntersectsPolygon 圆心 c、半径 r 的多边形相交判定。
func circleIntersectsPolygon(c Vec2, r float64, poly []Vec2) bool {
	if pointInPolygon(c, poly) {
		return true
	}
	n := len(poly)
	if n < 2 {
		return false
	}
	r2 := r * r
	for i := 0; i < n; i++ {
		if distPointSeg2(c, poly[i], poly[(i+1)%n]) <= r2 {
			return true
		}
	}
	return false
}

// cross 叉积 (b-a)×(c-a)，符号表示 c 相对有向线段 a→b 的左右。
func cross(a, b, c Vec2) float64 {
	return (b.X-a.X)*(c.Y-a.Y) - (b.Y-a.Y)*(c.X-a.X)
}

// segmentsIntersect 判断线段 p1p2 与 p3p4 是否相交（含端点接触）。
// 使用严格 >0 / <0 判断叉积符号，共线（叉积=0）时退化为端点投影检测，
// 避免两条平行共线但不重叠的线段被误判为相交。
func segmentsIntersect(p1, p2, p3, p4 Vec2) bool {
	const eps = 1e-9
	d1 := cross(p3, p4, p1)
	d2 := cross(p3, p4, p2)
	d3 := cross(p1, p2, p3)
	d4 := cross(p1, p2, p4)

	// 严格异号：两线段跨立
	if (d1 > eps && d2 < -eps) || (d1 < -eps && d2 > eps) {
		if (d3 > eps && d4 < -eps) || (d3 < -eps && d4 > eps) {
			return true
		}
		return false
	}

	// 至少一个叉积接近零（共线或端点接触），检查端点是否在另一线段上
	if math.Abs(d1) <= eps && onSegment(p3, p4, p1) {
		return true
	}
	if math.Abs(d2) <= eps && onSegment(p3, p4, p2) {
		return true
	}
	if math.Abs(d3) <= eps && onSegment(p1, p2, p3) {
		return true
	}
	if math.Abs(d4) <= eps && onSegment(p1, p2, p4) {
		return true
	}
	return false
}

// onSegment 判断点 q 是否在线段 pr 上（假设 p、q、r 共线）。
func onSegment(p, q, r Vec2) bool {
	const eps = 1e-9
	return q.X >= math.Min(p.X, r.X)-eps && q.X <= math.Max(p.X, r.X)+eps &&
		q.Y >= math.Min(p.Y, r.Y)-eps && q.Y <= math.Max(p.Y, r.Y)+eps
}

// polygonsIntersect 两个多边形是否相交（任一顶点互含，或任一对边相交）。
func polygonsIntersect(a, b []Vec2) bool {
	for _, p := range a {
		if pointInPolygon(p, b) {
			return true
		}
	}
	for _, p := range b {
		if pointInPolygon(p, a) {
			return true
		}
	}
	na, nb := len(a), len(b)
	for i := 0; i < na; i++ {
		ai, aj := a[i], a[(i+1)%na]
		for k := 0; k < nb; k++ {
			bi, bj := b[k], b[(k+1)%nb]
			if segmentsIntersect(ai, aj, bi, bj) {
				return true
			}
		}
	}
	return false
}
