// Package geom 提供引擎常用的数学几何原语：三维向量（Vec3）、
// 四元数（Quaternion）以及一批标量工具函数。纯标准库、零外部依赖、不依赖仓库内其它包。

// 设计要点：
//   - 所有类型均为值类型，方法多返回值（而非原地修改），便于链式组合与并发只读。
//   - 归一化对零向量做了安全处理（返回零向量 / 单位四元数），不会除零 panic。
//   - 四元数采用 Hamilton 约定（w 为实部），Rotate/Slerp 均基于此。

// 注：二维向量 Vec2 位于 collide 包（平面坐标统一用 collide.Vec2），本包不重复定义。
package geom

import "math"

// Vec3 是三维向量。
//
// JSON 约定：序列化为**小写** x/y/z。客户端（Unity `WorldSync`）与前端工具按小写取值，
// 且它们的字典查找是**区分大小写**的（`dict.TryGetValue("x")`）——无 tag 时会输出
// "X"/"Y"/"Z"，把坐标直接 marshal 给客户端会让位置被静默丢弃（解析返回 false，不报错）。
// 反方向不受影响：Go 的 json 解码在 tag 不精确匹配时会退化为大小写不敏感匹配，仍能读入 "X"。
type Vec3 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

// Add 返回两向量之和。
func (v Vec3) Add(o Vec3) Vec3 { return Vec3{v.X + o.X, v.Y + o.Y, v.Z + o.Z} }

// Sub 返回两向量之差（v - o）。
func (v Vec3) Sub(o Vec3) Vec3 { return Vec3{v.X - o.X, v.Y - o.Y, v.Z - o.Z} }

// Scale 返回向量按标量缩放的结果。
func (v Vec3) Scale(s float64) Vec3 { return Vec3{v.X * s, v.Y * s, v.Z * s} }

// Dot 返回两向量的点积。
func (v Vec3) Dot(o Vec3) float64 { return v.X*o.X + v.Y*o.Y + v.Z*o.Z }

// Cross 返回两向量的叉积。
func (v Vec3) Cross(o Vec3) Vec3 {
	return Vec3{
		v.Y*o.Z - v.Z*o.Y,
		v.Z*o.X - v.X*o.Z,
		v.X*o.Y - v.Y*o.X,
	}
}

// Len 返回向量长度（模）。
func (v Vec3) Len() float64 { return math.Sqrt(v.X*v.X + v.Y*v.Y + v.Z*v.Z) }

// LenSq 返回向量长度的平方。
func (v Vec3) LenSq() float64 { return v.X*v.X + v.Y*v.Y + v.Z*v.Z }

// Normalize 返回单位向量；零向量、含 NaN 或 ±Inf 的向量一律返回零向量。
//
// 先按最大分量缩放再求模：直接平方求模时，分量 >1e154 会平方上溢为 +Inf、
// <1e-162 下溢为 0，单位化结果被静默清零（Vec3{1e200,0,0} 本应得 {1,0,0}）；
// 且只判 `l == 0` 对 NaN 失效（NaN==0 为 false），NaN 会经除法律化成全 NaN 向下游扩散。
func (v Vec3) Normalize() Vec3 {
	m := math.Max(math.Abs(v.X), math.Max(math.Abs(v.Y), math.Abs(v.Z)))
	if m == 0 || math.IsNaN(m) || math.IsInf(m, 0) {
		return Vec3{0, 0, 0}
	}
	x, y, z := v.X/m, v.Y/m, v.Z/m
	// 缩放后模长必落在 [1, √3]，不会上溢/下溢。
	l := math.Sqrt(x*x + y*y + z*z)
	return Vec3{x / l, y / l, z / l}
}

// Distance 返回两向量之间的欧氏距离。
func (v Vec3) Distance(o Vec3) float64 { return v.Sub(o).Len() }

// Lerp 返回线性插值结果：t=0 时为 v，t=1 时为 o。
func (v Vec3) Lerp(o Vec3, t float64) Vec3 {
	return Vec3{
		v.X + (o.X-v.X)*t,
		v.Y + (o.Y-v.Y)*t,
		v.Z + (o.Z-v.Z)*t,
	}
}

// Quaternion 是四元数（Hamilton 约定，W 为实部）。
type Quaternion struct {
	W, X, Y, Z float64
}

// FromAxisAngle 由单位轴（会被归一化）与旋转角（弧度）构造四元数。
func FromAxisAngle(axis Vec3, angle float64) Quaternion {
	a := axis.Normalize()
	half := angle / 2
	s := math.Sin(half)
	return Quaternion{W: math.Cos(half), X: a.X * s, Y: a.Y * s, Z: a.Z * s}
}

// Normalize 返回归一化后的单位四元数；零四元数返回单位四元数 (1,0,0,0)。
func (q Quaternion) Normalize() Quaternion {
	l := q.Len()
	if l == 0 {
		return Quaternion{W: 1}
	}
	return Quaternion{q.W / l, q.X / l, q.Y / l, q.Z / l}
}

// Len 返回四元数的模。
func (q Quaternion) Len() float64 {
	return math.Sqrt(q.W*q.W + q.X*q.X + q.Y*q.Y + q.Z*q.Z)
}

// Mul 返回四元数乘法结果（q * o）。
func (q Quaternion) Mul(o Quaternion) Quaternion {
	return Quaternion{
		W: q.W*o.W - q.X*o.X - q.Y*o.Y - q.Z*o.Z,
		X: q.W*o.X + q.X*o.W + q.Y*o.Z - q.Z*o.Y,
		Y: q.W*o.Y - q.X*o.Z + q.Y*o.W + q.Z*o.X,
		Z: q.W*o.Z + q.X*o.Y - q.Y*o.X + q.Z*o.W,
	}
}

// Rotate 把向量 v 按该四元数旋转后返回。
func (q Quaternion) Rotate(v Vec3) Vec3 {
	qx, qy, qz, qw := q.X, q.Y, q.Z, q.W
	// t = 2 * cross(q.xyz, v)
	tx := 2 * (qy*v.Z - qz*v.Y)
	ty := 2 * (qz*v.X - qx*v.Z)
	tz := 2 * (qx*v.Y - qy*v.X)
	return Vec3{
		v.X + qw*tx + (qy*tz - qz*ty),
		v.Y + qw*ty + (qz*tx - qx*tz),
		v.Z + qw*tz + (qx*ty - qy*tx),
	}
}

// Slerp 返回与 o 的球面线性插值结果：t=0 时为 q，t=1 时为 o（自动取最短路径）。
func (q Quaternion) Slerp(o Quaternion, t float64) Quaternion {
	cosTheta := q.W*o.W + q.X*o.X + q.Y*o.Y + q.Z*o.Z
	q2 := o
	if cosTheta < 0 {
		q2 = Quaternion{-o.W, -o.X, -o.Y, -o.Z}
		cosTheta = -cosTheta
	}
	if cosTheta > 0.9995 {
		result := Quaternion{
			q.W + (q2.W-q.W)*t,
			q.X + (q2.X-q.X)*t,
			q.Y + (q2.Y-q.Y)*t,
			q.Z + (q2.Z-q.Z)*t,
		}
		return result.Normalize()
	}
	theta := math.Acos(cosTheta)
	sinTheta := math.Sqrt(1 - cosTheta*cosTheta)
	a := math.Sin((1-t)*theta) / sinTheta
	b := math.Sin(t*theta) / sinTheta
	return Quaternion{
		q.W*a + q2.W*b,
		q.X*a + q2.X*b,
		q.Y*a + q2.Y*b,
		q.Z*a + q2.Z*b,
	}
}

// 标量工具函数
// Clamp 把 v 限制到 [lo, hi] 区间。
func Clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// Lerp 标量线性插值：t=0 返回 a，t=1 返回 b。
func Lerp(a, b, t float64) float64 { return a + (b-a)*t }

// RadToDeg 弧度转角度。
func RadToDeg(r float64) float64 { return r * 180 / math.Pi }

// DegToRad 角度转弧度。
func DegToRad(d float64) float64 { return d * math.Pi / 180 }

// Sign 返回符号：正数 1、负数 -1、零 0。
func Sign(f float64) int {
	if f > 0 {
		return 1
	}
	if f < 0 {
		return -1
	}
	return 0
}

// 运动积分
//
// 全引擎只有这一套积分规则：**半隐式欧拉**（先用加速度更新速度，再用新速度位移）。
// 之所以统一在这里，是因为它同时被两个地方用：
//   - Scene.physicsStep（无重力的 Body：a = F/m）
//   - mover.stepVertical（重力/跳跃：a = -g，一维）
//
// 半隐式（而非显式 p += v·dt 后再更新 v）在变加速度下更稳，且与历史行为一致。

// IntegrateScalar 一维半隐式欧拉积分：v' = v + a·dt；p' = p + v'·dt。
func IntegrateScalar(p, v, a, dt float64) (newP, newV float64) {
	v2 := v + a*dt
	return p + v2*dt, v2
}

// Integrate 三维半隐式欧拉积分：逐轴调用 IntegrateScalar，保证与一维版本同一套语义
// （不会出现「三维走半隐式、一维走显式」这类各写一遍导致的漂移）。
func Integrate(p, v, accel Vec3, dt float64) (newP, newV Vec3) {
	px, vx := IntegrateScalar(p.X, v.X, accel.X, dt)
	py, vy := IntegrateScalar(p.Y, v.Y, accel.Y, dt)
	pz, vz := IntegrateScalar(p.Z, v.Z, accel.Z, dt)
	return Vec3{X: px, Y: py, Z: pz}, Vec3{X: vx, Y: vy, Z: vz}
}
