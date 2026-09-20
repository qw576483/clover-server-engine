// Package collide 提供碰撞检测与寻路原语。
//
//   - 碰撞基元：圆 / 矩形 / 线段；
//   - 广相：均匀网格 + 九宫格邻域 + 线段遍历，泛型空间哈希；
//   - 寻路：网格 A* + 视线检测 + 沿线段滑行到可达点，自包含网格 A*；
//   - 全部纯标准库、零外部依赖、可纯内存单测，与 aoi(视野)/object(身份)/scene(容器) 互补。
//
// 场景移动、怪物 AI、碰撞推送均可复用，也可做「圈选区域内所有对象」。
package collide

import "math"

// Vec2 是 2D 平面上的点 / 向量。
type Vec2 struct {
	X, Y float64
}

// Add 向量加。
func (v Vec2) Add(o Vec2) Vec2 { return Vec2{v.X + o.X, v.Y + o.Y} }

// Sub 向量减。
func (v Vec2) Sub(o Vec2) Vec2 { return Vec2{v.X - o.X, v.Y - o.Y} }

// Scale 标量乘。
func (v Vec2) Scale(s float64) Vec2 { return Vec2{v.X * s, v.Y * s} }

// Len 模长。
func (v Vec2) Len() float64 { return math.Hypot(v.X, v.Y) }

// Dist 到另一点的欧氏距离。
func (v Vec2) Dist(o Vec2) float64 { return math.Hypot(v.X-o.X, v.Y-o.Y) }

// Dot 点积。
func (v Vec2) Dot(o Vec2) float64 { return v.X*o.X + v.Y*o.Y }

// Normalize 归一化；零向量返回自身。
func (v Vec2) Normalize() Vec2 {
	l := v.Len()
	if l == 0 {
		return v
	}
	return Vec2{v.X / l, v.Y / l}
}

// Mid 中点。
func (v Vec2) Mid(o Vec2) Vec2 { return Vec2{(v.X + o.X) / 2, (v.Y + o.Y) / 2} }
