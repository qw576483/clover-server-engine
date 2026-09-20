package geom

import "math"

// TrigTable 是离散角度（整数度）的正弦/余弦查表，用于避免高频调用 math.Sin/Cos。
// 预计算 0..n-1 度的 sin/cos，访问时按度取模，适合场景朝向、角度步进等热路径。
type TrigTable struct {
	n   int
	sin []float64
	cos []float64
}

// NewTrigTable 创建查表，预计算 0..n-1 度。n<=0 时按 360 处理。
func NewTrigTable(n int) *TrigTable {
	if n <= 0 {
		n = 360
	}
	t := &TrigTable{
		n:   n,
		sin: make([]float64, n),
		cos: make([]float64, n),
	}
	for i := 0; i < n; i++ {
		rad := DegToRad(float64(i))
		t.sin[i] = math.Sin(rad)
		t.cos[i] = math.Cos(rad)
	}
	return t
}

// Sin 返回 deg 度的正弦值（deg 取模到 [0, n)）。
func (t *TrigTable) Sin(deg int) float64 {
	d := ((deg % t.n) + t.n) % t.n
	return t.sin[d]
}

// Cos 返回 deg 度的余弦值（deg 取模到 [0, n)）。
func (t *TrigTable) Cos(deg int) float64 {
	d := ((deg % t.n) + t.n) % t.n
	return t.cos[d]
}
