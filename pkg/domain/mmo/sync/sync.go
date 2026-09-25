// Package sync 提供网络物理同步策略：插值、外推、客户端预测。
// MMO 典型场景：服务端权威状态 → 客户端平滑渲染。
package sync

import "sync/atomic"

// SyncMode 状态同步模式。
type SyncMode int

const (
	// ModeInterpolation 插值（渲染延迟+历史平滑）。
	ModeInterpolation SyncMode = iota
	// ModeExtrapolation 外推（短暂丢失时按速度预测）。
	ModeExtrapolation
	// ModePrediction 客户端预测。
	ModePrediction
)

// State 通用状态快照（位置 / 旋转 / 速度 / 加速度 + 时间戳）。
//
// **定位说明（避免被当成重复的坐标类型）**：本类型是「同步快照记录」，不是「坐标类型」。
// 引擎里坐标的类型统一是 `geom.Vec3`（`aoi.Position` / `engine.Vec3` / `mmo.Vec3` 都是它的别名），
// 而这里的字段刻意用 `float32` 平铺：
//   - 快照是**面向体积与逐轴插值**的记录，13 个分量按轴平铺比 4 个 Vec3 更好按轴做插值/外推；
//   - float32 是刻意的精度取舍（快照量级大），换成 float64 会同时改变插值/预测的数值行为。
//
// 所以两者是不同概念、各司其职：**不要把 State 换成 Vec3，也不要用 State 代替坐标**。
// 需要跨边界时在调用处显式转换（`geom.Vec3{X: float64(s.PosX), ...}`）。
type State struct {
	Time       int64
	PosX       float32
	PosY       float32
	PosZ       float32
	RotX       float32
	RotY       float32
	RotZ       float32
	VelX       float32
	VelY       float32
	VelZ       float32
	AccelX     float32
	AccelY     float32
	AccelZ     float32
	ServerTick uint64
}

// LerpState 线性插值：result = a + (b-a)*t。
func LerpState(a, b State, t float32) State {
	return State{
		// 时间戳走 int64 精确插值：float32 只有 24 位尾数，毫秒级时间戳（~1e12）
		// 经 float32 会产生秒~分钟级误差，插值出的 Time 完全不可信。
		Time: a.Time + int64(float64(b.Time-a.Time)*float64(t)),
		PosX: a.PosX + (b.PosX-a.PosX)*t,
		PosY: a.PosY + (b.PosY-a.PosY)*t,
		PosZ: a.PosZ + (b.PosZ-a.PosZ)*t,
		RotX: a.RotX + (b.RotX-a.RotX)*t,
		RotY: a.RotY + (b.RotY-a.RotY)*t,
		RotZ: a.RotZ + (b.RotZ-a.RotZ)*t,
		VelX: a.VelX + (b.VelX-a.VelX)*t,
		VelY: a.VelY + (b.VelY-a.VelY)*t,
		VelZ: a.VelZ + (b.VelZ-a.VelZ)*t,
		// 加速度必须一并插值，否则插值/外推结果的加速度恒为 0。
		AccelX:     a.AccelX + (b.AccelX-a.AccelX)*t,
		AccelY:     a.AccelY + (b.AccelY-a.AccelY)*t,
		AccelZ:     a.AccelZ + (b.AccelZ-a.AccelZ)*t,
		ServerTick: b.ServerTick,
	}
}

// Syncer 同步器接口（插值/外推/预测统一抽象）。
type Syncer interface {
	AddSnapshot(state State)
	Interpolate(nowMS int64) (State, bool)
	Reset()
}

// SyncManager 同步管理器（按模式选择策略）。
type SyncManager interface {
	AddSnapshot(state State)
	Interpolate(nowMS int64) (State, bool)
	Reset()
	Mode() SyncMode
}

// InterpolationStrategy 插值策略接口。
type InterpolationStrategy interface {
	Syncer
	SnapCount() int
}

// ExtrapolationStrategy 外推策略接口。
type ExtrapolationStrategy interface {
	Syncer
}

// PredictionStrategy 客户端预测策略接口。
type PredictionStrategy interface {
	Syncer
}

// AtomicInt64 是并发安全的 int64 原子操作包装，供外部测试或跨包使用。
type AtomicInt64 struct {
	v int64
}

// Load 原子读取。
func (a *AtomicInt64) Load() int64 { return atomic.LoadInt64(&a.v) }

// Store 原子写入。
func (a *AtomicInt64) Store(v int64) { atomic.StoreInt64(&a.v, v) }
