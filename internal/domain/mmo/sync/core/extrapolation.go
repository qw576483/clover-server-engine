// Package sync 外推策略：基于最新快照的速度/加速度，预测未来位置。
package sync

import "sync"

// ExtrapolationStrategy 外推同步策略。
// 在服务端快照到达间隔 > renderDelayMS 时，基于上一帧的物理状态外推。
type ExtrapolationStrategy struct {
	mu          sync.Mutex
	lastState   State // 最新已知状态
	valid       bool
	maxExtrapMS int64 // 最大外推时间（超限返回 lastState）
}

// NewExtrapolation 创建外推策略。
// maxExtrapMS: 最大外推毫秒数（如 200ms），超限停止外推、等待新快照。
func NewExtrapolation(maxExtrapMS int64) *ExtrapolationStrategy {
	if maxExtrapMS <= 0 {
		maxExtrapMS = 200
	}
	return &ExtrapolationStrategy{maxExtrapMS: maxExtrapMS}
}

// AddSnapshot 记录最新服务端快照。
func (es *ExtrapolationStrategy) AddSnapshot(state State) {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.lastState = state
	es.valid = true
}

// Interpolate 外推当前渲染位置。
// nowMS: 当前渲染时间（毫秒）。
func (es *ExtrapolationStrategy) Interpolate(nowMS int64) (State, bool) {
	es.mu.Lock()
	defer es.mu.Unlock()

	if !es.valid {
		return State{}, false
	}

	dt := nowMS - es.lastState.Time
	if dt <= 0 {
		return es.lastState, true
	}

	// 超限停止外推
	if dt > es.maxExtrapMS {
		return es.lastState, true
	}

	dtSec := float32(dt) / 1000.0

	// 运动学外推：pos += vel*dt + 0.5*accel*dt^2, vel += accel*dt
	// RotX/RotZ 必须原样透传：只填 RotY 会让外推结果的横滚/俯仰被清零，
	// 表现为"角色一移动就把姿态摆正"（其余分量都是透传的，唯独漏了这两个）。
	return State{
		Time:       nowMS,
		PosX:       es.lastState.PosX + es.lastState.VelX*dtSec + 0.5*es.lastState.AccelX*dtSec*dtSec,
		PosY:       es.lastState.PosY + es.lastState.VelY*dtSec + 0.5*es.lastState.AccelY*dtSec*dtSec,
		PosZ:       es.lastState.PosZ + es.lastState.VelZ*dtSec + 0.5*es.lastState.AccelZ*dtSec*dtSec,
		RotX:       es.lastState.RotX, // 旋转暂不外推
		RotY:       es.lastState.RotY, // 旋转暂不外推
		RotZ:       es.lastState.RotZ, // 旋转暂不外推
		VelX:       es.lastState.VelX + es.lastState.AccelX*dtSec,
		VelY:       es.lastState.VelY + es.lastState.AccelY*dtSec,
		VelZ:       es.lastState.VelZ + es.lastState.AccelZ*dtSec,
		AccelX:     es.lastState.AccelX,
		AccelY:     es.lastState.AccelY,
		AccelZ:     es.lastState.AccelZ,
		ServerTick: es.lastState.ServerTick,
	}, true
}

// Reset 清空缓存。
func (es *ExtrapolationStrategy) Reset() {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.valid = false
}
