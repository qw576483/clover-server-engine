// Package sync 插值策略：缓存最近两个服务端快照，在当前渲染帧之间做线性插值。
package sync

import (
	"sort"
	"sync"
)

// InterpolationStrategy 插值同步策略。
// 维护快照历史（默认保留最近 6 帧），在延迟余量内平滑插值。
type InterpolationStrategy struct {
	mu            sync.Mutex
	snapshots     []State // 按 Time 排序的快照队列
	renderDelayMS int64   // 渲染延迟（插值在 now-delay 处计算）
	maxHistory    int     // 最大保留快照数
	lastIdx       int     // 上次匹配的快照索引（当前未使用）
	lastResult    State   // 上次插值结果（数据不足时复用）
	valid         bool    // lastResult 是否有效
}

// NewInterpolation 创建插值策略。
// renderDelayMS: 渲染延迟（通常为 100ms=2~3 帧），越大画面越平滑但延迟越高。
// maxHistory: 最大保留的快照数（默认 6）。
func NewInterpolation(renderDelayMS int64, maxHistory int) *InterpolationStrategy {
	if renderDelayMS <= 0 {
		renderDelayMS = 100
	}
	if maxHistory <= 0 {
		maxHistory = 6
	}
	return &InterpolationStrategy{
		renderDelayMS: renderDelayMS,
		maxHistory:    maxHistory,
		lastIdx:       -1,
	}
}

// AddSnapshot 添加服务端状态快照。
func (is *InterpolationStrategy) AddSnapshot(state State) {
	is.mu.Lock()
	defer is.mu.Unlock()

	// 二分插入保持有序
	idx := sort.Search(len(is.snapshots), func(i int) bool {
		return is.snapshots[i].Time >= state.Time
	})

	if idx < len(is.snapshots) && is.snapshots[idx].Time == state.Time {
		// 更新已有时间点
		is.snapshots[idx] = state
		return
	}

	is.snapshots = append(is.snapshots, State{})
	copy(is.snapshots[idx+1:], is.snapshots[idx:])
	is.snapshots[idx] = state

	// 裁剪超量历史
	if len(is.snapshots) > is.maxHistory {
		is.snapshots = is.snapshots[len(is.snapshots)-is.maxHistory:]
	}
}

// Interpolate 计算当前时间对应的内插状态。
// nowMS: 当前渲染时间（毫秒）。
func (is *InterpolationStrategy) Interpolate(nowMS int64) (State, bool) {
	is.mu.Lock()
	defer is.mu.Unlock()

	renderTime := nowMS - is.renderDelayMS

	if len(is.snapshots) == 0 {
		if is.valid {
			return is.lastResult, true
		}
		return State{}, false
	}

	if len(is.snapshots) == 1 || is.snapshots[len(is.snapshots)-1].Time <= renderTime {
		// 只有一个快照，或所有快照都早于渲染时间
		state := is.snapshots[len(is.snapshots)-1]
		is.lastResult = state
		is.valid = true
		return state, true
	}

	if is.snapshots[0].Time >= renderTime {
		// 所有快照都晚于渲染时间
		state := is.snapshots[0]
		is.lastResult = state
		is.valid = true
		return state, true
	}

	// 找到渲染时间所在的区间 [a, b]
	for i := 0; i < len(is.snapshots)-1; i++ {
		a := is.snapshots[i]
		b := is.snapshots[i+1]
		if a.Time <= renderTime && renderTime <= b.Time {
			span := float32(b.Time - a.Time)
			if span == 0 {
				is.lastResult = a
				is.valid = true
				return a, true
			}
			t := float32(renderTime-a.Time) / span
			result := LerpState(a, b, t)
			is.lastResult = result
			is.valid = true
			return result, true
		}
	}

	// 兜底：取最新
	state := is.snapshots[len(is.snapshots)-1]
	is.lastResult = state
	is.valid = true
	return state, true
}

// Reset 清空缓存。
func (is *InterpolationStrategy) Reset() {
	is.mu.Lock()
	defer is.mu.Unlock()
	is.snapshots = nil
	is.lastIdx = -1
	is.valid = false
}

// SnapCount 返回当前缓存快照数。
func (is *InterpolationStrategy) SnapCount() int {
	is.mu.Lock()
	defer is.mu.Unlock()
	return len(is.snapshots)
}
