// Package timewindow 提供线程安全的时间窗口计数器。
//
// 时间窗口计数器用于统计最近一段时间内的累计计数，适合限流、QPS 统计、
// 滑动窗口计数等场景。窗口内部使用环形缓冲区（circular buffer）
// 存储分桶计数，自动淘汰过期桶。
//
// 典型用途：
//   - 请求速率限制（Rate Limiting）
//   - 实时 QPS / TPS 统计
//   - 滑动窗口内的玩家行为计数
//
// 设计要点：
//   - 环形缓冲区：避免频繁分配，O(1) 时间窗口滑动
//   - 线程安全：sync.Mutex 保护所有操作
//   - 自动淘汰：每次 Incr/Sum 调用时清理过期桶
//   - 纯标准库、零外部依赖
package timewindow

import (
	"sync"
	"time"
)

// TimeWindow 是一个线程安全的时间窗口计数器。
type TimeWindow struct {
	mu         sync.Mutex
	buckets    []int64 // 环形桶数组
	timestamps []int64 // 每个桶对应的时间戳（unix nano）
	head       int     // 环形缓冲区的写入位置
	bucketSize int64   // 每个桶的时间跨度（纳秒）
	windowSize int64   // 总窗口大小（纳秒）
}

// NewTimeWindow 创建一个新的时间窗口计数器。
//
// bucketCount 是窗口内的分桶数量（如 6 个桶）；
// bucketDuration 是每个桶的时间跨度（如 10 秒）。
//
// 总窗口大小为 bucketCount * bucketDuration。bucketCount 越大精度越高，
// 但也消耗更多内存。默认最小桶数量为 1，最小桶跨度为 1 纳秒。
//
// 示例：
//
//	// 6 个桶，每个桶 10 秒 → 总窗口 60 秒
//	tw := timewindow.NewTimeWindow(6, 10*time.Second)
//	tw.Incr(1)
//	qps := tw.Sum() // 最近 60 秒的总请求数
func NewTimeWindow(bucketCount int, bucketDuration time.Duration) *TimeWindow {
	if bucketCount < 1 {
		bucketCount = 1
	}
	if bucketDuration <= 0 {
		bucketDuration = time.Second
	}
	bucketSize := bucketDuration.Nanoseconds()
	if bucketSize < 1 {
		bucketSize = 1
	}
	windowSize := bucketSize * int64(bucketCount)

	return &TimeWindow{
		buckets:    make([]int64, bucketCount),
		timestamps: make([]int64, bucketCount),
		bucketSize: bucketSize,
		windowSize: windowSize,
	}
}

// Incr 在当前时间对应的桶上增加 n（可为负数）。
func (tw *TimeWindow) Incr(n int64) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.align(time.Now().UnixNano())
	tw.buckets[tw.head] += n
}

// Sum 返回窗口内所有有效桶的累计值。
func (tw *TimeWindow) Sum() int64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.align(time.Now().UnixNano())
	var total int64
	for _, v := range tw.buckets {
		total += v
	}
	return total
}

// Reset 清空所有桶的计数。
func (tw *TimeWindow) Reset() {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	for i := range tw.buckets {
		tw.buckets[i] = 0
		tw.timestamps[i] = 0
	}
	tw.head = 0
}

// BucketCount 返回分桶数量。
func (tw *TimeWindow) BucketCount() int {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return len(tw.buckets)
}

// WindowDuration 返回窗口总时长。
func (tw *TimeWindow) WindowDuration() time.Duration {
	return time.Duration(tw.windowSize)
}

// align 将 head 指针移动到当前时间对应的桶，并清理中间经过的过期桶。
// 调用方须持锁。
//
// 三个必须成对成立的不变量：
//  1. 同桶快速路径必须同时满足 head==bucketIdx 且该桶时间戳未过期；
//  2. 过期判定用 >=：恰好跨满一整个窗口的旧桶（如桶时间戳 t 与 now=t+windowSize）
//     必须清掉，用严格 > 会把它留下，其计数与新增量叠加使 Sum 逐窗虚高；
//  3. 复用环形槽位前必须清零：同一槽位可能还留着上一圈（窗口内）的计数，
//     直接 += 会与新计数叠加。
func (tw *TimeWindow) align(nowNano int64) {
	n := int64(len(tw.buckets))
	bucketIdx := int((nowNano / tw.bucketSize) % n)

	// 同桶且数据新鲜：无需移动。
	// 时间戳为 0 只表示「未写入过」，不能当新鲜桶（首用时会误走快速路径，
	// 之后永不写时间戳，该槽位的数据形态从此不再受清理管辖）。
	if tw.head == bucketIdx && tw.timestamps[tw.head] != 0 &&
		nowNano-tw.timestamps[tw.head] < tw.windowSize {
		return
	}

	// 全表清理过期桶：进程暂停跨多圈恢复时，仅按一圈内步数遍历会漏掉更早的过期桶。
	// 时间差 >= 窗口大小即过期（见不变量 2）。
	for i := range tw.buckets {
		if tw.timestamps[i] != 0 && nowNano-tw.timestamps[i] >= tw.windowSize {
			tw.buckets[i] = 0
			tw.timestamps[i] = 0
		}
	}

	// 目标槽位复用前清零（见不变量 3）：槽位已被判定过期时上面已清零；
	// 仍在窗口内但已跨入新桶周期的槽位，也必须先归零再写新桶。
	if tw.timestamps[bucketIdx] != 0 && nowNano-tw.timestamps[bucketIdx] >= tw.bucketSize {
		tw.buckets[bucketIdx] = 0
		tw.timestamps[bucketIdx] = 0
	}

	tw.head = bucketIdx
	tw.timestamps[tw.head] = nowNano
}
