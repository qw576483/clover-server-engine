package timewindow

import (
	"testing"
	"time"
)

// 本组用例直接驱动 align（同包可见）验证核心不变量：
// Incr/Sum 内部读取 time.Now，无法注入时钟；而所有过期判定逻辑都在 align 内。

const (
	testBucketCount = 6
	testBucketSize  = int64(10 * time.Second)
)

// newTestWindow 构造 6 桶 × 10s（总窗口 60s）的窗口计数器。
func newTestWindow() *TimeWindow {
	return NewTimeWindow(testBucketCount, time.Duration(testBucketSize))
}

func sumBuckets(tw *TimeWindow) int64 {
	var total int64
	for _, v := range tw.buckets {
		total += v
	}
	return total
}

// TestAlignExpiresAtExactWindowBoundary 回归：
// 桶时间戳与当前时刻恰好相差一整个窗口时，旧计数必须清零，
// 否则它在同一槽位与新增量叠加，Sum 逐窗虚高。
func TestAlignExpiresAtExactWindowBoundary(t *testing.T) {
	tw := newTestWindow()
	base := int64(1_000_000) * testBucketSize // 任意非零基准（同时保证 (base/bs)%6 固定）
	tw.align(base)
	tw.buckets[tw.head] = 1
	tw.timestamps[tw.head] = base

	// 恰好一整个窗口（6×10s）之后。
	tw.align(base + testBucketCount*testBucketSize)
	if got := sumBuckets(tw); got != 0 {
		t.Fatalf("跨整窗口后旧计数未清零：sum=%d", got)
	}
	if tw.timestamps[tw.head] != base+testBucketCount*testBucketSize {
		t.Fatalf("head 槽位时间戳 = %d，应为 %d", tw.timestamps[tw.head], base+testBucketCount*testBucketSize)
	}
}

// TestAlignKeepsInWindowBuckets 窗口内的计数不得被误清：
// 前进 5 个桶（仍 < 60s 窗口）后，前一个桶的计数必须保留。
func TestAlignKeepsInWindowBuckets(t *testing.T) {
	tw := newTestWindow()
	base := int64(1_000_000) * testBucketSize
	tw.align(base)
	tw.buckets[tw.head] = 3

	// 前进 5 个桶：base 桶时间差 50s < 60s，仍在窗口内。
	tw.align(base + 5*testBucketSize)
	if got := sumBuckets(tw); got != 3 {
		t.Fatalf("窗口内计数被误清：sum=%d，期望 3", got)
	}

	// 再前进 5 个桶（距 base 共 100s）：base 桶必须过期，且不与该槽位的新桶叠加。
	tw.align(base + 10*testBucketSize)
	if got := sumBuckets(tw); got != 0 {
		t.Fatalf("过期桶未清零：sum=%d，期望 0", got)
	}
}

// TestAlignFirstUseWritesTimestamp 首次使用时不得走「同桶快速路径」提前返回：
// 时间戳保持 0 会让该槽位从此脱离清理管辖。
func TestAlignFirstUseWritesTimestamp(t *testing.T) {
	tw := newTestWindow()
	base := int64(1_000_000) * testBucketSize
	tw.align(base)
	if tw.timestamps[tw.head] != base {
		t.Fatalf("首次 align 后时间戳 = %d，应为 %d（未写入 = 永不清理）", tw.timestamps[tw.head], base)
	}
	// 同桶（1ns 后）再 align：head 与时间戳均不应变化。
	tw.align(base + 1)
	if tw.timestamps[tw.head] != base {
		t.Fatalf("同桶内 align 不应改写时间戳：%d", tw.timestamps[tw.head])
	}
}
