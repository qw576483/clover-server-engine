package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// 固定时钟：测试里手动推进，避免依赖真实时间。
func newTestClock() (*time.Time, Clock) {
	now := time.Unix(1_700_000_000, 0)
	return &now, func() time.Time { return now }
}

// TestSlidingSum_AccumulatesUntilLimit 累加到上限为止，边界含等号；被拒的不写入。
func TestSlidingSum_AccumulatesUntilLimit(t *testing.T) {
	_, clock := newTestClock()
	s := NewSlidingSumWithClock(12, time.Second, 10, clock)

	if !s.Allow(5) {
		t.Fatal("窗口内 0+5 <= 12，应放行")
	}
	if !s.Allow(7) {
		t.Fatal("5+7 = 12 恰好等于上限，应放行（边界含等号）")
	}
	if s.Allow(0.1) {
		t.Fatal("12+0.1 超过上限，应拒绝")
	}
	if got := s.Sum(); got != 12 {
		t.Errorf("被拒的请求不该累加：Sum = %v, 期望 12", got)
	}
	if got := s.Remaining(); got != 0 {
		t.Errorf("Remaining = %v, 期望 0", got)
	}
}

// TestSlidingSum_ManySmallAddsStillLimited 是本类型存在的理由：
// 40 次 0.3（合计 12）放行、第 41 次拒绝 —— 用"次数"限流（40 次/秒）**抓不到**这种超限。
func TestSlidingSum_ManySmallAddsStillLimited(t *testing.T) {
	_, clock := newTestClock()
	s := NewSlidingSumWithClock(12, time.Second, 10, clock)

	for i := 0; i < 40; i++ {
		if !s.Allow(0.3) {
			t.Fatalf("第 %d 次 0.3（累计 %.1f）不该被拒", i+1, float64(i+1)*0.3)
		}
	}
	if s.Allow(0.3) {
		t.Error("累计已到 12，再加 0.3 应拒绝（次数没超，但总量超了）")
	}
	if got := s.Sum(); got < 11.999 || got > 12.001 {
		t.Errorf("Sum = %v, 期望 12", got)
	}
}

// TestSlidingSum_WindowSlides 窗口滑过之后额度恢复（这是"限流"而不是"永久封禁"）。
func TestSlidingSum_WindowSlides(t *testing.T) {
	now, clock := newTestClock()
	s := NewSlidingSumWithClock(10, time.Second, 10, clock)

	if !s.Allow(10) {
		t.Fatal("首次 10 应放行")
	}
	if s.Allow(1) {
		t.Fatal("窗口已满，应拒绝")
	}

	// 推进到刚好一个窗口之后：旧桶全部过期。
	*now = now.Add(time.Second)
	if !s.Allow(1) {
		t.Fatal("窗口滑过后应恢复额度")
	}
	if got := s.Sum(); got != 1 {
		t.Errorf("过期数据必须被清掉：Sum = %v, 期望 1", got)
	}
}

// TestSlidingSum_SingleHugeJumpRejected 一次巨量位移直接拒绝（瞬移检测的语义）。
func TestSlidingSum_SingleHugeJumpRejected(t *testing.T) {
	_, clock := newTestClock()
	s := NewSlidingSumWithClock(12, time.Second, 10, clock)

	if s.Allow(50) {
		t.Error("单次 50 > 上限 12，必须拒绝（否则一帧瞬移 50 米也能过）")
	}
	if got := s.Sum(); got != 0 {
		t.Errorf("被拒的巨量不该占用窗口：Sum = %v, 期望 0", got)
	}
}

// TestSlidingSum_NonPositiveAlwaysAllowed 非正增量不产生"量"，永远放行。
func TestSlidingSum_NonPositiveAlwaysAllowed(t *testing.T) {
	_, clock := newTestClock()
	s := NewSlidingSumWithClock(1, time.Second, 10, clock)

	if !s.Allow(0) || !s.Allow(-5) {
		t.Error("n <= 0 应永远放行")
	}
	if got := s.Sum(); got != 0 {
		t.Errorf("非正增量不该累加：Sum = %v", got)
	}
}

// TestSlidingSum_Reset 清空后立刻可用。
func TestSlidingSum_Reset(t *testing.T) {
	_, clock := newTestClock()
	s := NewSlidingSumWithClock(5, time.Second, 10, clock)

	s.Allow(5)
	if s.Allow(1) {
		t.Fatal("测试前提不成立：应已满")
	}
	s.Reset()
	if !s.Allow(5) {
		t.Error("Reset 之后应恢复满额")
	}
}

// TestSlidingSum_Concurrent 并发调用不 panic、不超限（-race 下跑更有意义）。
func TestSlidingSum_Concurrent(t *testing.T) {
	_, clock := newTestClock()
	s := NewSlidingSumWithClock(100, time.Second, 10, clock)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Allow(1) {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != 100 {
		t.Errorf("放行数 = %d, 期望恰好 100（上限）", allowed)
	}
}
