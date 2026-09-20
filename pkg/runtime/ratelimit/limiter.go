// 本文件提供限流器公共抽象（Clock / Limiter）与三种主流算法：
// 令牌桶（TokenBucket）、固定窗口（FixedWindow）、滑动窗口（SlidingWindow）。
package ratelimit

import (
	"math"
	"sync"
	"time"
)

// Clock 抽象时间来源，便于测试注入虚拟时钟。默认 time.Now。
type Clock func() time.Time

func defaultClock() Clock { return time.Now }

// Limiter 单计数器限流器接口，对应一个 key（或 (key, policy) 组合）。
// 三种算法（令牌桶 / 固定窗口 / 滑动窗口）都实现它，Manager 可统一调度。
type Limiter interface {
	// Allow 是否放行 1 个请求。
	Allow() bool
	// AllowN 是否放行 n 个请求。
	AllowN(n int) bool
	// Reserve 尝试放行 1 个；若被限流返回 false 与「需等待多久才能再试」（retryAfter）。
	Reserve() (ok bool, retryAfter time.Duration)
	// Remaining 当前剩余可用额度。
	Remaining() int
	// Limit 该限流器容量上限。
	Limit() int
	// ResetIn 距离下一次额度补充 / 窗口重置的时长（提示「多久后恢复」）。
	ResetIn() time.Duration
	// Reset 立刻将额度恢复满。
	Reset()
}

// TokenBucket 令牌桶：以 rate（个/秒）持续补充，桶容量 burst。
// 适合「突发 + 平滑限速」（如登录/发消息/GM 操作频率）。可瞬间容纳 burst 个突发，
// 之后按 rate 平滑补充。
type TokenBucket struct {
	mu     sync.Mutex
	clock  Clock
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

// NewTokenBucket 构造令牌桶（默认时间源 time.Now）。
func NewTokenBucket(rate float64, burst int) *TokenBucket {
	return NewTokenBucketWithClock(rate, burst, defaultClock())
}

// NewTokenBucketWithClock 注入自定义时间源（测试用）。
func NewTokenBucketWithClock(rate float64, burst int, clock Clock) *TokenBucket {
	if rate <= 0 {
		rate = 1
	}
	if burst <= 0 {
		burst = 1
	}
	return &TokenBucket{
		clock:  clock,
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   clock(),
	}
}

func (t *TokenBucket) refill() {
	now := t.clock()
	elapsed := now.Sub(t.last).Seconds()
	if elapsed <= 0 {
		return
	}
	t.tokens += elapsed * t.rate
	if t.tokens > t.burst {
		t.tokens = t.burst
	}
	t.last = now
}

// Allow 放行 1 个请求。
func (t *TokenBucket) Allow() bool { return t.AllowN(1) }

// AllowN 放行 n 个请求；n<=0 视为放行。
func (t *TokenBucket) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill()
	if t.tokens >= float64(n) {
		t.tokens -= float64(n)
		return true
	}
	return false
}

// Reserve 放行 1 个；被限流时返回需等待时长。
func (t *TokenBucket) Reserve() (bool, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill()
	if t.tokens >= 1 {
		t.tokens -= 1
		return true, 0
	}
	deficit := 1 - t.tokens
	wait := time.Duration(deficit / t.rate * float64(time.Second))
	if wait <= 0 {
		wait = time.Nanosecond
	}
	return false, wait
}

// Remaining 当前剩余令牌数。
func (t *TokenBucket) Remaining() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return int(t.tokens)
}

// Limit 桶容量。
func (t *TokenBucket) Limit() int { return int(t.burst) }

// ResetIn 距离补充 1 个令牌的时长（已满则 0）。
func (t *TokenBucket) ResetIn() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tokens >= 1 {
		return 0
	}
	deficit := 1 - t.tokens
	wait := time.Duration(deficit / t.rate * float64(time.Second))
	if wait <= 0 {
		wait = time.Nanosecond
	}
	return wait
}

// Reset 立刻补满令牌。
func (t *TokenBucket) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tokens = t.burst
	t.last = t.clock()
}

// FixedWindow 固定窗口计数：每 window 时长最多 max 次，窗口到点直接清零。
// 实现最简单、内存最小；边界处可能出现「双倍突发」（窗口交界允许 2×max）。
type FixedWindow struct {
	mu     sync.Mutex
	clock  Clock
	max    int
	window time.Duration
	count  int
	start  time.Time
}

// NewFixedWindow 构造固定窗口（默认时间源 time.Now）。
func NewFixedWindow(max int, window time.Duration) *FixedWindow {
	return NewFixedWindowWithClock(max, window, defaultClock())
}

// NewFixedWindowWithClock 注入自定义时间源（测试用）。
func NewFixedWindowWithClock(max int, window time.Duration, clock Clock) *FixedWindow {
	if max <= 0 {
		max = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &FixedWindow{clock: clock, max: max, window: window, start: clock()}
}

// Allow 放行 1 个请求。
func (f *FixedWindow) Allow() bool { return f.AllowN(1) }

// AllowN 放行 n 个请求；窗口内余量不足则拒绝。
func (f *FixedWindow) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock()
	if now.Sub(f.start) >= f.window {
		f.start = now
		f.count = 0
	}
	if f.count+n <= f.max {
		f.count += n
		return true
	}
	return false
}

// Reserve 放行 1 个；被限流时返回至窗口重置的等待时长。
func (f *FixedWindow) Reserve() (bool, time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock()
	if now.Sub(f.start) >= f.window {
		f.start = now
		f.count = 0
	}
	if f.count < f.max {
		f.count++
		return true, 0
	}
	return false, f.start.Add(f.window).Sub(now)
}

// Remaining 当前窗口剩余可用次数。
func (f *FixedWindow) Remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.clock().Sub(f.start) >= f.window {
		return f.max
	}
	return f.max - f.count
}

// Limit 窗口上限。
func (f *FixedWindow) Limit() int { return f.max }

// ResetIn 距离窗口重置的时长（未满则 0）。
func (f *FixedWindow) ResetIn() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock()
	if now.Sub(f.start) >= f.window {
		return 0
	}
	return f.start.Add(f.window).Sub(now)
}

// Reset 立刻清零计数并重置窗口。
func (f *FixedWindow) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count = 0
	f.start = f.clock()
}

// SlidingWindow 滑动窗口计数器（双桶近似）：在 window 时长内平滑限 max 次，
// 边界处不会像固定窗口那样出现「2×max」突发；精度介于令牌桶与固定窗口之间、内存极小。
type SlidingWindow struct {
	mu     sync.Mutex
	clock  Clock
	max    int
	win    time.Duration
	cur    int
	curAt  time.Time
	prev   int
	prevAt time.Time
}

// NewSlidingWindow 构造滑动窗口（默认时间源 time.Now）。
func NewSlidingWindow(max int, window time.Duration) *SlidingWindow {
	return NewSlidingWindowWithClock(max, window, defaultClock())
}

// NewSlidingWindowWithClock 注入自定义时间源（测试用）。
func NewSlidingWindowWithClock(max int, window time.Duration, clock Clock) *SlidingWindow {
	if max <= 0 {
		max = 1
	}
	if window <= 0 {
		window = time.Second
	}
	now := clock()
	return &SlidingWindow{clock: clock, max: max, win: window, curAt: now, prevAt: now}
}

func (s *SlidingWindow) rotate(now time.Time) {
	// 防时钟跳变死循环：每次至少跳一个 win，最多 1000 次
	for i := 0; i < 1000 && now.Sub(s.curAt) >= s.win; i++ {
		s.prev = s.cur
		s.prevAt = s.curAt
		s.cur = 0
		s.curAt = s.curAt.Add(s.win)
	}
	// 极端跳变（>1000 个 win）直接重置
	if now.Sub(s.curAt) >= s.win {
		s.prev = 0
		s.cur = 0
		s.curAt = now.Truncate(s.win)
		s.prevAt = s.curAt.Add(-s.win)
	}
	if now.Sub(s.prevAt) >= s.win*2 {
		s.prev = 0
	}
}

// count 返回当前窗口（含上一桶按重叠权重折算）的近似计数。调用方须持锁。
func (s *SlidingWindow) count(now time.Time) int {
	s.rotate(now)
	elapsed := now.Sub(s.curAt).Seconds()
	w := 1.0
	if sec := s.win.Seconds(); sec > 0 {
		w = 1 - elapsed/sec
		if w < 0 {
			w = 0
		}
	}
	// 上一桶的衰减贡献向上取整再折算：用 int() 截断会把 <1 的贡献系统性抹成 0
	//（prev=1、w=0.9 时折算为 0），使计数低于真实加权值、同一窗口内可放行数超过 max。
	return s.cur + int(math.Ceil(float64(s.prev)*w))
}

// waitUntilAvail 计算「近似计数降到 < max、腾出 1 个额度」所需的等待时长。调用方须持锁、须已 rotate。
// 按双桶近似模型求解首次满足 cur + prev*w < max 的时刻：prev 桶随时间线性衰减会逐步腾出额度，直接返回整窗会高估等待。
// - 若 cur >= max：当前桶自身已占满，无论 prev 如何衰减都不够，须等当前桶轮转出局
// （此后 cur 成为 prev、新 cur=0），即等待到当前桶结束：win - elapsed。
// - 否则求解 prev*(1 - t/win) < max - cur，即 t > win*(1 - (max-cur)/prev)。
func (s *SlidingWindow) waitUntilAvail(now time.Time) time.Duration {
	sec := s.win.Seconds()
	elapsed := now.Sub(s.curAt).Seconds()
	untilRotate := s.win - now.Sub(s.curAt)
	if untilRotate < 0 {
		untilRotate = 0
	}
	// 当前桶自身即已达上限：必须等它轮转出去。
	if s.cur >= s.max || s.prev <= 0 || sec <= 0 {
		return untilRotate
	}
	room := float64(s.max - s.cur) // >0
	// 需要 prev 的加权贡献 < room。求 t（自 curAt 起的绝对偏移秒）。
	tAbs := sec * (1 - room/float64(s.prev))
	wait := tAbs - elapsed
	if wait <= 0 {
		return time.Nanosecond
	}
	need := time.Duration(wait * float64(time.Second))
	// 上界为当前桶轮转时刻：轮转后 prev 贡献一次性归零，必然腾出额度。
	if need > untilRotate {
		return untilRotate
	}
	if need <= 0 {
		return time.Nanosecond
	}
	return need
}

// Allow 放行 1 个请求。
func (s *SlidingWindow) Allow() bool { return s.AllowN(1) }

// AllowN 放行 n 个请求；窗口内余量不足则拒绝。
func (s *SlidingWindow) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count(s.clock())+n <= s.max {
		s.cur += n
		return true
	}
	return false
}

// Reserve 放行 1 个；被限流时返回至当前桶重置的等待时长。
func (s *SlidingWindow) Reserve() (bool, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if s.count(now) < s.max {
		s.cur++
		return true, 0
	}
	// count() 已 rotate；返回精确的「腾出 1 个额度」等待时长而非整窗。
	return false, s.waitUntilAvail(now)
}

// Remaining 当前窗口剩余可用次数（近似，非负）。
func (s *SlidingWindow) Remaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.max - s.count(s.clock())
	if r < 0 {
		return 0
	}
	return r
}

// Limit 窗口上限。
func (s *SlidingWindow) Limit() int { return s.max }

// ResetIn 距离当前桶重置的时长（未满则 0）。
func (s *SlidingWindow) ResetIn() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if s.count(now) < s.max {
		return 0
	}
	// 同 Reserve，返回精确的「腾出 1 个额度」等待时长（count 内已 rotate）。
	return s.waitUntilAvail(now)
}

// Reset 立刻清零计数。
func (s *SlidingWindow) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur, s.prev = 0, 0
	s.curAt, s.prevAt = s.clock(), s.clock()
}
