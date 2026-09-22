// 滑动窗口**累加量**限制器：补上「次数」类限流器表达不了的一类约束。
package ratelimit

import (
	"sync"
	"time"
)

// SlidingSum 滑动窗口累加量限制器：只要「窗口内已累加的总量 + n」不超过 limit 就放行并累加。
//
// 为什么必须有它（现有三种算法都做不到）：
// TokenBucket / FixedWindow / SlidingWindow 的计数单位是**次数**（int），只回答"多少次"。
// 而有一类约束的累加量是**浮点且每次不等**，典型就是移动校验「1 秒内累计位移不超过 12 米」——
// 每次加入的是本帧位移（受帧率与速度共同影响），不是 1。
//
// 用次数限流去近似它，会把约束悄悄换掉：40 条/秒 ≠ 12 米/秒。
// 现实反例：客户端降到 1Hz 发包、每包位移 50 米 —— 条数完全不超，
// 但一帧瞬移 50 米。位移约束必须按**量**算，不能按**次数**算。
//
// 实现：把窗口切成 bucketCount 个等长桶（环形复用），每桶记录自己的起始时间与累加值；
// 取数时把过期桶清零。精度 = 窗口 / bucketCount（默认 1s/10 = 100ms），内存恒定 O(bucketCount)。
type SlidingSum struct {
	mu      sync.Mutex
	clock   Clock
	limit   float64
	win     time.Duration
	buckets []sumBucket
}

type sumBucket struct {
	at  time.Time // 桶的起始时刻
	sum float64
}

// sumEpsilon 上限比较的相对容差。
//
// 为什么必须有：浮点累加不可避免带误差，`0.3` 加 40 次在 float64 下是 `12.000000000000002`。
// 严格比较会让"数学上恰好等于上限"的请求**随机**被拒（取决于累计顺序），
// 而阈值附近抖动（同一操作忽而被拒忽而放行）比多放行几个 1e-9 的量危害大得多。
const sumEpsilon = 1e-9

// NewSlidingSum 构造滑动窗口累加器（默认时间源 time.Now，10 个桶）。
// limit <= 0 视为 1；window <= 0 视为 1 秒。
func NewSlidingSum(limit float64, window time.Duration) *SlidingSum {
	return NewSlidingSumWithClock(limit, window, 10, defaultClock())
}

// NewSlidingSumWithClock 注入自定义时间源与桶数（测试用）。
func NewSlidingSumWithClock(limit float64, window time.Duration, bucketCount int, clock Clock) *SlidingSum {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	if bucketCount <= 0 {
		bucketCount = 10
	}
	return &SlidingSum{
		clock:   clock,
		limit:   limit,
		win:     window,
		buckets: make([]sumBucket, bucketCount),
	}
}

// bucketFor 定位 now 所属的桶；沿路把过期桶清零。
// 调用方须持锁。
func (s *SlidingSum) bucketFor(now time.Time) *sumBucket {
	span := s.win / time.Duration(len(s.buckets))
	if span <= 0 {
		span = time.Nanosecond
	}
	// 当前时间对应的全局桶序号：用它取模定位环形下标。
	slot := int(now.UnixNano()/int64(span)) % len(s.buckets)
	if slot < 0 {
		slot += len(s.buckets)
	}
	b := &s.buckets[slot]
	// 同一个下标可能是「上一圈的旧桶」——起始时间差超过一个窗口就必须清零，
	// 否则旧数据会被当成当前窗口内的量（表现为"明明过了很久还被限流"）。
	if b.at.IsZero() || now.Sub(b.at) >= s.win {
		b.at = now.Truncate(span)
		b.sum = 0
	}
	return b
}

// sumLocked 统计窗口内累加值（顺带清理过期桶）。调用方须持锁。
func (s *SlidingSum) sumLocked(now time.Time) float64 {
	var total float64
	for i := range s.buckets {
		b := &s.buckets[i]
		if !b.at.IsZero() && now.Sub(b.at) >= s.win {
			b.sum = 0
			b.at = time.Time{}
		}
		total += b.sum
	}
	return total
}

// Allow 若「窗口内总量 + n」不超过 limit，则累加并放行；否则拒绝且**不累加**。
//
// n <= 0 视为放行（不产生量的操作不该被限）。注意：被拒绝时**不**写入，所以
// 连续超限的请求不会把窗口越填越满——它们等窗口滑过去即可恢复，与"惩罚性累加"是两种语义。
func (s *SlidingSum) Allow(n float64) bool {
	if n <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if s.sumLocked(now)+n > s.limit*(1+sumEpsilon) {
		return false
	}
	s.bucketFor(now).sum += n
	return true
}

// Sum 当前窗口内已累加的总量。
func (s *SlidingSum) Sum() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sumLocked(s.clock())
}

// Remaining 当前窗口还能累加多少。
func (s *SlidingSum) Remaining() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.limit - s.sumLocked(s.clock())
	if r < 0 {
		return 0
	}
	return r
}

// Limit 窗口内的累加上限。
func (s *SlidingSum) Limit() float64 { return s.limit }

// Reset 立刻清空全部窗口。
func (s *SlidingSum) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.buckets {
		s.buckets[i].sum = 0
		s.buckets[i].at = time.Time{}
	}
}
