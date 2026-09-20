package ratelimit

import (
	"math"
	"sync"
	"time"
)

// 秒级浮点时间戳：GCRA 内部所有时间运算都用「unix 秒」浮点，避免 time.Time 的开销与取整误差。
func toSeconds(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// GCRA 通用信元速率算法（Generic Cell Rate Algorithm），是 Redis CL.THROTTLE、
// Cloudflare 等现代限流的事实标准。
//
// 与令牌桶/窗口算法相比，GCRA 用「单个理论到达时间(theoretical arrival time, TAT)」一个浮点
// 时间戳即可同时表达「持续速率 + 平滑突发」：无计数器漂移、无桶溢出阈值、状态极小（一个 float64）。
// 它等价于一个「无容量上限、但到达间隔被 TAT 约束」的虚拟漏桶，因而既防突发滥用又对合法流量最友好。
//
// 算法参数：
// - τ（tau，发射间隔）= 1/rate（秒/个）
// - T（tol，突发容差周期）= burst × τ（秒）
// 到达时刻 t 时：TAT = max(t, conform) + n×τ；若 TAT - t ≤ T 则放行并把 conform 推进到 TAT，
// 否则拒绝（conform 不变，重试需等到 TAT - t ≤ T）。
type GCRA struct {
	mu      sync.Mutex
	clock   Clock
	tau     float64 // 发射间隔（秒）
	tol     float64 // 突发容差周期（秒）
	conform float64 // 理论到达时间（unix 秒），即「已付出的时间债务」
}

// NewGCRA 构造 GCRA 限流器（默认时间源 time.Now）。rate 为持续速率（个/秒），burst 为允许的最大突发数。
func NewGCRA(rate float64, burst int) *GCRA {
	return NewGCRAWithClock(rate, burst, defaultClock())
}

// NewGCRAWithClock 注入自定义时间源（测试用）。
func NewGCRAWithClock(rate float64, burst int, clock Clock) *GCRA {
	if rate <= 0 {
		rate = 1
	}
	if burst <= 0 {
		burst = 1
	}
	tau := 1.0 / rate
	return &GCRA{clock: clock, tau: tau, tol: float64(burst) * tau}
}

func (g *GCRA) maxTAT(n int, now float64) (tat float64, ok bool) {
	tat = math.Max(now, g.conform) + float64(n)*g.tau
	return tat, tat-now <= g.tol+1e-9
}

// Allow 放行 1 个请求。
func (g *GCRA) Allow() bool { return g.AllowN(1) }

// AllowN 放行 n 个请求；n<=0 视为放行。
func (g *GCRA) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	tat, ok := g.maxTAT(n, toSeconds(g.clock()))
	if ok {
		g.conform = tat
	}
	return ok
}

// Reserve 放行 1 个；被限流时返回需等待的时长（直至下一枚额度可用）。
func (g *GCRA) Reserve() (bool, time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := toSeconds(g.clock())
	tat, ok := g.maxTAT(1, now)
	if ok {
		g.conform = tat
		return true, 0
	}
	// 需等到 max(t',conform)+tau - t' <= tol，即 t' >= conform+tau-tol。
	retry := g.conform + g.tau - g.tol - now
	if retry < 0 {
		retry = 0
	}
	return false, time.Duration(retry * float64(time.Second))
}

// Remaining 当前可立即放行的剩余额度（非负整数）。
func (g *GCRA) Remaining() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := toSeconds(g.clock())
	var avail float64
	if g.conform >= now {
		avail = (g.tol - (g.conform - now)) / g.tau
	} else {
		avail = g.tol / g.tau
	}
	if avail < 0 {
		avail = 0
	}
	// 可用额度须向下取整（不能谎报超出实际的额度），但先用极小 epsilon 吸收浮点误差再 Floor，
	// 保证「本应恰为整数 N」的场景稳定得到 N 而非 N-1。
	return int(math.Floor(avail + 1e-6))
}

// Limit 突发容量上限（= burst）。
// burst 是构造入参得到的确定整数，用 Round 消除 tol/tau 的浮点误差，避免少算 1。
func (g *GCRA) Limit() int { return int(math.Round(g.tol / g.tau)) }

// ResetIn 距离下一枚额度可用的时长（已满则 0）。
func (g *GCRA) ResetIn() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := toSeconds(g.clock())
	if g.conform <= now {
		return 0
	}
	t := g.conform + g.tau - g.tol
	if t <= now {
		return 0
	}
	return time.Duration((t - now) * float64(time.Second))
}

// Reset 立刻把时间债务清零，恢复满额（GM 解禁一次操作）。
func (g *GCRA) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.conform = toSeconds(g.clock())
}
