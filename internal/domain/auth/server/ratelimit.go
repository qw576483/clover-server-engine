package server

import (
	"net"
	"net/http"
	"sync"
	"time"

	"clover-server-engine/internal/domain/auth/state"
	"clover-server-engine/internal/foundation/ophttp"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/runtime/ratelimit"
)

// rateLimitedResp 限流的统一回包（429）。文案与状态码都不区分「哪个账号/哪一步」，
// 避免成为探测信道。
var rateLimitedResp = state.TokenResp{Err: "请求过于频繁，请稍后再试"}

// 账号服 HTTP 入口的 per-IP 限流默认参数。
//
// 为什么账号服必须自己限流：/auth/signup 与 /auth/login 是**撞库与账号枚举的第一接触面**。
// 域内的 per-account 撞库防护（state/loginGuard）只按账号计数，攻击者换账号名即可绕过；
// 传输层没有任何请求频率约束。这一层补的就是「同一个来源不能无限刷」。
//
// 默认值取向：账号链路是低频交互（登录/注册每次几十 ms 到几百 ms），
// 5 req/s + burst 10 对正常玩家（含 NAT 后共享出口 IP 的小群体）足够宽松，
// 对在线撞库则把速率压到不可行。P2P/代理场景可按配置调大。
const (
	defaultIPRatePerSec = 5
	defaultIPRateBurst  = 10
	// ipBucketIdleTTL 空闲桶保留时长：桶只在被访问时才创建，长期不活跃的来源必须回收，
	// 否则「海量随机来源 IP」会让这张 map 无上限增长（与 state/loginGuard 的惰性清扫同理）。
	ipBucketIdleTTL = 10 * time.Minute
	// ipSweepEvery 每 N 次放行检查触发一次清扫（摊还成本，避免每次请求扫全表）。
	ipSweepEvery = 1024
)

// ipBucket 单个来源 IP 的令牌桶 + 最后活跃时间。
type ipBucket struct {
	tb   *ratelimit.TokenBucket
	seen time.Time
}

// ipLimiter per-IP 令牌桶限流器（线程安全）。
type ipLimiter struct {
	mu        sync.Mutex
	buckets   map[string]*ipBucket
	rate      float64
	burst     int
	checks    int
	lastSweep time.Time
}

// newIPLimiter 构造 per-IP 限流器。perSec <= 0 / burst <= 0 时用内置默认值。
func newIPLimiter(perSec, burst int) *ipLimiter {
	if perSec <= 0 {
		perSec = defaultIPRatePerSec
	}
	if burst <= 0 {
		burst = defaultIPRateBurst
	}
	if burst < 1 {
		burst = 1
	}
	return &ipLimiter{
		buckets:   make(map[string]*ipBucket),
		rate:      float64(perSec),
		burst:     burst,
		lastSweep: time.Now(),
	}
}

// allow 判断该来源是否可放行一次请求。
func (l *ipLimiter) allow(ip string) bool {
	if l == nil {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[ip]
	if !ok {
		b = &ipBucket{tb: ratelimit.NewTokenBucket(l.rate, l.burst)}
		l.buckets[ip] = b
	}
	b.seen = now
	ok = b.tb.Allow()
	l.checks++
	if l.checks >= ipSweepEvery {
		l.checks = 0
		l.sweepLocked(now)
	}
	return ok
}

// sweepLocked 回收长期空闲的桶（调用方须持 l.mu）。
func (l *ipLimiter) sweepLocked(now time.Time) {
	for ip, b := range l.buckets {
		if now.Sub(b.seen) > ipBucketIdleTTL {
			delete(l.buckets, ip)
		}
	}
	l.lastSweep = now
}

// clientIP 取限流用的来源标识。
//
// **刻意不读 X-Forwarded-For / X-Real-IP**：那两个头由客户端完全可控，采信它等于
// 让攻击者每个请求换一个 key，限流形同不存在。只有在账号服确实部署在受信任反代之后、
// 且反代保证重写该头时才可以改这里（属于部署契约，不是代码默认）。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// guard 把 handler 包成「先过限流再执行」。limiter 为 nil 时直接透传。
func (l *ipLimiter) guard(path string, h func(w http.ResponseWriter, r *http.Request)) func(http.ResponseWriter, *http.Request) {
	if l == nil {
		return h
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !l.allow(ip) {
			// 非预期分支必须留痕；日志本身也可能被刷，故只记一行摘要不记 body。
			logger.Warnf("auth: rate limited %s from %s (per-ip limit %g/s burst %d)", path, r.RemoteAddr, l.rate, l.burst)
			ophttp.JSON(w, http.StatusTooManyRequests, rateLimitedResp)
			return
		}
		h(w, r)
	}
}
