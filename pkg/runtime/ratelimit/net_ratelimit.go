// 本文件提供「连接级」限流补充原语：单连接频率限流（ConnRateLimiter）
// 与全局连接数限流（GlobalLimiter）。二者均为纯内存计数，零外部依赖。
package ratelimit

import (
	"sync"
	"time"

	"clover-server-engine/pkg/foundation/logger"
)

// ConnRateLimiter 基于 token bucket 的连接频率限流器。
type ConnRateLimiter struct {
	rate   float64   // 每秒允许的消息数
	burst  int       // 突发容量
	tokens float64   // 当前 token 数量
	last   time.Time // 上次更新时间
	mu     sync.Mutex
}

// NewConnRateLimiter 创建连接限流器。
// rate: 每秒允许的消息数；burst: 最大突发消息数。
// rate <= 0 会导致桶永不补充（静默拒掉全部消息），与 TokenBucket 口径一致夹到 1 并留日志。
func NewConnRateLimiter(rate float64, burst int) *ConnRateLimiter {
	if rate <= 0 {
		logger.Warnf("ratelimit.NewConnRateLimiter: non-positive rate %v clamped to 1", rate)
		rate = 1
	}
	return &ConnRateLimiter{
		rate: rate,
		// 新建即满桶：与 TokenBucket（tokens: burst）口径一致。
		// 零值会让新连接的前 burst 条消息被直接拒绝，burst 要到长时间空闲后才可能生效。
		burst:  burst,
		tokens: float64(burst),
		last:   time.Now(),
	}
}

// Allow 消耗一个 token。返回 true 表示允许通过。
func (l *ConnRateLimiter) Allow() bool {
	return l.AllowN(1)
}

// AllowN 消耗 n 个 token。返回 true 表示允许通过。
func (l *ConnRateLimiter) AllowN(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.last).Seconds()
	l.tokens += elapsed * l.rate
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}
	l.last = now

	if l.tokens >= float64(n) {
		l.tokens -= float64(n)
		return true
	}
	return false
}

// Rate 返回当前限速值（与 SetRate 的写同一把锁，避免无锁读构成 data race）。
func (l *ConnRateLimiter) Rate() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate
}

// Burst 返回突发容量（加锁读，理由同 Rate）。
func (l *ConnRateLimiter) Burst() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.burst
}

// SetRate 动态更新速率。
// rate <= 0 会让桶永不补充（静默拒掉全部消息），与构造口径一致夹到 1 并留日志。
func (l *ConnRateLimiter) SetRate(rate float64) {
	if rate <= 0 {
		logger.Warnf("ratelimit.ConnRateLimiter.SetRate: non-positive rate %v clamped to 1", rate)
		rate = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rate = rate
}

// SetBurst 动态更新突发容量。
func (l *ConnRateLimiter) SetBurst(burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.burst = burst
}

// GlobalLimiter 全局连接数限流器。
type GlobalLimiter struct {
	maxConns int
	current  int
	mu       sync.Mutex
}

// NewGlobalLimiter 创建全局限流器。
func NewGlobalLimiter(maxConns int) *GlobalLimiter {
	return &GlobalLimiter{maxConns: maxConns}
}

// Acquire 尝试获取一个连接槽位，失败返回 false。
func (g *GlobalLimiter) Acquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.current >= g.maxConns {
		return false
	}
	g.current++
	return true
}

// Release 释放一个连接槽位。
func (g *GlobalLimiter) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.current > 0 {
		g.current--
	}
}

// Count 返回当前连接数。
func (g *GlobalLimiter) Count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.current
}

// Max 返回最大连接数。
func (g *GlobalLimiter) Max() int { return g.maxConns }
