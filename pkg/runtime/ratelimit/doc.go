// Package ratelimit 通用「限流 / 流量整形」原语。
//
// 纯 Go 接口 + 标准库实现，零外部依赖、可纯内存单测，是反作弊、防刷、接口限流的统一基础设施。
//
// 提供四种限流算法，业务按场景任选：
//   - TokenBucket 令牌桶：以 rate（个/秒）持续补充，桶容量 burst，适合突发 + 平滑限速。
//   - FixedWindow 固定窗口计数：每 window 时长最多 max 次，窗口到点清零。
//   - SlidingWindow 滑动窗口计数（双桶近似）：平滑限 max 次，无边界双倍突发。
//   - GCRA 通用信元速率算法（Redis CL.THROTTLE / Cloudflare 事实标准）：单浮点状态，无计数漂移。
//
// 多 key 场景用 Manager：按 (key, policy) 惰性创建限流器，支持命名策略、空闲 TTL 淘汰、
// 条目数上限，未注册策略时空 Allow 一律拒绝（fail-closed）。
//
// 典型用法：
//
//	lim := ratelimit.NewGCRA(100, 20) // 100/s，突发 20
//	if !lim.Allow() { return ErrTooManyRequests }
//
//	m := ratelimit.NewManager(ratelimit.WithMaxEntries(10000), ratelimit.WithIdleTTL(time.Hour))
//	m.SetDefault(ratelimit.GCRAPolicy(50, 10))
//	if !m.Allow("player:123") { return ErrTooManyRequests }
package ratelimit
