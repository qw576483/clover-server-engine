package ratelimit

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// PolicyFactory 创建一个限流器实例（Manager 在首次见到某 key 时调用）。
// 注入 clock 保证与 Manager 同源，便于测试统一推进虚拟时钟。
type PolicyFactory func(clock Clock) Limiter

// 策略构造器：直接交给 Manager.RegisterPolicy / SetDefault 使用。
// 策略工厂内部用 Manager 传入的 clock 造实例，无需调用方关心时间源。
func TokenBucketPolicy(rate float64, burst int) PolicyFactory {
	return func(clock Clock) Limiter { return NewTokenBucketWithClock(rate, burst, clock) }
}

// FixedWindowPolicy 固定窗口策略。
func FixedWindowPolicy(max int, window time.Duration) PolicyFactory {
	return func(clock Clock) Limiter { return NewFixedWindowWithClock(max, window, clock) }
}

// SlidingWindowPolicy 滑动窗口策略。
func SlidingWindowPolicy(max int, window time.Duration) PolicyFactory {
	return func(clock Clock) Limiter { return NewSlidingWindowWithClock(max, window, clock) }
}

// GCRAPolicy 通用信元速率算法（Redis CL.THROTTLE / Cloudflare 事实标准）：
// 用单个时间戳即可表达「平滑突发 + 持续速率」，无计数器漂移，状态极小。
func GCRAPolicy(rate float64, burst int) PolicyFactory {
	return func(clock Clock) Limiter { return NewGCRAWithClock(rate, burst, clock) }
}

// entry 一个 (key, policy) 对应的限流器实例及其最近访问时间。
type entry struct {
	key    string
	policy string
	lim    Limiter
	used   atomic.Int64 // 最近访问 unix 纳秒（原子读写，避免与 Manager 锁争用）
}

// Manager 多 key 限流器：按 (key, policy) 惰性创建并管理限流器实例。
//
// 典型用法：注册若干命名策略（如 "chat"=每玩家每秒 5 条、"login"=每 IP 每分钟 10 次），
// 之后每个请求 `m.Allow(owner, "chat")` 即可；策略工厂按需为每个 key 造一个实例。
//
// Keys() 列出所有被限流的主体的 key、Usage 看余量、Reset/Remove 解禁——
// 本包让频率控制可查询、可解禁。
type Manager struct {
	mu         sync.RWMutex
	clock      Clock
	policies   map[string]PolicyFactory
	defFactory PolicyFactory
	entries    map[string]*entry
	maxEntries int
	idleTTL    time.Duration
	lastSweep  int64 // 上次 idle 全表清扫的 unix 纳秒（用于摊销）
}

type managerOption struct {
	clock      Clock
	maxEntries int
	idleTTL    time.Duration
}

// ManagerOption 管理器选项。
type ManagerOption func(*managerOption)

// WithClock 注入时间源（测试用）。默认 time.Now。
func WithClock(c Clock) ManagerOption {
	return func(o *managerOption) { o.clock = c }
}

// WithMaxEntries 限制 entries 数量上限，超限时淘汰最久未访问者（防内存无限增长）。
func WithMaxEntries(n int) ManagerOption { return func(o *managerOption) { o.maxEntries = n } }

// WithIdleTTL 设定条目空闲过期时长，超过即淘汰（无论是否达上限）。
func WithIdleTTL(d time.Duration) ManagerOption { return func(o *managerOption) { o.idleTTL = d } }

// NewManager 构造管理器（未注册策略时空 Allow 一律拒绝，fail-closed）。
func NewManager(opts ...ManagerOption) *Manager {
	o := &managerOption{clock: defaultClock()}
	for _, fn := range opts {
		fn(o)
	}
	if o.clock == nil {
		o.clock = defaultClock()
	}
	return &Manager{
		clock:      o.clock,
		policies:   make(map[string]PolicyFactory),
		entries:    make(map[string]*entry),
		maxEntries: o.maxEntries,
		idleTTL:    o.idleTTL,
	}
}

const entrySep = "\x1f" // 不可见分隔符，组合 policy 与 key

func composite(policy, key string) string { return policy + entrySep + key }

// RegisterPolicy 登记一个命名策略。后续 Allow(key, name) 用对应工厂造实例。
func (m *Manager) RegisterPolicy(name string, f PolicyFactory) {
	m.mu.Lock()
	m.policies[name] = f
	m.mu.Unlock()
}

// SetDefault 设定缺省策略；Allow(key, "") 时使用。
func (m *Manager) SetDefault(f PolicyFactory) {
	m.mu.Lock()
	m.defFactory = f
	m.mu.Unlock()
}

// getOrCreate 取得 (key, policy) 对应限流器；不存在则按策略工厂惰性创建。
// 返回 (limiter, true)；策略缺失且无缺省时返回 (nil, false)（fail-closed 拒绝）。
func (m *Manager) getOrCreate(key, policy string) (Limiter, bool) {
	cp := composite(policy, key)
	m.mu.RLock()
	if e, ok := m.entries[cp]; ok {
		e.used.Store(m.clock().UnixNano())
		m.mu.RUnlock()
		return e.lim, true
	}
	m.mu.RUnlock()

	// 双检锁创建
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[cp]; ok {
		e.used.Store(m.clock().UnixNano())
		return e.lim, true
	}
	var f PolicyFactory
	if policy == "" {
		f = m.defFactory
	} else {
		f = m.policies[policy]
	}
	if f == nil {
		return nil, false
	}
	lim := f(m.clock)
	m.entries[cp] = &entry{key: key, policy: policy, lim: lim, used: atomic.Int64{}}
	m.entries[cp].used.Store(m.clock().UnixNano())
	// 触发淘汰：超上限 或 配置了空闲 TTL。
	if (m.maxEntries > 0 && len(m.entries) > m.maxEntries) || m.idleTTL > 0 {
		m.sweepLocked()
	}
	return lim, true
}

// sweepLocked 淘汰空闲过期 & 超出上限的条目；调用方须持写锁。
// idle 全表扫描做摊销——距上次扫描不足 idleTTL 时跳过（此期间不会有条目过期），
// 避免每次 create 都 O(n) 遍历全表。超上限的强制淘汰不受节流限制，须立即执行以守住内存上限。
func (m *Manager) sweepLocked() {
	now := m.clock().UnixNano()
	if m.idleTTL > 0 && now-m.lastSweep >= m.idleTTL.Nanoseconds() {
		m.lastSweep = now
		for k, e := range m.entries {
			if now-e.used.Load() > m.idleTTL.Nanoseconds() {
				delete(m.entries, k)
			}
		}
	}
	if m.maxEntries > 0 && len(m.entries) > m.maxEntries {
		type kv struct {
			k    string
			used int64
		}
		arr := make([]kv, 0, len(m.entries))
		for k, e := range m.entries {
			arr = append(arr, kv{k, e.used.Load()})
		}
		sort.Slice(arr, func(i, j int) bool { return arr[i].used < arr[j].used })
		for i := 0; i < len(arr)-m.maxEntries; i++ {
			delete(m.entries, arr[i].k)
		}
	}
}

// Allow 是否放行 key 在 policy 下的 1 个请求。策略缺失/无缺省返回 false（拒绝）。
func (m *Manager) Allow(key, policy string) bool {
	lim, ok := m.getOrCreate(key, policy)
	if !ok {
		return false
	}
	return lim.Allow()
}

// AllowN 是否放行 key 在 policy 下的 n 个请求。
func (m *Manager) AllowN(key, policy string, n int) bool {
	lim, ok := m.getOrCreate(key, policy)
	if !ok {
		return false
	}
	return lim.AllowN(n)
}

// Reserve 是否放行 key 在 policy 下的 1 个请求；被限流时返回需等待时长。
func (m *Manager) Reserve(key, policy string) (bool, time.Duration) {
	lim, ok := m.getOrCreate(key, policy)
	if !ok {
		return false, 0
	}
	return lim.Reserve()
}

// Usage 返回某 key 在策略下的当前余量 / 上限 / 距离恢复时长（展示「还剩多少、多久恢复」）。
// 只读语义：目标 (key, policy) 不存在时直接返回 ok=false，不创建限流器、不写 entries
// —— 观测操作不应改变 Manager 状态（Len 增长 / 触发淘汰）。
func (m *Manager) Usage(key, policy string) (remaining, limit int, reset time.Duration, ok bool) {
	m.mu.RLock()
	e, found := m.entries[composite(policy, key)]
	m.mu.RUnlock()
	if !found {
		return 0, 0, 0, false
	}
	lim := e.lim
	return lim.Remaining(), lim.Limit(), lim.ResetIn(), true
}

// Reset 清除某 (key, policy) 的计数（GM 解禁一次操作）。
func (m *Manager) Reset(key, policy string) {
	m.mu.Lock()
	if e, ok := m.entries[composite(policy, key)]; ok {
		e.lim.Reset()
		e.used.Store(m.clock().UnixNano())
	}
	m.mu.Unlock()
}

// ResetKey 清除某 key 在所有策略下的计数（GM 解禁某个玩家/IP）。
func (m *Manager) ResetKey(key string) {
	m.mu.Lock()
	for k, e := range m.entries {
		if e.key == key {
			e.lim.Reset()
			e.used.Store(m.clock().UnixNano())
			_ = k
		}
	}
	m.mu.Unlock()
}

// Remove 移除某 (key, policy) 的限流器实例（彻底解绑）。
func (m *Manager) Remove(key, policy string) {
	m.mu.Lock()
	delete(m.entries, composite(policy, key))
	m.mu.Unlock()
}

// Keys 返回所有被跟踪的 key（去重、字典序）。
func (m *Manager) Keys() []string {
	m.mu.RLock()
	set := make(map[string]struct{}, len(m.entries))
	for _, e := range m.entries {
		set[e.key] = struct{}{}
	}
	m.mu.RUnlock()
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Len 当前在册 (key, policy) 实例数。
func (m *Manager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}
