// Package cache 通用缓存原语。

// 提供分片、多淘汰策略、TTL、防击穿、统计观测的通用缓存。用 Go 泛型 + 标准库表达，
// 零外部依赖、并发安全、可纯内存单测。

// 典型用途：
//   - 在 data.Store 之前垫一层热数据缓存（玩家/配置/全局小数据），降低 MySQL/Redis 压力；
//   - 配置表 / 排行榜计算结果 / 跨服镜像的本地 TTL 缓存；
//   - 任意「计算贵、重复读」的中间结果（token 解析、权限判定、路由表）。

// 设计要点：
//   - 分片（默认 1 片，WithShards 提升并发）：每片一把锁 + 独立 LRU/FIFO 链表，降低争用；
//   - 三种淘汰策略：LRU（最近最少用）/ LFU（最不常使用）/ FIFO（先进先出），默认 LRU；
//   - per-item TTL（Set 时 WithItemTTL 覆盖默认） + 惰性过期（Get/Peek 命中即判过期）+ 可选后台 sweep 周期清过期；
//   - GetOrLoad：singleflight 合并，同一 key 并发只跑一次 loader，其余等待共享结果，防缓存击穿；
//   - 统计：Hits/Misses/Evictions/Sets/Deletes/Loads，便于观测命中率与失效率；
//   - 可注入 Clock 便于单测 TTL；可注入 OnEvict 回调（如回写/计数）；
//   - 并发安全；纯标准库、零外部依赖、可纯内存单测。
package cache

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	cloverHash "github.com/qw576483/clover-server-engine/pkg/shared/util"
)

// Policy 淘汰策略。
type Policy int

const (
	// PolicyLRU 最近最少使用：每次访问（Get/GetOrLoad 命中）把条目移到队首，淘汰队尾。
	PolicyLRU Policy = iota
	// PolicyLFU 最不常使用：按访问频率淘汰，频率相同淘汰更早插入者（birth 小者）。
	PolicyLFU
	// PolicyFIFO 先进先出：按插入顺序淘汰，访问不改变顺序。
	PolicyFIFO
)

// Stats 缓存统计（Get/Len 等只读操作不阻塞写，快照可能略滞后，属可观测用途可接受）。
type Stats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Sets      uint64
	Deletes   uint64
	Loads     uint64
}

// atomicStats 是 Stats 的原子版本，供各 shard 无锁增量、Stats() 无锁快照。
type atomicStats struct {
	Hits      atomic.Uint64
	Misses    atomic.Uint64
	Evictions atomic.Uint64
	Sets      atomic.Uint64
	Deletes   atomic.Uint64
	Loads     atomic.Uint64
}

func (a *atomicStats) snapshot() Stats {
	return Stats{
		Hits:      a.Hits.Load(),
		Misses:    a.Misses.Load(),
		Evictions: a.Evictions.Load(),
		Sets:      a.Sets.Load(),
		Deletes:   a.Deletes.Load(),
		Loads:     a.Loads.Load(),
	}
}

// Option 构造选项（非泛型：除 WithOnEvict 外的选项都不携带 K/V，
// 用非泛型 Option 可让 New[K,V](...opts) 直接透传而无需推导 K/V）。
type Option func(*rawConfig)

// ItemOption 单次写入的 per-item 选项（如 TTL 覆盖）。
type ItemOption func(*itemOpts)

type itemOpts struct {
	ttl time.Duration // >0 使用该值；== -1 表示永不过期；0 或其它负值沿用默认 TTL
}

// WithItemTTL 为本次 Set 的条目设置独立 TTL（-1 表示永不过期）。
func WithItemTTL(d time.Duration) ItemOption {
	return func(o *itemOpts) { o.ttl = d }
}

// rawConfig 构造期的非泛型配置；onEvict 以 any 携带，New 内按 K/V 还原类型安全。
type rawConfig struct {
	policy     Policy
	maxNum     int           // 每片容量上限（0 = 不限制）
	defaultTTL time.Duration // 默认 TTL（0 = 永不过期）
	shards     int           // 分片数（<1 视为 1）
	clock      func() time.Time
	onEvict    func(k, v any) // 由 WithOnEvict 包装成 any 回调
	sweep      time.Duration  // 后台过期清理间隔（0 = 不清理）
}

type entry[K comparable, V any] struct {
	key        K
	value      V
	expiry     int64  // 绝对过期时间（unix nano）；0 = 永不过期
	lastAccess uint64 // LRU：最近访问的逻辑时钟（单调逻辑序号）
	birth      uint64 // FIFO/LFU 同频时 tie-break 的插入序号
	freq       uint64
}

type shard[K comparable, V any] struct {
	mu      sync.Mutex
	items   map[K]*list.Element
	ll      *list.List // 元素值是 *entry；front=最新（LRU）/队首（FIFO）
	seq     uint64     // 逻辑时钟/插入序号，单调递增
	policy  Policy
	max     int
	defTTL  time.Duration
	clock   func() time.Time
	onEvict func(K, V)
	stats   atomicStats
}

// Cache 是并发安全的泛型缓存。
type Cache[K comparable, V any] struct {
	shards   []*shard[K, V]
	shardN   uint64
	mask     uint64
	clock    func() time.Time
	stop     chan struct{}
	stopOnce sync.Once

	sfMu     sync.Mutex
	inflight map[K]*call[V]
}

type call[V any] struct {
	done  chan struct{} // loader 结束后关闭，等待方据此共享结果（兼作 WaitGroup 的 happens-before 保证）
	val   V
	err   error
	owner uint64 // 发起该 loader 的 goroutine id，用于检测同 goroutine 同 key 递归
}

// ErrReentrantLoad 表示在 loader 内部对同一 Cache 的同一 key 递归调用了 GetOrLoad。
// 这种自引用会让发起者等待自身完成而永久死锁，故直接返回该错误而非挂起。
var ErrReentrantLoad = fmt.Errorf("cache: 检测到同一 key 的 GetOrLoad 递归调用（re-entrant load），已中止以避免死锁")

// goID 返回当前 goroutine 的 id（仅用于死锁自检，不用于业务逻辑）。
// 通过解析 runtime.Stack 的首行「goroutine N [...]」获取，开销可接受（仅递归自检路径命中）。

// 解析失败（栈输出被截断或格式不符预期，例如首行不足前缀长度、前缀后不是十进制数字）时返回 0。
// goroutine id 由运行时从 1 开始分配，故 0 可作「未知」哨兵，调用方据此跳过递归自检。
func goID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := buf[:n]
	const prefix = "goroutine "
	// 长度不足时直接按未知处理，避免 s[len(prefix):] 切片越界。
	if len(s) <= len(prefix) {
		return 0
	}
	s = s[len(prefix):]
	var id uint64
	var digits int
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			break
		}
		id = id*10 + uint64(ch-'0')
		digits++
	}
	if digits == 0 {
		return 0
	}
	return id
}

// 构造选项
// WithMaxNum 设置每片容量上限（超过即按策略淘汰）。0 表示不限制。
func WithMaxNum(n int) Option {
	return func(c *rawConfig) { c.maxNum = n }
}

// WithDefaultTTL 设置默认 TTL（0 = 永不过期）。
func WithDefaultTTL(d time.Duration) Option {
	return func(c *rawConfig) { c.defaultTTL = d }
}

// WithPolicy 设置淘汰策略，默认 PolicyLRU。
func WithPolicy(p Policy) Option {
	return func(c *rawConfig) { c.policy = p }
}

// WithShards 设置分片数（<1 视为 1）。分片降低锁争用，但每片独立命中率。
func WithShards(n int) Option {
	return func(c *rawConfig) { c.shards = n }
}

// WithClock 注入时钟（便于单测 TTL）。默认 time.Now。
func WithClock(clk func() time.Time) Option {
	return func(c *rawConfig) { c.clock = clk }
}

// WithOnEvict 注册淘汰/删除回调（参数为被移除的 key/value）。唯一携带 K/V 的泛型选项。
func WithOnEvict[K comparable, V any](fn func(K, V)) Option {
	return func(c *rawConfig) {
		c.onEvict = func(k, v any) {
			// 逃生舱：选项的 K/V 需与 Cache 的 K/V 一致，二者在编译期无法互相约束。
			// 类型不符时跳过回调（回调只用于观测/回写，丢弃一次通知好过让调用路径 panic）。
			kk, ok := k.(K)
			if !ok {
				return
			}
			vv, ok := v.(V)
			if !ok {
				return
			}
			fn(kk, vv)
		}
	}
}

// WithSweepInterval 启动后台周期清理过期条目（0 = 不启动）。Close 时停止。
func WithSweepInterval(d time.Duration) Option {
	return func(c *rawConfig) { c.sweep = d }
}

// New 构造缓存。
func New[K comparable, V any](opts ...Option) *Cache[K, V] {
	cfg := &rawConfig{
		policy: PolicyLRU,
		shards: 1,
		clock:  time.Now,
	}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.shards < 1 {
		cfg.shards = 1
	}
	onEvict := func(k K, v V) {
		if cfg.onEvict != nil {
			cfg.onEvict(k, v)
		}
	}
	n := cfg.shards
	if n < 1 {
		n = 1
	}
	// 向上取整到 2 的幂（分片选择用位掩码，更快且均衡）。
	// 分片数最多 2^20（约 100 万），避免后续 uint32 掩码溢出。
	const maxShards = 1 << 20
	if n > maxShards {
		n = maxShards
	}
	un := uint64(n)
	power := uint64(1)
	for power < un {
		power <<= 1
	}
	shards := make([]*shard[K, V], power)
	for i := range shards {
		shards[i] = &shard[K, V]{
			items:   make(map[K]*list.Element),
			ll:      list.New(),
			seq:     1,
			policy:  cfg.policy,
			max:     cfg.maxNum,
			defTTL:  cfg.defaultTTL,
			clock:   cfg.clock,
			onEvict: onEvict,
			stats:   atomicStats{}, // 每片独立原子统计，避免多片共享同一份在 -race 下竞争
		}
	}
	cc := &Cache[K, V]{
		shards:   shards,
		shardN:   power,
		mask:     power - 1,
		clock:    cfg.clock,
		stop:     make(chan struct{}),
		inflight: make(map[K]*call[V]),
	}
	if cfg.sweep > 0 {
		go cc.sweepLoop(cfg.sweep)
	}
	return cc
}

func (c *Cache[K, V]) shardOf(key K) *shard[K, V] {
	if c.shardN == 1 {
		return c.shards[0]
	}
	h := cloverHash.Fnv32(fmt.Sprintf("%v", key))
	// #nosec G115 -- c.mask 在 New 中已限制为 maxShards-1，可安全转换为 uint32。
	return c.shards[h&uint32(c.mask)]
}

func (s *shard[K, V]) expired(e *entry[K, V], now int64) bool {
	return e.expiry != 0 && e.expiry <= now
}

// expiryOf 由「当前时间 now（unix nano）」与 TTL 推导绝对过期时间，返回 0 表示永不过期。

// now+int64(ttl) 在 ttl 极大时会溢出 int64 而翻转成负数，使条目被判为「早已过期」
// 从而写入即失效。这里在会溢出时钳制到 math.MaxInt64，语义上是「远未见底的将来」，
// 保证 TTL 越长条目存活越久这一单调性不被溢出破坏。
func expiryOf(now int64, ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	d := int64(ttl)
	if d > math.MaxInt64-now {
		return math.MaxInt64
	}
	return now + d
}

// Get 读取并刷新访问序（LRU）。命中返回 (val, true)，未命中或已过期返回 (零值, false)。
func (c *Cache[K, V]) Get(key K) (V, bool) {
	s := c.shardOf(key)
	s.mu.Lock()
	now := s.clock().UnixNano()
	el, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		s.stats.Misses.Add(1)
		var zero V
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if s.expired(e, now) {
		pending := s.removeElement(key, el, e)
		s.mu.Unlock()
		s.flushEvict(pending)
		s.stats.Misses.Add(1)
		var zero V
		return zero, false
	}
	s.touch(e, el)
	// 必须在解锁前取值：Set 在锁内原地写 e.value，解锁后再读 e.value 是 data race
	// （V 为 string/切片/接口时会读到撕裂的头）。
	val := e.value
	s.mu.Unlock()
	s.stats.Hits.Add(1)
	return val, true
}

// Peek 只读不刷新访问序（不影响 LRU/LFU 计数）。用于「看看在不在」而不污染淘汰序。
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	s := c.shardOf(key)
	s.mu.Lock()
	now := s.clock().UnixNano()
	el, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		var zero V
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if s.expired(e, now) {
		pending := s.removeElement(key, el, e)
		s.mu.Unlock()
		s.flushEvict(pending)
		var zero V
		return zero, false
	}
	// 同 Get：解锁后读 e.value 与 Set 的原地写构成 data race，须锁内取值。
	val := e.value
	s.mu.Unlock()
	return val, true
}

// touch 在访问命中后更新淘汰序（调用方须持锁）。
func (s *shard[K, V]) touch(e *entry[K, V], el *list.Element) {
	e.lastAccess = s.seq
	s.seq++
	switch s.policy {
	case PolicyLRU:
		s.ll.MoveToFront(el)
	case PolicyLFU:
		e.freq++
	}
}

// Set 写入（或覆盖）一个条目。
func (c *Cache[K, V]) Set(key K, val V, opts ...ItemOption) {
	io := &itemOpts{}
	for _, o := range opts {
		o(io)
	}
	s := c.shardOf(key)
	s.mu.Lock()
	now := s.clock().UnixNano()
	ttl := s.defTTL
	if io.ttl > 0 {
		ttl = io.ttl
	}
	var expiry int64
	if io.ttl != -1 {
		expiry = expiryOf(now, ttl)
	}
	if el, ok := s.items[key]; ok {
		e := el.Value.(*entry[K, V])
		e.value = val
		e.expiry = expiry
		e.lastAccess = s.seq
		// 注意：覆盖已存在条目时 birth 不更新，即 FIFO 驱逐策略下重写条目保留首次创建时间，
		// 不因覆盖而重置驱逐优先级。若需时间重置语义，请 Delete + Set 重建。
		s.seq++
		if s.policy == PolicyLRU {
			s.ll.MoveToFront(el)
		} else if s.policy == PolicyLFU {
			e.freq++
		}
		s.mu.Unlock()
		s.stats.Sets.Add(1)
		return
	}
	e := &entry[K, V]{key: key, value: val, expiry: expiry, birth: s.seq, lastAccess: s.seq}
	s.seq++
	el := s.ll.PushFront(e)
	s.items[key] = el
	// evictTo 会修改 s.items / s.ll，必须在持锁期间调用（函数约定「调用方须持锁」）；
	// 其产出的 onEvict 回调在解锁后才触发（避免回写同 Cache 自死锁）。
	var pending []evictedKV[K, V]
	if s.max > 0 {
		pending = c.evictTo(s, 0)
	}
	s.mu.Unlock()
	s.flushEvict(pending)
	s.stats.Sets.Add(1)
}

// SetTTL 更新已存在条目的过期时间（d<=0 表示永不过期）。不存在返回 false。
func (c *Cache[K, V]) SetTTL(key K, d time.Duration) bool {
	s := c.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[key]
	if !ok {
		return false
	}
	e := el.Value.(*entry[K, V])
	e.expiry = expiryOf(s.clock().UnixNano(), d)
	return true
}

// evictedKV 一条已摘除、待在锁外回调 onEvict 的条目。
type evictedKV[K comparable, V any] struct {
	key K
	val V
}

// flushEvict 在**锁外**触发 onEvict。
//
// 分片锁是不可重入的 sync.Mutex，而 onEvict 由业务提供，
// 一旦回调里回写同一 Cache（Set/Get/Delete 命中同一分片）就会自死锁。
// 因此所有摘除路径都只负责「锁内改数据结构 + 收集待回调条目」，
// 真正的回调统一在解锁后由本函数执行（与同仓 fsm/timer/aoi/trace 的做法一致）。
func (s *shard[K, V]) flushEvict(pending []evictedKV[K, V]) {
	if s.onEvict == nil {
		return
	}
	for _, kv := range pending {
		s.onEvict(kv.key, kv.val)
	}
}

// evictTo 把分片条目数降到 <= max（extra 额外允许的富余，通常 0）。调用方须持锁。
//
// 返回被摘除、待在锁外回调的条目；容量不变量（len(s.items) <= s.max+extra）在锁内即已成立，
// 延迟回调不会让任何时刻的分片实际容量超限。
func (c *Cache[K, V]) evictTo(s *shard[K, V], extra int) []evictedKV[K, V] {
	if s.max <= 0 {
		return nil
	}
	var pending []evictedKV[K, V]
	// 先清掉已过期但未被惰性访问的「僵尸条目」：它们从未被 Get 到，惰性清理不会碰它们，
	// 若直接按策略挑 victim 就会淘汰掉仍有效的条目，使有效容量提前耗尽、命中率下降。
	if len(s.items) > s.max+extra {
		now := s.clock().UnixNano()
		var expired []*list.Element
		for el := s.ll.Front(); el != nil; el = el.Next() {
			if s.expired(el.Value.(*entry[K, V]), now) {
				expired = append(expired, el)
			}
		}
		for _, el := range expired {
			e := el.Value.(*entry[K, V])
			pending = append(pending, s.removeElement(e.key, el, e)...)
		}
	}
	for len(s.items) > s.max+extra {
		victim := c.pickVictim(s)
		if victim == nil {
			return pending
		}
		_ = s.ll.Remove(victim)
		e := victim.Value.(*entry[K, V])
		delete(s.items, e.key)
		s.stats.Evictions.Add(1)
		if s.onEvict != nil {
			pending = append(pending, evictedKV[K, V]{key: e.key, val: e.value})
		}
	}
	return pending
}

// pickVictim 选出待淘汰元素（不删除）。调用方须持锁。
func (c *Cache[K, V]) pickVictim(s *shard[K, V]) *list.Element {
	if s.ll.Len() == 0 {
		return nil
	}
	switch s.policy {
	case PolicyFIFO:
		return s.ll.Back() // 最早插入
	case PolicyLRU:
		return s.ll.Back() // 最久未访问
	case PolicyLFU:
		// 扫描最小 freq，tie-break 最小 birth。
		var best *list.Element
		var bf, bb uint64
		first := true
		for el := s.ll.Front(); el != nil; el = el.Next() {
			e := el.Value.(*entry[K, V])
			if first || e.freq < bf || (e.freq == bf && e.birth < bb) {
				best = el
				bf = e.freq
				bb = e.birth
				first = false
			}
		}
		return best
	}
	return s.ll.Back()
}

// removeElement 摘除一个条目。调用方须持锁。
//
// 返回待在锁外回调的条目（未注册 onEvict 时为 nil），调用方必须在解锁后 flushEvict。
func (s *shard[K, V]) removeElement(key K, el *list.Element, e *entry[K, V]) []evictedKV[K, V] {
	_ = s.ll.Remove(el)
	delete(s.items, key)
	s.stats.Deletes.Add(1)
	if s.onEvict != nil {
		return []evictedKV[K, V]{{key: e.key, val: e.value}}
	}
	return nil
}

// Delete 删除条目，返回是否真的删除了一条。
func (c *Cache[K, V]) Delete(key K) bool {
	s := c.shardOf(key)
	s.mu.Lock()
	el, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return false
	}
	e := el.Value.(*entry[K, V])
	pending := s.removeElement(key, el, e)
	s.mu.Unlock()
	s.flushEvict(pending)
	return true
}

// Contains 是否存在且未过期（不刷新访问序）。
func (c *Cache[K, V]) Contains(key K) bool {
	s := c.shardOf(key)
	s.mu.Lock()
	el, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return false
	}
	e := el.Value.(*entry[K, V])
	if s.expired(e, s.clock().UnixNano()) {
		// 惰性清理（回调在解锁后触发）
		pending := s.removeElement(key, el, e)
		s.mu.Unlock()
		s.flushEvict(pending)
		return false
	}
	s.mu.Unlock()
	return true
}

// Len 当前条目总数（跨分片求和，可能含未清理的过期项，属近似）。
func (c *Cache[K, V]) Len() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += len(s.items)
		s.mu.Unlock()
	}
	return n
}

// Keys 返回当前所有 key（含未清理的过期项）。
func (c *Cache[K, V]) Keys() []K {
	var keys []K
	for _, s := range c.shards {
		s.mu.Lock()
		for k := range s.items {
			keys = append(keys, k)
		}
		s.mu.Unlock()
	}
	return keys
}

// Clear 清空全部条目。
func (c *Cache[K, V]) Clear() {
	for _, s := range c.shards {
		s.mu.Lock()
		// 先收集、再清空，最后在锁外回调：Clear 在持锁循环里直接调 onEvict 时，
		// 回调回写同一 Cache 即自死锁（分片锁不可重入）。
		var pending []evictedKV[K, V]
		if s.onEvict != nil {
			for el := s.ll.Front(); el != nil; el = el.Next() {
				e := el.Value.(*entry[K, V])
				pending = append(pending, evictedKV[K, V]{key: e.key, val: e.value})
			}
		}
		s.items = make(map[K]*list.Element)
		s.ll.Init()
		s.mu.Unlock()
		s.flushEvict(pending)
	}
}

// Stats 返回统计快照（各片统计为原子计数器，快照无锁累加）。
func (c *Cache[K, V]) Stats() Stats {
	var s Stats
	for _, sh := range c.shards {
		st := sh.stats.snapshot()
		s.Hits += st.Hits
		s.Misses += st.Misses
		s.Evictions += st.Evictions
		s.Sets += st.Sets
		s.Deletes += st.Deletes
		s.Loads += st.Loads
	}
	return s
}

// GetOrLoad 读取；未命中则用 loader 加载并写入。同一 key 并发只跑一次 loader（singleflight），
// 其余调用方等待并共享同一结果，天然防缓存击穿。loader 返回 error 时不缓存、不计入 Loads 成功、
// 所有等待方都拿到同一 error。ctx 用于 loader 取消/超时；对等待方同样生效——
// ctx 取消/超时时等待方立即拿到 ctx.Err()，不会因 loader 迟迟不返回而被无限期挂起。

// 重要：loader 中严禁调用 GetOrLoad/Get/Set 等操作同一 Cache 的方法（包括间接通过 key 路径递归触发），
// 否则会在持锁期间产生死锁或导致缓存统计（Loads 计数）口径不准确。
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K, load func(ctx context.Context, key K) (V, error)) (V, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}
	// 合并：同一 key 只让一个 loader 跑
	// gid 为 0 表示 goroutine id 解析失败，此时无法判断归属，跳过递归自检。
	gid := goID()
	c.sfMu.Lock()
	if cl, ok := c.inflight[key]; ok {
		// 同一 goroutine 又对同一 key 发起 GetOrLoad：说明 loader 内部递归调用了自身，
		// 此时若等待会等待自己完成而永久死锁。直接返回错误中止，避免 inflight 泄漏。
		if gid != 0 && cl.owner == gid {
			c.sfMu.Unlock()
			var zero V
			return zero, ErrReentrantLoad
		}
		c.sfMu.Unlock()
		return c.awaitCall(ctx, cl)
	}
	cl := &call[V]{owner: gid, done: make(chan struct{})}
	c.inflight[key] = cl
	c.sfMu.Unlock()

	// done 标记 loader 是否正常返回。若 loader panic，cl.val/cl.err 不会被赋值，
	// 而 defer 里的 close(cl.done) 仍会放行所有等待方，使其拿到「零值 + nil error」
	// 的伪成功结果。这里在 panic 路径上补一个明确的错误再放行。
	done := false
	defer func() {
		if !done && cl.err == nil {
			var zero V
			cl.val = zero
			cl.err = fmt.Errorf("cache: loader panicked for key %v", key)
		}
		c.sfMu.Lock()
		delete(c.inflight, key)
		c.sfMu.Unlock()
		close(cl.done)
	}()

	val, err := load(ctx, key)
	cl.val = val
	cl.err = err
	done = true
	if err != nil {
		return val, err
	}
	c.Set(key, val)
	// Loads 仅在实际成功执行 loader 且写入缓存时 +1，与 Hits/Misses 口径一致：
	// 命中缓存不计 Loads、等待方共享结果不重复计、loader 失败不计。
	c.shardOf(key).stats.Loads.Add(1)
	return val, nil
}

// awaitCall 等待同 key 的 in-flight loader 结束并共享其结果。

// 等待期间同时响应 ctx：ctx 取消或超时时立即返回 ctx.Err()，让等待方有退路，
// 而不是在 loader 迟迟不返回时被无限期挂起（GetOrLoad 的调用方往往带着请求级 deadline）。
// 注意这里只让等待方退避，不会中断仍在运行的 loader，也不会撤销它随后的写入。
func (c *Cache[K, V]) awaitCall(ctx context.Context, cl *call[V]) (V, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-cl.done:
		return cl.val, cl.err
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	}
}

// sweepLoop 后台周期清理过期条目。
func (c *Cache[K, V]) sweepLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.sweepOnce()
		}
	}
}

func (c *Cache[K, V]) sweepOnce() {
	for _, s := range c.shards {
		s.mu.Lock()
		now := s.clock().UnixNano()
		var toRemove []*list.Element
		var pending []evictedKV[K, V]
		for el := s.ll.Front(); el != nil; el = el.Next() {
			e := el.Value.(*entry[K, V])
			if s.expired(e, now) {
				toRemove = append(toRemove, el)
			}
		}
		for _, el := range toRemove {
			e := el.Value.(*entry[K, V])
			pending = append(pending, s.removeElement(e.key, el, e)...)
		}
		s.mu.Unlock()
		s.flushEvict(pending)
	}
}

// Close 停止后台 sweep（若有）。缓存本身仍可继续读写，只是不再自动清理过期项。
// 重复调用安全（幂等）。
func (c *Cache[K, V]) Close() {
	c.stopOnce.Do(func() { close(c.stop) })
}
