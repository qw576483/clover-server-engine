package event

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// // 跨服事件幂等去重

// 接收方按事件 MsgID 去重，避免「发送方重投 / NATS 重复投递」导致业务被执行多次。
// 两级结构：
//   - LocalDeduper：进程内 LRU + TTL，零依赖、纳秒级，兜底保证单节点幂等。
//   - RedisDeduper：SETNX + TTL，跨节点幂等（同一事件被投到不同节点时也只处理一次）。
//   - ChainDeduper：串联两者；Redis 不可用时自动降级为「仅本地去重」，不阻断业务。
// // Deduper 幂等去重器。

// Seen 返回 true 表示该 msgID 此前已出现过（应丢弃当前这条）；
// 返回 false 表示首次出现，且本次调用已将其登记（原子的「检查并占位」语义）。

// Contains 是**只读**判重：返回 true 表示该 msgID 已登记（此前已完成处理），
// 且本调用**不改变任何内部状态**。用于「dispatch 之前先判重」——此时还不能登记，
// 否则 dispatch 失败后重投的同一 MsgID 会被误判为重复，事件永久丢失。

// 实现必须并发安全。任何内部故障都应「放行」（返回 false）而非阻断业务。
type Deduper interface {
	Seen(ctx context.Context, msgID string) bool
	Contains(ctx context.Context, msgID string) bool
}

// 去重默认参数。
const (
	defaultDedupCapacity = 10000            // 本地缓存最多保留的 msgID 条数
	defaultDedupTTL      = 10 * time.Minute // 单条 msgID 的有效期
)

// LocalDeduper
// dedupEntry 本地缓存中的一条记录。
type dedupEntry struct {
	msgID    string
	expireAt time.Time
}

// LocalDeduper 进程内去重器：LRU（容量上限）+ TTL（时间上限）双重淘汰。

// 容量满时淘汰最久未使用的记录；读取时若已过期则视为未见过并刷新。
type LocalDeduper struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	ll       *list.List               // 双向链表，队首=最近使用
	items    map[string]*list.Element // msgID -> 链表节点
	// now 便于测试注入假时钟；nil 时用 time.Now。
	now func() time.Time
}

// NewLocalDeduper 构造本地去重器。
// capacity <= 0 取默认 10000；ttl <= 0 取默认 10 分钟。
func NewLocalDeduper(capacity int, ttl time.Duration) *LocalDeduper {
	if capacity <= 0 {
		capacity = defaultDedupCapacity
	}
	if ttl <= 0 {
		ttl = defaultDedupTTL
	}
	return &LocalDeduper{
		capacity: capacity,
		ttl:      ttl,
		ll:       list.New(),
		items:    make(map[string]*list.Element, capacity/4+1),
	}
}

// timeNow 返回当前时间，支持测试注入。
func (d *LocalDeduper) timeNow() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

// Seen 检查并登记 msgID。已存在且未过期返回 true，否则登记后返回 false。
func (d *LocalDeduper) Seen(_ context.Context, msgID string) bool {
	if d == nil || msgID == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.timeNow()
	if el, ok := d.items[msgID]; ok {
		ent := el.Value.(*dedupEntry)
		if now.Before(ent.expireAt) {
			// 命中且未过期：刷新 LRU 位置，判定为重复。
			d.ll.MoveToFront(el)
			return true
		}
		// 已过期：当作首次出现，就地续期。
		ent.expireAt = now.Add(d.ttl)
		d.ll.MoveToFront(el)
		return false
	}

	// 新记录入队首。
	el := d.ll.PushFront(&dedupEntry{msgID: msgID, expireAt: now.Add(d.ttl)})
	d.items[msgID] = el

	// 顺手清理队尾已过期的记录，避免过期项长期占用容量。
	d.evictExpiredLocked(now)
	// 仍超容则按 LRU 淘汰队尾。
	for d.ll.Len() > d.capacity {
		d.removeOldestLocked()
	}
	return false
}

// Contains 只读判重：已登记且未过期返回 true。
// 不登记、不改动 LRU 顺序——调用方仅在 dispatch 前判重，登记由 Seen 在成功后完成。
func (d *LocalDeduper) Contains(_ context.Context, msgID string) bool {
	if d == nil || msgID == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[msgID]
	if !ok {
		return false
	}
	return d.timeNow().Before(el.Value.(*dedupEntry).expireAt)
}

// evictExpiredLocked 从队尾开始清理已过期记录。调用方须持锁。
func (d *LocalDeduper) evictExpiredLocked(now time.Time) {
	for {
		back := d.ll.Back()
		if back == nil {
			return
		}
		ent := back.Value.(*dedupEntry)
		if now.Before(ent.expireAt) {
			return // 队尾未过期，前面的更不会过期（按插入序近似）
		}
		d.ll.Remove(back)
		delete(d.items, ent.msgID)
	}
}

// removeOldestLocked 淘汰最久未使用的记录。调用方须持锁。
func (d *LocalDeduper) removeOldestLocked() {
	back := d.ll.Back()
	if back == nil {
		return
	}
	ent := back.Value.(*dedupEntry)
	d.ll.Remove(back)
	delete(d.items, ent.msgID)
}

// Len 返回当前缓存条数（含尚未清理的过期项），仅用于测试与观测。
func (d *LocalDeduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ll.Len()
}

// RedisDeduper
// RedisSetNX 抽象 Redis 的 SETNX 能力，便于测试替身，避免 event 包直接耦合 redis.Client。

// 语义：key 不存在时写入并返回 true；已存在返回 false。
type RedisSetNX interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error)
}

// RedisDeduper 基于 Redis SETNX 的分布式去重器。

// Redis 报错时返回 false（放行），由上层的本地去重兜底——
// 宁可极小概率重复处理，也不能因缓存故障丢事件。
type RedisDeduper struct {
	cli    RedisSetNX
	prefix string
	ttl    time.Duration
	// onError 可选的错误回调（用于打日志/埋点），不设则静默降级。
	onError func(err error)
}

// NewRedisDeduper 构造 Redis 去重器。
// prefix 为空取 "clover:evt:dedup:"；ttl <= 0 取默认 10 分钟。
func NewRedisDeduper(cli RedisSetNX, prefix string, ttl time.Duration) *RedisDeduper {
	if prefix == "" {
		prefix = "clover:evt:dedup:"
	}
	if ttl <= 0 {
		ttl = defaultDedupTTL
	}
	return &RedisDeduper{cli: cli, prefix: prefix, ttl: ttl}
}

// SetOnError 设置去重失败时的回调（如打日志）。
func (d *RedisDeduper) SetOnError(fn func(err error)) {
	if d != nil {
		d.onError = fn
	}
}

// Seen 通过 SETNX 检查并占位。SETNX 成功=首次出现返回 false；失败=已存在返回 true。
func (d *RedisDeduper) Seen(ctx context.Context, msgID string) bool {
	if d == nil || d.cli == nil || msgID == "" {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ok, err := d.cli.SetNX(ctx, d.prefix+msgID, 1, d.ttl)
	if err != nil {
		// Redis 不可用：降级放行，交由本地去重兜底。
		if d.onError != nil {
			d.onError(err)
		}
		return false
	}
	// ok==true 表示占位成功（首次出现）；ok==false 表示 key 已存在（重复）。
	return !ok
}

// Contains 只读判重。SETNX 原语没有只读查询能力（SetNX 本身即写占位），
// 故此处只能返回 false（放行）：跨节点重复的拦截仍由「处理完成后 Seen 登记」
// 与本地一级去重共同承担，绝不因本方法而阻断业务。
func (d *RedisDeduper) Contains(context.Context, string) bool { return false }

// ChainDeduper
// ChainDeduper 按顺序串联多个去重器：任一环节判定为「已见过」即返回 true。

// 注意会遍历「全部」去重器而不提前短路，确保每一级都完成登记，
// 否则本地命中时 Redis 未登记，会导致同一事件在其它节点无法被识别。
type ChainDeduper struct {
	dedupers []Deduper
}

// NewChainDeduper 构造链式去重器，自动忽略 nil 成员。
func NewChainDeduper(dedupers ...Deduper) *ChainDeduper {
	list := make([]Deduper, 0, len(dedupers))
	for _, d := range dedupers {
		if d != nil {
			list = append(list, d)
		}
	}
	return &ChainDeduper{dedupers: list}
}

// Seen 逐级检查并登记；任一级判定重复则整体判定重复。
func (d *ChainDeduper) Seen(ctx context.Context, msgID string) bool {
	if d == nil || msgID == "" {
		return false
	}
	seen := false
	for _, sub := range d.dedupers {
		// 不短路：保证每一级都写入登记。
		if sub.Seen(ctx, msgID) {
			seen = true
		}
	}
	return seen
}

// Contains 只读判重：任一级命中即返回 true；不登记、不改动任何一级的状态。
func (d *ChainDeduper) Contains(ctx context.Context, msgID string) bool {
	if d == nil || msgID == "" {
		return false
	}
	for _, sub := range d.dedupers {
		if sub.Contains(ctx, msgID) {
			return true
		}
	}
	return false
}

// nopDeduper 关闭去重时使用的空实现。
type nopDeduper struct{}

// Seen 恒返回 false（从不判重）。
func (nopDeduper) Seen(context.Context, string) bool { return false }

// Contains 恒返回 false（从不判重）。
func (nopDeduper) Contains(context.Context, string) bool { return false }

// NopDeduper 返回一个从不判重的去重器，用于关闭去重功能。
func NopDeduper() Deduper { return nopDeduper{} }
