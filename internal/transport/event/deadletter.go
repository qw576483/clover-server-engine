package event

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/qw576483/clover-server-engine/pkg/shared/timeutil"
)

// // 跨服事件死信队列

// 重试耗尽的事件落入死信队列，保留原事件与失败上下文，支持人工介入：
// 列出（List）→ 重投（Retry）→ 删除（Remove）。

// 两种实现：
//   - MemoryDLQ：进程内环形队列，容量上限，满了丢最老并计数（默认）。
//   - RedisDLQ ：Redis LIST + HASH 存储，带 TTL，跨节点可见、重启不丢。
//
// // defaultDLQCapacity 内存死信队列默认容量。
const defaultDLQCapacity = 1000

// 死信队列相关错误。
var (
	// ErrDeadLetterNotFound 指定 id 的死信不存在。
	ErrDeadLetterNotFound = errors.New("event: 死信记录不存在")
	// ErrRetryNotSupported 该 DLQ 未注入重投器，无法执行人工重投。
	ErrRetryNotSupported = errors.New("event: 死信队列未配置重投器")
)

// DeadLetter 一条死信记录：原事件 + 失败上下文。
type DeadLetter struct {
	// ID 记录唯一标识（默认等于 MsgID），人工介入时按此定位。
	ID string `json:"id"`
	// MsgID 原事件的全局唯一消息 ID。
	MsgID string `json:"msg_id"`
	// Event 原始跨服事件（可直接用于重投）。
	Event *CrossNodeEvent `json:"event"`
	// Reason 最终失败原因。
	Reason string `json:"reason"`
	// Attempts 累计尝试次数。
	Attempts int `json:"attempts"`
	// FirstAt 首次尝试时间。
	FirstAt time.Time `json:"first_at"`
	// LastAt 末次尝试（入队）时间。
	LastAt time.Time `json:"last_at"`
}

// DeadLetterHandler 死信回调：事件入 DLQ 后触发，用于告警等旁路处理。

// 回调内部 panic 会被捕获，不影响投递主流程。
type DeadLetterHandler func(dl DeadLetter)

// DeadLetterQueue 死信队列，支持人工介入。

// 实现必须并发安全。
type DeadLetterQueue interface {
	// Push 将一条死信入队。
	Push(ctx context.Context, dl DeadLetter) error
	// List 列出最近的死信记录（按入队时间倒序，最新在前）。limit<=0 表示全部。
	List(ctx context.Context, limit int) ([]DeadLetter, error)
	// Retry 人工重投指定死信；成功后从队列移除。
	Retry(ctx context.Context, id string) error
	// Remove 删除指定死信记录。
	Remove(ctx context.Context, id string) error
}

// DeadLetterRetrier 死信重投器：由可靠投递器注入，供 DLQ 的 Retry 调用。
type DeadLetterRetrier func(ctx context.Context, evt *CrossNodeEvent) error

// MemoryDLQ
// MemoryDLQ 进程内死信队列：固定容量，满时丢弃最老记录并计数。

// 适合默认场景（进程内人工排障）；需要跨节点可见/重启不丢时用 RedisDLQ。
type MemoryDLQ struct {
	mu       sync.RWMutex
	capacity int
	items    []DeadLetter // 按入队顺序追加，队首为最老
	// head 是首个有效元素的下标：满容量淘汰时只前移 head，
	// 索引里保存的**绝对**下标因此保持有效，不必每次淘汰都重排整张索引表。
	head    int
	index   map[string]int
	dropped atomic.Uint64 // 因容量满被丢弃的条数

	retryLock sync.RWMutex
	retrier   DeadLetterRetrier
}

// NewMemoryDLQ 构造内存死信队列。capacity<=0 取默认 1000。
func NewMemoryDLQ(capacity int) *MemoryDLQ {
	if capacity <= 0 {
		capacity = defaultDLQCapacity
	}
	return &MemoryDLQ{
		capacity: capacity,
		items:    make([]DeadLetter, 0, capacity),
		index:    make(map[string]int),
	}
}

// SetRetrier 注入重投器，使 Retry 可用。
func (q *MemoryDLQ) SetRetrier(fn DeadLetterRetrier) {
	q.retryLock.Lock()
	q.retrier = fn
	q.retryLock.Unlock()
}

// Push 入队；容量满时丢弃最老一条并累加 Dropped 计数。
func (q *MemoryDLQ) Push(_ context.Context, dl DeadLetter) error {
	if dl.ID == "" {
		dl.ID = dl.MsgID
	}
	if dl.ID == "" {
		return errors.New("event: 死信记录缺少 ID")
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	// 同 ID 覆盖（同一事件多次进死信时只保留最新状态）。
	if i, ok := q.index[dl.ID]; ok {
		q.items[i] = dl
		return nil
	}
	if q.liveLenLocked() >= q.capacity {
		// 丢最老一条：只前移 head（O(1)）。
		delete(q.index, q.items[q.head].ID)
		q.head++
		q.dropped.Add(1)
		// 摊还压缩：头部死区超过一半时才整体搬迁一次，均摊到每次淘汰仍是 O(1)。
		if q.head*2 >= len(q.items) {
			q.compactLocked()
		}
	}
	q.items = append(q.items, dl)
	q.index[dl.ID] = len(q.items) - 1
	return nil
}

// liveLenLocked 当前有效条数（调用方须持锁）。
func (q *MemoryDLQ) liveLenLocked() int { return len(q.items) - q.head }

// compactLocked 把有效窗口搬到数组头部并重建索引。O(n)，由 Push 摊还（每 n/2 次淘汰一次）。
func (q *MemoryDLQ) compactLocked() {
	n := copy(q.items, q.items[q.head:])
	q.items = q.items[:n]
	for i := 0; i < n; i++ {
		q.index[q.items[i].ID] = i
	}
	q.head = 0
}

// List 返回最近 limit 条死信，按入队时间倒序（最新在前）。
func (q *MemoryDLQ) List(_ context.Context, limit int) ([]DeadLetter, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	n := q.liveLenLocked()
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]DeadLetter, 0, limit)
	// 从队尾（最新）往前取，只取有效窗口。
	for i := len(q.items) - 1; i >= q.head && len(out) < limit; i-- {
		out = append(out, q.items[i])
	}
	return out, nil
}

// Retry 人工重投指定死信；投递请求成功后移除该记录。
// 注意：fn 返回 nil 仅表示投递请求已被接受（如异步投递已入队），不代表最终投递成功。
// 对于异步投递场景，Remove 后若投递最终失败，事件将进入可靠投递器的死信队列而非此处的 MemoryDLQ。
func (q *MemoryDLQ) Retry(ctx context.Context, id string) error {
	q.mu.RLock()
	i, ok := q.index[id]
	var dl DeadLetter
	if ok {
		dl = q.items[i]
	}
	q.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: id=%s", ErrDeadLetterNotFound, id)
	}

	q.retryLock.RLock()
	fn := q.retrier
	q.retryLock.RUnlock()
	if fn == nil {
		return ErrRetryNotSupported
	}
	// 重投前清空 MsgID 之外的尝试计数，让其重新走完整重试流程。
	evt := dl.Event.Clone()
	if evt == nil {
		return fmt.Errorf("event: 死信 %s 缺少原事件，无法重投", id)
	}
	evt.Attempt = 0
	if err := fn(ctx, evt); err != nil {
		return fmt.Errorf("event: 重投死信 %s 失败: %w", id, err)
	}
	// fn 成功（投递请求已被接受）后移除死信记录。
	// 对于同步投递：fn 返回 nil 即表示投递成功，Remove 安全。
	// 对于异步投递：fn 返回 nil 仅表示已入队，若最终失败事件会进入可靠投递器自身的 DLQ，此处不再保留。
	return q.Remove(ctx, id)
}

// Remove 删除指定死信记录。
func (q *MemoryDLQ) Remove(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	i, ok := q.index[id]
	if !ok {
		return fmt.Errorf("%w: id=%s", ErrDeadLetterNotFound, id)
	}
	delete(q.index, id)
	// 正好是最老一条：前移 head 即可（O(1)）。
	if i == q.head {
		q.head++
		if q.head >= len(q.items) {
			q.items = q.items[:0]
			q.head = 0
		}
		return nil
	}
	// 中间/尾部（人工操作，非热路径）：搬移尾部并重建尾部索引。
	copy(q.items[i:], q.items[i+1:])
	// 清掉被腾出的尾槽：原地搬移后底层数组末尾仍残留被删 DeadLetter（含 Event 指针），
	// 不清尾会延长这些对象的生命周期（直到后续追加覆盖）。
	q.items[len(q.items)-1] = DeadLetter{}
	q.items = q.items[:len(q.items)-1]
	for j := i; j < len(q.items); j++ {
		q.index[q.items[j].ID] = j
	}
	return nil
}

// Len 返回当前死信条数。
func (q *MemoryDLQ) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.liveLenLocked()
}

// Dropped 返回因容量满被丢弃的死信条数。
func (q *MemoryDLQ) Dropped() uint64 { return q.dropped.Load() }

// RedisDLQ
// RedisDLQStore 抽象 RedisDLQ 所需的最小 Redis 能力，便于测试替身。

// 采用 HASH（id → 死信 JSON）+ 有序集合语义的简化方案：
// 用 HSet 存内容、用 ZAdd 存入队时间以支持按时间倒序列出。
type RedisDLQStore interface {
	HSet(ctx context.Context, key string, values ...any) error
	HGet(ctx context.Context, key, field string) (string, error)
	HDel(ctx context.Context, key string, fields ...string) (int64, error)
	ZAdd(ctx context.Context, key string, score float64, member string) error
	ZRem(ctx context.Context, key string, members ...any) (int64, error)
	ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)
}

// RedisDLQ 基于 Redis 的死信队列：跨节点可见、进程重启不丢。

// 存储结构：
//   - {prefix}:data  HASH，field=id，value=死信 JSON
//   - {prefix}:index ZSET，member=id，score=入队时间戳（毫秒），用于倒序列出
type RedisDLQ struct {
	store  RedisDLQStore
	prefix string
	ttl    time.Duration

	retryLock sync.RWMutex
	retrier   DeadLetterRetrier
}

// NewRedisDLQ 构造 Redis 死信队列。
// prefix 为空取 "clover:evt:dlq"；ttl<=0 表示不设过期（死信需人工处理，默认长期保留）。
func NewRedisDLQ(store RedisDLQStore, prefix string, ttl time.Duration) *RedisDLQ {
	if prefix == "" {
		prefix = "clover:evt:dlq"
	}
	return &RedisDLQ{store: store, prefix: prefix, ttl: ttl}
}

// SetRetrier 注入重投器，使 Retry 可用。
func (q *RedisDLQ) SetRetrier(fn DeadLetterRetrier) {
	q.retryLock.Lock()
	q.retrier = fn
	q.retryLock.Unlock()
}

func (q *RedisDLQ) dataKey() string  { return q.prefix + ":data" }
func (q *RedisDLQ) indexKey() string { return q.prefix + ":index" }

// Push 将死信写入 Redis。
// 先写 index 再写 data：若 index 写入失败则 data 不写，避免 data 存在但 index 缺失导致死信无法 List。
func (q *RedisDLQ) Push(ctx context.Context, dl DeadLetter) error {
	if q == nil || q.store == nil {
		return errors.New("event: RedisDLQ 未配置存储")
	}
	if dl.ID == "" {
		dl.ID = dl.MsgID
	}
	if dl.ID == "" {
		return errors.New("event: 死信记录缺少 ID")
	}
	b, err := ujson.Marshal(dl)
	if err != nil {
		return fmt.Errorf("event: 序列化死信失败: %w", err)
	}
	// 用 IsZero 判零值：time.Time{} 的 UnixMilli() 是 -6795364578871 而非 0，
	// 用 `UnixMilli()==0` 判空永远不成立，零值时间会被写成远古时间戳，ZSET 排序/分页错乱。
	lastAtMS := dl.LastAt.UnixMilli()
	if dl.LastAt.IsZero() || lastAtMS <= 0 {
		lastAtMS = timeutil.NowMS()
	}
	score := float64(lastAtMS)
	// 先写索引：若索引写入失败则不写 data，避免 data 存在但 index 缺失导致死信无法 List。
	if err := q.store.ZAdd(ctx, q.indexKey(), score, dl.ID); err != nil {
		return fmt.Errorf("event: 写入死信索引失败: %w", err)
	}
	if err := q.store.HSet(ctx, q.dataKey(), dl.ID, string(b)); err != nil {
		return fmt.Errorf("event: 写入死信失败: %w", err)
	}
	if q.ttl > 0 {
		// 续期失败不推翻「死信已写入」的事实，但必须留日志：
		// 否则死信会永不过期、持续堆积，而现场没有任何痕迹。
		if _, err := q.store.Expire(ctx, q.dataKey(), q.ttl); err != nil {
			logger.Warnf("event: 死信 data key 续期失败 id=%s ttl=%s: %v", dl.ID, q.ttl, err)
		}
		if _, err := q.store.Expire(ctx, q.indexKey(), q.ttl); err != nil {
			logger.Warnf("event: 死信 index key 续期失败 id=%s ttl=%s: %v", dl.ID, q.ttl, err)
		}
	}
	return nil
}

// List 按入队时间倒序列出死信。
func (q *RedisDLQ) List(ctx context.Context, limit int) ([]DeadLetter, error) {
	if q == nil || q.store == nil {
		return nil, errors.New("event: RedisDLQ 未配置存储")
	}
	stop := int64(-1)
	if limit > 0 {
		stop = int64(limit - 1)
	}
	ids, err := q.store.ZRevRange(ctx, q.indexKey(), 0, stop)
	if err != nil {
		return nil, fmt.Errorf("event: 读取死信索引失败: %w", err)
	}
	out := make([]DeadLetter, 0, len(ids))
	for _, id := range ids {
		raw, err := q.store.HGet(ctx, q.dataKey(), id)
		if err != nil {
			// 索引与数据不一致时跳过，不阻断整体列出；但不能静默——
			// 否则「死信看不全」永远无人知道是读取失败还是本来就没有。
			logger.Warnf("event: 死信读取失败 id=%s: %v", id, err)
			continue
		}
		if raw == "" {
			logger.Warnf("event: 死信索引指向不存在的数据 id=%s（索引与数据不一致）", id)
			continue
		}
		var dl DeadLetter
		if err := ujson.Unmarshal([]byte(raw), &dl); err != nil {
			logger.Warnf("event: 死信反序列化失败 id=%s: %v", id, err)
			continue
		}
		out = append(out, dl)
	}
	return out, nil
}

// Retry 人工重投指定死信；成功后移除。
func (q *RedisDLQ) Retry(ctx context.Context, id string) error {
	if q == nil || q.store == nil {
		return errors.New("event: RedisDLQ 未配置存储")
	}
	raw, err := q.store.HGet(ctx, q.dataKey(), id)
	if err != nil || raw == "" {
		return fmt.Errorf("%w: id=%s", ErrDeadLetterNotFound, id)
	}
	var dl DeadLetter
	if err := ujson.Unmarshal([]byte(raw), &dl); err != nil {
		return fmt.Errorf("event: 解析死信 %s 失败: %w", id, err)
	}

	q.retryLock.RLock()
	fn := q.retrier
	q.retryLock.RUnlock()
	if fn == nil {
		return ErrRetryNotSupported
	}
	evt := dl.Event.Clone()
	if evt == nil {
		return fmt.Errorf("event: 死信 %s 缺少原事件，无法重投", id)
	}
	evt.Attempt = 0
	if err := fn(ctx, evt); err != nil {
		return fmt.Errorf("event: 重投死信 %s 失败: %w", id, err)
	}
	return q.Remove(ctx, id)
}

// Remove 删除指定死信记录。
//
// 先删索引（ZSET）再删数据（HASH），与 Push 的顺序**相反**：
// 索引是可见性的唯一来源，先删索引后中途失败只会留下一个孤儿 HASH 字段
// （List 看不到、最终由 TTL 清掉）；反过来则会留下「索引指向不存在的数据」的脏条目，
// List 会拿到空值、解析失败，且没有任何清理路径。
func (q *RedisDLQ) Remove(ctx context.Context, id string) error {
	if q == nil || q.store == nil {
		return errors.New("event: RedisDLQ 未配置存储")
	}
	if _, err := q.store.ZRem(ctx, q.indexKey(), id); err != nil {
		return fmt.Errorf("event: 删除死信索引失败: %w", err)
	}
	n, err := q.store.HDel(ctx, q.dataKey(), id)
	if err != nil {
		return fmt.Errorf("event: 删除死信失败: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: id=%s", ErrDeadLetterNotFound, id)
	}
	return nil
}

// nopDLQ 关闭死信功能时使用的空实现。
type nopDLQ struct{}

func (nopDLQ) Push(context.Context, DeadLetter) error          { return nil }
func (nopDLQ) List(context.Context, int) ([]DeadLetter, error) { return nil, nil }
func (nopDLQ) Retry(context.Context, string) error             { return ErrRetryNotSupported }
func (nopDLQ) Remove(context.Context, string) error            { return ErrDeadLetterNotFound }

// NopDLQ 返回一个丢弃所有死信的空队列。
func NopDLQ() DeadLetterQueue { return nopDLQ{} }
