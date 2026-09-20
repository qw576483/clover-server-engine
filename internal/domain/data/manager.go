package data

import (
	"context"
	"reflect"
	"sync"
)

// Manager 内存实体管理器：在进程内缓存经 Store 加载的实体对象（按 Key 索引），
// 让你能「一次加载、多次读取、批量持有多个玩家 / 账号数据」，并在需要时统一写回 Store。
//
// 泛型 V 为实体类型（如 *player.EPlayer、*account.EAccount，或任意业务结构体指针）。
// 典型用法：
//
// pm := data.NewManager[*player.EPlayer](store)
// p, _ := pm.Load(ctx, data.Key{OwnerPlayer, owner, "profile"}) // 内存优先；未命中则经 Store 加载并缓存
// pm.Put(ctx, data.Key{OwnerPlayer, owner, "profile"}, p) // 写内存 + 持久化
// batch, _ := pm.LoadMany(ctx, []data.Key{k1, k2, k3}) // 一次取多个玩家
// pm.Range(func(k data.Key, v *player.EPlayer) bool { return true }) // 遍历全部已加载玩家
//
// 注意：Manager 不自动淘汰；需要 LRU / 按 logout 换出时，业务自行 Range + Delete，或未来扩展。
type Manager[V any] struct {
	store *Store
	mu    sync.RWMutex
	items map[Key]V
	// batchConcurrency 批量 IO（LoadMany / Flush）的并发上限；<=0 表示用 defaultBatchConcurrency。
	batchConcurrency int
}

// NewManager 用给定的 Store 构造实体管理器。
func NewManager[V any](s *Store) *Manager[V] {
	return &Manager[V]{store: s, items: make(map[Key]V)}
}

// defaultBatchConcurrency 批量 IO（LoadMany / Flush）的默认并发上限。
//
// 为什么必须有上限：此前每个 key 直接 go 一个协程且不设限，上万 key 会同时压在
// Redis / MySQL 连接池上 —— 连接池被占满后其余请求全部排队等超时，错误放大成风暴。
// 批量 IO 是吞吐型任务：并发超过连接池容量不会更快，只会把压力推给下游。
const defaultBatchConcurrency = 16

// batchLimit 返回生效的批量并发上限。
func (m *Manager[V]) batchLimit() int {
	m.mu.RLock()
	n := m.batchConcurrency
	m.mu.RUnlock()
	if n <= 0 {
		return defaultBatchConcurrency
	}
	return n
}

// SetBatchConcurrency 设置批量 IO 的并发上限；n<=0 表示恢复默认值（defaultBatchConcurrency）。
// 取值建议不超过下游连接池容量：调大不会更快，只会让等待从本进程转移到 Redis / MySQL。
func (m *Manager[V]) SetBatchConcurrency(n int) {
	m.mu.Lock()
	m.batchConcurrency = n
	m.mu.Unlock()
}

// Get 仅返回内存中已缓存的实体；未加载返回零值与 false（不触发持久层读取）。
func (m *Manager[V]) Get(key Key) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.items[key]
	return v, ok
}

// Cache 仅放入内存（不持久化）。用于「从持久层加载后顺手缓存」以避免重复 IO。
func (m *Manager[V]) Cache(key Key, v V) {
	m.mu.Lock()
	m.items[key] = v
	m.mu.Unlock()
}

// Load 内存优先；未命中则经 Store.LoadJSON 反序列化到新实体并缓存后返回。
func (m *Manager[V]) Load(ctx context.Context, key Key) (V, error) {
	if v, ok := m.Get(key); ok {
		return v, nil
	}
	var v V
	if err := m.store.LoadJSON(ctx, key, &v); err != nil {
		return v, err
	}
	// 防零值缓存——LoadJSON 成功但反序列化出零值时（如空 JSON {}），
	// 跳过内存缓存，避免业务侧 Get 命中零值误判为"已加载"。
	if !isZeroValue(v) {
		m.Cache(key, v)
	}
	return v, nil
}

// LoadMany 批量加载多个 Key（内存优先），返回与 keys 等长的切片；任一失败立即返回错误。
// 并发加载以提升批量 IO 性能，但**并发有上限**（见 batchLimit）：超过上限的 key 排队等待，
// 避免上万 key 同时打爆下游连接池。ctx 取消后不再派发剩余 key，已派发的等其收尾。
func (m *Manager[V]) LoadMany(ctx context.Context, keys []Key) ([]V, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	out := make([]V, len(keys))
	sem := make(chan struct{}, m.batchLimit())
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	for i, k := range keys {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// 停止派发：剩余 key 未加载，与「任一失败立即返回错误」同一语义。
			once.Do(func() { firstErr = ctx.Err() })
			wg.Wait()
			return nil, firstErr
		}
		wg.Add(1)
		go func(i int, k Key) {
			defer wg.Done()
			defer func() { <-sem }() // 释放令牌，让后续 key 进入
			v, err := m.Load(ctx, k)
			if err != nil {
				once.Do(func() { firstErr = err })
				return
			}
			out[i] = v
		}(i, k)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// Put 写入内存并持久化（Store.SaveJSON）。
func (m *Manager[V]) Put(ctx context.Context, key Key, v V) error {
	if err := m.store.SaveJSON(ctx, key, v); err != nil {
		return err
	}
	m.Cache(key, v)
	return nil
}

// Delete 从内存与持久层同时删除。
// 先删持久层再删内存——持久层删除失败时返回 error，
// 内存数据保留，调用方可据此重试，避免"持久层有、内存无"的半删状态。
func (m *Manager[V]) Delete(ctx context.Context, key Key) error {
	if err := m.store.Delete(ctx, key); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.items, key)
	m.mu.Unlock()
	return nil
}

// Range 遍历内存中所有已加载实体。
// 在 RLock 下快照键值对列表，释放锁后再遍历调用回调，避免回调中调用 Put/Delete 导致死锁。
func (m *Manager[V]) Range(f func(Key, V) bool) {
	m.mu.RLock()
	snapshot := make(map[Key]V, len(m.items))
	for k, v := range m.items {
		snapshot[k] = v
	}
	m.mu.RUnlock()
	for k, v := range snapshot {
		if !f(k, v) {
			return
		}
	}
}

// Len 当前内存中已缓存实体数量。
func (m *Manager[V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

// Flush 将内存中所有实体写回持久层（SaveJSON）。
// 先在读锁下快照，再释放锁做 I/O，避免 SaveJSON 网络 I/O 期间阻塞所有 Get/Load/Cache 操作。
// 并发写入以提升批量 IO 性能，但**并发有上限**（见 batchLimit）：超过上限的条目排队等待，
// 避免全量刷盘瞬间打爆下游连接池。ctx 取消后不再派发剩余条目（已派发的等其收尾），
// 把「取消」的响应从「等全部写完」缩短到「等当前在途的一批」；未派发的条目留给下次 Flush。
// 任一写入失败返回错误，但其余条目仍会写完（不会因一条失败就丢下剩余未处理的部分）。
func (m *Manager[V]) Flush(ctx context.Context) error {
	m.mu.RLock()
	items := make(map[Key]V, len(m.items))
	for k, v := range m.items {
		items[k] = v
	}
	m.mu.RUnlock()
	if len(items) == 0 {
		return nil
	}
	sem := make(chan struct{}, m.batchLimit())
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	for k, v := range items {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			once.Do(func() { firstErr = ctx.Err() })
			wg.Wait()
			return firstErr
		}
		wg.Add(1)
		go func(k Key, v V) {
			defer wg.Done()
			defer func() { <-sem }() // 释放令牌，让后续条目进入
			if err := m.store.SaveJSON(ctx, k, v); err != nil {
				once.Do(func() { firstErr = err })
			}
		}(k, v)
	}
	wg.Wait()
	return firstErr
}

// isZeroValue 利用反射判断泛型值是否为零值（含 nil 指针 / 零结构体）。
// 用于 Manager.Load 防止缓存「空 JSON 反序列化出的零值指针/对象」。
//
// 切片 / 映射 / 数组类型不应被当作零值跳过缓存。合法的空集合（如 JSON "[]" / "{}"
// 反序列化出的非 nil 空切片 / 空 map，或 nil 集合表示「已加载但为空」）都是有效数据，
// 若按零值跳过缓存，业务侧每次 Get 未命中 → 反复回源持久层，既低效又可能误判「未加载」。
// 因此对集合类 Kind 一律视为非零，正常缓存。
func isZeroValue(v any) bool {
	if v == nil {
		return true
	}
	// 快速路径：对常见基础类型直接类型断言，避免反射开销。
	switch val := v.(type) {
	case int:
		return val == 0
	case int8:
		return val == 0
	case int16:
		return val == 0
	case int32:
		return val == 0
	case int64:
		return val == 0
	case uint:
		return val == 0
	case uint8:
		return val == 0
	case uint16:
		return val == 0
	case uint32:
		return val == 0
	case uint64:
		return val == 0
	case float32:
		return val == 0
	case float64:
		return val == 0
	case bool:
		return !val // false 是零值
	case string:
		return val == ""
	case []byte:
		return len(val) == 0
	}
	// 回退到反射路径处理指针/结构体等复杂类型。
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return true
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array:
		// 集合类型（含空集合）都是合法已加载数据，不按零值跳过。
		return false
	case reflect.Ptr:
		// 指针指向集合时同理：非 nil 指针即视为已加载。
		if !rv.IsNil() {
			switch rv.Elem().Kind() {
			case reflect.Slice, reflect.Map, reflect.Array:
				return false
			}
		}
	}
	return rv.IsZero()
}
