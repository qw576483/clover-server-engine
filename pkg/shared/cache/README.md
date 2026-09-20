# cache 模块

## 模块职责

`cache` 提供**通用泛型缓存原语**：分片降锁争用、三种淘汰策略（LRU/LFU/FIFO）、per-item TTL、singleflight 防击穿、统计观测。用 Go 泛型 + 标准库表达，零外部依赖（仅内部依赖 `shared/util`）、并发安全、可纯内存单测。典型用途是在 `data.Store` 之前垫一层热数据缓存（玩家 / 配置 / 全局小数据）降低 MySQL/Redis 压力，也可缓存配置表、排行榜计算结果、跨服镜像等「计算贵、重复读」的中间结果。时钟可注入以便单测 TTL，淘汰可注册回调用于回写或计数。

## 规则与约束

1. **容量为分片级配额**：`WithMaxNum` 设定的是**每片**容量上限，全局容量上限等于「每片容量 × 分片数」；分片数增大同时会拉低单片命中率，必须按并发度与总量权衡设定。
2. **loader 中禁止操作本 Cache**：`GetOrLoad` 的 loader 内不得调用同一 Cache 的任何方法；同 goroutine 同 key 递归会返回 `ErrReentrantLoad`，跨 key 或跨 goroutine 的循环等待无法被自检机制识别，会永久阻塞。
3. **`onEvict` 的执行上下文**：`onEvict` 在**释放分片锁之后**调用（锁内只做摘除与收集），因此回调可以回写本 Cache；但仍不建议在回调里做耗时 IO（会阻塞调用方的后续回调）。回调触发顺序与摘除顺序一致，不保证跨分片全局有序。
4. **TTL 选项的取值语义**：`WithItemTTL(d>0)` 覆盖默认 TTL，`WithItemTTL(-1)` 表示永不过期，`WithItemTTL(0)` 表示沿用默认 TTL；修改已存在条目的过期时间必须使用 `SetTTL`，其 `d<=0` 等价于永不过期。
5. **覆盖写入不重置插入序**：`Set` 覆盖已存在条目时保留首次创建的 `birth`，FIFO/LFU 策略下不重置驱逐优先级；需要重置驱逐优先级时必须先 `Delete` 再 `Set`。
6. **LFU 的淘汰复杂度**：`PolicyLFU` 的 victim 选取为 O(n) 全链表扫描，大容量分片必须改用 `PolicyLRU`/`PolicyFIFO`，或通过增加分片数压小单片规模。
7. **`Len` 与 `Keys` 为近似值**：两者均包含尚未清理的过期条目，且 `Keys` 需逐片加锁遍历全表，不得在热路径调用；需要精确计数必须启用 sweep 或接受该误差。
8. **`Close` 的调用职责**：启用 `WithSweepInterval` 的实例必须由调用方 `defer Close()`，否则后台 goroutine 泄漏；`Close` 只停止周期清理、不清空数据，关闭后仍可继续读写，惰性过期继续生效。
9. **分片路由的开销**：路由键由 `fmt.Sprintf("%v", key)` 生成，每次读写都有一次格式化与堆分配，热路径必须使用低成本的 key 类型；`%v` 表示相同的不同 key 会落到同一分片。
10. **淘汰序使用逻辑时钟**：`lastAccess` 取自分片内单调递增的逻辑序号，与 `WithClock` 注入的时钟无关；`WithClock` 只影响 TTL 判定，不得用于改变淘汰顺序。
11. **统计口径**：`Peek` 不计入 Hits/Misses；`GetOrLoad` 命中时计 Hits 不计 Loads，等待方共享结果不重复计 Loads，loader 失败不计 Loads 但其内部首次 `Get` 已计一次 Misses；`Clear` 不计入 Deletes。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `cache.go` | 701 | 全部内容：`Policy` 淘汰策略枚举、`Stats`/`atomicStats` 统计、`Option`/`ItemOption` 选项体系、`entry`/`shard`/`Cache` 核心结构、singleflight（`call`/`goID`/`ErrReentrantLoad`）、读写 API（Get/Peek/Set/SetTTL/Delete/Contains/Len/Keys/Clear/Stats/GetOrLoad）、淘汰（evictTo/pickVictim）、后台清理（sweepLoop/sweepOnce）与 `Close` |

## 核心类型与接口

### `Policy`（淘汰策略枚举）

| 常量 | 值 | 语义 |
| --- | --- | --- |
| `PolicyLRU` | 0（默认） | 最近最少使用：Get/GetOrLoad 命中把条目移到链表队首，淘汰队尾 |
| `PolicyLFU` | 1 | 最不常使用：按 `freq` 淘汰，频率相同时淘汰 `birth` 更小者（更早插入） |
| `PolicyFIFO` | 2 | 先进先出：按插入顺序淘汰，访问不改变顺序 |

### `Stats`

| 字段 | 含义 |
| --- | --- |
| `Hits` | `Get`/`GetOrLoad` 命中次数 |
| `Misses` | 未命中（含过期）次数 |
| `Evictions` | 因容量超限被淘汰的条目数 |
| `Sets` | `Set` 调用次数（含覆盖） |
| `Deletes` | `Delete` / 惰性过期清理 / sweep 清理触发的移除数 |
| `Loads` | loader **成功执行且写入缓存**的次数 |

`Stats()` 遍历各分片的 `atomicStats` 无锁求和，是**近似快照**，可能略滞后。

### `entry[K, V]`（私有）

| 字段 | 含义 |
| --- | --- |
| `key` / `value` | 键值 |
| `expiry` | 绝对过期时间（unix nano）；`0` = 永不过期 |
| `lastAccess` | LRU 的逻辑时钟序号（单调递增，非墙钟） |
| `birth` | 插入序号，FIFO / LFU 同频时的 tie-break 依据 |
| `freq` | LFU 访问计数 |

### `shard[K, V]`（私有）

| 字段 | 含义 |
| --- | --- |
| `mu` | 分片互斥锁，保护 `items`/`ll`/`seq`/`entry` 字段 |
| `items` | `map[K]*list.Element`，key → 链表节点，O(1) 定位 |
| `ll` | `container/list` 双向链表，front = 最新（LRU）/ 队首（FIFO） |
| `seq` | 逻辑时钟兼插入序号，单调递增，从 1 起 |
| `policy` / `max` / `defTTL` / `clock` / `onEvict` | 从配置复制的分片级参数 |
| `stats` | **每片独立**的原子统计（避免多片共享一份在 `-race` 下竞争） |

### `Cache[K comparable, V any]`

| 字段 | 含义 |
| --- | --- |
| `shards` | 分片数组，长度为 2 的幂 |
| `shardN` / `mask` | 分片数与 `shardN-1` 位掩码 |
| `clock` | 时钟函数 |
| `stop` / `stopOnce` | 后台 sweep 的停止信号与幂等保护 |
| `sfMu` / `inflight` | singleflight 的全局锁与在途调用表（**跨分片共享一把锁**） |

**并发安全性**：`Cache` 的所有导出方法**并发安全**。写操作走分片互斥锁；统计走原子计数器；`GetOrLoad` 的合并表另有一把 `sfMu`。`Close` 幂等。`onEvict` 回调是**在释放分片锁之后**调用的（锁内只做摘除与收集，见「规则与约束」第 3 条）。

## 算法与实现原理

### 分片与路由

`New` 把请求的分片数**向上取整到 2 的幂**（上限 `1<<20`），这样分片选择可用位掩码 `h & mask` 代替取模。路由函数：

```go
h := hash.Fnv32(fmt.Sprintf("%v", key))
shard = shards[h & uint32(mask)]
```

`shardN == 1` 时短路直接返回 `shards[0]`，避免哈希开销。每片一把锁 + 独立链表 + 独立容量上限。

### 淘汰策略实现

统一入口 `evictTo(s, extra)`：当 `len(s.items) > s.max+extra` 时，**先扫描并摘除全部分片内已过期条目**（防止「过期僵尸」占着名额把仍有效的条目挤掉），再循环调用 `pickVictim` 摘除受害者；摘除产生的 `onEvict` 由调用方在解锁后统一触发。

- **FIFO / LRU**：`pickVictim` 直接返回 `s.ll.Back()`，**O(1)**。二者差别在于 LRU 在 `touch` 时会 `MoveToFront`，FIFO 不会。
- **LFU**：无堆结构，**全链表线性扫描** 找最小 `freq`（同频取最小 `birth`），**O(n)**。

`max <= 0` 表示不限容量，`evictTo` 直接返回。

### TTL 与过期判定

- `expired(e, now)` = `e.expiry != 0 && e.expiry <= now`，即 `expiry == 0` 表示永不过期。
- `Set` 时 TTL 优先级：`WithItemTTL(d>0)` > `defaultTTL` > 永不过期；**`WithItemTTL(-1)` 是特判**，强制 `expiry = 0`（永不过期），可用于在有默认 TTL 的缓存里写入常驻项。
- **惰性过期**：`Get`/`Peek`/`Contains` 命中已过期条目时**立即删除**（计入 `Deletes` 并触发 `onEvict`）并返回未命中。
- **主动 sweep**：`WithSweepInterval(d)` 启动后台 goroutine，每 `d` 遍历全部分片，先收集过期节点再统一移除（避免遍历中改链表）。

### singleflight 防击穿

`GetOrLoad` 流程：

1. 先 `Get`，命中直接返回（计 Hits）。
2. 取 `sfMu`，查 `inflight[key]`：
   - 已有在途调用且 `owner != 当前 goroutine id` → 释放锁、`wg.Wait()`、共享 `(val, err)`。
   - 已有在途调用且 `owner == 当前 gid` → **递归自调用**，若 Wait 会等自己而永久死锁，直接返回 `ErrReentrantLoad`。
3. 否则登记 `call{owner: gid}`、`wg.Add(1)`、释放锁，执行 `load(ctx, key)`。
4. 成功则 `Set` 并 `Loads++`；失败**不缓存**、不计 Loads，所有等待方拿到同一 error。
5. `defer` 中：若 loader panic（`done == false`）则把 `cl.err` 置为 `"cache: loader panicked for key %v"`，避免等待方拿到「零值 + nil error」的伪成功；随后从 `inflight` 摘除并 `wg.Done()`。

`goID()` 通过解析 `runtime.Stack` 首行 `"goroutine N [...]"` 提取，**仅用于递归死锁自检**，不参与业务逻辑。

## 对外 API

### 构造与选项

```go
func New[K comparable, V any](opts ...Option) *Cache[K, V]

func WithMaxNum(n int) Option            // 每片容量上限，0=不限
func WithDefaultTTL(d time.Duration) Option // 默认 TTL，0=永不过期
func WithPolicy(p Policy) Option         // 默认 PolicyLRU
func WithShards(n int) Option            // 分片数，<1 视为 1，向上取整 2 的幂
func WithClock(clk func() time.Time) Option // 注入时钟，默认 time.Now
func WithOnEvict[K comparable, V any](fn func(K, V)) Option // 淘汰/删除回调
func WithSweepInterval(d time.Duration) Option // 后台清理间隔，0=不启动
```

```go
c := cache.New[string, *Player](
    cache.WithMaxNum(1024),
    cache.WithShards(16),
    cache.WithPolicy(cache.PolicyLRU),
    cache.WithDefaultTTL(5*time.Minute),
    cache.WithSweepInterval(time.Minute),
    cache.WithOnEvict[string, *Player](func(k string, v *Player) {
        log.Printf("evicted %s", k)
    }),
)
defer c.Close()
```

### 读写

```go
func (c *Cache[K, V]) Get(key K) (V, bool)   // 命中刷新访问序
func (c *Cache[K, V]) Peek(key K) (V, bool)  // 只读，不刷新访问序
func (c *Cache[K, V]) Set(key K, val V, opts ...ItemOption)
func WithItemTTL(d time.Duration) ItemOption // -1 表示永不过期
func (c *Cache[K, V]) SetTTL(key K, d time.Duration) bool // d<=0 => 永不过期
func (c *Cache[K, V]) Delete(key K) bool
func (c *Cache[K, V]) Contains(key K) bool
```

```go
c.Set("p:1001", p)                                    // 用默认 TTL
c.Set("cfg:global", cfg, cache.WithItemTTL(-1))       // 常驻不过期
c.Set("tmp:x", v, cache.WithItemTTL(10*time.Second))  // 单条覆盖 TTL

if v, ok := c.Get("p:1001"); ok { use(v) }
if v, ok := c.Peek("p:1001"); ok { /* 不污染 LRU 序 */ _ = v }
c.SetTTL("p:1001", time.Hour)
c.Delete("tmp:x")
```

### 批量与观测

```go
func (c *Cache[K, V]) Len() int      // 跨片求和，近似（可能含未清理的过期项）
func (c *Cache[K, V]) Keys() []K     // 全部 key（含未清理的过期项）
func (c *Cache[K, V]) Clear()        // 清空，逐条触发 onEvict
func (c *Cache[K, V]) Stats() Stats  // 无锁快照
func (c *Cache[K, V]) Close()        // 停止后台 sweep，幂等
```

```go
st := c.Stats()
rate := float64(st.Hits) / float64(st.Hits+st.Misses)
log.Printf("hit rate=%.2f%% evictions=%d", rate*100, st.Evictions)
```

### 防击穿加载

```go
func (c *Cache[K, V]) GetOrLoad(
    ctx context.Context, key K,
    load func(ctx context.Context, key K) (V, error),
) (V, error)

// 实际定义在 cache.go：
var ErrReentrantLoad = fmt.Errorf("cache: 检测到同一 key 的 GetOrLoad 递归调用（re-entrant load），已中止以避免死锁")
```

```go
p, err := c.GetOrLoad(ctx, "p:1001", func(ctx context.Context, k string) (*Player, error) {
    return store.LoadPlayer(ctx, k) // 同一 key 并发只跑一次
})
if errors.Is(err, cache.ErrReentrantLoad) {
    // loader 内部递归调了同一 key
}
```

## 依赖关系

- **标准库**：`container/list`（LRU/FIFO 链表）、`context`、`fmt`、`runtime`（goID）、`sync`、`sync/atomic`、`time`。
- **引擎内部**：`clover-server-engine/pkg/shared/util`（`Fnv32` 用于分片路由）。
- 无第三方依赖，可纯内存单测；上层通常垫在 `data.Store` 之前。
