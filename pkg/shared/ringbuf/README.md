# ringbuf 模块

## 模块职责

`ringbuf` 提供**通用泛型环形缓冲**，用一段连续的固定容量数组循环复用来做 FIFO 队列。核心价值是提供 Go 原生 `chan` 没有的能力：**零锁、零分配的热路径，以及可随时观测的 `Len()` / `Full()` / `Empty()` 状态**。包内提供两个变体：`Ring[T]` 是纯 SPSC（单生产者单消费者）无锁环形缓冲，`MpmcRing[T]` 是多生产者多消费者变体（内部互斥锁）。两者 API 完全对称且**全部非阻塞**——满就返回 false、空就返回 false，阻塞等待由消费方在自己的 tick/select 里决定（actor 风格）。设计源于 MMO 引擎 `MsgQueue` 的「一边读一边写可不加锁」环形字节区。

## 规则与约束

1. **`Ring[T]` 必须严格用于 SPSC 场景**：恰好一个协程调用 `Push`、恰好一个协程调用 `Pop`。多生产者、多消费者，或「同协程既 Push 又 Pop 且与第三方并发」的用法，必须改用 `MpmcRing[T]` 或由调用方自行同步；违反 SPSC 不会 panic，只会静默覆盖未读数据。
2. **容量一律以 `Cap()` 为准**：构造容量会被向上取整到 2 的幂（`New(10)` → 16），实际内存占用可能接近请求值的 2 倍。
3. **`Push` 的返回值必须检查**：满时返回 false，既不阻塞也不扩容，忽略返回值会导致数据被静默丢弃。
4. **`Len()` / `Empty()` / `Full()` 只能作为监控用的瞬时快照，不得用于控制流**：「先判满再 Push」存在 TOCTOU 窗口，是否写入成功必须以 `Push` 的返回值为唯一依据。
5. **元素类型为指针或含指针时，及时回收必须由调用方负责**：`Pop` 只返回槽位副本并推进游标，不清空槽位，被取出的对象会保留引用直到该槽位被下一轮 `Push` 覆盖；本包不在 `Pop` 中置零。
6. **`Cap()` 可在任意协程调用**：其读取的是构造期确定、此后不再变化的底层数组长度，无需与其它方法保持同样的加锁风格。
7. **读写游标必须保持 `uint64` 单调递增并以 `pos & mask` 计算下标**，依赖无符号回绕语义处理计数器溢出；不得改为有符号整型。
8. **`Ring.Drain()` 不是原子快照**：其实现为循环 `Pop`，并发生产者持续写入时可能长时间不返回；需要某一确定时刻的快照时必须改用 `MpmcRing.Drain()`。
9. **热路径必须循环 `Pop` 而非 `Drain()`**：`Drain()` 每次调用都新分配切片，高频调用会造成 GC 压力。
10. **`TryPush` / `TryPop` 是 `Push` / `Pop` 的等价别名**，不含任何重试或退避逻辑。
11. **构造容量 `<= 1`（含 0 与负值）时实际容量恒为 1**：此类队列可用但不具备缓冲能力。
12. **等待策略必须由调用方实现**：全部 API 均为非阻塞语义，满时的丢弃 / 退避重试 / 降级与空时的等待，均由调用方在自己的 tick 或 select 中决定。
13. **本包不提供 `Peek`**：需要「查看但不取出」能力时必须由调用方自行扩展。
14. **极高吞吐场景的伪共享开销必须由调用方评估**：`Ring` 的 `w` 与 `r` 相邻且未做缓存行填充，生产者与消费者分别写这两个变量会导致缓存行在核间反复失效。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `ringbuf.go` | 290 | 全部内容：包文档、`nextPow2` 转发、`Ring[T]` SPSC 无锁环形缓冲（New/Cap/Push/Pop/Len/Empty/Full/TryPush/TryPop/Drain）、`MpmcRing[T]` MPMC 互斥锁环形缓冲（NewMpmc + 同名 9 个方法） |

## 核心类型与接口

### `Ring[T any]`（SPSC 无锁）

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `buf` | `[]T` | 底层存储，长度恒为 2 的幂 |
| `mask` | `uint64` | `cap - 1`，用于位与取模 |
| `w` | `atomic.Uint64` | 下一个**写入**位置，**单调递增、不回绕重置** |
| `r` | `atomic.Uint64` | 下一个**读取**位置，**单调递增、不回绕重置** |

**并发安全性**：**恰好一个 goroutine 调用 Push、恰好一个 goroutine 调用 Pop 时无锁安全**。其它组合（两个生产者、两个消费者、同一协程既读又写与第三方并发）**都不安全**，需外部同步或改用 `MpmcRing`。`Len`/`Empty`/`Full` 是观测方法，可在任意协程调用（结果是瞬时快照，可能立即过期）。

### `MpmcRing[T any]`（MPMC 加锁）

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `mu` | `sync.Mutex` | 保护以下全部字段 |
| `buf` | `[]T` | 底层存储，长度恒为 2 的幂 |
| `mask` | `uint64` | `cap - 1` |
| `w`, `r` | `uint64` | 单调递增游标（无符号；与 SPSC 的 `Ring[T]` 一致，借回绕语义处理计数器溢出） |

**并发安全性**：**任意数量的 goroutine 都可并发调用任意方法**，全部方法入口加锁。

## 算法与实现原理

### 容量归整到 2 的幂

```go
cap := nextPow2(capacity)  // 本地 nextPow2 先做上限夹紧，再调 util.NextPow2
if cap < 1 { cap = 1 }
mask = uint64(cap - 1)
```

为什么必须是 2 的幂：这样 `pos % cap` 可以用 `pos & mask` 替代。位与是单条 CPU 指令（约 1 周期），而整数取模需要除法指令（约 20~40 周期）。在每次 Push/Pop 都要做一次索引计算的热路径上，这个优化很关键。

**注意 `New(10)` 实际容量是 16**，不是 10。`Cap()` 返回的是**实际**容量。

### 读写指针：单调递增而非回绕

这是本包设计上最精妙的一点。常见的朴素实现是让读写指针在 `[0, cap)` 内回绕：

```
w = (w + 1) % cap   // 朴素做法
```

但这样**无法区分「空」与「满」**（两种情况下 `w == r` 都成立），必须额外维护一个 count 或牺牲一个槽位。

本包的做法是让 `w` / `r` **永远单调递增，永不重置**，只在访问数组时才 `& mask`：

```
元素个数 = w - r       // 天然可算，0 表示空
是否满   = w - r >= cap
数组下标 = pos & mask
```

于是空（`w == r`）与满（`w - r == cap`）**天然可区分**，不浪费槽位，也不需要额外计数器。

### uint64 溢出的正确处理（Ring）

`w` / `r` 是 `uint64`，理论上会在累计 2⁶⁴ 次操作后溢出。本包依赖 **Go 无符号整数的定义良好的回绕语义**来正确处理这种情况：

```go
if w-rval >= uint64(len(r.buf)) { return false }
```

假设 `w` 刚溢出回到 `3`，而 `r` 还是 `math.MaxUint64 - 2`。无符号减法 `3 - (MaxUint64-2)` 在模 2⁶⁴ 意义下等于 `6`——**正是真实的环形距离**。所以溢出被自动、正确地处理了，无需任何特判。

同理 `Empty()` 用 `r == w`（等值判断而非大小比较）也是溢出安全的。

**这也是 `Ring` 与 `MpmcRing` 的游标都用 `uint64` 的原因**：两者都依赖无符号回绕语义处理计数器溢出——`MpmcRing` 虽然全程持锁，比较处仍全用 `uint64(len(...))`，与 SPSC 的 `Ring[T]` 保持同一套游标语义。

### SPSC 无锁的内存序设计

`Ring` 不加锁却能保证正确性，靠的是 **atomic 操作的顺序一致性 + 精心设计的 Load 顺序**：

**Push（生产者）**：
```go
rval := r.r.Load()  // ① 先读对方（消费者）控制的位置
w := r.w.Load()     // ② 再读自己控制的位置
if w - rval >= cap { return false }
buf[w & mask] = v   // ③ 写数据
r.w.Store(w + 1)    // ④ 最后发布写指针
```

**Pop（消费者）**：
```go
w := r.w.Load()     // ① 先读对方（生产者）控制的位置
pos := r.r.Load()   // ② 再读自己控制的位置
if pos == w { return zero, false }
v := buf[pos & mask] // ③ 读数据
r.r.Store(pos + 1)  // ④ 最后发布读指针
```

**为什么是「先读对方，再读自己」**：

自己控制的指针只有自己会改，读取时值一定是最新的。对方的指针可能随时变化，但**只会朝着「对我更有利」的方向变**——消费者只会让 `r` 增大（腾出更多空间），生产者只会让 `w` 增大（提供更多数据）。先读对方就意味着**拿到的是一个偏保守的旧值**：

- Push 用旧的 `r` 计算空间 → 可能误判为满（少写一次，安全）；绝不会误判为有空间（否则会覆盖未读数据）。
- Pop 用旧的 `w` 判断有无数据 → 可能误判为空（少读一次，安全）；绝不会误判为有数据（否则会读到未写入的槽）。

**如果顺序反了**（先读自己再读对方），在两次 Load 之间对方可能推进指针，导致算出一个「过于乐观」的空间/数量，进而越界覆盖或读到脏数据。

**步骤 ③④ 的顺序也关键**：先写数据再发布指针。Go 的 `atomic.Store` 具有 release 语义，`atomic.Load` 具有 acquire 语义，保证 ③ 的普通内存写在 ④ 之前对消费者可见。

### Len 的钳制

```go
diff := w.Load() - r.r.Load()
n := int(diff)
if n < 0 { return 0 }
if n > len(buf) { return len(buf) }
return n
```

由于 `Len()` 可能在并发修改中调用，两次 Load 不是原子的，`diff` 理论上可能是个异常值（如 `int` 转换后为负，或大于容量）。这里做了双向钳制，保证返回值**永远落在 `[0, cap]`**，不会让调用方拿到荒谬的数字。

`MpmcRing.Len()` 虽然全程持锁不会出现异常值，但保留了同样的钳制逻辑（防御性）。

### Drain 的两种实现

- **`Ring.Drain()`**：循环调用 `Pop()` 直到返回 false。**不持锁**（SPSC 本就无锁），每次 Pop 都是一次原子操作。预分配 `make([]T, 0, r.Len())`，但因为并发中 `Len()` 可能变化，`append` 仍可能扩容。
- **`MpmcRing.Drain()`**：**一次性持锁**遍历整个区间，直接从 buf 取值、递增 `r`，不重复调用 `Pop`（避免重复加锁）。这是正确且高效的做法。

### 复杂度总结

| 操作 | Ring（SPSC） | MpmcRing（MPMC） |
| --- | --- | --- |
| `Push` / `Pop` | O(1)，2 次 atomic Load + 1 次 Store，**无锁无分配** | O(1) + 一次锁 |
| `Len` / `Empty` / `Full` | O(1)，2 次 atomic Load | O(1) + 一次锁 |
| `Drain` | O(n)，n 次 Pop | O(n)，**一次**加锁 |
| `Cap` | O(1)，无同步 | O(1)，无同步 |

## 对外 API

### `Ring[T]`（SPSC 无锁）

```go
var ErrCapacityOverflow                 // 请求容量超出可表示的 2 的幂上限
func New[T any](capacity int) *Ring[T]  // 容量向上取整到 2 的幂，最小 1；超上限时夹紧到上限
func NewChecked[T any](capacity int) (*Ring[T], error) // 超上限返回 ErrCapacityOverflow，适合校验来自配置的容量

func (r *Ring[T]) Cap() int
func (r *Ring[T]) Push(v T) bool        // 满则 false
func (r *Ring[T]) Pop() (T, bool)       // 空则 (零值, false)
func (r *Ring[T]) Len() int
func (r *Ring[T]) Empty() bool
func (r *Ring[T]) Full() bool
func (r *Ring[T]) TryPush(v T) bool     // Push 的别名
func (r *Ring[T]) TryPop() (T, bool)    // Pop 的别名
func (r *Ring[T]) Drain() []T           // 取出全部（最旧在前）
```

```go
// 单生产者单消费者：网络收包 → 逻辑处理
q := ringbuf.New[*Packet](1024) // 实际容量 1024（已是 2 的幂）

// 生产者协程（恰好一个）
go func() {
    for pkt := range netCh {
        if !q.Push(pkt) {
            metrics.Drop() // 满了，丢弃或稍后重试
        }
    }
}()

// 消费者协程（恰好一个）：actor 风格 tick
for range ticker.C {
    for {
        pkt, ok := q.Pop()
        if !ok {
            break
        }
        handle(pkt)
    }
}
```

### `MpmcRing[T]`（MPMC 加锁）

```go
func NewMpmc[T any](capacity int) *MpmcRing[T]                  // 超上限时夹紧到上限
func NewMpmcChecked[T any](capacity int) (*MpmcRing[T], error)  // 超上限返回 ErrCapacityOverflow

func (r *MpmcRing[T]) Cap() int
func (r *MpmcRing[T]) Push(v T) bool
func (r *MpmcRing[T]) Pop() (T, bool)
func (r *MpmcRing[T]) Len() int
func (r *MpmcRing[T]) Empty() bool
func (r *MpmcRing[T]) Full() bool
func (r *MpmcRing[T]) TryPush(v T) bool
func (r *MpmcRing[T]) TryPop() (T, bool)
func (r *MpmcRing[T]) Drain() []T
```

```go
// 多个协程投递任务，多个 worker 消费
q := ringbuf.NewMpmc[Task](256)

// 多生产者
for i := 0; i < 4; i++ {
    go func() {
        for t := range src {
            for !q.Push(t) {
                time.Sleep(time.Millisecond) // 满了退避重试
            }
        }
    }()
}

// 多消费者
for i := 0; i < 8; i++ {
    go func() {
        for {
            t, ok := q.Pop()
            if !ok {
                time.Sleep(time.Millisecond)
                continue
            }
            t.Run()
        }
    }()
}
```

### 批量排空

```go
// 一次性取走所有元素
all := q.Drain()
for _, v := range all {
    process(v) // 最旧的在前
}

// 观测
log.Printf("cap=%d len=%d full=%v", q.Cap(), q.Len(), q.Full())
```

## 依赖关系

- **标准库**：`sync`（MpmcRing 的互斥锁）、`sync/atomic`（Ring 的 `atomic.Uint64`）。
- **引擎内部**：`pkg/shared/util`（`NextPow2` 容量归整）。
- 无第三方依赖，可纯内存单测。
- **设计参考**：MMO 引擎的 `MsgQueue`（环形字节区）；**语义对齐**：与 `MsgQueue` 一样是非阻塞 API。

