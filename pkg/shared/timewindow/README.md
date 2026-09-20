# timewindow 模块

## 模块职责

`pkg/shared/timewindow` 提供**线程安全的时间窗口计数器**。内部使用环形缓冲区（circular buffer）分桶存储计数，适合限流、QPS 统计、滑动窗口计数等场景：把统计窗口等分为若干桶，写入时按当前时刻定位到目标桶累加，读取时对窗口内所有有效桶求和，桶随时间自动淘汰复用，全程无内存分配、无后台协程。

## 规则与约束

1. **构造参数 `bucketCount` 与 `bucketDuration` 不得为非法值**：`bucketCount < 1` 强制回落为 `1`，`bucketDuration <= 0` 强制回落为 `1s`，其纳秒值小于 1 时强制为 `1ns`。窗口总时长由 `WindowDuration()` 给出，恒等于 `bucketCount × bucketDuration`。
2. **窗口以桶为粒度离散滑动**：同一桶内的多次 `Incr` 合并计入同一桶，`Sum` 是窗口内的近似值，误差上界为一个桶跨度。需要更精确统计时须增大 `bucketCount` 以减小桶跨度。
3. **过期桶在 `Incr` 与 `Sum` 调用时惰性淘汰**：桶的时间戳距当前时刻超过窗口总时长才清零，长期无调用后首次 `Sum` 会先完成对齐再统计。
4. **`Incr` 接受负数**：可用于记录净变化量，`Sum` 结果可能为负，用作限流判据时须自行做下界保护。
5. **所有方法由 `sync.Mutex` 保护，可并发调用**：`WindowDuration` 读取的是构造后只读字段，无需加锁。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `timewindow.go` | 151 | 包文档、`TimeWindow` 结构体、`NewTimeWindow`、`Incr`、`Sum`、`Reset`、`BucketCount`、`WindowDuration`、环形桶对齐逻辑 `align` |

## 核心类型与接口

### TimeWindow

```go
type TimeWindow struct {
    mu         sync.Mutex
    buckets    []int64 // 环形桶数组
    timestamps []int64 // 每个桶对应的时间戳（unix nano）
    head       int     // 环形缓冲区的写入位置
    bucketSize int64   // 每个桶的时间跨度（纳秒）
    windowSize int64   // 总窗口大小（纳秒）
}
```

| 字段 | 含义 |
| --- | --- |
| `mu` | 保护全部字段的互斥锁 |
| `buckets` | 环形桶计数数组，长度 `bucketCount` |
| `timestamps` | 每个桶最近一次写入的 unix 纳秒时间戳，`0` 表示该桶从未写入 |
| `head` | 当前写入位置（桶下标） |
| `bucketSize` | 单桶跨度（纳秒） |
| `windowSize` | 窗口总时长（纳秒），构造后只读 |

**并发安全性**：**是**。`Incr` / `Sum` / `Reset` / `BucketCount` 全程持锁；`WindowDuration` 读取只读字段。

## 对外 API

### NewTimeWindow

```go
func NewTimeWindow(bucketCount int, bucketDuration time.Duration) *TimeWindow
```

创建时间窗口计数器。`bucketCount` 为分桶数量，`bucketDuration` 为单桶跨度，窗口总时长为二者之积——分桶越多精度越高，内存占用也越大。

```go
// 6 个桶，每桶 10 秒 → 总窗口 60 秒
tw := timewindow.NewTimeWindow(6, 10*time.Second)
tw.Incr(1)
qps := tw.Sum() // 最近 60 秒的总请求数

// 30 个桶，每桶 10 秒 → 总窗口 5 分钟
tw := timewindow.NewTimeWindow(30, 10*time.Second)
```

### Incr

```go
func (tw *TimeWindow) Incr(n int64)
```

在当前时间桶上增加 `n`（可传负数做减法）。调用时先对齐桶再累加。

### Sum

```go
func (tw *TimeWindow) Sum() int64
```

返回窗口内所有有效桶的累计值。调用时自动淘汰过期桶。

### Reset

```go
func (tw *TimeWindow) Reset()
```

清空所有桶计数与时间戳，并把写入位置归零。

### BucketCount / WindowDuration

```go
func (tw *TimeWindow) BucketCount() int
func (tw *TimeWindow) WindowDuration() time.Duration
```

分别返回分桶数量与窗口总时长。

## 实现原理

使用环形缓冲区（circular buffer）避免频繁的内存分配：

1. 将时间窗口等分为 `bucketCount` 个桶，桶跨度 `bucketSize = bucketDuration.Nanoseconds()`，窗口总长 `windowSize = bucketSize × bucketCount`。
2. 每次 `Incr` / `Sum` 时调用 `align(nowNano)`：
   - 目标桶下标 `bucketIdx = (nowNano / bucketSize) % bucketCount`。
   - 若当前桶时间戳距现在不超过一个桶跨度且下标未变，判定仍在同一桶，直接返回。
   - 若当前桶从未写入（`timestamps[head] == 0`），直接把 `head` 移到目标桶并记录时间戳。
   - 否则**遍历全部桶**（而非仅遍历一圈内的步数），凡时间戳距现在超过 `windowSize` 的一律清零——进程暂停后跨越多个环形周期时，仅按步数遍历会漏掉更早的过期桶，导致 `Sum` 虚高。
   - 最后 `head = bucketIdx` 并写入当前时间戳。
3. `Sum` 遍历所有桶累计。

**桶淘汰的时间基准**：淘汰判据是桶自己的时间戳，而非桶下标差值，因此进程长时间暂停、或写入跨越多个环形周期后恢复，仍能一次性清理全部过期数据。

## 依赖关系

纯标准库（`sync`、`time`），零外部/内部依赖。

## 相关包

- `shared/semaphore`：基于计数的并发准入控制，与本地窗口限流互补。
- `shared/timeutil`：引擎统一时间中枢，本包只使用 `time.Now()` 的纳秒读数。
