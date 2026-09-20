# timeutil 模块

## 模块职责

`pkg/shared/timeutil` 是引擎的**统一时间中枢**，负责两件事：一是统一时间戳口径（引擎内的消息耗时统计、日志埋点、性能监控、协议时间字段都从这里取时间，避免各处各自写 `time.Now().UnixMilli()` 或 `time.Now().UnixNano()/1e6` 造成毫秒/微秒/纳秒单位不一致）；二是统一时区口径（引擎启动时 `Init` 一次时区，之后所有「按自然日」「按本地时间」的判断都以它为基准，避免各服本地时区不同导致每日重置、活动开闭时间不一致）。

能力集包括：全局时区管理（`Init`/`Location`/`ParseTimezone`）、UTC epoch 时间戳（`NowMS`/`NowSec`）、带时区的时间构造与格式化（`NowTime`/`TodayZero`/`TodayElapsed`/`NowStr`/`Date`/`ParseDate`）、跨零点判定（`IsNewDay`）。

## 规则与约束

1. **耗时测量必须使用 `time.Since`**：`NowMS()` / `NowSec()` 读取墙钟，会受 NTP 校时、手动改时间、虚拟机时钟同步影响，两次取值相减可能出现负值或虚高，仅可用于容忍偶发异常值的粗粒度埋点。
2. **`NowMS()` 的精度为毫秒且向下取整**：亚毫秒级操作测得 `0`，需要更细粒度时用 `time.Since` 配合 `Nanoseconds()` / `Microseconds()`。
3. **极热路径不得逐次调用取时函数**：`time.Now()` 即使经 vDSO 优化仍有约 20~50ns 开销，每帧每个对象调用一次的场景须在帧/批次开始时取一次并缓存复用。
4. **`NowMS()` 的返回值不保证单调递增**：禁止把时间戳用作排序键、唯一性判断或过期判定的唯一依据；`shared/id.GenUID` 同样依赖时间戳，受同一约束影响。
5. **本包不提供时钟注入**：需要确定性时间的模块须在自身结构体中保存 `clock func() time.Time` 字段（如 `shared/cache` 的 `WithClock`），不得直接依赖 `timeutil.NowMS()` 编写与时间相关的单测。
6. **跨模块传递时间用 `time.Time`，只在序列化边界转时间戳**：`NowMS()` / `NowSec()` 返回裸 `int64`，无类型安全，易与其它 `int64` 参数（例如误传秒级时间戳）混淆。
7. **秒级与毫秒级时间戳禁止混用**：两者相差 1000 倍，字段命名必须显式标注单位（如 `created_at_ms` / `expire_at_sec`）。
8. **跨零点判定须以 `IsNewDay` 为准**：`TodayZero` / `TodayElapsed` / `NowStr` 均使用引擎统一时区，业务逻辑不得自行用 `time.Now()` 按本地时区计算自然日。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `timeutil.go` | 128 | 全部内容：包文档、全局时区 `atomic.Pointer[time.Location]` 与 `Init`/`Location`/`ParseTimezone`、时间戳 `NowMS`/`NowSec`、带时区时间函数 `NowTime`/`TodayZero`/`TodayElapsed`/`NowStr`/`Date`/`ParseDate`/`IsNewDay` |

## 核心类型与接口

本包**不定义任何导出类型**，只导出一组包级函数与一个包级状态：

| 状态 | 类型 | 含义 |
| --- | --- | --- |
| `loc` | `atomic.Pointer[time.Location]` | 引擎统一时区。`init()` 中默认存 `time.Local`，`Init` 时整体替换，之后只读 |

**并发安全性**：全局时区用 `atomic.Pointer` 实现无锁读写，`Init` 可安全并发调用；所有函数无共享可变状态，**完全并发安全**。

**时区回退规则**：`ParseTimezone("")` 返回系统本地时区；时区名无效时用 `logger.Warnf` 打印警告并回退 `time.UTC`，保证服务能启动而非 panic。

## 算法与实现原理

### `time.Now()` 的双时钟机制

Go 的 `time.Time` 内部同时携带两个时钟读数：

| 时钟 | 来源 | 特性 |
| --- | --- | --- |
| **墙钟（wall clock）** | 系统当前时间 | 会被 NTP 校时、手动改时间、夏令时**跳变** |
| **单调时钟（monotonic clock）** | 系统启动后的单调递增计数 | 只增不减，不受校时影响 |

**关键**：`UnixMilli()` / `Unix()` 读取的是**墙钟**部分，`t2.Sub(t1)` 与 `time.Since(t)` 才会优先使用**单调时钟**。因此 `NowMS()` 的返回值可能回拨，而 `time.Since` 测得的耗时恒定可靠。

### 值域与精度

- **返回类型** `int64`，`NowMS` 单位毫秒、`NowSec` 单位秒，起点为 Unix 纪元（1970-01-01 00:00:00 UTC）。
- **值域**：`int64` 的毫秒可表示约 ±2.9 亿年，**永不溢出**（不存在 2038 问题——那是 32 位秒级时间戳的问题）。
- **精度**：毫秒。底层 `time.Now()` 通常有纳秒级精度，`UnixMilli()` 做的是**向下取整**（截断而非四舍五入）。
- **时区无关**：Unix 时间戳本身是 UTC 绝对时刻，与本地时区设置无关；带时区语义一律走 `NowTime` 系列。

### 与其它写法的等价性

```go
time.Now().UnixMilli()                          // ← 本包用的（Go 1.17+）
time.Now().UnixNano() / int64(time.Millisecond) // 等价但更啰嗦
time.Now().Unix() * 1000                        // ✗ 丢失毫秒精度
```

### 跨零点判定

`IsNewDay(ref)` 比较当前时刻与参考时刻的 `YearDay()` 与 `Year()`，两者任一不同即判定为新的一天。`ref` 通常为上次检查时保存的时间；**只比较 `YearDay` 会在跨年时出现误判**，故必须同时比较 `Year`。

## 对外 API

### 全局时区

```go
func Init(name string)
func Location() *time.Location
func ParseTimezone(name string) *time.Location
```

```go
// 引擎启动时调用一次（如从配置读取 "Asia/Shanghai"）
timeutil.Init(cfg.Timezone)

// 之后所有带时区语义的时间都以引擎时区为准
fmt.Println(timeutil.Location()) // Asia/Shanghai
```

`Init` 未调用时 `Location()` 返回系统本地时区，保证未初始化也能正常工作。

### 时间戳（UTC epoch，时区无关）

```go
func NowMS() int64   // 毫秒级
func NowSec() int64  // 秒级
```

```go
ts := timeutil.NowMS()
fmt.Println(ts) // 如 1785000000000
```

**典型场景 1：粗粒度耗时埋点**

```go
func handleMessage(c *Ctx) {
    start := timeutil.NowMS()
    defer func() {
        cost := timeutil.NowMS() - start
        if cost > 100 {
            logger.Warnf("慢消息 msgID=%d cost=%dms", c.MsgID(), cost)
        }
    }()
    // ... 业务处理
}
```

**典型场景 2：日志时间戳字段**

```go
logger.With(
    "ts", timeutil.NowMS(),
    "trace_id", c.TraceID(),
).Info("请求开始")
```

**典型场景 3：写入数据结构的时间字段**

```go
type Record struct {
    ID        string `json:"id"`
    CreatedAt int64  `json:"created_at_ms"` // 毫秒时间戳，字段名显式标注单位
}

r := &Record{ID: uid, CreatedAt: timeutil.NowMS()}
```

**典型场景 4：与客户端对时 / 协议时间字段**

```go
// 客户端普遍使用毫秒时间戳（JS 的 Date.now()），口径一致
resp.ServerTime = timeutil.NowMS()
```

### 带时区的时间

```go
func NowTime() time.Time                    // 当前时刻（引擎时区）
func TodayZero() time.Time                  // 今天零点（引擎时区）
func TodayElapsed() int                     // 今天已过去的秒数
func NowStr() string                        // "2006-01-02 15:04:05"
func Date(year int, month time.Month, day, hour, min, sec, nsec int) time.Time
func ParseDate(value string) (time.Time, error)
func IsNewDay(ref time.Time) bool
```

```go
// 每日 5 点重置
const resetSec = 5 * 3600
if timeutil.TodayElapsed() >= resetSec && !alreadyReset {
    doDailyReset()
}

// 解析配置中的日期串（"2006-01-02"、"2006-01-02 15:04:05"、"2006-01-02T15:04:05"）
start, err := timeutil.ParseDate(cfg.ActivityStart)

// 跨零点检测：ref 为上次检查的时刻
lastCheck := timeutil.NowTime()
if timeutil.IsNewDay(lastCheck) {
    onNewDay()
}
```

`ParseDate` 依次尝试三种布局，全部失败时返回 `timeutil: cannot parse date %q` 错误。

## 依赖关系

- **依赖**：Go 标准库 `fmt` / `sync/atomic` / `time`，以及引擎内部的 `pkg/foundation/logger`（时区名无效时告警）。
- 零第三方依赖；**依赖引擎内部的 `pkg/foundation/logger`**，因此不是「零引擎内部依赖」的叶子包，但仍不 import `internal`（属自包含包）。

## 相关包

- `shared/timewindow`：时间窗口计数器，与本地时间取值无关。
- `shared/traceid`：`Span.Duration` 用 `time.Since` 走单调时钟，与本地墙钟口径不同。
- `shared/id`：`GenUID` 依赖时间戳，同样受时钟回拨约束。
