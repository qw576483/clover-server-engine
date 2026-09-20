# util —— 通用工具包总览

`pkg/shared/util` 是 clover-server-engine 的**零业务依赖工具库**，为引擎各层提供可复用的通用原语。

> 路径说明（2026-09 结构反转后）：本包只有 `util.go`（哈希 / 切片函数），**没有子目录**——
> 按 `结构规则.md` §3.6 的平铺规则（`util/` 是普通一级子包，内部不再嵌套），各能力
> 平铺在 `pkg/shared/<名字>/`；配置、协议、重试、TLS 这几个带引擎语义的落在
> `internal/shared/<名字>/`。下方「子包索引」表列的是 **shared 层**的整体清单，不是本目录的子目录。



它的定位是：

- **叶子节点**：绝大多数子包只依赖 Go 标准库，不依赖任何引擎内部业务类型，可被任意上层模块安全引用，不会形成循环依赖。
- **统一收敛点**：把散落各处的压缩、缓存、排行榜、几何运算等共性能力收敛到一处，便于整体替换实现和统一安全审计。
- **并发语义各异**：每个子包在文档中显式标注「是否并发安全」，调用方须按需选择（见下方索引与注意事项）。

本目录根包 `util.go` 提供哈希和数学工具函数，其余实现落在各子包内（详见各子包 `README.md`）。

## 根包 API（util.go）

| 函数 | 一句话职责 | 并发安全 |
| --- | --- | --- |
| `NextPow2(n int) int` | 返回 ≥ n 的最小 2 的幂 | 是（纯函数） |
| `Contains[T comparable](s []T, v T) bool` | 切片是否包含元素（泛型版） | 是（纯函数） |
| `Equal[T comparable](a, b []T) bool` | 两切片是否等长且逐元素相等（顺序敏感） | 是（纯函数） |
| `Fnv32(s string) uint32` | FNV-1a 32 位哈希，用于分片路由 | 是（纯函数） |
| `Fnv32Key(fields ...string) uint32` | 多字段 FNV-1a 哈希，字段间 `\x00` 分隔 | 是（纯函数） |
| `Xxhash64(s string) uint64` | xxHash-64 哈希，高频分片路由 | 是（纯函数） |
| `Xxhash64Key(fields ...string) uint64` | 多字段 xxHash-64 哈希 | 是（纯函数） |

## 子包索引

| 子包 | 一句话职责 | 主要导出 API | 是否并发安全 |
| --- | --- | --- | --- |
| `bitset` | 动态位集合（置位/复位/并交差/大端序列化） | `BitSet`、`New`、`Bytes`/`FromBytes` | 否（需外部加锁） |
| `bloom` | 布隆过滤器（存在性预判，绝无假阴性） | `Filter`、`NewBloomFilter` | 否（需外部加锁） |
| `cache` | 泛型分片缓存 + singleflight 防击穿 + TTL + 淘汰策略 | `Cache[K,V]`、`New`、`Get`/`Set`/`GetOrLoad`、`WithClock`/`WithShards` | 是 |
| `compress` | 基于标准库 flate 的数据压缩封装 | `Compress`/`CompressLevel`/`Decompress` | 是（无包级状态） |
| `config`（`internal/shared/config`） | 带默认值回退的配置读取辅助 | `DefDuration`/`DefInt`/`DefString` | 是（纯函数） |
| `conv` | 类型与数据转换（字符串↔数值、结构体拷贝、map 合并） | `ToInt`/`ToBool`/`ToString`、`FormatIntBase`、`StructCopy`/`StructCopyStrict`、`MapMerge` | 是（纯函数） |
| `geom` | 几何原语（Vec3/四元数/三角函数表） | `Vec3`、`Quaternion`、`TrigTable`、`NewTrigTable` | 是（值类型 / 无共享状态） |
| `graph` | 图搜索内核（全引擎唯一一份 A*） | `Graph[N]`、`Edge[N]`、`SearchAStar[N]` | 是（纯算法，无共享状态） |
| `hyperloglog` | HyperLogLog 基数估算 | `HLL`、`NewHLL` | 是 |
| `id` | 全局唯一 ID 生成（UID / RequestID / TraceID） | `GenUID`/`GenRequestID`/`GenTraceID` | 是 |
| `json` | JSON 编解码薄封装 | `Marshal`/`Unmarshal` | 是（纯函数） |
| `jwt` | JWT（HS256）签发与校验 | `Claims`、`Sign`、`Verify` | 是（纯函数） |
| `memrank` | 通用有序榜（内存排行榜 + 段位门槛） | `SortedSet`、`Manager`、`New`/`NewManager`（`Threshold`/`Thresholds` 真身在 `pkg/domain/master`） | 是（读写锁） |
| `proto` | 对外协议门面（消息号真身） | `EMsg*` / `EPush*` 常量 | 是（常量表） |
| `rand` | 加权随机抽样 + 通用随机源 | `WeightedPicker`、`Source`、`NewWeightedPicker`/`NewSource` | WeightedPicker 是（内部加锁） |
| `ringbuf` | 环形缓冲（SPSC 无锁 / MPMC 加锁） | `Ring[T]`、`MpmcRing[T]`、`New`/`NewChecked`/`NewMpmc`/`NewMpmcChecked` | SPSC 按约定安全；MPMC 是 |
| `safe` | goroutine panic 兜底 | `SafeRun`/`GoSafe` | 是 |
| `semaphore` | 带超时的计数信号量 | `Semaphore`、`NewSemaphore` | 是 |
| `timeutil` | 时间工具（时区、取整点、格式化） | `NowMS`/`NowTime`、`Init`、`ParseTimezone`、`Layout` | 是（纯函数） |
| `timewindow` | 滑动时间窗口计数器 | `TimeWindow`、`NewTimeWindow` | 是 |
| `traceid` | TraceID / SpanID 生成与传递 | `Span`、`StartSpan`、`NewTraceID` | 是（无共享状态） |
| `validate` | 基于 struct tag 的字段校验 | `Struct`、`Var` | 是（纯函数） |

## 选型指引

- **需要缓存 / 防击穿** → `cache`（支持注入时钟便于单测）。
- **需要排行榜 / TopN / 段位门槛** → `memrank`（内存实现默认线程安全；需 Redis 后端则实现 `SortedSet` 注入 `Manager`）。
- **需要高性能压缩** → `compress`（默认级别）。
- **需要几何运算** → `geom`（Vec3 / 四元数 / 三角函数表）。
- **需要基数估算** → `hyperloglog`（误差 < 2%）。
- **需要唯一 ID** → `id`（注意受墙钟回拨影响）。
- **需要后台协程 panic 不拖垮进程** → `safe.GoSafe` / `safe.SafeRun`。
- **需要并发限流** → `semaphore`（带超时）。
- **需要滑动窗口计数** → `timewindow`。
- **需要配置默认值回退** → `config.DefXxx` 系列函数。

## 注意事项

1. **并发安全属性各不相同**：`cache`、`memrank`、`semaphore`、`timewindow` 内部已加锁；`geom` 为值类型天然安全。使用前务必对照上表与子包 README。
2. **不要过度依赖 util 做性能关键路径**：`compress` 有压缩/解压开销、`id` 有系统调用开销——超高频场景应做局部缓存或换更快实现。
3. **与标准库同名函数语义可能不同**：`NextPow2` 对负数返回 1 不报错。
4. **零依赖带来的边界**：util 子包刻意不引入 logger 等内部依赖（如 `safe` 默认写 `os.Stderr`），接入日志系统须在启动期调用对应 `SetXxx` 注入。
5. **`id` 受墙钟回拨影响**：生成的 ID 不保证单调递增，切勿用作强排序键或唯一性判定。
