# metrics

> Prometheus 指标导出（HTTP 端点 + 各模块埋点）

自研轻量级指标库：Counter / Gauge / Histogram + Prometheus 文本格式导出。

## 核心类型

- `Registry` — 指标注册中心
- `Counter` — 累计计数器
- `Gauge` — 瞬时值
- `Histogram` — 分桶统计

## 典型用法

```go
// 包级快捷函数（默认注册表），labels 为 key,value 交替的可选标签
reqCounter := metrics.CounterOf("rpc_requests_total", "method")
reqCounter.Inc()

g := metrics.GaugeOf("connections_active")
g.Set(42)
g.Dec()

h := metrics.HistogramOf("rpc_duration_seconds", metrics.DurationBuckets, "method")
h.Observe(12.5)

// 模块一键埋点
mm := metrics.ForModule("rpc")
mm.Count("requests_total", metrics.LabelStatus, metrics.StatusOK)
defer mm.Timer("request_duration_seconds", metrics.LabelMethod, "GetPlayer")()
```

## 运行时 / 进程指标（自动采集，无需业务埋点）

`metrics.Handler()`（挂在 admin 的 `/metrics`）每次抓取都会先跑 `CollectRuntimeTo`，
刷新下面两组指标，保证是**抓取时刻**的真实值：

| 指标 | 来源 | 说明 |
|---|---|---|
| `go_*` | `runtime.MemStats`（`runtime.go`） | goroutine / OS 线程 / GC / 堆内存等，命名遵循 Prometheus 官方 `go_*` 约定，现成 Grafana Go Runtime 看板可直接复用 |
| `process_start_time_seconds` / `process_uptime_seconds` | 包内启动时间 | 进程启动时间与已运行秒数 |
| `process_cpu_seconds_total` | `getrusage(2)` / `GetProcessTimes`（`proc_*.go`） | 进程累计消耗 CPU 秒数（用户态 + 内核态） |
| `process_cpu_ratio` | 上面那一项的两次采样差 | 相对上一次抓取的使用率，**1.0 = 跑满一个核**（4 核满载约 4.0；换算百分比要除以 `GOMAXPROCS`） |

行为约定：

- **平台不支持就不导出**（`proc_other.go`）：宁可不导出，也不给一条恒为 0 的假曲线。
- `process_cpu_ratio` **首次抓取不导出**（累计量必须两次采样才能求速率），第二次起才有值。
- 采集成本是「一次系统调用」，远小于同路径上的 `runtime.ReadMemStats`（后者是 **STW**，
  耗时随堆大小增长，见 `runtime.go` 的性能注意）；因此**绝不要把 `CollectRuntimeTo` 放进游戏主循环**。
- 需要自己算进程 CPU 时用 `metrics.ProcessCPUSeconds()`（只读、无锁、无分配），
  例如 `internal/app/watchdog.go` 的「进程 CPU 饱和」巡检规则。

## 埋点规范（新增埋点前必读 `naming.go`）

- 命名：`clover_<模块>_<指标>_<单位>`，模块名取 `Module*` 常量（新增模块先登记，禁止自造）。
- 单位用基础单位：`_seconds` / `_bytes` / `_ratio`；Counter 必须以 `_total` 结尾。
- **严禁高基数 label**：`player_id` / `conn_id` / `trace_id` / 错误原文一律不得作为 label
  （十万在线玩家 = 十万条时间序列 = 抓取端 OOM + 注册表无界增长）；
  label 值必须来自有限枚举，单个指标的 label 组合数 ≤ 1000。
- 优先复用 `Label*` 常量（含 `LabelRule` / `LabelLevel`），不要自造同义词。

## 谁在用（排查指标含义时的入口）

| 模块 | 埋点位置 | 典型指标 |
|---|---|---|
| 网关 | `internal/transport/gateway/gwcore/metrics.go` | `clover_gateway_*` |
| 事件总线 | `internal/transport/event/metrics.go` | `clover_event_*`（**`crossnode_dead_letter_total` 非零 = 有事件永久丢失**） |
| master | `internal/domain/master/metrics/mmetrics.go` | `clover_master_*` |
| 看门狗 | `pkg/runtime/watchdog/metrics.go` | `clover_watchdog_*` |
