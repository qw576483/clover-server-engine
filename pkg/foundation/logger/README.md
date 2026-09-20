# logger 模块

## 模块职责

`logger` 是引擎的日志基础设施，基于 [zap](https://github.com/uber-go/zap) 封装出一套全局单例日志系统。它负责根据配置构建 encoder（JSON / Console）、writer（按天滚动文件 + 控制台）与 core，并注入 `service` / `env` 等固定字段。

模块同时提供结构化日志（`LogInfo(msg, fields...)`）与格式化日志（`LogInfof(template, args...)`）两套 API，前者走 `*zap.Logger`，后者走 `*zap.SugaredLogger`。此外还有一组基于 `context.Context` 的链路日志（`CtxInfo` 等），可将 trace 字段绑定进 ctx 后自动携带。

全局实例通过 `atomic.Pointer` 存储，保证 `InitZap` / `CloseFile` 与高频日志调用之间既无 data race 也不会 nil panic——未初始化或已关闭时一律降级为 nop logger 静默丢弃。

`facade.go` 中的 `Info` / `Infof` 等是 `LogInfo` / `LogInfof` 的**同包薄转发别名**（二者同属 `package logger`），仅为调用方便而保留。

调用方注意：`InitZap` 中固定使用 `zap.AddCallerSkip(2)`，caller 行号是按「业务 → `facade.go` 的 `Info`/`Infof` → `zap.go` 的 `LogInfo`/`LogInfof` → zap」这条链路精确调校的；直接调 `LogInfo` 会少一层，caller 会上移一级。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `logger.go` | 23 | 最小类型定义：`InitConfig` 初始化配置结构体、`levelMap` 字符串到 `zapcore.Level` 的映射表 |
| `zap.go` | 219 | 核心实现：全局变量（`globalZap` / `globalSugar` / `globalDaily` / `mu` / `globalTraceKey` / nop 兜底实例）、`parseLevel`、`prefixEncoder`（console 模式把 service 提升为行首 `【service】` 前缀）、`InitZap`、`GetZap`、`SyncZap`、`CloseFile`、`GetTraceKey`、`safeLogger` / `safeSugar`，以及 12 个 `LogXxx` / `LogXxxf` 底层便利方法 |
| `facade.go` | 122 | 对外调用面：`Debug`/`Info`/…/`Fatalf` 共 12 个同包别名、`Config` 结构体（带 yaml tag）、`DefaultConfig`、`Init`、`Get`、`Sync`、`DefaultTraceKey`、`WithTrace`、`Field`，以及 context 链路日志（`ctxLoggerKey`、`CtxWithFields`、`FieldsFromCtx`、`mergeCtxFields`、`CtxDebug`/`CtxInfo`/`CtxWarn`/`CtxError`） |
| `daily.go` | 91 | 按天滚动写入器 `dailyWriter`：路径 `{baseDir}/{YYYY-MM-DD}-{name}.log`，跨天自动切文件并关闭旧句柄，提供 `Sync` / `Close`，已接入 `InitZap` 的 dir 模式 |
| `ctx.go` | 95 | context 感知日志：`LogXxxCtx` 系列（`LogDebugCtx` / `LogInfoCtx` / `LogWarnCtx` / `LogErrorCtx` / `LogPanicCtx` / `LogFatalCtx`），首参收 `context.Context`，自动把 ctx 上的 `trace_id` / `span_id` 提取为 zap 字段注入日志；另有 `TraceIDFrom` / `WithTraceID` / `EnsureTraceID`。 |
| `level.go` | 269 | 运行时日志级别热切换：包级 `AtomicLevel`（`globalLevel` / `levelHolder`）、`ErrInvalidLevel`、`SetLevel` / `GetLevel`、HTTP 级别处理器等。 |
| `sampler.go` | 113 | 日志采样器：`SamplerConfig`（`Initial` / `Thereafter` / `Tick`）、`NewSampler`（`Sampler.Allow` 判定是否输出）、`NewLevelSampler`（按级别采样），用于高频路径降低日志 IO。 |
| `once.go` | 101 | 「同一 key 只打一条日志」的去重闸门：`Oncef`/`OnceDebugf`/`OnceInfof`/`OnceWarnf`/`OnceErrorf` 与 `OnceSeen`/`OnceReset`/`OnceResetAll`/`OnceLen`；key 为空时不做去重。 |

## 规则与约束

1. **日志调用必须走 `Info` / `Infof` 这一层**：`InitZap` 固定 `zap.AddCallerSkip(2)`，按「业务 → `facade.go` 的 `Info`/`Infof` → `zap.go` 的 `LogXxx`」调校。直接调 `LogXxx` 会少一层，caller 行号上移一级。
2. **`Format` 只有两种取值**：等于 `"json"` 走 JSON encoder，其余任何值（含 `"console"`、空串、拼错的 `"JSON"`）一律走 Console encoder，且不报错。
3. **`InitZap` 不做路径校验**：恒返回 `nil`，`Dir` 指向不可写目录也不会在初始化时失败，只在首次写日志时才暴露。路径有效性由部署方保证。
4. **`CloseFile` 只用于两种场景**：测试清理，或"随后立即 `InitZap`"的运行时重载。生产环境进程退出前用 `Sync()`。`CloseFile` 后 `LogXxx` 全部降级为 nop、静默丢弃。
5. **重复 `InitZap` 依赖其内部的旧文件关闭逻辑**，不要绕过；否则 Windows 上旧日志文件句柄泄漏、文件被永久占用无法删除。
6. **`mergeCtxFields` 必须新建切片**，禁止简化为 `append(FieldsFromCtx(ctx), fields...)`——并发 `CtxXxx` 会写入同一底层数组，导致字段互相覆盖与 data race。
7. **`CtxXxx` 与 `LogXxx` 的 nop 兜底实例不同**：`LogXxx` 走 `safeLogger()` 返回预分配实例；`CtxXxx` 走 `Get()`，未初始化时每次新建 `zap.NewNop()`。高频路径优先用 `LogXxx`。
8. **`globalTraceKey` 只读不可写**：无 setter，恒为 `"trace_id"`；`WithTrace` 永远产出 `trace_id` 字段。
9. **保证至少一个输出目标**：`Stdout:false` 且 `Dir:""` 时仍会强制追加 `os.Stdout`，日志不会被完全丢弃。
10. **`LogFatal` / `LogFatalf` 的 `os.Exit(1)` 与初始化无关**：实现里在 zap 调用之后**无条件**执行 `os.Exit(1)`（见 `zap.go`），因此**即使未 `Init`**（此时日志本身静默丢弃）进程仍会退出。反之 `LogPanic` / `LogPanicf` 走 nop core，未初始化时**不会**触发 panic。依赖"Fatal 会退出"的代码可用；依赖"Fatal 会打印"的代码必须先 `Init`。

## 核心类型与接口

### InitConfig（`logger.go`）

底层初始化配置（`InitZap` 的入参）。与 `facade.go` 的 `Config` 字段一致，由 `Init` 逐字段拷贝构造。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `Level` | `string` | 日志级别：`debug`/`info`/`warn`/`error`/`panic`/`fatal` |
| `Format` | `string` | 输出格式：`json` / `console` |
| `Dir` | `string` | 日志根目录，非空则按 `{dir}/{YYYY-MM-DD}-{service}.log` 按天落盘 |
| `Stdout` | `bool` | 是否同时输出到控制台 |
| `Service` | `string` | 服务名称，注入为固定字段 `service`，同时作为按天日志文件名后缀 |
| `Env` | `string` | 运行环境：`dev`/`test`/`prod`，注入为固定字段 `env` |

### Config（`facade.go`）

日志配置（带 `yaml` / `mapstructure` tag，供 viper 解析）。字段构成与 `InitConfig` 完全一致（`Level`/`Format`/`Dir`/`Stdout`/`Service`/`Env`），`Init` 负责把 `*Config` 逐字段拷贝为 `InitConfig` 后调用 `InitZap`。

### 全局状态（`zap.go`）

| 变量 | 类型 | 含义与并发保护 |
| --- | --- | --- |
| `globalZap` | `atomic.Pointer[zap.Logger]` | 全局结构化 logger，**原子读写**，无锁安全 |
| `globalSugar` | `atomic.Pointer[zap.SugaredLogger]` | 全局 sugared logger，**原子读写** |
| `globalDaily` | `*dailyWriter` | 按天滚动文件写入器，供 `CloseFile` 释放句柄；**由 `mu` 保护** |
| `mu` | `sync.RWMutex` | 保护 `globalDaily` 与 `globalTraceKey` |
| `globalTraceKey` | `string` | 链路追踪字段名，固定初值 `"trace_id"`；**由 `mu` 保护** |
| `nopLogger` / `nopSugar` | `*zap.Logger` / `*zap.SugaredLogger` | 预分配的 nop 实例，未初始化时兜底，避免 nil panic |

**并发安全性总结**：日志读路径（`safeLogger` / `safeSugar` / `GetZap`）走 `atomic.Pointer.Load()`，完全无锁；初始化与关闭路径（`InitZap` / `CloseFile`）持写锁 `mu` 保护 `globalDaily`，再用 `Store` 原子替换 logger 指针。因此高频日志调用与运行时重载之间**无 data race**。

### Level 与级别常量（`facade.go`）

```go
type Level = zapcore.Level

const (
    DebugLevel = zapcore.DebugLevel
    InfoLevel  = zapcore.InfoLevel
    WarnLevel  = zapcore.WarnLevel
    ErrorLevel = zapcore.ErrorLevel
    PanicLevel = zapcore.PanicLevel
    FatalLevel = zapcore.FatalLevel
)
```

### ctxLoggerKey

`type ctxLoggerKey struct{}`，私有空结构体，作为 context value 的 key，避免与其他包的 key 冲突。ctx 中存放的是 `[]zap.Field`。

## 关键流程

### 1. 初始化链路：Init → InitZap

`Init(cfg *Config) error` 是入口。若 `cfg == nil` 则回退到 `DefaultConfig()`（`Level: "info"`, `Format: "json"`, `Stdout: true`），随后逐字段拷贝构造 `InitConfig` 并调用 `InitZap`。

`InitZap(cfg InitConfig) error` 的完整步骤（全程持写锁 `mu.Lock()`）：

1. **解析级别**：`parseLevel(cfg.Level)` 查 `levelMap`，未命中的未知级别**默认降级为 `zapcore.InfoLevel`**（不报错）。
2. **构建 encoder config**：以 `zap.NewProductionEncoderConfig()` 为基底，覆写 `TimeKey = "time"`、`EncodeTime = ISO8601TimeEncoder`、`EncodeLevel = CapitalLevelEncoder`、`EncodeCaller = ShortCallerEncoder`。
3. **选择 encoder**：`cfg.Format == "json"` 时用 `zapcore.NewJSONEncoder`，**其余任何值**（含 `"console"` 与空串）一律走 `zapcore.NewConsoleEncoder`。
4. **关闭旧文件写入器**：若 `globalDaily != nil` 先 `Close()` 并置 nil。这是为运行时重载准备的——否则旧句柄会泄漏，Windows 上旧日志文件将被永久占用无法删除。
5. **组装 writers**：
   - `cfg.Dir != ""` 时 `newDailyWriter(...)` 创建按天滚动写入器并存入 `globalDaily`，加入 writers；
   - `cfg.Stdout == true` **或** writers 为空（即没配日志目录）时追加 `os.Stdout`——保证至少有一个输出目标；
   - `zapcore.NewMultiWriteSyncer(writers...)` 合并。
6. **构建 core**：`zapcore.NewCore(encoder, ws, level)`。
7. **注入固定字段**：`cfg.Service != ""` 时加 `zap.String("service", ...)`，`cfg.Env != ""` 时加 `zap.String("env", ...)`。
8. **构建并原子存储 logger**：`zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2), zap.Fields(fields...))` → `globalZap.Store(...)`，再 `globalSugar.Store(globalZap.Load().Sugar())`。

恒返回 `nil`（当前实现无失败分支）。

### 2. caller skip 链路（AddCallerSkip(2) 的由来）

源码注释明确了这条调用链：

```
业务代码
  → logger.Infof   (facade.go，薄转发)
    → LogInfof     (zap.go)
      → safeSugar().Infof
        → zap 内部
```

- `skip=1` 只能跳到 `facade.go` 的 `Infof`，caller 行号恒为门面自身行号，无意义；
- `skip=2` 才能跨过 `zap.go` 的 `LogInfof` 与 `facade.go` 的 `Infof` 两层，落到真正的业务调用方。

因此 **`AddCallerSkip(2)` 与「业务 → `Info`/`Infof` → `LogXxx`」这条链路强绑定**。若直接调用 `logger.LogInfo(...)`（少一层），caller 会多跳一级、指向业务的上一级调用者，行号不准。

### 3. 日志写入链路：LogXxx / LogXxxf

- **结构化**：`LogDebug` / `LogInfo` / `LogWarn` / `LogError` / `LogPanic` / `LogFatal` → `safeLogger().Xxx(msg, fields...)`；
- **格式化**：`LogDebugf` / `LogInfof` / `LogWarnf` / `LogErrorf` / `LogPanicf` / `LogFatalf` → `safeSugar().Xxxf(template, args...)`。

`safeLogger()` / `safeSugar()` 从 `atomic.Pointer` 读取，为 nil 时返回预分配的 `nopLogger` / `nopSugar`，因此 `InitZap` 之前调用任何日志函数都是安全的（静默丢弃，不 panic）。

### 4. context 链路日志：CtxWithFields → CtxInfo

1. 业务通过 `CtxWithFields(ctx, zap.String("trace_id", id), ...)` 把字段挂到 context（key 为私有的 `ctxLoggerKey{}`）；
2. 调用 `CtxInfo(ctx, msg, extraFields...)` 时，内部走 `mergeCtxFields(ctx, fields)`；
3. `mergeCtxFields` 先 `FieldsFromCtx(ctx)` 取出基底字段，然后**新建切片** `make([]zap.Field, 0, len(base)+len(fields))` 再依次 append。
   - **为什么必须新建切片**：若直接写 `append(FieldsFromCtx(ctx), fields...)`，当 ctx 里存的切片尚有富余容量（业务用 `slice...` 展开传入时很常见），并发的两次 `CtxXxx` 会向**同一底层数组**写入，导致字段互相覆盖与 data race。
4. 最终经 `Get()`（即 `GetZap()`）输出。

注意：`CtxXxx` 系列走的是 `Get()` 而非 `safeLogger()`，未初始化时 `GetZap()` 返回**新构造的** `zap.NewNop()`（而非预分配的 `nopLogger`），行为一致但每次都会新建一个 nop 对象。

### 5. 关闭链路：CloseFile

1. 先 `globalZap.Load().Sync()` 刷盘（若非 nil）；
2. 持写锁关闭 `globalDaily` 并置 nil，同时把 `globalTraceKey` 复位为 `"trace_id"`；
3. 解锁后 `globalZap.Store(nil)` / `globalSugar.Store(nil)`。

关闭后所有 `LogXxx` / `LogXxxf` 经 `safeLogger()` / `safeSugar()` 降级为 nop——**这是预期行为而非故障**。源码注释明确该函数只应在两种场景使用：**测试清理**，或**运行时重载（随后立即 `InitZap`）**。

### 6. 刷盘：SyncZap

`SyncZap()` 对非 nil 的 `globalZap` 调用 `Sync()` 刷新缓冲区，应在程序退出前调用（`defer logger.Sync()`）。

## 配置项 / 默认值

### Config / InitConfig 字段

| 名称 | 类型 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `Level` | `string` | `"info"`（`DefaultConfig`）；未知值经 `parseLevel` 亦回退 `info` | 日志级别，可选 `debug`/`info`/`warn`/`error`/`panic`/`fatal` |
| `Format` | `string` | `"json"`（`DefaultConfig`）；非 `"json"` 一律按 console 处理 | 输出格式，`json` 用于生产结构化日志，`console` 用于开发可读格式 |
| `Dir` | `string` | `""` | 日志根目录；为空则不创建文件写入器，仅输出控制台 |
| `Stdout` | `bool` | `true`（`DefaultConfig`） | 是否同步输出到控制台；即使为 `false`，只要没配 `Dir` 也会强制加上 stdout |
| `Service` | `string` | `""` | 服务名称（如 `clover-gateway`）；非空时注入固定字段 `service`，同时作为按天日志文件名后缀 |
| `Env` | `string` | `""` | 运行环境 `dev`/`test`/`prod`；非空时注入固定字段 `env` |

**注意**：`DefaultConfig()` 只显式设置了 `Level` / `Format` / `Stdout` 三项，其余字段均为 Go 零值。

### 硬编码常量与固定值

| 名称 | 类型 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `DefaultTraceKey` | `const string` | `"trace_id"` | 默认链路追踪字段名 |
| `globalTraceKey` | `var string` | `"trace_id"` | 运行时链路追踪字段名；`CloseFile` 会复位为此值 |
| encoder `TimeKey` | `string` | `"time"` | 时间字段名 |
| 时间编码 | — | `zapcore.ISO8601TimeEncoder` | ISO8601 时间格式 |
| 级别编码 | — | `zapcore.CapitalLevelEncoder` | 大写级别名（如 `INFO`） |
| caller 编码 | — | `zapcore.ShortCallerEncoder` | 短路径 caller |
| caller skip | `int` | `2` | 固定跳过 internal 实现 + pkg 门面两层 |

## 对外 API

### 初始化与生命周期

```go
func Init(cfg *Config) error          // nil 时使用 DefaultConfig()
func InitZap(cfg InitConfig) error    // 底层初始化
func DefaultConfig() *Config
func Get() *zap.Logger                // 等价 GetZap()
func GetZap() *zap.Logger             // 未初始化返回 zap.NewNop()
func Sync()                           // 等价 SyncZap()
func SyncZap()
func CloseFile()                      // 关闭文件句柄，logger 降级为 nop
func GetTraceKey() string
```

```go
func main() {
    err := logger.Init(&logger.Config{
        Level:   "debug",
        Format:  "console",
        Dir:     "/var/log/clover",
        Stdout:  true,
        Service: "clover-gateway",
        Env:     "dev",
    })
    if err != nil {
        panic(err)
    }
    defer logger.Sync()

    logger.Info("server started", zap.Int("port", 8080))
}
```

### 结构化日志

```go
func LogDebug(msg string, fields ...zap.Field)
func LogInfo(msg string, fields ...zap.Field)
func LogWarn(msg string, fields ...zap.Field)
func LogError(msg string, fields ...zap.Field)
func LogPanic(msg string, fields ...zap.Field)
func LogFatal(msg string, fields ...zap.Field)

// facade.go 中的同名别名（internal 层推荐使用）
func Debug(msg string, fields ...zap.Field)
func Info(msg string, fields ...zap.Field)
func Warn(msg string, fields ...zap.Field)
func Error(msg string, fields ...zap.Field)
func Panic(msg string, fields ...zap.Field)
func Fatal(msg string, fields ...zap.Field)
```

```go
ilog.LogError("etcd load config failed",
    zap.String("key", key),
    zap.Error(err),
)
```

### 格式化日志

```go
func LogDebugf(template string, args ...any)
func LogInfof(template string, args ...any)
func LogWarnf(template string, args ...any)
func LogErrorf(template string, args ...any)
func LogPanicf(template string, args ...any)
func LogFatalf(template string, args ...any)

// facade.go 别名
func Debugf(template string, args ...any)
func Infof(template string, args ...any)
func Warnf(template string, args ...any)
func Errorf(template string, args ...any)
func Panicf(template string, args ...any)
func Fatalf(template string, args ...any)
```

```go
logger.Infof("player %d entered space %d", playerID, spaceID)
```

### 字段构造

```go
func WithTrace(id string) zap.Field       // zap.String(GetTraceKey(), id)
func Field(key string, val any) zap.Field // zap.Any(key, val)
```

```go
logger.Info("handle request",
    logger.WithTrace(traceID),
    logger.Field("payload", req),
)
```

### context 链路日志

```go
func CtxWithFields(ctx context.Context, fields ...zap.Field) context.Context
func FieldsFromCtx(ctx context.Context) []zap.Field
func CtxDebug(ctx context.Context, msg string, fields ...zap.Field)
func CtxInfo(ctx context.Context, msg string, fields ...zap.Field)
func CtxWarn(ctx context.Context, msg string, fields ...zap.Field)
func CtxError(ctx context.Context, msg string, fields ...zap.Field)
```

```go
ctx = logger.CtxWithFields(ctx,
    logger.WithTrace(traceID),
    zap.Int64("player_id", pid),
)

// 后续任意位置，自动携带 trace_id 与 player_id
logger.CtxInfo(ctx, "load player data")
logger.CtxError(ctx, "load failed", zap.Error(err))
```

## 依赖关系

**依赖的外部库**

- `go.uber.org/zap` / `go.uber.org/zap/zapcore`：日志核心
- 标准库：`context`、`os`、`sync`、`sync/atomic`、`time`、`path/filepath`

**依赖的内部包**

- `pkg/foundation/trace`：`ctx.go` 的 `LogXxxCtx` 系列从中提取 `trace_id` / `span_id`（同层依赖，无循环）。
  它是追踪上下文的**唯一真身 key**；`pkg/shared/traceid` 的 Span 也镜像写入该 key，
  故请求链路的 trace 在第一个分支即可命中。`ctx.go` 里对 `traceid.FromContext` 的兜底分支
  只作失效保险（正常路径不再执行），**不要**据此认为两个包仍各有一套 context key。

本模块可被所有层安全引用而不产生循环依赖。

**被谁依赖**

几乎全引擎。已知调用方包括 `internal/foundation/config`（`ilog.LogInfo` / `LogError`）、`internal/transport/net/*`、`internal/domain/data/*`、`internal/transport/*`、`internal/app/*`、`internal/transport/gateway/*`、`internal/domain/mmo/*`、`internal/runtime/*` 等，以及对外门面 `pkg/foundation/logger`。

