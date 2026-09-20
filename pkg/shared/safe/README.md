# safe 模块

## 模块职责

`safe` 提供 **goroutine panic 安全兜底原语**。核心目标是：让异步回调、后台协程中的意外 panic **不拖垮整个进程**——单个协程崩溃时只打印堆栈到指定输出，服务继续运行。包内只有两个执行原语（`SafeRun` 同步执行 + recover，`GoSafe` 启动协程 + recover）和一对输出目标读写器（`SetErrWriter` / `ErrWriter`）。设计极简且刻意不引入 logger 依赖，避免与 `pkg/foundation/logger` 形成循环依赖——默认写 `os.Stderr`，需要接入日志系统时在启动阶段注入即可。

## 规则与约束

1. **不得依赖本包兜底 runtime 级致命错误**：map 并发读写、栈溢出、OOM、死锁等 `fatal error` 以及 `os.Exit()` 不进入 defer/recover 流程，进程会直接退出。
2. **关键路径必须以 `error` 显式返回失败**：`SafeRun` 在 recover 后正常返回且不提供任何错误返回值，panic 仅留一行堆栈日志，不得用兜底输出替代业务错误处理。
3. **传入 `fn` 的共享状态一致性由调用方保证**：`SafeRun` 只保证进程存活，中断在半途的修改（半更新结构、未回滚事务）会保持不一致，`fn` 必须自行用 defer 完成清理与回滚。
4. **`fn` 内加锁必须使用 `defer Unlock()`**，禁止裸调 `Lock()`；持锁期间 panic 会使该锁永久无法释放。
5. **需要等待 `GoSafe` 完成时必须由调用方自行同步**：`GoSafe` 是 fire-and-forget，等待须组合 `sync.WaitGroup`，且 `defer wg.Done()` 必须写在 `fn` 内部。
6. **`GoSafe` 的并发数量必须由调用方限流**：本包不限制协程数，循环或批量启动必须接入 worker pool 或信号量。
7. **`SetErrWriter` 必须在启动阶段调用一次**：运行期切换会使 panic 日志分散到不同目标。
8. **注入 `SetErrWriter` 的 `io.Writer` 必须由调用方保证并发安全**：本包只保证对该 interface 值的读写安全，不保证多协程并发 `Write` 安全。
9. **panic 频率必须由调用方控制**：兜底路径会调用 `debug.Stack()` 格式化完整调用栈，开销为几十微秒至毫秒级，高频 panic 会造成性能塌陷与日志爆量。
10. **检索 panic 日志必须以 `SafeRun recovered panic` 为关键字**，不得依赖 `[util]` 前缀。
11. **自定义 recover 回调须由调用方自行实现**：本包不提供钩子，需要监控上报时只能在注入的 `io.Writer` 中解析输出。
12. **传入 `SafeRun` / `GoSafe` 的 `fn` 必须非 nil**：nil 函数会触发一次 panic 并被自身 recover，最终静默无操作。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `safe.go` | 68 | 全部内容：包级 `errWriterMu`/`errWriter` 状态、`SetErrWriter` 设置输出目标、`getErrWriter` 内部读取、`SafeRun` 同步安全执行、`reportPanic` 兜底输出（内含二次 recover）、`GoSafe` 安全启动协程、`ErrWriter` 只读访问器 |

## 核心类型与接口

本包**不定义任何类型**，只有包级状态与四个函数。

### 包级状态

| 标识符 | 类型 | 含义 |
| --- | --- | --- |
| `errWriterMu` | `sync.RWMutex` | 保护 `errWriter` 的读写并发安全 |
| `errWriter` | `io.Writer` | 兜底日志输出目标，**默认 `os.Stderr`** |

**并发安全性**：

- `SetErrWriter`：写锁保护，**并发安全**。
- `ErrWriter` / `getErrWriter`：读锁保护，**并发安全**。
- `SafeRun` / `GoSafe`：本身无共享状态（只在 recover 时通过 `getErrWriter()` 读一次输出目标），**并发安全**。但**被执行的 `fn` 的并发安全性由调用方自行保证**。

## 算法与实现原理

### recover 兜底机制

`SafeRun` 的实现是 Go panic 恢复的标准范式：

```go
func SafeRun(fn func()) {
    defer func() {
        if r := recover(); r != nil {
            fmt.Fprintf(getErrWriter(),
                "[util] SafeRun recovered panic: %v\n%s\n", r, debug.Stack())
        }
    }()
    fn()
}
```

要点：

1. **`defer` 必须在 `fn()` 之前注册**，否则 panic 发生时 defer 尚未入栈，无法拦截。
2. **`recover()` 只在 defer 函数中直接调用才有效**。若包一层辅助函数再调 `recover()` 会返回 nil。
3. **`debug.Stack()` 在 defer 中调用**，此时 panic 的调用栈尚未完全展开（Go 会在 recover 后才真正 unwind），因此能拿到**发生 panic 时的完整堆栈**而不是 defer 自身的堆栈。
4. panic 被吞掉后，`SafeRun` **正常返回**，调用方感知不到异常——这是设计意图（兜底），也是最大的风险点。

### GoSafe 的组合

```go
func GoSafe(fn func()) { go SafeRun(fn) }
```

一行组合。**注意 `SafeRun` 是在新协程内执行的**，所以 recover 也发生在新协程内——这正是必需的，因为 **Go 的 recover 无法跨 goroutine**：父协程的 defer/recover 绝对拦不住子协程的 panic。这也是本包存在的根本原因。

### 输出目标的读写分离

「一个协程写、另一个协程 panic 读」会产生**数据竞争**（`-race` 检测器会报警），故读写分离：

- `SetErrWriter(w)` —— 写锁独占。
- `ErrWriter()` —— 读锁共享，返回当前值的副本（interface 值拷贝）。

`getErrWriter()` 是内部版本，`ErrWriter()` 是导出的只读访问器，两者实现相同。

**RWMutex 而非 Mutex**：读远多于写（只在启动期设置一次，之后每次 panic 才读一次），读写锁能让并发 panic 场景不互相阻塞。

## 对外 API

### `SafeRun`

```go
func SafeRun(fn func())
```

用途：同步执行 `fn`，内部自带 recover。`fn` 中的 panic 不向上传播，只打印堆栈。

```go
// 保护第三方回调
safe.SafeRun(func() {
    userCallback(event) // 即使 panic 也不会炸掉当前流程
})

// 在循环中保护每一次迭代
for _, h := range handlers {
    safe.SafeRun(func() { h.Handle(msg) }) // 单个 handler 崩溃不影响其余
}
```

### `GoSafe`

```go
func GoSafe(fn func())
```

用途：安全启动 goroutine，等价于 `go SafeRun(fn)`。

```go
// 后台任务
safe.GoSafe(func() {
    for range ticker.C {
        doPeriodicWork()
    }
})

// 异步处理消息
safe.GoSafe(func() {
    processMessage(msg)
})
```

### `SetErrWriter` / `ErrWriter`

```go
func SetErrWriter(w io.Writer)
func ErrWriter() io.Writer
```

用途：设置 / 读取兜底日志输出目标。**建议在启动阶段调用一次**，避免运行时频繁切换。

```go
// 启动阶段：把 panic 堆栈接入日志系统
func init() {
    safe.SetErrWriter(logger.Writer()) // 或任意 io.Writer
}

// 单测中：捕获输出做断言
var buf bytes.Buffer
safe.SetErrWriter(&buf)
safe.SafeRun(func() { panic("boom") })
if !strings.Contains(buf.String(), "boom") {
    t.Fatal("未捕获 panic")
}

// 读取当前目标
w := safe.ErrWriter()
```

### 输出格式

```
[util] SafeRun recovered panic: <panic 值>
<完整 goroutine 堆栈>
```

例如：

```
[util] SafeRun recovered panic: runtime error: index out of range [5] with length 3
goroutine 18 [running]:
runtime/debug.Stack()
    /usr/local/go/src/runtime/debug/stack.go:24 +0x64
...
```

## 依赖关系

- **仅依赖 Go 标准库**：`fmt`（格式化输出）、`io`（`Writer` 接口）、`os`（默认 `Stderr`）、`runtime/debug`（`Stack()`）、`sync`（`RWMutex`）。
- 零第三方依赖、**零引擎内部依赖**——这是刻意为之，避免与 `pkg/foundation/logger` 形成循环依赖，使本包可被任意底层模块安全引用。

