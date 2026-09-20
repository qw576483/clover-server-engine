# semaphore 模块

## 模块职责

`semaphore` 提供**加权信号量**，用于控制对有限资源的并发访问。基于 `sync.Mutex` + `sync.Cond` 实现，支持 context 取消和超时。

## 规则与约束

1. **`Release` 必须与 `Acquire` / `TryAcquire` 严格配对，且释放权重与获取权重相等**：未获取即释放或超额释放会返回 `ErrNotHeld`（不改变持有量），不会阻塞。
2. **权重参数必须为正数**：`weight <= 0` 时由实现统一修正为 1，调用方不得依赖该兜底传入 0 或负值。
3. **获取权重必须小于或等于 `Capacity()`**：权重超过总容量的 `Acquire` / `TryAcquire` 立即返回 `ErrExceedsCapacity` / `false`（不会挂起等待 ctx 取消）。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `semaphore.go` | ~190 | `Semaphore` 结构体、`NewSemaphore`、`Acquire`、`Release`、`TryAcquire`、`Available`/`Capacity`、`AcquireWithTimeout`、错误值 `ErrExceedsCapacity`/`ErrNotHeld`/`ErrWouldBlock`（后者当前无返回点） |

## 核心类型与接口

### Semaphore

```go
type Semaphore struct {
    // 未导出字段
}
```

**并发安全性**：**是**。底层使用 `sync.Mutex` + `sync.Cond`，持有量记账在互斥锁内完成。

## 对外 API

### NewSemaphore

```go
func NewSemaphore(n int64) *Semaphore
```

创建容量为 n 的信号量。

### Acquire

```go
func (s *Semaphore) Acquire(ctx context.Context, weight int64) error
```

以权重 weight 获取信号量。全有全无语义：要么一次拿到全部权重、要么一个都不拿；ctx 被取消时返回错误且不获取任何资源。

### Release

```go
func (s *Semaphore) Release(weight int64) error
```

释放权重为 weight 的资源。超额释放（含未持有即释放）返回 `ErrNotHeld` 且不改变持有量。

### TryAcquire

```go
func (s *Semaphore) TryAcquire(weight int64) bool
```

非阻塞尝试获取，成功返回 true。

### AcquireWithTimeout

```go
func (s *Semaphore) AcquireWithTimeout(weight int64, timeout time.Duration) error
```

Acquire 的超时便捷封装。

### Available / Capacity

查询当前可用资源和总容量。

## 使用示例

```go
sem := semaphore.NewSemaphore(10)

// 获取权重 3 资源，最多等待 5 秒
if err := sem.AcquireWithTimeout(3, 5*time.Second); err != nil {
    // 超时处理
}
defer func() { _ = sem.Release(3) }() // Release 返回 error（超额释放为 ErrNotHeld）

// 或使用 context
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := sem.Acquire(ctx, 3); err != nil {
    return err
}
defer func() { _ = sem.Release(3) }()
```

## 依赖

纯标准库，零外部/内部依赖。

