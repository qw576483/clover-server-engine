# traceid 模块

## 模块职责

`traceid` 提供**分布式请求追踪基础能力（TraceID + SpanID）**。它是一个零依赖的轻量实现，解决全链路追踪的核心问题：一次请求从网关进入，经过逻辑服、存储层，可能跨多个进程与 NATS 消息，如何把散落各处的日志串成一条完整链路。做法是——请求入口生成一个 **TraceID**（贯穿全链路不变），每个处理阶段生成新的 **SpanID**（并透传父 SpanID 形成调用树），通过 `context.Context` 在进程内传递、通过 HTTP header / NATS header 跨进程传播。Span 支持打标签（`WithTag`）、记录耗时（`Duration`）、结束回调（`SetEndHook`，用于上报 metrics）。设计上刻意与 OpenTelemetry 的概念对齐，**后续可无缝升级到 OTel SDK**（同接口，只需替换实现）。

## 规则与约束

1. **`FromContext` 在无 Span 时返回 `nil`**：调用方必须先判空再取 ID，`traceid.FromContext(ctx).TraceID()` 会 panic。
2. **`SetEndHook` 只保留最后一次注册的回调**：后注册的回调直接覆盖前一个且无任何提示，需要多个回调时必须自行合并成一个函数。
3. **`SetEndHook` 在 Span 已结束时会立即同步执行回调**：执行发生在调用方 goroutine 上，回调必须保持轻量，慢操作须投递到独立 goroutine。
4. **`Duration()` 的语义是「创建至今」**：`End()` 不记录结束时刻，必须在 `End()` 之前或 `endFn` 回调内部读取，回调内读到的即为结束时刻。
5. **`InjectNATSHeader` 必须传入非 nil 的 map**：nil map 会被静默忽略，追踪信息丢失且无任何提示；必须传 `map[string]string{}` 而非 `var h map[string]string`。
6. **消费端只读取 `X-Trace-ID` 与 `X-Span-ID`**：上游的 `X-Span-ID` 成为本段 Span 的 `parentID`；`X-Span-Parent-ID` 只注入不消费，仅供外部追踪系统还原完整拓扑。
7. **ID 唯一性只在正常路径上得到保证**：`newID` 正常路径使用 `crypto/rand`；`crypto/rand` 失败时的降级路径以纳秒时间戳生成，熵低且可预测，仅作为兜底，不得依赖其全局唯一性。
8. **`tags` 不得存放大字符串**：Span 为指针类型且会被 context 长期持有，标签内容会延长内存生命周期。
9. **本包不支持采样率配置**：每个 `StartSpan` 都会实打实生成 ID（2 次 `crypto/rand`）、分配 map 并记录时间，高 QPS 场景须评估开销后再全量接入。
10. **`WithContext` 返回新的 context，必须接收返回值**：未回写 `ctx = span.WithContext(ctx)` 会导致后续 `StartSpan` 找不到父 Span，链路断裂。
11. **TraceID 格式必须与全链路统一**：本包为 **32 位**纯十六进制（16 字节，无前缀），`shared/id.GenTraceID` 为 `trc_` + 32 位十六进制，两者不可互换，混用会导致链路无法关联。
    长度与 W3C Trace Context / OTel 的 trace-id 位宽一致：`NewTraceID` 直接转发 `pkg/foundation/trace.NewTraceID`（**唯一真身**），`StartSpan` / `StartNATSSpan` 的 trace_id 同样是 16 字节 —— 全链路长度统一。
    ⚠️ 历史形态是两处各自生成（本包 24 hex / `foundation/trace` 32 hex）导致长度漂移，**不要**再让任一侧自建 ID 生成逻辑。
12. **context 传播的真身不在本包**：追踪上下文的唯一 key 属于 `pkg/foundation/trace`。本包 `Span.WithContext` 会往该 key 镜像写入、`FromContext` 未命中时会回读该 key 合成只读 Span —— 两包双向可读，**不要**再给本包新增独立的 context key（那会重新引入跨包断链）。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `traceid.go` | 246 | 全部内容：包文档与接入示例、`Span` 结构、`spanKey` context 键、`StartSpan` 创建（自动继承父 TraceID）、ID 访问器（TraceID/SpanID/ParentID）、context 传递（WithContext/FromContext）、`WithTag` 打标签、`End`/`SetEndHook` 结束与回调、`Duration` 耗时、`SetHeader` HTTP 注入、`InjectNATSHeader`/`StartNATSSpan` NATS 传播、`NewTraceID`/`newID` 随机 ID 生成 |

## 核心类型与接口

### `Span`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `mu` | `sync.Mutex` | 保护 `tags` 与 `endFn` |
| `traceID` | `string` | **跨进程追踪标识**，32 个十六进制字符（16 字节）。同一条链路上所有 Span 共享同一个值 |
| `spanID` | `string` | **当前段标识**，16 个十六进制字符（8 字节），每个 Span 唯一 |
| `parentID` | `string` | 父 Span 的 SpanID，根 Span 为空串 |
| `name` | `string` | 段名（如 `"gateway.recv"`），私有字段 |
| `startTime` | `time.Time` | 创建时刻，用于计算 `Duration` |
| `tags` | `map[string]string` | 键值标签，由 `WithTag` 写入 |
| `endOnce` | `sync.Once` | 保证 `End()` 只生效一次 |
| `endFn` | `func(*Span)` | 结束回调，触发后被清空 |
| `ended` | `atomic.Bool` | 是否已结束，供 `SetEndHook` 无锁快速判定 |

### `spanKey`

```go
type spanKey struct{}
```

**空结构体作为 context key** —— Go 官方推荐的做法：私有类型保证不会与其它包的 key 冲突，空结构体零内存占用。

### ID 位宽约定

| ID | 字节数 | 十六进制字符数 | 说明 |
| --- | --- | --- | --- |
| TraceID | 16 | **32** | 与 W3C Trace Context / OpenTelemetry 的 trace-id 位宽一致 |
| SpanID | 8 | **16** | 与 OTel 的 span-id 位宽一致 |

位宽与 W3C Trace Context / OTel 对齐，与接入 OpenTelemetry 生态的上下游系统可直接互通。

### header 键名约定

| 键 | 含义 |
| --- | --- |
| `X-Trace-ID` | 链路 ID |
| `X-Span-ID` | 当前段 ID（消费端读取后作为自己的 parentID） |
| `X-Span-Parent-ID` | 父段 ID（仅在非空时注入） |

**并发安全性**：

- `WithTag` / `End` / `SetEndHook`：**并发安全**（`mu` 保护 + `sync.Once` + `atomic.Bool`）。
- `TraceID()` / `SpanID()` / `ParentID()` / `Duration()`：读取的都是**创建后不再修改**的字段，**并发只读安全**（无锁）。
- `InjectNATSHeader`：只读 Span 字段，写入调用方的 map。**Span 侧安全，但传入的 map 若被并发访问需调用方保证**。
- `WithContext` / `FromContext` / `StartSpan` / `StartNATSSpan` / `SetHeader`：无共享状态，**并发安全**。
- **`tags` 无导出读取方法**，外部只能写入不能读取，因此不存在读 tags 的竞争。

## 算法与实现原理

### TraceID 的继承与 SpanID 的链式生成

`StartSpan` 是核心：

```go
s := &Span{
    traceID:   newID(16),   // 先假设是根 Span
    spanID:    newID(8),    // 总是新生成
    startTime: time.Now(),
    tags:      map[string]string{},
}
if p, ok := parent.Value(spanKey{}).(*Span); ok {
    s.traceID = p.traceID   // ★ 继承父的 TraceID（覆盖刚生成的）
    s.parentID = p.spanID   // ★ 父的 SpanID 成为自己的 parentID
}
```

于是形成一棵**调用树**：

```
TraceID = abc123...（全链路唯一，所有节点相同）

gateway.recv     span=aaa, parent=""
  └─ logic.handle  span=bbb, parent=aaa
       └─ db.query   span=ccc, parent=bbb
       └─ cache.get  span=ddd, parent=bbb
```

**注意**：即使是根 Span 也会先 `newID(16)` 生成一个 TraceID，然后在发现父 Span 时被覆盖。这多做了一次随机调用，但代码更简洁。

### 跨进程传播（NATS）

**生产端**：

```go
func (s *Span) InjectNATSHeader(h map[string]string) {
    if h == nil { return }           // nil 保护，避免 panic
    h["X-Trace-ID"] = s.traceID
    h["X-Span-ID"] = s.spanID
    if s.parentID != "" {
        h["X-Span-Parent-ID"] = s.parentID
    }
}
```

**消费端**：

```go
func StartNATSSpan(parent context.Context, name string, h map[string]string) *Span {
    traceID := h["X-Trace-ID"]
    if traceID == "" {
        return StartSpan(parent, name)   // ★ 无追踪信息 → 降级为新根 Span
    }
    return &Span{
        traceID:  traceID,           // 沿用上游 TraceID
        spanID:   newID(8),          // 生成自己的新 SpanID
        parentID: h["X-Span-ID"],    // ★ 上游的 SpanID 成为自己的 parent
        ...
    }
}
```

**关键设计**：消费端把上游的 `X-Span-ID` 当作自己的 `parentID`，而**不读** `X-Span-Parent-ID`（那是上游的父，与自己无关）。`X-Span-Parent-ID` 的注入主要是给日志/追踪系统还原完整拓扑用的。

**降级路径**：header 中无 `X-Trace-ID`（如来自未接入追踪的老服务）时，`StartNATSSpan` 退化为 `StartSpan`——链路会断成两截，但不会报错。

### End 与 SetEndHook 的双重触发防护

这是本包最精细的一段逻辑，解决「End 与 SetEndHook 存在竞态时可能重复上报 metrics」的问题。

**`End()`**：

```go
s.endOnce.Do(func() {
    s.ended.Store(true)
    s.mu.Lock()
    fn := s.endFn
    s.endFn = nil       // ★ 取出后立即清空
    s.mu.Unlock()
    if fn != nil { fn(s) }   // ★ 锁外调用回调
})
```

- `sync.Once` 保证 End 只生效一次，重复调用无副作用（这使得 `defer span.End()` 与显式 `span.End()` 并存也安全）。
- **取出 fn 后立即置 nil**：确保这个回调只可能被触发一次。
- **回调在锁外执行**：避免回调中再调 Span 方法造成自死锁，也避免慢回调阻塞其它协程。

**`SetEndHook(fn)`**：

```go
s.mu.Lock()
if s.ended.Load() {
    s.mu.Unlock()
    if fn != nil { fn(s) }   // ★ 已结束 → 立即同步执行，且不保存
    return
}
s.endFn = fn                  // 未结束 → 保存，等 End 时触发
s.mu.Unlock()
```

- **已结束时立即触发且不保存**：因为 `End` 已经把 `endFn` 清空了，如果这里再存进去就永远不会被调用（`endOnce` 已消耗）。直接同步执行是唯一正确的语义。
- **`ended.Load()` 在锁内检查**：与 `End` 中的 `ended.Store(true)` + 加锁清空形成互斥，杜绝「End 读到旧的 nil endFn，同时 SetEndHook 写入新 fn 却永不触发」的竞态窗口。

**代价**：`SetEndHook` 在 Span 已结束时会**同步阻塞**调用方执行回调。

### newID：随机 ID 生成与降级

```go
func newID(n int) string {
    b := make([]byte, n)
    if _, err := rand.Read(b); err != nil {
        now := uint64(time.Now().UnixNano())
        for i := 0; i < n; i++ {
            if i > 0 && i%8 == 0 {
                now = uint64(time.Now().UnixNano()) ^ (now * 1099511628211)
            }
            b[i] = byte(now >> uint((i%8)*8))
        }
    }
    return hex.EncodeToString(b)
}
```

**正常路径**：`crypto/rand.Read(b)` 填充 n 字节强随机，`hex.EncodeToString` 编码为 2n 个小写十六进制字符。

**降级路径**（`crypto/rand` 失败，实际几乎不可能）：用纳秒时间戳逐字节填充。关键细节是**每消费 8 字节重取一次时钟并混淆**：

- 降级路径按 8 字节分段重新读取纳秒时间戳，并使用 FNV-1a 64 位质数混淆，避免重复字节模式。

即便如此，降级 ID 的熵仍远低于正常路径，且**可预测**。

## 对外 API

### 创建 Span

```go
func StartSpan(parent context.Context, name string) *Span
func StartNATSSpan(parent context.Context, name string, h map[string]string) *Span
```

```go
// 网关入口（根 Span）
span := traceid.StartSpan(ctx, "gateway.recv").WithTag("msg_id", strconv.Itoa(msgID))
defer span.End()

// 子 Span（自动继承 TraceID）
ctx = span.WithContext(ctx)
child := traceid.StartSpan(ctx, "logic.handle")
defer child.End()
```

### ID 访问

```go
func (s *Span) TraceID() string
func (s *Span) SpanID() string
func (s *Span) ParentID() string
func (s *Span) Duration() time.Duration
```

```go
logger.With(
    "trace_id", span.TraceID(),
    "span_id",  span.SpanID(),
    "parent",   span.ParentID(),
).Info("处理中")

log.Printf("耗时 %v", span.Duration())
```

### 独立 TraceID

```go
func NewTraceID() string   // 32 位十六进制（16 字节，转发 pkg/foundation/trace.NewTraceID）
```

纯 ID 函数，不创建 Span，用于需要独立 TraceID 的非 Span 场景（如直接往 NATS header 透传）。`shared/id.GenTraceID` 即由它提供随机熵。

### context 传递

```go
func (s *Span) WithContext(ctx context.Context) context.Context
func FromContext(ctx context.Context) *Span   // 不存在返回 nil
```

```go
ctx = span.WithContext(ctx)
// ... 往下传 ctx ...

if s := traceid.FromContext(ctx); s != nil {
    log.Printf("当前 trace=%s", s.TraceID())
}
```

**跨包可读（同一个 key）**：`WithContext` 同时写入 `pkg/foundation/trace` 的规范 key，
因此 `trace.SpanContextFrom(ctx)` / `trace.InjectInto(ctx, ...)`（跨节点注入）/ logger 的
ctx 字段都能识别本包起的 trace；反向 `FromContext` 也会回读该 key
（合成只读 Span，只保证 `TraceID()` / `SpanID()` / `ParentID()` 可用）。

### 标签

```go
func (s *Span) WithTag(k, v string) *Span   // 链式返回自身
```

```go
span := traceid.StartSpan(ctx, "db.query").
    WithTag("table", "player").
    WithTag("op", "select")
```

### 结束与回调

```go
func (s *Span) End()
func (s *Span) SetEndHook(fn func(*Span))
```

```go
span := traceid.StartSpan(ctx, "logic.handle")
span.SetEndHook(func(s *traceid.Span) {
    metrics.ObserveLatency(s.Duration())
    if s.Duration() > 100*time.Millisecond {
        logger.Warnf("慢操作 trace=%s cost=%v", s.TraceID(), s.Duration())
    }
})
defer span.End()   // 触发 hook
```

### HTTP 传播

```go
func SetHeader(h http.Header, traceID string)
```

```go
// 写入响应 header，方便客户端上报问题时提供 trace_id
traceid.SetHeader(w.Header(), span.TraceID())

// 读取入站 header：按本包的 header 键名约定自行取值后续接链路
if tid := req.Header.Get("X-Trace-ID"); tid != "" {
    // 用 StartNATSSpan 的 map 形式续接链路
}
```

### NATS 传播

```go
func (s *Span) InjectNATSHeader(h map[string]string)
```

```go
// 生产端
hdr := map[string]string{}
span.InjectNATSHeader(hdr)
for k, v := range hdr {
    msg.Header.Set(k, v)
}

// 消费端
hdr := map[string]string{
    "X-Trace-ID": msg.Header.Get("X-Trace-ID"),
    "X-Span-ID":  msg.Header.Get("X-Span-ID"),
}
span := traceid.StartNATSSpan(ctx, "worker.handle", hdr)
defer span.End()
```

### 完整链路示例

```go
// 1) 网关：生成根 Span
span := traceid.StartSpan(ctx, "gateway.recv").WithTag("msg_id", "1001")
ctx = span.WithContext(ctx)
defer span.End()

// 2) 跨 NATS 发布
hdr := map[string]string{}
span.InjectNATSHeader(hdr)
publishWithHeader(subject, payload, hdr)

// 3) 消费端：延续同一 TraceID
func onMessage(ctx context.Context, hdr map[string]string, payload []byte) {
    s := traceid.StartNATSSpan(ctx, "logic.handle", hdr)
    s.SetEndHook(reportMetrics)
    defer s.End()

    ctx = s.WithContext(ctx)
    doWork(ctx)  // 内部可继续 StartSpan 建子 Span
}
```

## 依赖关系

- **依赖 Go 标准库**：`context`、`crypto/rand`（强随机）、`encoding/hex`（ID 编码）、`net/http`（`http.Header`）、`sync`（Mutex/Once）、`sync/atomic`（`atomic.Bool`）、`time`。
- **引擎内依赖仅一个**：`pkg/foundation/trace`（追踪上下文的规范 key 与最小传播单元真身，见规则 12）——不再是「零引擎内部依赖」的叶子包，但仍是自包含包（不 import `internal`）。
- 零第三方依赖。
- **相关包**：`shared/id` 提供 `GenTraceID()`（`trc_` + 32 位十六进制，其随机熵即来自本包 `NewTraceID`），与本包的 TraceID 格式**不同**（本包是纯 32 位十六进制无前缀），两者不可混用。
- **升级路径**：接口概念与 OpenTelemetry 对齐（TraceID 16 字节 / SpanID 8 字节 / Span 树），后续可替换为 OTel SDK 实现。
