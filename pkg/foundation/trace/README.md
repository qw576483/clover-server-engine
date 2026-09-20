# trace

分布式 Tracing 传输无关核心原语

trace_id / span_id 生成、context 传播、跨进程注入/提取。不感知任何具体传输（NATS/HTTP/TCP/rpc 都不 import），以字符串 kv 载体为唯一契约。

## 核心接口

- `NewTraceID()` / `NewSpanID()` — ID 生成（32/16 hex）
- `SpanContext` — 最小传播单元（TraceID + SpanID + ParentID + Sampled）
- `WithSpanContext(ctx, sc)` / `SpanContextFrom(ctx)` — context 读写
- `EnsureTraceID(ctx)` — 保证 ctx 上有 trace_id（入口兜底）
- `InjectInto(ctx, func(k,v))` — 注入到任意协议头
- `ExtractFrom(ctx, func(k))` — 从任意协议头提取

> **本包是「trace_id / span_id + context 传播」的唯一真身**：`pkg/shared/traceid` 的
> `Span.WithContext` 会往**本包的 context key** 镜像写入，`traceid.FromContext` 也会回读本包的
> key ⇒ 两包双向可读（任一方 set、另一方都能 get），同一条 trace 不再断在包边界上。
> traceid 保留的只是「带 tags / End 钩子的请求段对象」。
>
> ★ 两个包的 `NewTraceID` 长度**已统一为 32 hex**：`traceid.NewTraceID` 直接转发本包实现
> （对外 `shared/id.GenTraceID` 的随机熵也来自本包），ID 生成只保留这一处真身。

## 典型用法

```go
// 入口
ctx, traceID := trace.EnsureTraceID(ctx)

// 出口
trace.InjectInto(ctx, func(k, v string) { msg.Header.Set(k, v) })

// 下游入口
ctx = trace.ExtractFrom(ctx, func(k string) string { return msg.Header.Get(k) })
```
