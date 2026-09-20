package trace

import (
	"context"
	"strings"
)

// Setter 是注入侧的写入回调：把一对 kv 写进具体协议的头部。
type Setter func(key, value string)

// Getter 是提取侧的读取回调：按 key 从具体协议的头部读值，不存在返回空串。
type Getter func(key string) string

// Inject 把 ctx 中的追踪上下文导出为一个 map，供无结构化 header 的传输直接携带。
// ctx 上没有有效追踪上下文时返回 nil（调用方无需判空即可 range）。
func Inject(ctx context.Context) map[string]string {
	sc := SpanContextFrom(ctx)
	if !sc.Valid() {
		return nil
	}
	m := make(map[string]string, 4)
	m[HeaderTraceID] = sc.TraceID
	if sc.SpanID != "" {
		m[HeaderSpanID] = sc.SpanID
	}
	if sc.ParentID != "" {
		m[HeaderParentID] = sc.ParentID
	}
	m[HeaderSampled] = boolToFlag(sc.Sampled)
	return m
}

// InjectInto 把 ctx 中的追踪上下文经 set 回调写入任意载体。
// 这是**跨三层打通的核心适配点**，各层只需一行适配：

//	// event 层（NATS header）
//	trace.InjectInto(ctx, func(k, v string) { msg.Header.Set(k, v) })
//	// net 层（自定义帧的 meta map）
//	trace.InjectInto(ctx, func(k, v string) { frame.Meta[k] = v })
//	// rpc 层（调用元数据）
//	trace.InjectInto(ctx, func(k, v string) { req.Metadata[k] = v })

// ctx 无有效追踪上下文或 set 为 nil 时静默返回，不会 panic。
func InjectInto(ctx context.Context, set Setter) {
	if set == nil {
		return
	}
	sc := SpanContextFrom(ctx)
	if !sc.Valid() {
		return
	}
	set(HeaderTraceID, sc.TraceID)
	if sc.SpanID != "" {
		set(HeaderSpanID, sc.SpanID)
	}
	if sc.ParentID != "" {
		set(HeaderParentID, sc.ParentID)
	}
	set(HeaderSampled, boolToFlag(sc.Sampled))
}

// InjectMap 把追踪上下文写入一个已存在的 map（不覆盖 map 中的其它 key）。
// carrier 为 nil 时静默返回。
func InjectMap(ctx context.Context, carrier map[string]string) {
	if carrier == nil {
		return
	}
	InjectInto(ctx, func(k, v string) { carrier[k] = v })
}

// Extract 从 map 载体还原追踪上下文，挂到 context.Background() 上返回。
// 载体中无 trace_id 时返回一个**新生成**的根上下文 —— 保证下游永远有 trace_id 可用。
func Extract(carrier map[string]string) context.Context {
	return ExtractTo(context.Background(), carrier)
}

// ExtractTo 从 map 载体还原追踪上下文并挂到给定的 parent ctx 上。
func ExtractTo(parent context.Context, carrier map[string]string) context.Context {
	if carrier == nil {
		return ensureFrom(parent, SpanContext{})
	}
	return ExtractFrom(parent, func(k string) string {
		if v, ok := carrier[k]; ok {
			return v
		}
		// 大小写兜底：部分传输（如 HTTP/1.1 经 textproto 规范化）会把
		// x-trace-id 变成 X-Trace-Id，直接查会 miss，导致链路在这里断掉。
		for ck, cv := range carrier {
			if strings.EqualFold(ck, k) {
				return cv
			}
		}
		return ""
	})
}

// ExtractFrom 经 get 回调从任意载体还原追踪上下文，挂到 parent ctx 上返回。
// 这是**跨三层打通的核心适配点**，与 InjectInto 对称：

//	// event 层
//	ctx = trace.ExtractFrom(ctx, func(k string) string { return msg.Header.Get(k) })
//	// rpc 层
//	ctx = trace.ExtractFrom(ctx, func(k string) string { return req.Metadata[k] })

// 关键语义：**上游的 span_id 会成为本地的 parent_id，本地生成新的 span_id**，
// 这样才能在链路图上正确串出父子调用关系。
// 载体中无 trace_id 时（上游未接入追踪）会新建根上下文，不会返回空 trace_id。
func ExtractFrom(parent context.Context, get Getter) context.Context {
	if get == nil {
		return ensureFrom(parent, SpanContext{})
	}
	sc := SpanContext{
		TraceID:  strings.TrimSpace(get(HeaderTraceID)),
		ParentID: strings.TrimSpace(get(HeaderSpanID)),
		Sampled:  flagToBool(get(HeaderSampled)),
	}
	// 上游没给 span_id（即上游自己就是根），回退读取 parent 头。
	if sc.ParentID == "" {
		sc.ParentID = strings.TrimSpace(get(HeaderParentID))
	}
	return ensureFrom(parent, sc)
}

// ensureFrom 用提取到的 sc 构造新 ctx；sc 无效时生成全新的根上下文。
func ensureFrom(parent context.Context, sc SpanContext) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	if !sc.Valid() {
		// 上游未接入追踪：新建根 trace，保证本节点及其下游链路完整。
		return WithSpanContext(parent, SpanContext{
			TraceID: NewTraceID(),
			SpanID:  NewSpanID(),
			Sampled: true,
		})
	}
	sc.SpanID = NewSpanID() // 每跳必须换新 span_id
	return WithSpanContext(parent, sc)
}

// boolToFlag / flagToBool 是采样标记与字符串之间的转换。
// 缺省（空串）视为**已采样**：宁可多采一条也不要因为上游没设标记而丢失整条链路。
func boolToFlag(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func flagToBool(s string) bool {
	switch strings.TrimSpace(s) {
	case "0", "false", "FALSE", "False":
		return false
	default:
		return true
	}
}
