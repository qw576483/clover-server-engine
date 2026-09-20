package trace

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Span 表示一个可计时的追踪段。
//
// 生命周期：StartSpan 创建 → SetAttr / RecordError 打标 → End 结束。
// End 是幂等的（sync.Once 保护），重复调用无副作用，因此可以放心
// `defer span.End()` 的同时在出错分支里提前 End。
//
// 并发安全：属性写入由 mu 保护，可跨 goroutine 使用。
type Span struct {
	mu       sync.Mutex
	sc       SpanContext
	name     string
	start    time.Time
	end      time.Time
	attrs    map[string]string
	err      error
	endOnce  sync.Once
	finished bool
}

// Exporter 是 Span 结束时的上报回调。
//
// 通过全局钩子而非依赖注入，是为了让埋点方（三层业务代码）与上报方
// （metrics / 日志 / 未来的 OTLP exporter）彻底解耦：业务只管 StartSpan，
// 至于结果送去哪由启动期一次性决定。
type Exporter func(*Span)

var (
	exporterMu sync.RWMutex
	exporters  []Exporter
)

// RegisterExporter 注册一个 Span 结束回调。可注册多个，按注册顺序依次调用。
// 应在启动期完成注册；nil 会被忽略。
func RegisterExporter(e Exporter) {
	if e == nil {
		return
	}
	exporterMu.Lock()
	exporters = append(exporters, e)
	exporterMu.Unlock()
}

// ResetExporters 清空全部 Exporter，主要用于测试隔离。
func ResetExporters() {
	exporterMu.Lock()
	exporters = nil
	exporterMu.Unlock()
}

// StartSpan 在 ctx 上开启一个名为 name 的新 Span，返回携带该 Span 的新 ctx 与 Span 本身。
//
// 行为：
//   - ctx 上已有追踪上下文 → 继承其 TraceID，把其 SpanID 作为本 Span 的 ParentID；
//   - ctx 上没有 → 新建根 Span（生成新 TraceID）。
//
// 典型写法：
//
//	ctx, span := trace.StartSpan(ctx, "event.publish")
//	defer span.End()
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent := SpanContextFrom(ctx)
	sc := SpanContext{SpanID: NewSpanID(), Sampled: true}
	if parent.Valid() {
		sc.TraceID = parent.TraceID
		sc.ParentID = parent.SpanID
		sc.Sampled = parent.Sampled
	} else {
		sc.TraceID = NewTraceID()
	}
	s := &Span{sc: sc, name: name, start: time.Now(), attrs: make(map[string]string, 4)}
	return WithSpanContext(ctx, sc), s
}

// SpanContext 返回本 Span 的追踪上下文（值拷贝）。
func (s *Span) SpanContext() SpanContext { return s.sc }

// TraceID 返回 trace_id。
func (s *Span) TraceID() string { return s.sc.TraceID }

// SpanID 返回本段的 span_id。
func (s *Span) SpanID() string { return s.sc.SpanID }

// ParentID 返回父段 span_id，根 Span 为空串。
func (s *Span) ParentID() string { return s.sc.ParentID }

// Name 返回 Span 名。
func (s *Span) Name() string { return s.name }

// StartTime 返回开始时间。
func (s *Span) StartTime() time.Time { return s.start }

// SetAttr 设置一个属性（如 msg_id / player_id / handler 名）。返回自身便于链式调用。
//
// 【重要】属性值最终会进 label / 日志字段，**不要放高基数值**（如完整 payload），
// 也不要在 End 之后调用（会被忽略，避免与上报竞态）。
func (s *Span) SetAttr(k, v string) *Span {
	if s == nil || k == "" {
		return s
	}
	s.mu.Lock()
	if !s.finished {
		s.attrs[k] = v
	}
	s.mu.Unlock()
	return s
}

// RecordError 记录本段发生的错误。err 为 nil 时不做任何事。
// 多次调用只保留**第一个**错误（首个错误通常才是根因，后续多为衍生错误）。
func (s *Span) RecordError(err error) *Span {
	if s == nil || err == nil {
		return s
	}
	s.mu.Lock()
	if !s.finished && s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	return s
}

// Err 返回已记录的错误（无则 nil）。
func (s *Span) Err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Attrs 返回属性快照（拷贝，调用方可安全持有）。
func (s *Span) Attrs() map[string]string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.attrs))
	for k, v := range s.attrs {
		out[k] = v
	}
	return out
}

// AttrKeys 返回排序后的属性 key 列表，便于确定性遍历与测试断言。
func (s *Span) AttrKeys() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	keys := make([]string, 0, len(s.attrs))
	for k := range s.attrs {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	sort.Strings(keys)
	return keys
}

// Duration 返回本段耗时：已 End 则为最终耗时，未 End 则为「到此刻为止」的耗时。
func (s *Span) Duration() time.Duration {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.end.IsZero() {
		return s.end.Sub(s.start)
	}
	return time.Since(s.start)
}

// Ended 报告本 Span 是否已结束。
func (s *Span) Ended() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

// End 结束本段并触发全部 Exporter。幂等，重复调用无副作用。
//
// Exporter 在**锁外**调用：上报逻辑可能反过来读 Span 的属性（Attrs / Duration），
// 持锁调用会直接自死锁。
func (s *Span) End() {
	if s == nil {
		return
	}
	s.endOnce.Do(func() {
		s.mu.Lock()
		s.end = time.Now()
		s.finished = true
		s.mu.Unlock()

		exporterMu.RLock()
		list := exporters
		exporterMu.RUnlock()
		for _, e := range list {
			e(s)
		}
	})
}
