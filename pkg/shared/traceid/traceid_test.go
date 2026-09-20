package traceid_test

import (
	"context"
	"testing"

	"github.com/qw576483/clover-server-engine/pkg/foundation/trace"
	"github.com/qw576483/clover-server-engine/pkg/shared/traceid"
)

// TestSpanContextKeyShared_TraceidToTrace 钉住「两包共用同一个 context key」：
// traceid 写入的追踪上下文，foundation/trace 必须读得到。
//
// 这正是修复前的断链点 —— 两包各用私有 key、互不识别，
// 请求链路（traceid）的 trace_id 到不了 logger / 跨节点注入（trace）。
func TestSpanContextKeyShared_TraceidToTrace(t *testing.T) {
	span := traceid.StartSpan(context.Background(), "gateway.recv")
	ctx := span.WithContext(context.Background())

	sc := trace.SpanContextFrom(ctx)
	if !sc.Valid() {
		t.Fatal("trace.SpanContextFrom 读不到 traceid 写入的追踪上下文（context key 未统一，链路断链）")
	}
	if sc.TraceID != span.TraceID() || sc.SpanID != span.SpanID() {
		t.Fatalf("trace 侧读到的 id 不一致：got trace_id=%q span_id=%q, want %q / %q",
			sc.TraceID, sc.SpanID, span.TraceID(), span.SpanID())
	}
	// 日志字段路径：logger.ctxFields 正是走 trace.SpanContextFrom。
	if got := trace.FromContext(ctx); got != span.TraceID() {
		t.Fatalf("trace.FromContext = %q, 期望 %q", got, span.TraceID())
	}
}

// TestSpanContextKeyShared_TraceToTraceid 反向：foundation/trace 写入的上下文，
// traceid 也必须取得到（任一方 set、另一方都能 get）。
func TestSpanContextKeyShared_TraceToTraceid(t *testing.T) {
	ctx := trace.WithSpanContext(context.Background(), trace.SpanContext{
		TraceID:  "0123456789abcdef0123456789abcdef",
		SpanID:   "0123456789abcdef",
		ParentID: "fedcba9876543210",
		Sampled:  true,
	})

	s := traceid.FromContext(ctx)
	if s == nil {
		t.Fatal("traceid.FromContext 取不到 trace 写入的追踪上下文（反向断链）")
	}
	if s.TraceID() != "0123456789abcdef0123456789abcdef" ||
		s.SpanID() != "0123456789abcdef" ||
		s.ParentID() != "fedcba9876543210" {
		t.Fatalf("traceid 侧读到的 id 不一致：trace=%q span=%q parent=%q",
			s.TraceID(), s.SpanID(), s.ParentID())
	}
	// 合成 Span 只保证 ID 可用；空 ctx / 无追踪时仍必须是 nil（调用方按 nil 判空）。
	if traceid.FromContext(context.Background()) != nil {
		t.Fatal("无追踪上下文的 ctx 应返回 nil")
	}
	if traceid.FromContext(nil) != nil {
		t.Fatal("nil ctx 应返回 nil（不得 panic）")
	}
}

// TestStartSpanInheritsTraceWrittenByOtherPackage 父子关系要跨包成立：
// 父上下文由 foundation/trace 写入时，traceid.StartSpan 仍继承其 trace_id 并挂上 parent_id。
func TestStartSpanInheritsTraceWrittenByOtherPackage(t *testing.T) {
	parent := trace.WithSpanContext(context.Background(), trace.SpanContext{
		TraceID: "aaaabbbbccccddddeeeeffff00001111",
		SpanID:  "1122334455667788",
		Sampled: true,
	})
	child := traceid.StartSpan(parent, "logic.dispatch")
	if child.TraceID() != "aaaabbbbccccddddeeeeffff00001111" {
		t.Fatalf("子 Span 未继承父 trace_id：%q", child.TraceID())
	}
	if child.ParentID() != "1122334455667788" {
		t.Fatalf("子 Span 的 parent_id = %q，期望父 span_id %q", child.ParentID(), "1122334455667788")
	}
}

// TestCrossNodePropagationKeepsSameTrace 端到端：网关侧（traceid）起的 trace，
// 经 foundation/trace 注入协议头 → 对端提取，trace_id 必须原样贯通（修复前的断链场景）。
func TestCrossNodePropagationKeepsSameTrace(t *testing.T) {
	span := traceid.StartSpan(context.Background(), "gateway.recv")
	carrier := map[string]string{}
	trace.InjectInto(span.WithContext(context.Background()), func(k, v string) { carrier[k] = v })

	if carrier[trace.HeaderTraceID] != span.TraceID() {
		t.Fatalf("注入的 %s = %q，期望 traceid 的 %q（跨包断链）",
			trace.HeaderTraceID, carrier[trace.HeaderTraceID], span.TraceID())
	}

	remote := trace.ExtractFrom(context.Background(), func(k string) string { return carrier[k] })
	if got := trace.FromContext(remote); got != span.TraceID() {
		t.Fatalf("对端提取到的 trace_id = %q，期望 %q", got, span.TraceID())
	}
	if s := traceid.FromContext(remote); s == nil || s.TraceID() != span.TraceID() {
		t.Fatalf("对端 traceid.FromContext 未认出同一 trace：%v", s)
	}
}

// TestTraceIDLengthsPinned 钉住「两个 NewTraceID 长度一致」这一结论。
//
// 历史形态是 traceid.NewTraceID 输出 24 hex（12 字节）、foundation/trace 输出 32 hex，
// 两处各自生成 ⇒ 长度漂移。现已统一为 32 hex（W3C Trace Context / OTel 位宽）：
// traceid 直接转发 foundation/trace，本用例防止任一侧再被改回短长度。
func TestTraceIDLengthsPinned(t *testing.T) {
	if got := len(traceid.NewTraceID()); got != 32 {
		t.Fatalf("traceid.NewTraceID 长度 = %d，期望 32 hex（16 字节，W3C traceparent 对齐）", got)
	}
	if got := len(trace.NewTraceID()); got != 32 {
		t.Fatalf("trace.NewTraceID 长度 = %d，期望 32 hex（16 字节，W3C traceparent 对齐）", got)
	}
}
