package logger

import (
	"context"

	"go.uber.org/zap"

	"clover-server-engine/pkg/foundation/trace"
	"clover-server-engine/pkg/shared/traceid"
)

// context 感知日志（自动注入 trace_id）

// 这组函数与 LogDebug / LogInfo / ... 一一对应，差别只在于**首参接收 context**，
// 并自动把 ctx 上的 trace_id / span_id 提取为 zap 字段注入日志。

// 【调用深度约定】
// 现有 InitZap 里写死了 AddCallerSkip(2)，对应调用链：

//	业务代码 → logger.InfoCtx(facade，同包) → logger.LogInfoCtx(ctx.go) → zap
//	          ^skip 2                          ^skip 1                    ^caller

// 因此本文件的每个函数都必须**恰好被 pkg 门面直接调用一层**，
// 中间不能再套辅助函数（否则 caller 行号会漂到 logger 内部）。
// 这也是下面每个函数都把 ctxFields 内联展开、而不抽公共 helper 包一层的原因 ——
// ctxFields 只做「造字段切片」不打日志，不占调用栈层级，是安全的。

// 若业务想直接用 internal 版本（不经 pkg 门面），caller 行号会指向业务的上一层，
// 这是 skip=2 约定的固有限制。

// ctxFields 把 ctx 上的追踪信息与本次调用的 fields 合并为一条新切片。

// 关键：**必须分配新底层数组**，不能 append 到调用方传入的切片上 ——
// 变参 fields 的底层数组由调用方持有，就地扩容写入会造成跨 goroutine 的字段覆盖。

// 无 trace_id 时原样返回 fields（零分配快路径），保证未接入追踪的调用无额外开销。
func ctxFields(ctx context.Context, fields []zap.Field) []zap.Field {
	if ctx == nil {
		return fields
	}
	traceID, spanID := "", ""
	// 追踪上下文的规范 key 只有 pkg/foundation/trace 一个：pkg/shared/traceid 的 Span
	// 现在也往同一个 key 镜像写入，所以请求链路（网关收包 / 逻辑服派发）在第一个分支就能命中。
	if sc := trace.SpanContextFrom(ctx); sc.Valid() {
		traceID, spanID = sc.TraceID, sc.SpanID
	} else if sp := traceid.FromContext(ctx); sp != nil {
		// 兜底分支（正常路径不再执行）：万一上游只写了 traceid 的私有 key
		//（例如镜像写入被改动），日志也不至于静默丢掉 trace_id / span_id。
		traceID, spanID = sp.TraceID(), sp.SpanID()
	}
	if traceID == "" {
		return fields
	}
	// 追踪字段前置：JSON 日志里 trace_id 排在业务字段之前，人肉排障时更好找。
	out := make([]zap.Field, 0, len(fields)+2)
	out = append(out, zap.String(GetTraceKey(), traceID))
	if spanID != "" {
		out = append(out, zap.String(trace.LogKeySpanID, spanID))
	}
	return append(out, fields...)
}

// TraceIDFrom 返回 ctx 上的 trace_id，不存在时返回空串。
// 供需要手工拼日志字段或回包带 trace_id 的场景使用。
func TraceIDFrom(ctx context.Context) string { return trace.FromContext(ctx) }

// WithTraceID 在 ctx 上挂一个 trace_id，返回新 ctx。
func WithTraceID(ctx context.Context, id string) context.Context {
	return trace.WithTraceID(ctx, id)
}

// EnsureTraceID 保证 ctx 上有 trace_id（无则新建），返回新 ctx 与该 trace_id。
func EnsureTraceID(ctx context.Context) (context.Context, string) {
	return trace.EnsureTraceID(ctx)
}

// LogDebugCtx 带 context 的 Debug 日志，自动注入 trace_id / span_id。
func LogDebugCtx(ctx context.Context, msg string, fields ...zap.Field) {
	safeLogger().Debug(msg, ctxFields(ctx, fields)...)
}

// LogInfoCtx 带 context 的 Info 日志，自动注入 trace_id / span_id。
func LogInfoCtx(ctx context.Context, msg string, fields ...zap.Field) {
	safeLogger().Info(msg, ctxFields(ctx, fields)...)
}

// LogWarnCtx 带 context 的 Warn 日志，自动注入 trace_id / span_id。
func LogWarnCtx(ctx context.Context, msg string, fields ...zap.Field) {
	safeLogger().Warn(msg, ctxFields(ctx, fields)...)
}

// LogErrorCtx 带 context 的 Error 日志，自动注入 trace_id / span_id。
func LogErrorCtx(ctx context.Context, msg string, fields ...zap.Field) {
	safeLogger().Error(msg, ctxFields(ctx, fields)...)
}

// LogPanicCtx 带 context 的 Panic 日志，自动注入 trace_id / span_id。输出后 panic。
func LogPanicCtx(ctx context.Context, msg string, fields ...zap.Field) {
	safeLogger().Panic(msg, ctxFields(ctx, fields)...)
}

// LogFatalCtx 带 context 的 Fatal 日志，自动注入 trace_id / span_id。输出后退出进程。
func LogFatalCtx(ctx context.Context, msg string, fields ...zap.Field) {
	safeLogger().Fatal(msg, ctxFields(ctx, fields)...)
}
