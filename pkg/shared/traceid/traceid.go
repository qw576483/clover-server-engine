// Package traceid 提供分布式请求追踪基础能力（TraceID + SpanID）。
//
// 零依赖实现核心追踪传播：
//   - TraceID：请求入口生成，贯穿网关→逻辑服→存储全链路（NATS header / HTTP header 传播）
//   - SpanID：每个处理阶段生成新的 SpanID，父 SpanID 透传
//
// 接口与 OpenTelemetry 概念对齐，后续可替换为 OTel SDK 实现。
//
// 接入：
//
//	// 网关入口
//	span := traceid.StartSpan(ctx, "gateway.recv").WithTag("msg_id", msgID)
//	defer span.End()
//
//	// HTTP 中间件
//	ctx = traceid.SetHeader(req.Header, span.TraceID())
//
//	// NATS 发布
//	msg.Header.Set("X-Trace-ID", span.TraceID())
//	msg.Header.Set("X-Span-ID", span.SpanID())
//
// # 与 pkg/foundation/trace 的关系（★ context 传播的真身不在本包）
//
// 「trace_id / span_id + context 传播」的**唯一真身是 `pkg/foundation/trace`**：
// 本包的 `Span.WithContext` / `FromContext` 都走那边的 context key（镜像写 + 回读兜底），
// 两包双向可读 —— 任一方 set、另一方都能 get，同一条 trace 不会在包边界断链。
// 本包保留的只是「带 tags / End 钩子的请求段对象」这一层能力。
//
// **ID 长度统一为 32 hex（16 字节）**：本包 `NewTraceID` 转发
// `pkg/foundation/trace.NewTraceID`（W3C Trace Context / OTel 的 trace-id 位宽），
// 因此本包 / `StartSpan` / 对外 `shared/id.GenTraceID` 三处长度一致，可直接互通。
package traceid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/trace"
)

// Span 轻量 Span，零外部依赖。
type Span struct {
	mu        sync.Mutex
	traceID   string
	spanID    string
	parentID  string
	name      string
	startTime time.Time
	tags      map[string]string
	endOnce   sync.Once
	endFn     func(*Span)
	ended     atomic.Bool
}

type spanKey struct{}

// StartSpan 从 context 创建新 Span（自动继承父 Span 的 TraceID）。
//
// 父 trace 经 FromContext 读取 ⇒ 父上下文的追踪上下文无论由本包还是
// `pkg/foundation/trace` 写入，都能被继承（两包共用同一个 context key）。
func StartSpan(parent context.Context, name string) *Span {
	s := &Span{
		traceID:   newID(16),
		spanID:    newID(8),
		name:      name,
		startTime: time.Now(),
		tags:      map[string]string{},
	}
	if p := FromContext(parent); p != nil {
		s.traceID = p.TraceID()
		s.parentID = p.SpanID()
	}
	return s
}

// spanContext 把 Span 映射为规范的最小传播单元（真身类型在 pkg/foundation/trace）。
// traceid 不做采样决策，故 Sampled 恒为 true。
func (s *Span) spanContext() trace.SpanContext {
	return trace.SpanContext{
		TraceID:  s.traceID,
		SpanID:   s.spanID,
		ParentID: s.parentID,
		Sampled:  true,
	}
}

// TraceID 返回跨进程追踪标识。
func (s *Span) TraceID() string { return s.traceID }

// SpanID 返回当前段标识。
func (s *Span) SpanID() string { return s.spanID }

// ParentID 返回父 SpanID。
func (s *Span) ParentID() string { return s.parentID }

// WithContext 将 Span 注入 context。
//
// 写**两个** key，二者语义不同、不冲突：
//   - `pkg/foundation/trace` 的规范 key：追踪上下文的唯一真身（InjectInto / ExtractFrom /
//     logger 的 ctx 字段都读它）—— 没有这一步，本条 trace 跨包即断链；
//   - 本包私有 key：镜像富对象（tags / End 钩子），保证 `FromContext` 取回的是
//     原始 `*Span` 而非合成对象。
//
// ctx 为 nil 时由 trace 侧归一为 Background。
func (s *Span) WithContext(ctx context.Context) context.Context {
	if s == nil {
		return ctx
	}
	ctx = trace.WithSpanContext(ctx, s.spanContext())
	return context.WithValue(ctx, spanKey{}, s)
}

// FromContext 从 context 取出 Span；不存在追踪上下文时返回 nil。
//
// 先取本包私有 key（富对象）；未命中时回读 `pkg/foundation/trace` 的规范 key，
// 用其中的最小传播单元合成一个只读 Span —— 于是「上一跳只写了 trace 的 key」
// 时本包也能取到（两包双向可读）。
// 注意：合成 Span 没有 tags / End 钩子 / 真实起始时刻，取到后请只读 ID 字段。
func FromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	if s, ok := ctx.Value(spanKey{}).(*Span); ok {
		return s
	}
	if sc := trace.SpanContextFrom(ctx); sc.Valid() {
		return &Span{
			traceID:   sc.TraceID,
			spanID:    sc.SpanID,
			parentID:  sc.ParentID,
			startTime: time.Now(), // 合成对象：不给真实起点，避免 Duration 报出「零时刻至今」的荒数
			tags:      map[string]string{},
		}
	}
	return nil
}

// WithTag 添加标签（并发安全）。
func (s *Span) WithTag(k, v string) *Span {
	s.mu.Lock()
	s.tags[k] = v
	s.mu.Unlock()
	return s
}

// End 结束 Span，触发 endFn 回调（如上报 metrics）。
// 仅第一次调用生效，重复调用无副作用。触发后清空 endFn，
// 确保 End 之后再 SetEndHook 时该钩子只会被 SetEndHook 自身触发一次，不会重复上报。
func (s *Span) End() {
	s.endOnce.Do(func() {
		s.ended.Store(true)
		s.mu.Lock()
		fn := s.endFn
		s.endFn = nil // 清空，防止 End 与后续 SetEndHook 双重触发
		s.mu.Unlock()
		if fn != nil {
			fn(s)
		}
	})
}

// SetEndHook 设置结束时回调（用于上报 metrics）。
// 若 Span 尚未结束，回调在 End() 时触发；若 Span 已结束，则立即同步执行且不再保留，
// 避免与 End() 已触发的回调叠加造成重复上报。加锁与 End() 互斥，杜绝并发双触发。
func (s *Span) SetEndHook(fn func(*Span)) {
	s.mu.Lock()
	if s.ended.Load() {
		// 已结束：不保存 endFn（End 已清空并触发过其它钩子），仅本地触发一次。
		s.mu.Unlock()
		if fn != nil {
			fn(s)
		}
		return
	}
	s.endFn = fn
	s.mu.Unlock()
}

// Duration 返回 Span 耗时。
func (s *Span) Duration() time.Duration { return time.Since(s.startTime) }

// SetHeader 将 TraceID 写入 HTTP response header。
func SetHeader(h http.Header, traceID string) {
	h.Set("X-Trace-ID", traceID)
}

// InjectNATSHeader 将 TraceID + SpanID 注入 NATS message header。
// 若 h 为 nil 则直接返回，避免 panic。
func (s *Span) InjectNATSHeader(h map[string]string) {
	if h == nil {
		return
	}
	h["X-Trace-ID"] = s.traceID
	h["X-Span-ID"] = s.spanID
	if s.parentID != "" {
		h["X-Span-Parent-ID"] = s.parentID
	}
}

// StartNATSSpan 从 NATS header 创建 Span（消费端入口）。
func StartNATSSpan(parent context.Context, name string, h map[string]string) *Span {
	traceID := h["X-Trace-ID"]
	if traceID == "" {
		return StartSpan(parent, name)
	}
	s := &Span{
		traceID:   traceID,
		spanID:    newID(8),
		parentID:  h["X-Span-ID"],
		name:      name,
		startTime: time.Now(),
		tags:      map[string]string{},
	}
	return s
}

// NewTraceID 生成 32 位十六进制 TraceID（16 字节），与 W3C Trace Context / OpenTelemetry
// 的 trace-id 位宽一致。
// 纯 ID 函数，不创建 Span。用于需要独立 TraceID 的非 Span 场景（如 NATS header 透传）。
//
// 实现直接转发 `pkg/foundation/trace.NewTraceID`：ID 的熵源与长度只保留**一处真身**，
// 避免两处各自生成后长度再次漂移。对外 `shared/id.GenTraceID`（`trc_` + 本函数输出）
// 的随机熵也来自这里，故长度随之统一为 32 hex，与 `StartSpan` / `StartNATSSpan` 一致。
func NewTraceID() string { return trace.NewTraceID() }

func newID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// rand.Read 极少失败；失败时用纳秒时间戳作为备选源，且每消费 8 字节重取一次时钟，
		// 避免 (8*i)%64 环绕后高位字节复用 / 坍缩，保证不产生全零或高度重复的 ID。
		now := uint64(time.Now().UnixNano())
		for i := 0; i < n; i++ {
			if i > 0 && i%8 == 0 {
				now = uint64(time.Now().UnixNano()) ^ (now * 1099511628211)
			}
			// #nosec G115 -- 逐字节提取 uint64 各字节，无溢出风险；i%8 范围 [0,7]。
			b[i] = byte(now >> uint((i%8)*8))
		}
	}
	return hex.EncodeToString(b)
}
