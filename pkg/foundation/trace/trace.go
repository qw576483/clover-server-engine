// Package trace 提供分布式链路追踪的**传输无关**核心原语：
// trace_id / span_id 生成、context 传播、以及跨进程的注入（Inject）与提取（Extract）。

// # 设计定位

// 本包刻意**不感知任何具体传输**（NATS / HTTP / TCP / rpc 都不 import），
// 只以「字符串 kv 载体」为唯一契约：

//	注入侧：InjectInto(ctx, func(k, v string){ ... 写进你的协议头 ... })
//	提取侧：ExtractFrom(ctx, func(k string) string { ... 从你的协议头读 ... })

// 这样 event（跨节点消息头）、net（连接/消息上下文）、rpc（调用元数据）三层
// 各自只需写 3 行适配代码即可打通，而 trace 包本身不产生任何反向依赖。

// # 与 pkg/shared/traceid 的关系（★ 追踪上下文的唯一真身在本包）
//
//   - **trace_id / span_id 与 context 传播**的唯一真身在本包：`SpanContext`（值类型）
//     加本包私有的 context key。`traceid.Span` 往**同一个 key** 镜像写入，读侧也认本包
//     的 key ⇒ 两包双向可读（任一方 set、另一方都能 get），链路不再断在包边界上。
//     历史形态是两包各用私有 key、互不识别，同一条 trace 跨包即断链。
//   - `traceid` 只保留「带 tags 与 End 钩子的请求段对象」这一层（业务/耗时指标用），
//     不再自带独立的追踪语义。
//
// **ID 长度已统一为 32 hex（16 字节，与 W3C traceparent 对齐）**：`traceid.NewTraceID`
// 已改为直接转发本包的 `NewTraceID`（traceid 的 `StartSpan` / `StartNATSSpan` 内部 trace_id
// 本来就是 16 字节），因此本包 / traceid / 对外 `shared/id.GenTraceID` 三处长度一致。

// # 典型用法

//	// 入口（网关收包）：无上游 trace 时新建，有则续接
//	ctx, span := trace.StartSpan(ctx, "gateway.recv")
//	defer span.End()
//	span.SetAttr("msg_id", "10001")

//	// 出口（发往下游）：注入到协议头
//	trace.InjectInto(ctx, func(k, v string) { msg.Header.Set(k, v) })

//	// 下游入口：从协议头提取
//	ctx = trace.ExtractFrom(ctx, func(k string) string { return msg.Header.Get(k) })

// // 日志自动带上 trace_id
// logger.LogInfoCtx(ctx, "handle done")
package trace

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"math/bits"
	"sync/atomic"
	"time"
)

// 跨进程传播用的载体 key。

// 全部小写：HTTP/2 强制小写 header，NATS header 大小写敏感，
// 统一小写是唯一能在四种传输上都不出错的写法。Extract 侧会做大小写兼容兜底。
const (
	HeaderTraceID  = "x-trace-id"       // 全链路唯一 ID，跨进程不变
	HeaderSpanID   = "x-span-id"        // 当前段 ID，每跳重新生成
	HeaderParentID = "x-span-parent-id" // 父段 ID（上游的 span_id）
	HeaderSampled  = "x-trace-sampled"  // 采样标记："1" 采样 / "0" 不采样
)

// LogKeyTraceID 是 trace_id 写入日志时的字段名，
// 与 pkg/foundation/logger 的 globalTraceKey 保持一致。
const (
	LogKeyTraceID = "trace_id"
	LogKeySpanID  = "span_id"
)

// spanContextKey 是 context 中存放 SpanContext 的私有 key 类型。
// 用空结构体自定义类型（而非字符串）可杜绝与其它包的 key 冲突。
type spanContextKey struct{}

// SpanContext 是可跨进程传播的最小追踪单元（不可变值类型）。

// 之所以设计成值类型而非指针：它会被塞进 context 并在多 goroutine 间共享，
// 值语义天然免疫 data race，无需任何锁。
type SpanContext struct {
	TraceID  string // 32 位 hex（16 字节）
	SpanID   string // 16 位 hex（8 字节）
	ParentID string // 父 SpanID，根 span 为空
	Sampled  bool   // 是否采样
}

// Valid 报告该 SpanContext 是否携带有效的 TraceID。
func (sc SpanContext) Valid() bool { return sc.TraceID != "" }

// WithSpanContext 把 SpanContext 放入 context。
func WithSpanContext(ctx context.Context, sc SpanContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, spanContextKey{}, sc)
}

// SpanContextFrom 从 context 取出 SpanContext；不存在时返回零值（Valid() == false）。
func SpanContextFrom(ctx context.Context) SpanContext {
	if ctx == nil {
		return SpanContext{}
	}
	if sc, ok := ctx.Value(spanContextKey{}).(SpanContext); ok {
		return sc
	}
	return SpanContext{}
}

// WithTraceID 把一个已有的 trace_id 放入 context（保留原 span/parent 信息）。
// 用于上游只透传了 trace_id、没有 span 语义的简化场景。
func WithTraceID(ctx context.Context, id string) context.Context {
	sc := SpanContextFrom(ctx)
	sc.TraceID = id
	if sc.SpanID == "" {
		sc.SpanID = NewSpanID()
	}
	return WithSpanContext(ctx, sc)
}

// FromContext 返回 context 中的 trace_id；不存在时返回空串。
// 这是给 logger 等只关心 trace_id 的调用方用的最轻量入口。
func FromContext(ctx context.Context) string { return SpanContextFrom(ctx).TraceID }

// SpanIDFromContext 返回 context 中的 span_id；不存在时返回空串。
func SpanIDFromContext(ctx context.Context) string { return SpanContextFrom(ctx).SpanID }

// EnsureTraceID 保证 ctx 上一定有 trace_id：已有则原样返回，
// 没有则生成一个新的根 SpanContext。返回新 ctx 与其 trace_id。

// 适合放在所有入口（网关收包 / HTTP 中间件 / 定时任务起点）做兜底，
// 避免下游日志出现空 trace_id。
func EnsureTraceID(ctx context.Context) (context.Context, string) {
	if sc := SpanContextFrom(ctx); sc.Valid() {
		return ctx, sc.TraceID
	}
	sc := SpanContext{TraceID: NewTraceID(), SpanID: NewSpanID(), Sampled: true}
	return WithSpanContext(ctx, sc), sc.TraceID
}

// ID 生成
// randState 是随机源的分片状态。

// 为什么不直接用 crypto/rand：每次调用都要走系统调用（Linux getrandom / Windows
// RtlGenRandom），在每秒数十万次埋点的游戏服热路径上是明确的性能瓶颈。
// 这里改用「crypto/rand 播种一次 + xorshift64* 快速推进 + 原子计数器混入」的方案：
// 无锁、每次 ID 生成只有几十纳秒，且熵源来自 crypto/rand 因此不可预测。
// trace_id 只需全局唯一 + 不可猜测，不需要密码学强度，这个取舍是安全的。
var (
	randSeed atomic.Uint64 // xorshift64* 状态
	randSalt atomic.Uint64 // 单调计数器，杜绝同一时刻的碰撞
)

func init() {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 极少失败；退化为纳秒时钟 + 地址熵，保证不产生全零种子。
		n := uint64(time.Now().UnixNano())
		binary.LittleEndian.PutUint64(b[0:8], n)
		binary.LittleEndian.PutUint64(b[8:16], n*1099511628211)
	}
	seed := binary.LittleEndian.Uint64(b[0:8])
	if seed == 0 {
		seed = 0x9E3779B97F4A7C15 // xorshift 的状态不能为 0，否则永久输出 0
	}
	randSeed.Store(seed)
	randSalt.Store(binary.LittleEndian.Uint64(b[8:16]))
}

// next64 返回一个伪随机 uint64（无锁、并发安全）。

// CAS 失败时不自旋重试而是直接用本地推进值：多个 goroutine 竞争时，
// 各自拿到的中间值也是互不相同的合法随机数，再异或上单调 salt 保证唯一性。
// 这样即使高并发也没有任何自旋开销。
func next64() uint64 {
	old := randSeed.Load()
	x := old
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	randSeed.CompareAndSwap(old, x)
	salt := randSalt.Add(0x9E3779B97F4A7C15)
	// 乘以奇数常量做雪崩扩散，再与 salt 旋转异或，避免低位规律性。
	return (x * 0x2545F4914F6CDD1D) ^ bits.RotateLeft64(salt, 32)
}

// NewTraceID 生成 32 位十六进制的 trace_id（16 字节），与 W3C traceparent 长度一致。
//
// ★ **本函数是 trace_id 的唯一真身**：`pkg/shared/traceid.NewTraceID` 直接转发到这里
// （对外 `shared/id.GenTraceID` 也经它取熵），改本函数的长度/熵源会同时影响三处。
func NewTraceID() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], next64())
	binary.BigEndian.PutUint64(b[8:16], next64())
	return hex.EncodeToString(b[:])
}

// NewSpanID 生成 16 位十六进制的 span_id（8 字节）。
func NewSpanID() string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], next64())
	return hex.EncodeToString(b[:])
}
