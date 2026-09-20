// Package event 是 clover 通用「事件 + 事件驱动逻辑服内核」的公开门面。
//
// 业务只需：
//
//	g.OnMsg(msgID, func(c event.Ctx) error { ... g.Reply(c, v); return nil })
//	g.OnEvent("player.OnCreateRole", func(c event.Ctx) error { ... })
//
// 框架负责解信封、派发、回包与玩家注入。
package event

import (
	"context"
	"net/http"
	"sync"
)

// Handler 事件驱动业务处理函数：拿到 Ctx 做逻辑、回包、返回 error。
// 返回非 nil 时框架自动以 EErrorReply 回包；返回 nil 且未调用 MarkReplied 则不回包（fire-and-forget）。
type Handler func(c Ctx) error

// Ctx 派发给业务 handler 的上下文：框架在派发前已完成解信封、加载玩家（若可）、注入，
// 业务只需取用。本接口由 internal 的 *Ctx 满足（见 compile-time 断言）。
type Ctx interface {
	// SetNoAutoReply 抑制框架自动给本次请求回包（业务已自行回包或本就不回包时调用）。
	SetNoAutoReply()
	// NoAutoReply 是否被设为抑制框架自动回包。
	NoAutoReply() bool
	// SetNoPush 关闭「数据变更自动推送」。
	SetNoPush()
	// NoPush 是否已关闭数据变更自动推送。
	NoPush() bool
	// Context 返回底层 context（框架已注入玩家对象等）。
	Context() context.Context
	// TraceID 返回本次请求的全链路追踪 ID（未携带时返回空串）。
	TraceID() string
	// SpanID 返回当前处理阶段的 Span ID（用于定位具体哪一步）。
	SpanID() string
	// Account 返回当前连接的账号名。
	Account() string
	// Line 返回消息来源的线路标识（协议类型）。
	Line() string
	// PlayerID 返回当前连接的角色 ID；未设置时为空串。
	PlayerID() string
	// IsLoggedIn 当前连接是否已登录（判据是账号名非空，与 PlayerID 无关）。
	// 注意：PlayerID 是「角色 ID」，创角 / 进入游戏后才有值 —— 不要用它判断登录态。
	IsLoggedIn() bool
	// RequestID 返回本次请求的关联 ID（推送消息为 0）。
	RequestID() uint32
	// SetPlayerID 设置当前连接的角色 ID，并持久化到连接级 KV、同步网关索引。
	SetPlayerID(pid string)
	// SetConnValue 向当前连接的 KV 存储写入自定义数据（同连接后续请求可读）。
	SetConnValue(key string, value any)
	// ConnValue 从当前连接的 KV 存储读取自定义数据。
	ConnValue(key string) (any, bool)
	// SetConnBag 注入连接级 KV 存储（由 Game 层调用，业务无需关心）。
	SetConnBag(bag *sync.Map)
	// MsgID 返回本次请求的消息号。
	MsgID() uint32
	// ConnID 返回来源连接 ID（回包路由用，业务一般无需关心）。
	ConnID() string
	// Body 返回请求体的只读副本（修改不影响其他 handler）。
	Body() []byte
	// BodyRef 返回原始请求体引用（零分配，只读场景，勿修改）。
	BodyRef() []byte
	// BindMsg 将请求体按 JSON 反序列化到 dst（业务自定义请求结构体）。
	BindMsg(dst any) error
	// Payload 取回领域事件载荷（OnEvent handler 用）。
	Payload() any
	// MarkReplied 标记本次请求已回包（仅存储回包，先到先得；业务常用 Game.Reply 写入）。
	MarkReplied(body []byte)
	// BindEvent 将领域事件载荷反序列化到 v（优先类型断言赋值，失败回退 JSON）。
	BindEvent(v any) error
	// TargetID 返回推送路由 ID：PlayerID 优先，未设置时回退到 Account。
	TargetID() string
}

// HTTPHandler 「HTTP 事件」处理函数（handler 签名 func(c HTTPCtx) error）。
//
// 与 OnMsg/OnEvent 的 Handler 不同：HTTP 事件**无会话、无玩家上下文**，
// 请求一次性、无状态，适用于 GMT / 运营后台（策划面板 POST /admin/xxx）。
type HTTPHandler func(c HTTPCtx) error

// HTTPCtx 「HTTP 事件」派发给业务 handler 的上下文接口（无状态、无玩家对象）。
// 由 internal 的 *HTTPCtx 满足（见编译期断言）。
type HTTPCtx interface {
	// Req 返回底层 *http.Request。
	Req() *http.Request
	// Context 返回请求的 context.Context（含超时 / 取消）。
	Context() context.Context
	// Query 取 URL 查询参数（?key=val）。
	Query(name string) string
	// Param 取路径参数（OnHTTP 注册 ":name" 捕获段）。
	Param(name string) string
	// Bind 把请求体按 JSON 解出到 dst。
	Bind(dst any) error
	// SetStatus 设置回包 HTTP 状态码（默认 200；须在 Reply / ReplyRaw 前调用）。
	SetStatus(code int)
	// Reply 以 JSON 编码 v 并回包。同一 Ctx 仅首次生效。
	Reply(v any) error
	// ReplyRaw 以原始字节回包（自定义编码 / 状态码）。
	ReplyRaw(code int, body []byte)
}
