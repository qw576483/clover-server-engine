package event

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// maxHTTPBodySize 控制面请求体字节上限：每个用 Bind 的业务 handler 都能被灌爆内存，
// 必须封顶（与 MaxHeaderBytes 同为 1MB 量级）。
const maxHTTPBodySize = 1 << 20

// HTTPHandler 「HTTP 事件」处理函数。统一签名：拿到 HTTPCtx 做逻辑，用 c.Reply 回包，返回 error。
//
// 与 On(msgID, Handler) 的根本区别：HTTP 事件**无会话 / 无玩家对象上下文**——
// 请求是一次性、无状态的，handler 只能从请求参数（Query / Param / Bind）里拿到目标标识
// （如 player_id / account），再经 data / entity / objstore 等通用地基「按 ID 改某人数据」。
// 这正是 GMT / 本地后台（策划面板 POST /admin/xxx）的诉求：没有在线连接，也能改数据。
type HTTPHandler func(c *HTTPCtx) error

// HTTPCtx 「HTTP 事件」派发给业务 handler 的上下文（无状态、无玩家对象）。
//
// 框架在每次 HTTP 请求时构造并调用对应 handler；业务只取请求、改数据、回包，
// 不直接碰网关 / 账号 / 数据库细节（数据访问经 data / entity / objstore 等注入闭包）。
//
// 与 Ctx（On/OnEvent 用，带 Owner / 玩家对象 / Push / Emit）不同，
// 本 Ctx **没有 Owner() / Push() / Emit()** —— 它就是一个 HTTP 请求-响应通道。
type HTTPCtx struct {
	w      http.ResponseWriter
	r      *http.Request
	params map[string]string
	body   []byte
	status int

	mu           sync.Mutex
	replied      bool
	bodyReadDone bool  // 标记 Body 已读完并缓存，防止重复读导致 io.EOF / 空切片
	bodyErr      error // 首次读取失败的错误，缓存后每次 Bind 都如实返回
}

// Req 返回底层 *http.Request。
func (c *HTTPCtx) Req() *http.Request { return c.r }

// Context 返回请求的 context.Context（含超时 / 取消）。
func (c *HTTPCtx) Context() context.Context { return c.r.Context() }

// Query 取 URL 查询参数（?key=val）。
func (c *HTTPCtx) Query(name string) string { return c.r.URL.Query().Get(name) }

// Param 取路径参数（OnHTTP 注册 ":name" 捕获段）。
func (c *HTTPCtx) Param(name string) string { return c.params[name] }

// Bind 把请求体按 JSON 解出到 dst；业务自行定义请求结构体。
// 请求体惰性读取：首次调用才从 Body 读出并缓存。
//
// bodyReadDone 只在读取成功后置位：读取失败时 body 仍是空切片，
// 若一并置位，后续 Bind 会对空 body 做反序列化，把真正的 IO 错误
// 掩盖成「解析成功，字段全零」。首次错误缓存在 bodyErr 中反复返回。
func (c *HTTPCtx) Bind(dst any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.bodyReadDone && c.bodyErr == nil {
		// 多读 1 字节探测超限：读到 maxHTTPBodySize+1 即说明请求体超限，拒绝并缓存错误。
		b, err := io.ReadAll(io.LimitReader(c.r.Body, maxHTTPBodySize+1))
		if err != nil {
			c.bodyErr = err
			return err
		}
		if len(b) > maxHTTPBodySize {
			c.bodyErr = fmt.Errorf("http: request body too large (limit %d bytes)", maxHTTPBodySize)
			return c.bodyErr
		}
		c.body = b
		c.bodyReadDone = true
	}
	if c.bodyErr != nil {
		return c.bodyErr
	}
	return ujson.Unmarshal(c.body, dst)
}

// SetStatus 设置回包 HTTP 状态码（默认 200；须在 Reply 前调用）。
func (c *HTTPCtx) SetStatus(code int) {
	c.mu.Lock()
	c.status = code
	c.mu.Unlock()
}

// Reply 以 JSON 编码 v 并回包（状态码取 SetStatus 指定值，否则 200）。同一 Ctx 仅首次生效。
func (c *HTTPCtx) Reply(v any) error {
	b, err := ujson.Marshal(v)
	if err != nil {
		return err
	}
	c.writeJSON(b)
	return nil
}

// ReplyRaw 以原始字节回包（自定义编码 / 状态码）。
func (c *HTTPCtx) ReplyRaw(code int, body []byte) {
	c.mu.Lock()
	c.status = code
	c.mu.Unlock()
	c.writeJSON(body)
}

func (c *HTTPCtx) writeJSON(body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replied {
		return
	}
	c.replied = true
	code := c.status
	if code == 0 {
		code = http.StatusOK
	}
	c.w.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.w.WriteHeader(code)
	_, _ = c.w.Write(body)
}

// httpRoute 一条 HTTP 事件路由（pattern + handler）。
type httpRoute struct {
	pattern string
	h       HTTPHandler
}

// matchRoute 把请求路径与注册 pattern 做段级匹配，支持 ":name" 捕获段。
// 例：pattern "/admin/player/:pid" 匹配 "/admin/player/123" 且 params["pid"]="123"。
// 段数不一致或静态段不相等即不匹配。
func matchRoute(pattern, path string) (map[string]string, bool) {
	pp := strings.Split(strings.Trim(pattern, "/"), "/")
	sp := strings.Split(strings.Trim(path, "/"), "/")
	if len(pp) != len(sp) {
		return nil, false
	}
	params := make(map[string]string)
	for i, seg := range pp {
		if strings.HasPrefix(seg, ":") {
			params[seg[1:]] = sp[i]
			continue
		}
		if seg != sp[i] {
			return nil, false
		}
	}
	return params, true
}

// errorBody 把错误文本包成 JSON {"error":"..."}（handler 返回 error 时由 serveHTTP 调用）。
func errorBody(msg string) []byte {
	b, _ := ujson.Marshal(map[string]string{"error": msg})
	return b
}
