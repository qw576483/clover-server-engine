// Package server 提供账号服的 HTTP 传输层。
//
// 与 domain/{master,log}/server 同构（那两个把 state 挂到自有 TCP 服务端），
// 差别只在传输形态：账号服是 HTTP JSON，监听由角色内核的控制面负责，
// 所以这里不 Create/Listen，只 **Register 路由**（见 Register）。
//
// 本包有两件事，别的一概不做：
//   - 路由与请求/响应编解码（参数校验）；
//   - 领域错误 → HTTP 状态码映射（业务层 state 不感知 HTTP）。
package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"clover-server-engine/internal/domain/auth/state"
	"clover-server-engine/internal/foundation/ophttp"
	"clover-server-engine/internal/transport/event"
	"clover-server-engine/pkg/foundation/logger"
)

// Registrar 把 HTTP 路由挂到角色内核的控制面（*app.AuthGame 满足该接口）。
type Registrar interface {
	OnHTTP(pattern string, h event.HTTPHandler)
}

// Option 注册选项（让宿主注入配置而不必改本包签名）。
type Option func(*registerOptions)

type registerOptions struct {
	ratePerSec int
	rateBurst  int
}

// WithIPRateLimit 设置 /auth/* 的 per-IP 限流（perSec<=0 或 burst<=0 时回落内置默认）。
func WithIPRateLimit(perSec, burst int) Option {
	return func(o *registerOptions) {
		o.ratePerSec = perSec
		o.rateBurst = burst
	}
}

// Register 把账号服四条路由挂到控制面。
//
// 调用时机与 master / log 一致：业务挂载前完成，保证首批请求就能命中 handler。
//
// 凭据入口（/auth/signup、/auth/login、/auth/verify）一律套 per-IP 限流：
// 域内的 per-account 撞库防护按账号计数，换账号名即可绕过，必须有来源维度的速率约束。
// /auth/health 不套限流：它是探针端点、无凭据面，限流会让编排系统的健康检查被误判为不健康。
func Register(r Registrar, svc *state.Service, opts ...Option) {
	o := registerOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	limiter := newIPLimiter(o.ratePerSec, o.rateBurst)
	r.OnHTTP(state.PathSignup, adapt(limiter.guard(state.PathSignup, func(w http.ResponseWriter, req *http.Request) {
		handleSignup(w, req, svc)
	})))
	r.OnHTTP(state.PathLogin, adapt(limiter.guard(state.PathLogin, func(w http.ResponseWriter, req *http.Request) {
		handleLogin(w, req, svc)
	})))
	r.OnHTTP(state.PathVerify, adapt(limiter.guard(state.PathVerify, func(w http.ResponseWriter, req *http.Request) {
		handleVerify(w, req, svc)
	})))
	r.OnHTTP(state.PathHealth, adapt(handleHealth))
}

// statusOf 领域错误分类 → HTTP 状态码。
//
// 放在传输层是刻意的：state 只回答「错在哪一类」，换传输形态时不用动业务代码。
func statusOf(kind state.Kind) int {
	switch kind {
	case state.KindBadParam:
		return http.StatusBadRequest
	case state.KindAccountExists:
		// 与 KindBadParam 同为 400：状态码也不能成为枚举信道。
		// 若这里回 409、「参数不合法」回 400，攻击者用一个格式合法的账号试注册即可区分
		// 「已存在」与「不存在」（后者会直接建号成功）。真实原因只进服务端日志。
		return http.StatusBadRequest
	case state.KindBadCredential, state.KindTicketInvalid:
		// 凭证类一律 401：不区分「账号不存在 / 密码错 / 票据无效」，避免账号枚举与探测。
		return http.StatusUnauthorized
	case state.KindAccountLocked:
		return http.StatusTooManyRequests
	case state.KindChannelUnsupported:
		// 501 而非 400：这是「服务端没接渠道」，不是客户端请求错——
		// 区分开，业务才不会去查渠道 SDK。
		return http.StatusNotImplemented
	case state.KindChannelUnavailable:
		// 渠道存储/依赖不可用属「服务端暂时故障（可重试）」：语义应为 503。
		// 此前无显式 case，静默走 default 映射为 500。
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// handleSignup 注册。成功后直接签发 token（注册即登录），客户端无需再调一次登录。
func handleSignup(w http.ResponseWriter, r *http.Request, svc *state.Service) {
	if !requirePost(w, r) {
		return
	}
	req, ok := decodeCred(w, r)
	if !ok {
		return
	}
	resp, derr := svc.Signup(r.Context(), req.Account, req.Password)
	writeToken(w, resp, derr)
}

// handleLogin 登录：账号密码或渠道票据，由 state.Login 按请求体分派。
//
// 填入 req.ClientIP（非线协议字段，json:"-"）：域内撞库防护需要「账号 + 来源」双维度计数，
// 而来源只有传输层知道。取值规则与限流器同源（clientIP，见 ratelimit.go）——
// 刻意不采信 X-Forwarded-For，否则攻击者每个请求换一个来源即可绕过来源维度。
func handleLogin(w http.ResponseWriter, r *http.Request, svc *state.Service) {
	if !requirePost(w, r) {
		return
	}
	req, ok := decodeCred(w, r)
	if !ok {
		return
	}
	req.ClientIP = clientIP(r)
	resp, derr := svc.Login(r.Context(), req)
	writeToken(w, resp, derr)
}

// handleVerify 校验 token：游戏服每次登录都会调本接口换取 owner（见 auth.verify_addr）。
//
// 返回契约：token 无效（过期 / 签名不符 / 伪造）不是服务端错误，
// 用 200 + valid=false 表达，便于游戏服统一解析；5xx 留给账号服自身故障。
func handleVerify(w http.ResponseWriter, r *http.Request, svc *state.Service) {
	if !requirePost(w, r) {
		return
	}
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, state.MaxBodyBytes)).Decode(&req); err != nil {
		// 非预期分支（坏报文）必须留日志，否则反向探测/集成错误不可排查。
		logger.Warnf("auth: /auth/verify decode request failed (remote=%s): %v", r.RemoteAddr, err)
		ophttp.JSON(w, http.StatusBadRequest, state.VerifyResp{Err: "invalid json body"})
		return
	}
	ophttp.JSON(w, http.StatusOK, svc.Verify(req.Token))
}

// handleHealth 存活探针。
func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		// 拒绝路径留日志：健康探针被配错方法时编排系统只会看到「不健康」，
		// 有这行日志才能一眼看出是探针配错而不是账号服真挂了。
		logger.Warnf("auth: /auth/health rejected: GET only (got %s from %s)", r.Method, r.RemoteAddr)
		ophttp.JSON(w, http.StatusMethodNotAllowed, state.TokenResp{Err: "GET only"})
		return
	}
	ophttp.JSON(w, http.StatusOK, state.HealthResp{OK: true, Service: "auth", Time: time.Now().Unix()})
}

// writeToken 统一回包注册 / 登录结果：领域错误按 Kind 映射状态码，成功回 200 + token。
func writeToken(w http.ResponseWriter, resp *state.TokenResp, derr *state.Error) {
	if derr != nil {
		ophttp.JSON(w, statusOf(derr.Kind), state.TokenResp{Err: derr.Text})
		return
	}
	ophttp.JSON(w, http.StatusOK, resp)
}

// requirePost 校验请求方法；非 POST 时写响应并返回 false。
//
// 拒绝路径留日志：非 POST 打到凭据端点通常意味着客户端 / 探针用错了请求形态，
// 静默 405 会让这类集成问题只能靠抓包发现。
func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodPost {
		return true
	}
	logger.Warnf("auth: %s rejected: POST only (got %s from %s)", r.URL.Path, r.Method, r.RemoteAddr)
	ophttp.JSON(w, http.StatusMethodNotAllowed, state.TokenResp{Err: "POST only"})
	return false
}

// decodeCred 解析注册 / 登录请求体（仅做 JSON 解码，字段校验交给 state）。
//
// 这里刻意不校验 account/password 非空：/auth/login 同时承载账号密码登录与渠道登录
// （{channel, ticket}），后者本就没有 account/password——在此拦截会让渠道登录永远进不去。
func decodeCred(w http.ResponseWriter, r *http.Request) (state.CredReq, bool) {
	var req state.CredReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, state.MaxBodyBytes)).Decode(&req); err != nil {
		// 注册/登录入口的坏报文必须留日志（此前直接回 400，无任何记录）。
		logger.Warnf("auth: %s decode request failed (remote=%s): %v", r.URL.Path, r.RemoteAddr, err)
		ophttp.JSON(w, http.StatusBadRequest, state.TokenResp{Err: "invalid json body"})
		return req, false
	}
	return req, true
}

// adapt 把既有 (w http.ResponseWriter, r *http.Request) 形态的 handler
// 适配成控制面的 event.HTTPHandler：handler 照常写响应，返回后一次性回包。
//
// 这样账号 handler 的状态码 / body 细节 / 4KiB 上限原样保留，只换传输外壳。
func adapt(h func(w http.ResponseWriter, r *http.Request)) event.HTTPHandler {
	return func(c *event.HTTPCtx) error {
		w := &bufferedResponse{code: http.StatusOK}
		h(w, c.Req())
		c.ReplyRaw(w.code, w.body.Bytes())
		return nil
	}
}

// bufferedResponse 承接 handler 的写响应（缓冲到内存），最后由 HTTPCtx 统一回包。
// 账号 handler 只写 JSON body（Content-Type 由 HTTPCtx.ReplyRaw 统一设置），
// 故 Header() 返回一个被忽略的 header 集即可。
type bufferedResponse struct {
	code int
	body bytes.Buffer
}

func (w *bufferedResponse) Header() http.Header         { return http.Header{} }
func (w *bufferedResponse) WriteHeader(code int)        { w.code = code }
func (w *bufferedResponse) Write(b []byte) (int, error) { return w.body.Write(b) }
