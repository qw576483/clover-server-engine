// Package state 定义账号服（server_type=auth）的核心能力与对外线协议。
//
// 分层与 domain/{master,log} 同构：
//   - 本包 = 业务核心（注册 / 登录 / 验签 / 渠道登录 + 撞库防护）与线协议契约；
//   - server 子包 = 传输适配（HTTP 路由与状态码映射）；
//   - client 子包 = 调用侧（game 校验登录凭证）；
//   - 父包 auth = 域级配置与业务扩展点（渠道票据校验器）。
//
// 与 master / log 的差别只有传输形态：它们是自有 TCP 报文（wire.go 里是消息号），
// 账号服对外是 HTTP JSON（这里就是路径常量 + 请求/响应载体）。
package state

// HTTP 路径：账号服对客户端公开的四条接口。
//
// 客户端（经账号服 HTTP）与游戏服（/auth/verify）都走这几条；
// 消息通道（auth.rpc_listen）另有一套业务消息号，登录校验**不走**它。
const (
	// PathSignup 注册；成功后直接签发 token（注册即登录）。
	PathSignup = "/auth/signup"
	// PathLogin 登录；按请求体是否带 channel 分派「账号密码」或「渠道票据」。
	PathLogin = "/auth/login"
	// PathVerify 校验 token 换 owner；游戏服每次登录都会调。
	PathVerify = "/auth/verify"
	// PathHealth 存活探针。
	PathHealth = "/auth/health"
)

const (
	// MaxBodyBytes 请求体上限。注册 / 登录只有两个短字段，超长一律视为异常（防内存放大）。
	MaxBodyBytes = 4 << 10
	// MaxChannelLen 渠道标识的长度上限，与 account_channel.channel（VARCHAR(32)）对齐。
	// 按字符数（rune）而非字节数判断，保证多字节渠道名也能与 MySQL 的语义一致。
	MaxChannelLen = 32
)

// CredReq 注册 / 登录请求体。
//
// 两种形态二选一（/auth/login 按「带没带 channel」分派）：
//   - 账号密码：{account, password}
//   - 渠道登录：{channel, ticket}——ticket 由渠道 SDK 签发，交给业务注册的 ChannelVerifier 校验。
type CredReq struct {
	Account  string `json:"account"`
	Password string `json:"password"`

	// Channel 渠道标识（如 wechat / qq / steam / apple），非空即走渠道登录。
	Channel string `json:"channel"`
	// Ticket 渠道票据（微信 code / Steam ticket / 自建账号中心凭证）。
	Ticket string `json:"ticket"`

	// ClientIP 本次请求的来源 IP，**不属于线协议**（json:"-" 使其无法由请求体注入）。
	//
	// 由传输层在调用前填入（见 domain/auth/server 的 handleLogin），域内撞库防护据此做
	// 「账号 + 来源」双维度计数。刻意用 json:"-" 而不是新开一个线字段：
	// 让攻击者无法通过伪造请求体把失败计数打到别人头上（那会把防护变成「陷害信道」）。
	// 留空时的语义见 guard.go 的 guardSourceKey。
	ClientIP string `json:"-"`
}

// TokenResp 注册 / 登录响应体。
// 字段命名与引擎内部 proto.ELoginReply 保持一致（owner / token / success / err），
// 让客户端两处解析逻辑可以复用同一套心智。
type TokenResp struct {
	Owner   string `json:"owner,omitempty"`
	Token   string `json:"token,omitempty"`
	Success bool   `json:"success"`
	Err     string `json:"err,omitempty"`
	// Exp token 过期时间（Unix 秒）。客户端可据此提前刷新，避免到期才发现。
	Exp int64 `json:"exp,omitempty"`
}

// VerifyResp /auth/verify 响应体。
//
// token 无效（过期 / 签名不符 / 伪造）**不是服务端错误**：用 200 + Valid=false 表达，
// 便于游戏服统一解析；5xx 留给账号服自身故障。
type VerifyResp struct {
	Valid bool   `json:"valid"`
	Owner string `json:"owner,omitempty"`
	Exp   int64  `json:"exp,omitempty"`
	Err   string `json:"err,omitempty"`
}

// HealthResp /auth/health 响应体。
type HealthResp struct {
	OK      bool   `json:"ok"`
	Service string `json:"service"`
	Time    int64  `json:"time"`
}
