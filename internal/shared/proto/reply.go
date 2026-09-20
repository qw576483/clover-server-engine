// Package proto（消息分类：逻辑服 → 客户端，回包 / reply）

// 本文件集中定义「逻辑服响应某次客户端请求」的引擎内核回包（S2C reply），以及其结构。
// 请求侧见 msg.go，主动推送见 push.go。

// 回包不携带独立的回包消息号：客户端帧按 requestID 配对（回包帧 msgID 恒为 0），
// 客户端按 requestID 对号入座。错误回包使用特殊值 0xFFFFFFFF 供客户端识别。
package proto

import "encoding/json"

// ELoginReply 登录回包体。
// Owner 为「已认证对象标识」（如账号名、玩家 UID），网关用该字段 generically 绑定会话
// （见 auth.ExtractOwnerID），因此登录结构由 base 定义、业务无需解析包体。
// Success/Err 仅用于客户端展示；Token 为登录成功后下发的 token。
type ELoginReply struct {
	Owner   string `json:"owner"`
	Token   string `json:"token,omitempty"`
	Success bool   `json:"success,omitempty"`
	Err     string `json:"err,omitempty"`

	// SessionKey 会话密钥（base64 编码的 32B AES-256 密钥，供网关启用通道加密）。
	//
	// 生效条件：客户端在 ELoginRequest.Encrypt 声明支持时才有值（登录 handler 现生成、
	// 网关 auth.ExtractSessionKey 提取并为本连接启用 AES-GCM）。未声明时为**空**，
	// 本次会话保持明文 —— 压测 / 联调 / 网页工具都不声明，行为与加此能力前一致。
	//
	// 该帧自身**始终明文**发送（客户端要靠它才能解密后续帧），故它只能作为
	// 「TLS 之上的第二层」，不是"链接层加密的替代品"。
	//
	// 客户端**不要**把它当断线重连凭证：重连凭证是
	// `EPushPlayerFullSync.session_token`（由 sessiontoken.Store 按 playerID 生成）。
	SessionKey string `json:"session_key,omitempty"`
}

// EResumeSessionReply 恢复会话回包体。
// Success=true 时客户端无需做任何事（会话已恢复，数据未变）。
// Success=false 时 err 说明原因（session expired / token mismatch），客户端跳转登录。
type EResumeSessionReply struct {
	PlayerID string `json:"player_id"`
	Success  bool   `json:"success"`
	Err      string `json:"err,omitempty"`

	// Owner 归属对象标识（账号）。
	//
	// **必须下发，不是可选信息**：网关（gwcore）只在「上游回包能被
	// auth.ExtractOwnerID 解出非空 owner」时才把连接绑回 owner。而恢复会话走的是
	// **一条新连接**（没有任何登录动作），若不在这里补上 owner，网关永远不会绑定它，
	// 该连接随即被登录门禁拒掉后续**每一条**业务消息（401 unauthenticated）——
	// 表现为「ResumeSession 成功，但连接立刻失能」，且客户端侧只看到一个
	// Net.OnUnauthorized 事件，极易被误判为网络抖动。
	Owner string `json:"owner,omitempty"`
}

// EErrorReply 通用错误回包体（逻辑服派发遇到 handler 返回 error 时回包）。
// 错误回包消息号使用特殊值 0xFFFFFFFF（见 event/logic.go 的 defaultErrorMsgID），
// 不属于任何区间，客户端据此识别错误回包。
//
// Code 为机器可读错误码（取值见 pkg/shared/proto 的 ErrCode* 常量）：客户端据此做统一处理
// （如 401 未认证 → 回到登录流程），无需匹配错误文案——文案会变，码不会。
// Code 缺省（0）表示未分类错误，此时客户端只能按 Err 文案展示。
type EErrorReply struct {
	Err  string `json:"err"`
	Code int32  `json:"code,omitempty"`
}

// ERankEntry 排行榜单条记录（Rank 为 1-based）。
type ERankEntry struct {
	Member string          `json:"member"`
	Score  float64         `json:"score"`
	Rank   int             `json:"rank"`
	Extra  json.RawMessage `json:"extra,omitempty"`
}

// ERankQueryReply 排行榜统一查询回包体。
// Member 为客户端传入的索引键在该榜中的排名+数据（未上榜时 MemberFound=false）。
// Range 为 [Start,Stop] 排名区间的条目列表。
// Total 为榜内总人数。
type ERankQueryReply struct {
	Board       string       `json:"board"`
	Member      ERankEntry   `json:"member"`
	MemberFound bool         `json:"member_found"`
	Range       []ERankEntry `json:"range"`
	Total       int          `json:"total"`
	Err         string       `json:"err,omitempty"`
}
