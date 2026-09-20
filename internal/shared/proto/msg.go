// Package proto 集中定义引擎内核消息的请求体结构与内部协议 opcode。

// 本文件包含两类内容：
//   - 客户端 → 逻辑服（C2S）的**请求体结构**：ELoginRequest、
//     EResumeSessionRequest、ERankQueryRequest。业务不亲手解析这些结构
//     （handler 用 c.Bind(&业务结构体{})），故留在 internal 不对外暴露。
//   - 引擎内部协议（game↔master）：EMasterRoomRegister/Unregister/Find/TakeoverClaim（6001–6004）。

// C2S opcode（EMsgLogin/ResumeSession/RankQuery/BindUDP/UDPBindGrant）是业务编排
// 消息号时必须引用的契约，已上移到 pkg/shared/proto/msg.go；本包再导出，引擎内部调用点不变。

// 与之对应的回包见 reply.go，主动推送 opcode 见 pkg/shared/proto/push.go。
package proto

// ELoginRequest 客户端上行登录请求体（通用线类型，不绑定账号/渠道实现细节）。
// 引擎只有一种登录方式：客户端先 HTTP 到账号服换 JWT，再把 Token 交给游戏服；
// 游戏服调账号服 /auth/verify 换 Owner，业务服（game）只关心登录后的 Owner。

// 字段说明：
//   - Token：**登录唯一凭证**（账号服下发的 JWT）。
//
// 账号密码**不在此结构中**：它们只发给账号服 /auth/login，永不进入游戏长连接。
// （原 account/password 字段已删除——服务端从不读取，留着只会诱导客户端把明文密码发进游戏服。）
type ELoginRequest struct {
	Token string `json:"token"` // 登录唯一凭证（账号服下发的 JWT）

	// Encrypt 客户端声明「支持会话通道加密」（AES-256-GCM），置 true 才会在登录回包里
	// 拿到 session_key；不声明则本次会话保持明文（老客户端 / 压测与联调工具不受影响）。
	//
	// 为什么由客户端选择而不是服务端强制：加解密由客户端实现，无法解密的客户端
	// 一旦收到 key 就会在下一个下行帧上崩掉；声明制让「不支持」等价于「不加密」，
	// 而不是「登录后被踢」。WebGL 等拿不到平台 AES-GCM 的平台据此自动退回明文。
	Encrypt bool `json:"encrypt,omitempty"`
}

// EResumeSessionRequest 恢复会话请求体。
type EResumeSessionRequest struct {
	PlayerID     string `json:"player_id"`
	SessionToken string `json:"session_token"`
}

// ERankQueryRequest 排行榜统一查询请求体。
type ERankQueryRequest struct {
	Board  string `json:"board"`  // 排行榜名称（如 "level"/"score"）
	Member string `json:"member"` // 索引键（通常为 playerID）
	Start  int    `json:"start"`  // 排名区间起始（1-based）
	Stop   int    `json:"stop"`   // 排名区间结束（1-based，含）
}

// EMasterRoomRegister game→master：注册房间 owner。
// EMasterRoomUnregister game→master：注销房间 owner（可附带 takeover 目标与运行态）。
// EMasterRoomFind game→master：查询房间 owner。
// EMasterRoomTakeoverClaim game→master：新 owner 认领 takeover 暂存态。

// 这 4 条消息是引擎内建的 game↔master 协议，不由客户端发出；
// 消息号归属引擎保留段 [1, 10000]，不由业务侧分配。
const EMasterRoomRegister uint32 = 6001
const EMasterRoomUnregister uint32 = 6002
const EMasterRoomFind uint32 = 6003
const EMasterRoomTakeoverClaim uint32 = 6004
