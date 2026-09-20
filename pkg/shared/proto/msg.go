// Package proto（消息分类：客户端 → 服务器，C2S opcode）

// 本文件集中定义引擎内核的客户端上行 opcode，业务在 def 包中以
// `const MsgXxx = proto.EMsgXxx` 复用，并据此编排自己的消息号。
// 对应的回包体与推送体见 push.go；请求体结构体属引擎内部契约，留在 internal。
package proto

// 客户端 → 逻辑服：登录请求
// EMsgLogin 客户端 → 逻辑服：登录请求 opcode。
// 引擎唯一的登录入口：游戏服收到后调账号服 /auth/verify 换 owner（见 internal/app 的 iauth.Handler）。
// 属引擎 C2S 区间（=2）。
//
// 号位 1 曾经是 EMsgSignup（注册）：注册已**完全移到账号服 HTTP**（POST {账号服}/auth/signup），
// 游戏服不再接收注册报文，该号位**作废但保留**——号位是协议契约的一部分，
// 回收复用会让旧客户端/旧配置发出语义完全不同的报文，且极难排查。
const EMsgLogin uint32 = 2

// 客户端 → 逻辑服：恢复会话（重连）
// EMsgResumeSession 客户端 → 逻辑服：WS 重连后请求恢复会话。
// 属引擎 C2S 区间（=3）。session_token 取自上次 EPushPlayerFullSync 下发值。
// 引擎内部校验：token 匹配 → 恢复 → 不发全量同步（数据未变），仅回 EResumeSessionReply{success=true}。
// token 不匹配（被顶号/过期）→ 回 EResumeSessionReply{success=false} + 踢连接。
const EMsgResumeSession uint32 = 3

// 客户端 → 逻辑服：排行榜 - 统一查询
// EMsgRankQuery 客户端 → 逻辑服：排行榜统一查询 opcode。
// 属引擎 C2S 区间（=4）。客户端传排行榜名 + 索引键（自己的ID）+ 排名区间，
// 服务器返回该成员的排名/数据 + [Start,Stop] 区间的条目。
const EMsgRankQuery uint32 = 4

// 客户端 ↔ 网关：UDP 端点绑定（引擎内部帧，网关拦截，不转发逻辑服）
// EMsgBindUDP 客户端 → 网关：上报不可靠推送用的常驻 UDP 端点。
// 属引擎 C2S 区间（=5）。由网关直接处理，不转发逻辑服：
//   - 体 = 网关此前下发的绑定令牌（EMsgUDPBindGrant），非自报账号——防止攻击者
//     谎报他人账号把不可靠推送劫持到自己的 socket；
//   - 网关校验令牌有效后，把该 UDP 数据包的来源地址登记为令牌所属 owner 的最新
//     不可靠推送端点（裸 UDP 0x55）；同令牌重绑 = 替换为最新来源地址。
//
// 适用场景：TCP/WS 连接的玩家没有不可靠通道，登录后客户端另起一个
// 常驻 UDP socket 并周期性上报绑定帧（兼 NAT 保活），服务器不可靠推送即可直接打该端点。
const EMsgBindUDP uint32 = 5

// EMsgUDPBindGrant 网关 → 客户端：下发一次性不可靠通道绑定令牌（S2C 引擎帧，=6）。
// 属引擎 S2C 区间，由网关在「会话绑定 owner 成功」后直接下发（不经过逻辑服，requestID=0）：
//   - 体 = 随机绑定令牌（十六进制串，会话唯一）；
//   - 客户端登录成功后须等收到本帧，再以令牌上报 EMsgBindUDP，网关才登记其 UDP 端点；
//   - 令牌与会话同生命周期：会话断开即撤销；同会话换绑 owner 或重连会重新下发。
const EMsgUDPBindGrant uint32 = 6

// EMsgQueuePosition 网关 → 客户端：排队位置通知（S2C 引擎帧，=7）。
// 属引擎 S2C 区间，由**网关直发**（不经过逻辑服，requestID=0）——排队中的连接尚未建立会话，
// 「逻辑服 → 网关 → 客户端」那条路走不通：
//   - 体 = EQueuePositionNotify（JSON）；
//   - 入队时下发一次，队列前进后按固定间隔刷新（位置未变则不下发）；
//   - 排队未启用（gateway.queue_cap=0）时不会出现本帧：超限直接拒绝并断连。
const EMsgQueuePosition uint32 = 7

// EQueuePositionNotify 排队位置通知体（EMsgQueuePosition 的 body，JSON 编码）。
// 客户端据此渲染「您前面还有 N 人」；放行后不再下发本帧（登录回包到达即表示已放行）。
type EQueuePositionNotify struct {
	Ahead  int   `json:"ahead"`  // 前面还有多少人（0 = 已到队首，下一个就放行）
	Total  int   `json:"total"`  // 当前队列总人数（含自己）
	Ticket int64 `json:"ticket"` // 排队编号（入队序号，单调递增；仅用于展示与排障，不参与排队逻辑）
}

// EMsgError 通用错误回包 opcode（body 为 EErrorReply{err, code}）。
//
// 客户端据该特殊值识别「本条不是成功回包，而是失败」。不属于任何消息号区间：
// 普通回包按 requestID 配对且 msgID 恒为 0，失败语义需要额外一位来区分。
//
// 来源有两处，客户端无需区分（都按 requestID 配对）：
//   - 逻辑服：handler 或前置钩子返回 error；
//   - 网关：登录门禁拒绝（未登录且不在免登录白名单，见 gwcore.Config.AuthDisabled）。
const EMsgError uint32 = 0xFFFFFFFF
