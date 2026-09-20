// Package proto（消息分类：逻辑服 → 客户端，主动推送 / push + 传输模式）

// 本文件集中定义引擎内核的推送 opcode（S2C push），以及业务下发推送时会亲手构造的
// 载体与传输模式。请求侧见 msg.go。

// 推送经消息总线以 NotifyPush（MsgID = 对应推送 opcode）下发，网关按 Target 路由到客户端会话。
package proto

import "encoding/json"

// 逻辑服 → 客户端：玩家全量数据同步推送
// EPushPlayerFullSync 逻辑服 → 客户端：该玩家的**全量数据快照**，客户端据此整体应用玩家状态，
// 无需再逐条拉取。属引擎推送区间（从 4001 起）。
//
// ★ 谁触发：**业务**，在「登录成功且该账号已有角色」或「业务创建角色成功」之后调
// `g.PushPlayerFullSync(playerID, account)`（pkg/app 上有完整文档）。
// 引擎**不会**自动调用——全量同步的语义是"客户端可以开始玩游戏了"，而那个时机是业务语义
// （选服 / 选角 / 过新手引导），引擎无从判断。
// 漏调的后果是静默的：客户端 PlayerID 恒为空、OnFullSync 不触发、断线恢复凭证不下发。
const EPushPlayerFullSync uint32 = 4001

// 逻辑服 → 客户端：系统弹窗推送
// EPushAlert 逻辑服 → 客户端：弹窗提示（alert）推送 opcode，与 push 推送同级、专为「单纯
// 向客户端发一条提示消息」设计，支持全服 / 指定玩家 / 指定房间三种广播范围。
// 属引擎推送区间（从 4001 起），紧接 EPushPlayerFullSync。
const EPushAlert uint32 = 4002

// EAlertNotify 弹窗提示内容（EPushAlert 推送载体，JSON 编码为 NotifyPush.Body）。
// 统一收归 proto（与 EPlayerFullSyncNotify 等推送载体同级），作为「统一消息定义」的一部分；
// 向当前客户端弹窗用 Ctx.Alert(*) / Game.Alert；向任意目标弹窗用
// push.Alert(&push.PlayerChannel/AllChannel/SceneChannel{...}, &EAlertNotify{...})；
// 底层经 NotifyPush（EPushAlert=4002）下行，网关按 Target 路由到客户端。
type EAlertNotify struct {
	Title   string `json:"title"`           // 弹窗标题
	Content string `json:"content"`         // 弹窗正文
	Level   string `json:"level,omitempty"` // 提示级别：info / success / warning / error（UI 决定配色）
	Style   string `json:"style,omitempty"` // 可选 UI 风格：toast / modal（不填由客户端默认）
	TTL     int    `json:"ttl,omitempty"`   // 可选：自动关闭毫秒数；0 或不填表示需用户手动关闭
}

// 逻辑服 → 客户端：增量数据同步推送（引擎内置，不依赖业务 RegisterSync）
// EPushDataSync 逻辑服 → 客户端：增量数据同步推送 opcode。
// commitEdits 提交时，若该 key.Type 未在 SyncRegistry 显式登记，则自动以本消息号推送。
// 同一 handler 内多个未注册 type 变更时会合并为一条批量消息下发。
// 属引擎推送区间（从 4001 起），紧接 EPushAlert。
const EPushDataSync uint32 = 4003

// 逻辑服 → 客户端：房间接管恢复推送
// EPushRoomTakeover 引擎推送：跨节点房间接管完成后，向受影响在线玩家下发恢复包。
// 通知客户端新 owner 地址、恢复基准帧与追帧增量，客户端按此做快照恢复后重连。
// 属引擎推送区间（从 4001 起），紧接 EPushDataSync。
const EPushRoomTakeover uint32 = 4004

// ERoomTakeoverNotify 房间接管恢复推送载体（EPushRoomTakeover 的 NotifyPush.Body，JSON 编码）。
// 命名对齐 EAlertNotify / ESceneInfoNotify（统一 *Notify 后缀），收归 proto 作为「统一消息定义」的一部分。
// Recovery 是内核导出的「可迁移恢复包」JSON（帧同步为快照 + 追帧增量，状态同步可为最新全量快照）：
// 外壳不解析其内容，因此 proto 既不依赖 internal/domain/room，也不依赖任何具体同步实现。
// 客户端对应 FrameRoom.OnTakeover（稳定字段可直接绑定，recovery 为动态结构交 MiniJson 消费）。
type ERoomTakeoverNotify struct {
	RoomID   string          `json:"room_id"`            // 房间 ID
	NodeAddr string          `json:"node_addr"`          // 新 owner 节点地址
	Frame    int64           `json:"frame"`              // 恢复基准帧（RecoveredUntil）
	Hash     uint64          `json:"hash"`               // 基准帧状态哈希
	Recovery json.RawMessage `json:"recovery,omitempty"` // iframe.RecoveryPack 的 JSON
}

// 逻辑服 → 客户端：场景标识推送
// EPushSceneInfo 逻辑服 → 客户端：玩家进入场景 / 切换场景 / 跨服转移完成后，
// 由场景下发其当前所在的场景标识（scene id + instance id + 场景名）。
// 客户端据此建立「服务端场景」投影（CloverScene），并与本地 Unity 关卡做映射。
// 属引擎推送区间（从 4001 起），紧接 EPushRoomTakeover。
const EPushSceneInfo uint32 = 4005

// ESceneInfoNotify 场景标识推送载体（EPushSceneInfo 的 NotifyPush.Body，JSON 编码）。
// 语义与服务端 mmo.Scene 一致：SceneID 是逻辑地图，InstanceID 是地图内的分线。
// 客户端对应概念为 CloverScene（命名约定见 clover-doc/client/concepts/concept-naming）。
type ESceneInfoNotify struct {
	// SceneID 逻辑地图 id（服务端 Scene.ID()）。
	//
	// ⚠️ 符号性约束：两端已**统一为无符号 64 位** —— 服务端 uint64 ↔ 客户端 ulong
	//（clover-client-unity-engine/Runtime/Network/Protocol.cs 的 ESceneInfoNotify.scene_id，
	// 其消费方 ICloverScene.SceneID / IMapData.SceneId 同为 ulong）。
	// 此前客户端按**有符号** long 解析，2^63 以上的 id 会被读成负值、CloverScene 映射错配；
	// 现已按无符号对齐（客户端 Tests/Editor/ProtocolTests.cs 有 2^63 / 2^63+1 的往返用例守住），
	// 因此高位 id（雪花 id 等）不再需要回避 —— 0 仍是「无有效场景」哨兵值，不要用它当真实 id。
	SceneID    uint64 `json:"scene_id"`
	InstanceID uint32 `json:"instance_id"`    // 地图内分线 id（服务端 Instance id）
	Name       string `json:"name,omitempty"` // 场景名（服务端 Scene.Name()）
}

// 推送传输模式
// DeliveryMode 推送传输模式（PushToPlayer / PushToScene / PushToAll / AlertTo* 的选填参数）。
// 不传默认可靠传输；普通飘字等非关键消息可显式传 DeliveryModeBestEffort 指定尽力而为。
type DeliveryMode int

// 传输模式取值。
const (
	// DeliveryModeBestEffort 尽力而为（零值）：不保证送达，无 ACK 确认。
	DeliveryModeBestEffort DeliveryMode = 0
	// DeliveryModeReliable 可靠传输：需要 ACK 确认，支持重试。
	DeliveryModeReliable DeliveryMode = 1
	// DeliveryModePersistent 持久化：离线消息暂存，上线后推送。
	DeliveryModePersistent DeliveryMode = 2
)

// 广播契约
// ESceneBroadcaster 「可广播的场景」抽象：*pkg/domain/mmo.Scene 及其测试替身均满足。
// Broadcast 向场景全部成员经 NATS 广播（与 PushToScene 语义一致）。
// 定义于 proto 作为统一广播契约，PushToScene / AlertToScene 共用，避免重复声明。
type ESceneBroadcaster interface {
	Broadcast(msgID uint32, body []byte)
}
