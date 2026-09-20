// Package proto（消息分类：逻辑服 → 客户端，主动推送 / push 载体）

// 本文件保留引擎内部的推送载体结构（全量同步）。这些载体由引擎自行构造并下发，
// 业务不亲手 new，故留在 internal 不对外暴露。

// 推送 opcode（EPushPlayerFullSync/Alert/DataSync/RoomTakeover）与业务会亲手构造的载体
// EAlertNotify、传输模式 DeliveryMode、广播契约 ESceneBroadcaster 已上移到
// pkg/shared/proto/push.go；本包（经 proto.go 再导出）保持引擎内部调用点不变。
package proto

import "encoding/json"

// EPlayerSyncView player 角色档案的安全视图（不含内部主键 id）。
type EPlayerSyncView struct {
	PlayerID string `json:"player_id"`
	Name     string `json:"name"`
	Account  string `json:"account"`
	ServerID uint32 `json:"server_id"`
}

// EAccountSyncView account 账号记录的安全视图（剔除密码 / token）。
type EAccountSyncView struct {
	Account    string `json:"account"`
	CreateTime string `json:"create_time,omitempty"`
	LoginTime  string `json:"login_time,omitempty"`
}

// EPlayerFullSyncNotify 玩家全量数据同步载体。

// 由 Game.PushPlayerFullSync(playerID, account) 构造并下发（**业务触发**，引擎不自动调；
// 触发时机与理由见 pkg/shared/proto/push.go 的同名常量注释）：
//   - Player：player 角色档案（脱敏）；
//   - AccountInfo：account 账号安全字段（不含密码 / token）；
//   - Data：kind → type → 原始 JSON（包含 player 及 server 等额外 kind 的快照数据）；
//   - AccountData：账号数据（扁平的 type → 原始 JSON，已剔除 password/token/cred 等密钥）。

// 客户端按 EPushPlayerFullSync(4001) 识别，整体应用即可获得进入游戏所需的全部玩家数据。
type EPlayerFullSyncNotify struct {
	PlayerID     string                                `json:"player_id"`
	Account      string                                `json:"account"`
	Player       EPlayerSyncView                       `json:"player"`
	AccountInfo  *EAccountSyncView                     `json:"account_info,omitempty"`
	Data         map[string]map[string]json.RawMessage `json:"data,omitempty"`
	AccountData  map[string]json.RawMessage            `json:"account_data,omitempty"`
	SessionToken string                                `json:"session_token,omitempty"`
}
