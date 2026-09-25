package state

import (
	"encoding/json"

	"github.com/qw576483/clover-server-engine/pkg/domain/master"
)

// TCP 消息 ID（替代原 NATS subject）。
// game ↔ master 之间的请求-响应走 TCP 传输，
// NATS 保留用于广播（跨服事件、下行推送）。
const (
	// MsgAuth 连接鉴权握手（master 内部 RPC 的共享密钥校验）。
	//
	// 放在 1：业务消息号从 4 起，1~3 留给传输级握手，避免与业务消息号冲突。
	// 仅在配置了 master_token（即允许 master 绑非回环地址）时启用：
	// 客户端连上后**第一帧**必须是它，否则连接被拒并关闭
	// （见 domain/master/server 的握手闸门与 tcpmsg.Server.SetConnAuth）。
	MsgAuth uint32 = 1

	MsgNodesByType  uint32 = 4
	MsgNodesByTag   uint32 = 5 // 按 tag 查询存活节点
	MsgRegisterNode uint32 = 10
	MsgRemoveNode   uint32 = 11

	MsgRankTop          uint32 = 30
	MsgRankAdd          uint32 = 31
	MsgRankAddHigher    uint32 = 32
	MsgRankIncr         uint32 = 33
	MsgRankIncrHigher   uint32 = 34
	MsgRankGet          uint32 = 35
	MsgRankGetRank      uint32 = 36
	MsgRankByRankRange  uint32 = 37
	MsgRankByScoreRange uint32 = 38
	MsgRankRemove       uint32 = 39
	MsgRankClear        uint32 = 40
	MsgRankLen          uint32 = 41

	MsgRankBackupAll  uint32 = 50
	MsgRankBackup     uint32 = 51
	MsgRankRestoreAll uint32 = 52
	MsgRankRestore    uint32 = 53

	MsgRankSetThresholds uint32 = 54

	MsgPlayerRegister uint32 = 60
	MsgPlayerRemove   uint32 = 61
	MsgPlayerLookup   uint32 = 62

	MsgSessionNew      uint32 = 70
	MsgSessionValidate uint32 = 71
	MsgSessionDelete   uint32 = 72
	MsgSessionCurrent  uint32 = 73
	MsgSessionRefresh  uint32 = 74
)

// —— 通用响应 ——

type Resp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// AuthReq 连接鉴权握手请求（token = 配置里的 master_token 共享密钥）。
// 响应复用通用 Resp。
type AuthReq struct {
	Token string `json:"token"`
}

// NodesByTypeReq 按类型查询存活节点列表（用于跨服事件总线感知节点存活）。
type NodesByTypeReq struct {
	Type string `json:"type"`
}
type NodesByTypeResp struct {
	OK      bool     `json:"ok"`
	Error   string   `json:"error,omitempty"`
	NodeIDs []string `json:"node_ids"`
}

// NodesByTagReq 按 tag 查询存活节点列表。
type NodesByTagReq struct {
	Tag string `json:"tag"`
}
type NodesByTagResp struct {
	OK      bool     `json:"ok"`
	Error   string   `json:"error,omitempty"`
	NodeIDs []string `json:"node_ids"`
}

type RegisterReq struct {
	Node Node `json:"node"`
}
type RemoveReq struct {
	NodeID string `json:"node_id"`
}

// —— 玩家定位载体 ——

type PlayerRegisterReq struct {
	UID    string `json:"uid"`
	NodeID string `json:"node_id"`
}

type PlayerRemoveReq struct {
	UID string `json:"uid"`
	// 可选：仅当 uid 映射与 nodeID 一致时才删除。
	// tag 必须是 node_id（snake_case，与同文件其它字段一致）：写作 nodeID 时，
	// 按协议文档构造报文（node_id）的外部端解不到该字段 → NodeID 为空 → 条件删除
	// 退化为无条件删除（误删他人记录）。
	NodeID string `json:"node_id,omitempty"`
}

type PlayerLookupReq struct {
	UID string `json:"uid"`
}
type PlayerLookupResp struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	NodeID string `json:"node_id"`
	Found  bool   `json:"found"`
}

// —— 排行榜载体 ——

type RankTopReq struct {
	Board string `json:"board"`
	N     int    `json:"n"`
}

type RankTopResp struct {
	OK      bool                `json:"ok"`
	Error   string              `json:"error,omitempty"`
	Entries []master.RankMember `json:"entries"`
}

type RankAddReq struct {
	Board  string          `json:"board"`
	Member string          `json:"member"`
	Score  float64         `json:"score"`
	Extra  json.RawMessage `json:"extra,omitempty"`
}

type RankIncrReq struct {
	Board  string  `json:"board"`
	Member string  `json:"member"`
	Delta  float64 `json:"delta"`
}

type RankIncrResp struct {
	OK    bool    `json:"ok"`
	Error string  `json:"error,omitempty"`
	Score float64 `json:"score"`
}

type RankIncrHigherReq struct {
	Board  string  `json:"board"`
	Member string  `json:"member"`
	Delta  float64 `json:"delta"`
}

// RankIncrHigherResp 当前未使用：服务端 MsgRankIncrHigher 实际回的是 RankIncrResp。
// 保留仅为占位；对接时不要按本类型编解码（字段虽相同，语义后续可能分叉）。
type RankIncrHigherResp struct {
	OK    bool    `json:"ok"`
	Error string  `json:"error,omitempty"`
	Score float64 `json:"score"`
}

type RankGetReq struct {
	Board  string `json:"board"`
	Member string `json:"member"`
}

type RankGetResp struct {
	OK    bool              `json:"ok"`
	Error string            `json:"error,omitempty"`
	Entry master.RankMember `json:"entry"`
	Found bool              `json:"found"`
}

type RankGetRankReq struct {
	Board  string `json:"board"`
	Member string `json:"member"`
}

type RankGetRankResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Rank  int    `json:"rank"`
	Found bool   `json:"found"`
}

type RankByRankReq struct {
	Board string `json:"board"`
	Start int    `json:"start"`
	Stop  int    `json:"stop"`
}

type RankByRankResp struct {
	OK      bool                `json:"ok"`
	Error   string              `json:"error,omitempty"`
	Entries []master.RankMember `json:"entries"`
}

type RankByScoreReq struct {
	Board string  `json:"board"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

type RankByScoreResp struct {
	OK      bool                `json:"ok"`
	Error   string              `json:"error,omitempty"`
	Entries []master.RankMember `json:"entries"`
}

// RankAddHigherReq 仅当成员已存在且新分更高时覆盖。
type RankAddHigherReq struct {
	Board  string  `json:"board"`
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

// RankAddHigherResp 当前未使用：服务端 MsgRankAddHigher 实际回的是通用 Resp。
// 保留仅为占位；对接时以服务端实际回包类型为准。
type RankAddHigherResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// RankRemoveReq 移除单个成员。
type RankRemoveReq struct {
	Board  string `json:"board"`
	Member string `json:"member"`
}

// RankClearReq 删除整个榜。
type RankClearReq struct {
	Board        string `json:"board"`
	DeleteBackup bool   `json:"delete_backup"`
}

// RankLenReq 查询榜内总人数。
type RankLenReq struct {
	Board string `json:"board"`
}
type RankLenResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Total int    `json:"total"`
}

// —— Session Token（跨节点断线重连验证）——

type SessionNewReq struct {
	PlayerID string `json:"player_id"`
}

type SessionNewResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Token string `json:"token"`
}

type SessionValidateReq struct {
	PlayerID string `json:"player_id"`
	Token    string `json:"token"`
}

type SessionValidateResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Valid bool   `json:"valid"`
}

type SessionDeleteReq struct {
	PlayerID string `json:"player_id"`
}

type SessionDeleteResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type SessionCurrentReq struct {
	PlayerID string `json:"player_id"`
}

type SessionCurrentResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Token string `json:"token"`
}

// SessionRefreshReq 续期请求：携带 playerID + 待校验的 token，
// master 校验一致后刷新 TTL（sliding session）。
type SessionRefreshReq struct {
	PlayerID string `json:"player_id"`
	Token    string `json:"token"`
}

type SessionRefreshResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// —— 排行榜备份/恢复 ——

type RankBackupReq struct {
	Board string `json:"board"`
}
type RankRestoreReq struct {
	Board string `json:"board"`
}

type RankSetThresholdsReq struct {
	Board      string            `json:"board"`
	Thresholds master.Thresholds `json:"thresholds"`
}
