package state

import "time"

// DefaultHeartbeatInterval 节点心跳上报的默认周期。
//
// 放在协议定义旁，而不是 config / client 各写一个：client 与 master 两侧都要用它，
// 而 master 根包 import client（见 rank.go），client 无法反向引用 master 的常量——
// 两边各写一个 3s 会在调整默认值时悄悄漂移。
//
// 服务端仍可通过 HeartbeatResp.IntervalMS 动态下发实际间隔；本常量决定节点**首次**
// 上报的间隔，以及服务端判定阈值的基准。
const DefaultHeartbeatInterval = 3 * time.Second

// 节点健康相关的 TCP 消息 ID。
// 位于 session 之后、config center（80..85）之前。
const (
	// MsgHeartbeat 节点 → master 周期性心跳上报。
	// 注：74 归 MsgSessionRefresh；78 当前未复用。
	MsgHeartbeat uint32 = 75
	// MsgNodeHealth 运维 → master 查询全部节点健康视图。
	MsgNodeHealth uint32 = 79
)

// —— 心跳 ——
//
// HeartbeatReq 是节点上报的心跳。
type HeartbeatReq struct {
	NodeID string `json:"node_id"`
	// Load 当前负载（在线数 / 实体数），顺带刷新，省一次 UpdateLoad 调用。
	Load int `json:"load"`
}

// HeartbeatResp 是 master 对心跳的响应。
// 通过它把服务端的心跳间隔下发给节点，实现"阈值集中配置"。
type HeartbeatResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	// IntervalMS 服务端期望的心跳间隔（毫秒）。节点据此调整上报频率。
	IntervalMS int64 `json:"interval_ms"`
	// Known 为 false 表示 master 不认识该节点（如 master 重启后状态丢失），
	// 节点应重新发起注册。
	Known bool `json:"known"`
}

// —— 健康视图 ——
//
// NodeHealthEntry 是单个节点的健康快照条目。
type NodeHealthEntry struct {
	NodeID string `json:"node_id"`
	Health string `json:"health"`
	// LastHeartbeatMS 最后心跳 Unix 毫秒。
	LastHeartbeatMS int64 `json:"last_heartbeat_ms"`
	// SilenceMS 静默时长（毫秒）。
	SilenceMS int64 `json:"silence_ms"`
}

// NodeHealthResp 是全节点健康视图响应。
type NodeHealthResp struct {
	OK    bool              `json:"ok"`
	Error string            `json:"error,omitempty"`
	Nodes []NodeHealthEntry `json:"nodes"`
}
