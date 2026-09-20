// Package state 定义 log 服的 TCP 消息号与请求/响应载体。
package state

import plogbuf "clover-server-engine/pkg/foundation/logbuf"

// TCP 消息 ID（log 服专用，独立于 master 的消息号空间）。
const (
	// MsgLogBatch game 批量上报日志。
	MsgLogBatch uint32 = 1
)

// LogEntry 单条业务日志。
//
// 真身定义在 pkg/foundation/logbuf（业务经 AddLog 的 Option 直接操作它），
// 此处再导出以保持引擎内部调用点不变。
type LogEntry = plogbuf.LogEntry

// LogBatchReq 批量上报请求。
type LogBatchReq struct {
	Source  string     `json:"source"`  // 来源节点标识（nodeID）
	Entries []LogEntry `json:"entries"` // 日志条目
}

// LogBatchResp 批量上报响应。
type LogBatchResp struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Written int    `json:"written"` // 实际写入条数
}
