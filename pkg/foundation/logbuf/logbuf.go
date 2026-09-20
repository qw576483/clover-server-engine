// Package logbuf 定义业务日志条目（LogEntry）与其选填项（Option）的真身。
//
// 业务经 app.Game.AddLog(ownerType, ownerID, type, info, opts...) 写业务日志时，
// 用本包的 WithXxx 构造选填项即可：
//
//	g.AddLog("player", playerID, "login", info, logbuf.WithLevel("info"))
//
// 缓冲器本体（Buffer / Options / New）由引擎在 app 层装配，
// 业务不直接 new / 持有，故不在 pkg 暴露。
package logbuf

// LogEntry 单条业务日志。
//
// Time 与 Source 由引擎自动填充；OwnerType/OwnerID/Type/Info 为必填项，
// 其余为选填项（经 Option 设置）。
type LogEntry struct {
	Time   int64  `json:"time"`   // Unix 毫秒时间戳（引擎自动填充）
	Source string `json:"source"` // 来源节点标识（引擎自动填充）

	OwnerType string `json:"owner_type"` // 归属类型（如 player / guild / room）
	OwnerID   string `json:"owner_id"`   // 归属 ID
	Type      string `json:"type"`       // 日志类型（主分类）
	Info      string `json:"info"`       // 日志内容（JSON 字符串）

	// 选填（option 模式）
	SubType   string `json:"sub_type,omitempty"`   // 子类型
	Reason    string `json:"reason,omitempty"`     // 原因
	SubReason string `json:"sub_reason,omitempty"` // 子原因
	Level     string `json:"level,omitempty"`      // 日志级别（debug/info/warn/error）
	TraceID   string `json:"trace_id,omitempty"`   // 链路追踪 ID
}

// Option 业务日志选填项，作为 app.Game.AddLog 的可选参数。
type Option func(*LogEntry)

// WithSubType 设置子类型。
func WithSubType(v string) Option { return func(e *LogEntry) { e.SubType = v } }

// WithReason 设置原因。
func WithReason(v string) Option { return func(e *LogEntry) { e.Reason = v } }

// WithSubReason 设置子原因。
func WithSubReason(v string) Option { return func(e *LogEntry) { e.SubReason = v } }

// WithLevel 设置日志级别（debug/info/warn/error）。
func WithLevel(v string) Option { return func(e *LogEntry) { e.Level = v } }

// WithTraceID 设置链路追踪 ID。
func WithTraceID(v string) Option { return func(e *LogEntry) { e.TraceID = v } }
