package frame

// Room 是单个运行中的锁步帧同步房间（业务可达操作）。
//
// 类型真身在本包，底层为 internal 的 wait-for-all lockstep 实现；
// internal 提供适配器把内部 *Room 换成该接口。
type Room interface {
	// ID 返回房间 ID。
	ID() string
	// Config 返回房间配置副本。
	Config() Config
	// Frame 返回当前已完成帧号。
	Frame() int64
	// Join 向房间加入一个玩家。
	Join(playerID string) error
	// Leave 从房间移除一个玩家。
	Leave(playerID string) error
	// Input 投递一帧输入。
	Input(playerID string, input Input) error
	// MarkDisconnected 标记玩家断线但暂不移出房间。
	MarkDisconnected(playerID string) error
	// Reconnect 重新标记玩家在线并返回恢复包。
	Reconnect(playerID string) (RecoveryPack, error)
	// Recovery 返回完整恢复包。
	Recovery(playerID string) (RecoveryPack, error)
	// Snapshot 返回房间最近一份快照。
	Snapshot() Snapshot
	// ExportState 导出完整房间运行态。
	ExportState() RoomState
	// ImportState 使用导出状态覆盖当前房间运行态。
	ImportState(state RoomState) error
	// Info 返回房间摘要。
	Info() RoomInfo
}
