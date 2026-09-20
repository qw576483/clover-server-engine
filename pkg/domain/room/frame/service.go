package frame

// Service 统一管理多个 frame room（业务可达的房间管理操作）。
//
// 类型真身在本包，实现由 internal/domain/room/frame 提供并反向 import 本包；
// engine 装配时构造内部实现，经 internal 的适配器换成该接口交给业务。
type Service interface {
	// NewRoom 创建一个房间。已存在则返回 ErrRoomExists。
	NewRoom(roomID string, opts ...Option) (Room, error)
	// EnsureRoom 确保房间存在；不存在时创建，存在则返回已有房间。
	EnsureRoom(roomID string, opts ...Option) (Room, error)
	// Get 返回房间句柄。
	Get(roomID string) (Room, bool)
	// MustGet 语义化地返回房间或错误。
	MustGet(roomID string) (Room, error)
	// SetInputApplier 设置所有后续创建房间的默认输入应用器。
	SetInputApplier(fn InputApplier)
	// Join 让玩家加入房间。
	Join(roomID, playerID string) error
	// Leave 让玩家离开房间。
	Leave(roomID, playerID string) error
	// Input 投递一帧输入。
	Input(roomID, playerID string, input Input) error
	// Destroy 销毁一个房间。
	Destroy(roomID string) error
	// Snapshot 返回该房间最近一份快照。
	Snapshot(roomID string) (Snapshot, error)
	// ExportState 导出房间完整运行态。
	ExportState(roomID string) (RoomState, error)
	// ImportState 导入完整房间状态。
	ImportState(state RoomState) (Room, error)
	// Recovery 返回完整恢复包。
	Recovery(roomID, playerID string) (RecoveryPack, error)
	// Reconnect 标记玩家重连并返回恢复包。
	Reconnect(roomID, playerID string) (RecoveryPack, error)
	// Disconnect 仅标记玩家断线，不立即离房。
	Disconnect(roomID, playerID string) error
	// Info 返回房间摘要。
	Info(roomID string) (RoomInfo, error)
	// ListRooms 返回全部房间 ID。
	ListRooms() []string
	// Close 停止并清空所有房间。
	Close()
	// Metrics 返回当前运行时指标的原子快照。
	Metrics() Metrics
}
