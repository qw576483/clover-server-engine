package room

import "encoding/json"

// ExportPack 是内核导出的「可迁移态」，供跨节点接管使用。外壳不解析 State 的内容。
//
//	State    交给新 owner 的内核恢复运行态（由 Kernel.ImportState 消费）
//	Recovery 可选的客户端恢复包：接管完成后外壳向房间玩家下发；为空则不下发
//	Frame    可选的恢复基准帧（帧同步客户端据此对齐）；业务内核不需要时留 0
//	Hash     可选的基准帧状态哈希；业务内核不需要时留 0
type ExportPack struct {
	State    json.RawMessage `json:"state,omitempty"`
	Recovery json.RawMessage `json:"recovery,omitempty"`
	Frame    int64           `json:"frame,omitempty"`
	Hash     uint64          `json:"hash,omitempty"`
}

// Kernel 是「房间内核」：决定房间内部到底怎么同步。
//
// 外壳（Module）只依赖本接口——谁管这个房间（owner）、服务器挂了怎么搬（takeover）、
// 房间从生到死（生命周期）都与同步方式无关；「怎么同步」由内核决定。
//
// 两种用法：
//   - 引擎内置内核：Config 传 FrameCfg / FrameSvc / FrameSvcOpts → 帧同步内核；
//   - 业务自写内核：实现本接口后经 Config.Kernel 传入（例如状态同步房间）。
//
// 实现注意：所有方法都可能被不同连接的 goroutine 并发调用，需自行保证并发安全；
// 房间不存在、参数非法等非预期分支必须返回错误并打日志，不要静默成功。
type Kernel interface {
	// EnsureRoom 确保房间存在（幂等：已存在直接返回 nil）。
	EnsureRoom(roomID string) error
	// Join 玩家进房（已在房内视为成功）。
	Join(roomID, playerID string) error
	// Leave 玩家离房。
	Leave(roomID, playerID string) error
	// Destroy 销毁房间。
	Destroy(roomID string) error
	// ExportState 导出可迁移态。
	ExportState(roomID string) (ExportPack, error)
	// ImportState 导入可迁移态（接管时由外壳调用）。房间不存在时应先创建。
	ImportState(roomID string, state json.RawMessage) error
	// Players 返回房间当前玩家，外壳据此下发接管恢复包。
	Players(roomID string) []string
	// Close 关闭内核并释放资源（停止 ticker / 清空房间）。
	Close()
}
