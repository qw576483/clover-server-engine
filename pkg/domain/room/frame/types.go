// Package frame 定义锁步帧同步房间的**类型真身**（数据 / 配置 / 接口 / 错误）。
//
// 依赖方向（见 结构规则.md 5.1 铁律）：
//   - 类型真身在本包（pkg）：struct / 接口 / 错误，F12 一跳到位；
//   - 实现留在 internal/domain/room/frame，反向 import 本包去实现 Service / Room；
//   - 本包**禁止** import internal。
//
// 为此 Service 级选项改为「配置结构体 ServiceConfig + ServiceOption」，
// 不再用 func(*Service) 直接写实现结构体的私有字段。
package frame

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/shared/proto"
)

// Broadcaster 向指定玩家推送一条消息。
type Broadcaster func(playerID string, msgID uint32, v any) error

// BroadcasterWithMode 向指定玩家推送一条消息，支持指定传输模式。
type BroadcasterWithMode func(playerID string, msgID uint32, v any, mode proto.DeliveryMode) error

// Config 单个帧同步房间的运行配置。
type Config struct {
	TargetFPS                 int    `json:"target_fps"`                  // 目标帧率，决定 ticker 间隔
	SnapshotEvery             int    `json:"snapshot_every"`              // 每 N 帧生成一次完整快照
	SnapshotLimit             int    `json:"snapshot_limit"`              // 内存中最多保留的快照数量
	HistoryLimit              int    `json:"history_limit"`               // 内存中最多保留的历史帧数量
	RecoveryMaxFrames         int    `json:"recovery_max_frames"`         // 追帧恢复最多回传的历史增量帧数
	InputTimeoutTicks         int    `json:"input_timeout_ticks"`         // 输入超时帧数：超过此帧数无新输入视为超时
	IdleTimeoutTicks          int    `json:"idle_timeout_ticks"`          // 空闲超时帧数：无任何输入超过此帧数自动关闭
	DisconnectRetentionFrames int64  `json:"disconnect_retention_frames"` // 断线玩家保留帧数：超时后从房间移除
	MaxInputLead              int64  `json:"max_input_lead"`              // 客户端可预发送输入的最大领先帧数
	PushMessageID             uint32 `json:"push_message_id"`             // 帧广播消息号（0 = 不广播：见 DefaultConfig 的 ⚠️）
	CloseMessageID            uint32 `json:"close_message_id"`            // 房间关闭广播消息号
	AutoStart                 bool   `json:"auto_start"`                  // 首名玩家加入后自动启动主循环
	AutoDestroyEmpty          bool   `json:"auto_destroy_empty"`          // 所有玩家离开后自动销毁房间
	// MaxPlayers 房间人数上限；<=0 表示不限（默认，与历史行为一致）。
	// 校验点在 Room.Join：满员时新玩家被拒（ErrRoomFull），
	// 但**已在房内的玩家（断线重连）不受限** —— 否则满员时掉线的人再也回不来。
	MaxPlayers int `json:"max_players"`
}

// DefaultConfig 返回适合大多数帧同步房间的默认配置。
//
// ⚠️ 有两项**必须业务显式覆盖**，否则是静默失效（两处都在 room.go 里判 0/阈值后直接 return）：
//  1. PushMessageID 默认 0 ⇒ 帧广播静默空转（帧在推进、客户端收不到），必须 WithPushMessageID(...)；
//  2. InputTimeoutTicks 默认 90，而兜底判据 `frame - LastFrame >= InputTimeoutTicks` 在
//     "Join 时 LastFrame=0、推进目标恒为 1" 的场景下永不成立 ⇒ 无人提交输入时帧自锁不推进。
//     要兜底需把阈值调到 ≤ 每次推进的帧号增量（通常 1）。回归见 internal/domain/room/frame/room_test.go。
func DefaultConfig() Config {
	return Config{
		TargetFPS:                 30,
		SnapshotEvery:             15,
		SnapshotLimit:             8,
		HistoryLimit:              120,
		RecoveryMaxFrames:         120,
		InputTimeoutTicks:         90,
		IdleTimeoutTicks:          900,
		DisconnectRetentionFrames: 180,
		MaxInputLead:              3,
		AutoStart:                 true,
		AutoDestroyEmpty:          true,
	}
}

// Option 修改房间配置。
type Option func(*Config)

// WithFPS 设置目标帧率。需大于 0。
func WithFPS(fps int) Option {
	return func(c *Config) {
		if fps > 0 {
			c.TargetFPS = fps
		}
	}
}

// WithSnapshotEvery 设置快照生成间隔帧数。需大于 0。
func WithSnapshotEvery(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.SnapshotEvery = n
		}
	}
}

// WithSnapshotLimit 设置快照保留数量上限。需大于 0。
func WithSnapshotLimit(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.SnapshotLimit = n
		}
	}
}

// WithPushMessageID 设置帧广播推送的消息号。
func WithPushMessageID(msgID uint32) Option {
	return func(c *Config) { c.PushMessageID = msgID }
}

// WithCloseMessageID 设置房间关闭广播的消息号。
func WithCloseMessageID(msgID uint32) Option {
	return func(c *Config) { c.CloseMessageID = msgID }
}

// WithHistoryLimit 设置帧历史保留数量上限。需大于 0。
func WithHistoryLimit(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.HistoryLimit = n
		}
	}
}

// WithRecoveryMaxFrames 设置追帧恢复中增量帧的最大回传数量。需大于 0。
func WithRecoveryMaxFrames(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.RecoveryMaxFrames = n
		}
	}
}

// WithInputTimeoutTicks 设置输入超时帧数：超过此帧数无新输入的玩家标记为 timed_out。需大于 0。
func WithInputTimeoutTicks(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.InputTimeoutTicks = n
		}
	}
}

// WithIdleTimeoutTicks 设置空闲超时帧数：无任何输入超过此帧数自动关闭房间。需大于 0。
func WithIdleTimeoutTicks(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.IdleTimeoutTicks = n
		}
	}
}

// WithDisconnectRetentionFrames 设置断线玩家保留帧数：超时后从房间彻底移除。需大于 0。
func WithDisconnectRetentionFrames(n int64) Option {
	return func(c *Config) {
		if n > 0 {
			c.DisconnectRetentionFrames = n
		}
	}
}

// WithMaxInputLead 设置客户端可预发送输入的最大领先帧数。需大于 0。
func WithMaxInputLead(n int64) Option {
	return func(c *Config) {
		if n > 0 {
			c.MaxInputLead = n
		}
	}
}

// WithAutoStart 控制首名玩家加入后是否自动启动主循环。
func WithAutoStart(auto bool) Option {
	return func(c *Config) { c.AutoStart = auto }
}

// WithAutoDestroyEmpty 控制所有玩家离开后是否自动销毁房间。
func WithAutoDestroyEmpty(auto bool) Option {
	return func(c *Config) { c.AutoDestroyEmpty = auto }
}

// WithMaxPlayers 设置房间人数上限（如 1v1 填 2、3v3 填 6）。需大于 0；0 或负数表示不限（不覆盖）。
//
// 满员判定发生在 Room.Join：新玩家被拒（返回 ErrRoomFull），已在房内的玩家
// （断线重连）不受限。业务若要「开局后不再进人」等更强约束，需在此之上自加判断。
func WithMaxPlayers(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.MaxPlayers = n
		}
	}
}

// ServiceConfig 是 Service 的构造配置，由 ServiceOption 填充。
//
// 采用「配置结构体」而不是 func(*Service) 直接写实现私有字段，
// 是为了让 Service 的结构体实现留在 internal、类型/选项真身留在 pkg。
type ServiceConfig struct {
	Push          Broadcaster         // 消息推送函数，由业务层注入
	PushReliable  BroadcasterWithMode // 可靠消息推送函数，由业务层注入
	Now           func() time.Time    // 时间源，默认 time.Now，可注入 mock 时钟
	TakeoverHook  func(roomID string) // 房间操作前的接管检查钩子，由引擎层注入
	OnRoomCreated func(roomID string) // 房间创建回调，引擎借此自动 RegisterRoom
	OnRoomDestroy func(roomID string) // 房间销毁回调，引擎借此自动 UnregisterRoom
	InputApplier  InputApplier        // 输入应用器，由业务层注入（生产环境必须设置）
	// DefaultRoomConfig 是后续所有新建房间的默认房间配置（TargetFPS / 快照频率等）。
	// 未设置时各房间起步于 DefaultConfig()；NewRoom/EnsureRoom 传入的 Option 在其之上逐项覆盖。
	DefaultRoomConfig *Config
}

// ServiceOption 修改 Service 构造配置。
type ServiceOption func(*ServiceConfig)

// WithBroadcaster 设置消息广播函数，用于向指定玩家推送消息。
func WithBroadcaster(fn Broadcaster) ServiceOption {
	return func(c *ServiceConfig) { c.Push = fn }
}

// WithBroadcasterWithMode 设置支持传输模式的消息广播函数。
func WithBroadcasterWithMode(fn BroadcasterWithMode) ServiceOption {
	return func(c *ServiceConfig) { c.PushReliable = fn }
}

// WithTimeNow 设置自定义时间源，默认使用 time.Now，测试时可注入 mock 时钟。
func WithTimeNow(fn func() time.Time) ServiceOption {
	return func(c *ServiceConfig) {
		if fn != nil {
			c.Now = fn
		}
	}
}

// WithTakeoverHook 设置接管钩子：在 Join/Leave 等房间操作前自动调用。
// 钩子内部幂等检查本节点是否为 takeover 目标，是则 claim + import + push。
func WithTakeoverHook(hook func(roomID string)) ServiceOption {
	return func(c *ServiceConfig) {
		if hook != nil {
			c.TakeoverHook = hook
		}
	}
}

// WithOnRoomCreated 设置房间创建回调：NewRoom 成功后调用，引擎借此自动 RegisterRoom。
func WithOnRoomCreated(fn func(roomID string)) ServiceOption {
	return func(c *ServiceConfig) {
		if fn != nil {
			c.OnRoomCreated = fn
		}
	}
}

// WithOnRoomDestroy 设置房间销毁回调：最后一个玩家离开且 AutoDestroyEmpty 后调用，引擎借此自动 UnregisterRoom。
func WithOnRoomDestroy(fn func(roomID string)) ServiceOption {
	return func(c *ServiceConfig) {
		if fn != nil {
			c.OnRoomDestroy = fn
		}
	}
}

// WithInputApplier 设置输入应用器：引擎每帧推进时调用此函数将输入应用到游戏状态。
// 若不设置，玩家状态不会被更新（生产环境必须设置）。
func WithInputApplier(fn InputApplier) ServiceOption {
	return func(c *ServiceConfig) {
		if fn != nil {
			c.InputApplier = fn
		}
	}
}

// WithDefaultRoomConfig 设置后续所有新建房间的默认房间配置。
//
// 语义是「逐项覆盖 DefaultConfig()」：**零值字段表示不覆盖**。
// 因此不能用它把 AutoStart / AutoDestroyEmpty 关成 false ——
// 要显式关闭请在该房间的创建 Option 里用 frame.WithAutoStart(false) / frame.WithAutoDestroyEmpty(false)。
// 房间创建时传入的 Option 在它之上再做覆盖。
func WithDefaultRoomConfig(cfg Config) ServiceOption {
	return func(c *ServiceConfig) {
		// 每次应用都再拷一份：同一个 ServiceOption 复用到多个 ServiceConfig 时，
		// 直接存闭包捕获的 cfg 地址会让它们共享同一 *Config，一方修改彼此可见。
		cp := cfg
		c.DefaultRoomConfig = &cp
	}
}

// InputApplier 由业务层注入的输入应用器。
// 引擎在每帧推进时调用此函数，将收集到的玩家输入应用到游戏状态。
//
// 参数:
//   - roomID: 房间 ID
//   - frame:  当前推进帧号
//   - inputs: playerID → Input 映射（包含超时玩家的 fallback 输入）
//
// 返回: 应用输入后的 playerID → PlayerState 映射（不允许返回 nil map）。
//
// ★ 并发契约（引擎保证 + 实现方义务）：
//   - 本回调在**不持房锁**的状态下调用（帧号推进与输入出队已先行完成），
//     因此实现内**可以**安全调用 Join / Leave / Input / Info / Push 等房间方法；
//   - 代价是不再与那些方法串行：回调执行期间可能有玩家进出房间，也可能发生
//     ImportState（节点接管）或房间销毁。此时：本次返回的状态只按**仍在房间内的
//     玩家**逐个合并（已离场者不会被"复活"，期间新进房者保留其加入时的状态），
//     房间已被销毁/接管则整体丢弃该次结果并留痕；
//   - 返回的 *PlayerState 在返回后**不得再被实现方改写**（引擎会直接放进房间玩家表，
//     其他 goroutine 会并发读它）；要复用请返回副本。
type InputApplier func(roomID string, frame int64, inputs map[string]Input) map[string]*PlayerState

// Input 是一帧输入。
// Frame 由引擎管理（帧号、排序、去重），Payload 为业务自定义数据，引擎透传不解析。
type Input struct {
	Frame   int64  `json:"frame,omitempty"`
	Payload []byte `json:"payload,omitempty"`
}

// PlayerState 是房间内对外暴露的玩家状态。
type PlayerState struct {
	PlayerID   string `json:"player_id"`
	PosX       int    `json:"pos_x"`
	PosY       int    `json:"pos_y"`
	HP         int    `json:"hp"`
	LastFrame  int64  `json:"last_frame"`
	LastAction string `json:"last_action,omitempty"`
}

// Metrics 提供帧同步房间运行时的关键指标，用于监控与问题诊断。
type Metrics struct {
	RoomCount    int64 // 当前存活房间数
	PlayerCount  int64 // 当前在线玩家总数
	TotalFrames  int64 // 所有房间累计推进帧数
	InputDropped int64 // 被丢弃的输入帧数（过期/重复/超远）
	JoinErrors   int64 // Join 失败计数
}

// Snapshot 返回当前指标的原子快照，所有计数均为近似值。
func (m *Metrics) Snapshot() Metrics {
	return Metrics{
		RoomCount:    atomic.LoadInt64(&m.RoomCount),
		PlayerCount:  atomic.LoadInt64(&m.PlayerCount),
		TotalFrames:  atomic.LoadInt64(&m.TotalFrames),
		InputDropped: atomic.LoadInt64(&m.InputDropped),
		JoinErrors:   atomic.LoadInt64(&m.JoinErrors),
	}
}

// Snapshot 是房间某一帧的完整状态快照。
type Snapshot struct {
	RoomID    string                  `json:"room_id"`
	Frame     int64                   `json:"frame"`
	Hash      uint64                  `json:"hash"`
	Players   map[string]*PlayerState `json:"players"`
	CreatedAt time.Time               `json:"created_at"`
}

// FrameDelta 是给重连追帧使用的一帧增量。
type FrameDelta struct {
	Frame  int64                   `json:"frame"`
	Inputs map[string]Input        `json:"inputs"`
	State  map[string]*PlayerState `json:"state"`
	Hash   uint64                  `json:"hash"`
}

// RecoveryPack 是快照 + 历史增量的重连恢复包。
type RecoveryPack struct {
	RoomID         string       `json:"room_id"`
	Snapshot       Snapshot     `json:"snapshot"`
	Deltas         []FrameDelta `json:"deltas"`
	RecoverFrom    int64        `json:"recover_from"`
	RecoveredUntil int64        `json:"recovered_until"`
	FrameHash      uint64       `json:"frame_hash"`
}

// PresenceState 是玩家在线/断线存在性状态，供房间导出/导入与接管恢复使用，
// 同时作为房间内存态。
type PresenceState struct {
	Connected         bool  `json:"connected"`
	LastSeenFrame     int64 `json:"last_seen_frame"`
	DisconnectAtFrame int64 `json:"disconnect_at_frame,omitempty"`
	LastInputFrame    int64 `json:"last_input_frame,omitempty"` // 最后一次提交输入的帧号，用于断线 Retention 计时
}

// RoomState 是房间可导出/导入的完整运行态快照，用于 owner 接管与节点迁移恢复。
type RoomState struct {
	RoomID            string                     `json:"room_id"`
	Config            Config                     `json:"config"`
	Frame             int64                      `json:"frame"`
	LastHash          uint64                     `json:"last_hash"`
	Players           map[string]*PlayerState    `json:"players"`
	Presence          map[string]PresenceState   `json:"presence"`
	Pending           map[int64]map[string]Input `json:"pending"`
	History           []FrameDelta               `json:"history"`
	Snapshots         []Snapshot                 `json:"snapshots"`
	LastTimedOut      []string                   `json:"last_timed_out,omitempty"`
	LastProgressFrame int64                      `json:"last_progress_frame"`
	CloseReason       string                     `json:"close_reason,omitempty"`
	Running           bool                       `json:"running"`
}

// RoomClosedPush 是房间关闭时的事件。
type RoomClosedPush struct {
	RoomID string `json:"room_id"`
	Frame  int64  `json:"frame"`
	Reason string `json:"reason"`
}

// FramePush 是房间每帧自动广播给成员的结构。
type FramePush struct {
	RoomID         string                  `json:"room_id"`
	Frame          int64                   `json:"frame"`
	FPS            int                     `json:"fps"`
	Waiting        bool                    `json:"waiting"`
	WaitingReason  string                  `json:"waiting_reason,omitempty"`
	Missing        []string                `json:"missing,omitempty"`
	TimedOut       []string                `json:"timed_out,omitempty"`
	Disconnected   []string                `json:"disconnected,omitempty"`
	PlayerCount    int                     `json:"player_count,omitempty"`
	SnapshotFrame  int64                   `json:"snapshot_frame,omitempty"`
	FrameHash      uint64                  `json:"frame_hash,omitempty"`
	RecoveredUntil int64                   `json:"recovered_until,omitempty"`
	Players        map[string]*PlayerState `json:"players"`
}

// RoomInfo 提供房间对外查看的摘要信息。
type RoomInfo struct {
	RoomID          string   `json:"room_id"`
	Frame           int64    `json:"frame"`
	TargetFPS       int      `json:"target_fps"`
	PlayerCount     int      `json:"player_count"`
	Players         []string `json:"players"`
	Running         bool     `json:"running"`
	Waiting         bool     `json:"waiting"`
	WaitingReason   string   `json:"waiting_reason,omitempty"`
	Missing         []string `json:"missing,omitempty"`
	Disconnected    []string `json:"disconnected,omitempty"`
	TimedOut        []string `json:"timed_out,omitempty"`
	SnapshotSize    int      `json:"snapshot_size"`
	HistorySize     int      `json:"history_size"`
	NextFrame       int64    `json:"next_frame"`
	FrameHash       uint64   `json:"frame_hash,omitempty"`
	RecoverableFrom int64    `json:"recoverable_from,omitempty"`
	CloseReason     string   `json:"close_reason,omitempty"`
}

// 帧同步房间错误值（真身在本包，internal 反向引用）。
var (
	ErrRoomExists      = errors.New("room/frame: room already exists")
	ErrRoomNotFound    = errors.New("room/frame: room not found")
	ErrPlayerNotIn     = errors.New("room/frame: player not in room")
	ErrPlayerExists    = errors.New("room/frame: player already in room")
	ErrRoomDestroyed   = errors.New("room/frame: room destroyed")
	ErrFrameTooOld     = errors.New("room/frame: input frame too old")
	ErrFrameTooFar     = errors.New("room/frame: input frame too far ahead")
	ErrInputDuplicated = errors.New("room/frame: duplicate input frame")
	ErrRoomFull        = errors.New("room/frame: room is full")
)
