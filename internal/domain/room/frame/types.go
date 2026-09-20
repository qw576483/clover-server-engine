package frame

import pframe "clover-server-engine/pkg/domain/room/frame"

// 类型真身已上移到 pkg/domain/room/frame（依赖方向：internal → pkg，见 结构规则.md 5.1）。
// 本包保留同义别名，使实现代码（room.go / service.go）无需逐处加包名前缀；
// 选项工厂与错误值同样转发 pkg，避免两份实现漂移。

// 类型别名。
type (
	Broadcaster         = pframe.Broadcaster
	BroadcasterWithMode = pframe.BroadcasterWithMode
	Config              = pframe.Config
	Option              = pframe.Option
	ServiceConfig       = pframe.ServiceConfig
	ServiceOption       = pframe.ServiceOption
	InputApplier        = pframe.InputApplier
	Input               = pframe.Input
	PlayerState         = pframe.PlayerState
	Snapshot            = pframe.Snapshot
	FrameDelta          = pframe.FrameDelta
	RecoveryPack        = pframe.RecoveryPack
	PresenceState       = pframe.PresenceState
	RoomState           = pframe.RoomState
	RoomClosedPush      = pframe.RoomClosedPush
	FramePush           = pframe.FramePush
	RoomInfo            = pframe.RoomInfo
	Metrics             = pframe.Metrics
)

// 配置默认值与选项工厂（转发 pkg 实现）。
var (
	DefaultConfig                 = pframe.DefaultConfig
	WithFPS                       = pframe.WithFPS
	WithSnapshotEvery             = pframe.WithSnapshotEvery
	WithSnapshotLimit             = pframe.WithSnapshotLimit
	WithPushMessageID             = pframe.WithPushMessageID
	WithCloseMessageID            = pframe.WithCloseMessageID
	WithHistoryLimit              = pframe.WithHistoryLimit
	WithRecoveryMaxFrames         = pframe.WithRecoveryMaxFrames
	WithInputTimeoutTicks         = pframe.WithInputTimeoutTicks
	WithIdleTimeoutTicks          = pframe.WithIdleTimeoutTicks
	WithDisconnectRetentionFrames = pframe.WithDisconnectRetentionFrames
	WithMaxInputLead              = pframe.WithMaxInputLead
	WithAutoStart                 = pframe.WithAutoStart
	WithAutoDestroyEmpty          = pframe.WithAutoDestroyEmpty
	WithMaxPlayers                = pframe.WithMaxPlayers
	WithBroadcaster               = pframe.WithBroadcaster
	WithBroadcasterWithMode       = pframe.WithBroadcasterWithMode
	WithTimeNow                   = pframe.WithTimeNow
	WithTakeoverHook              = pframe.WithTakeoverHook
	WithOnRoomCreated             = pframe.WithOnRoomCreated
	WithOnRoomDestroy             = pframe.WithOnRoomDestroy
	WithInputApplier              = pframe.WithInputApplier
	WithDefaultRoomConfig         = pframe.WithDefaultRoomConfig
)

// 错误值（转发 pkg）。
var (
	ErrRoomExists      = pframe.ErrRoomExists
	ErrRoomNotFound    = pframe.ErrRoomNotFound
	ErrPlayerNotIn     = pframe.ErrPlayerNotIn
	ErrPlayerExists    = pframe.ErrPlayerExists
	ErrRoomDestroyed   = pframe.ErrRoomDestroyed
	ErrFrameTooOld     = pframe.ErrFrameTooOld
	ErrFrameTooFar     = pframe.ErrFrameTooFar
	ErrInputDuplicated = pframe.ErrInputDuplicated
	ErrRoomFull        = pframe.ErrRoomFull
)
