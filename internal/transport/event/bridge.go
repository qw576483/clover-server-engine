package event

// bridge.go 将 pkg/transport/event 的 Bus / Envelope / Pattern 等类型重导出到 internal，
// 使同包内的 logic / crossnode / deadletter 等文件无需修改引用。
// 本文件仅做类型别名和函数变量绑定，不含任何实现。

import (
	pevent "github.com/qw576483/clover-server-engine/pkg/transport/event"
)

// 类型重导出
type (
	Envelope              = pevent.Envelope
	Target                = pevent.TargetKind
	ClientRequestPayload  = pevent.ClientRequestPayload
	ServerNotifyPayload   = pevent.ServerNotifyPayload
	InternalServerPayload = pevent.InternalServerPayload
	EventOption           = pevent.EventOption
	BusHandler            = pevent.BusHandler
	Bus                   = pevent.Bus
	BusOption             = pevent.BusOption
	Matcher               = pevent.Matcher
)

// 常量重导出
const (
	EventClientRequest  = pevent.EventClientRequest
	EventServerNotify   = pevent.EventServerNotify
	EventServerInternal = pevent.EventServerInternal

	TargetPlayer = pevent.TargetPlayer
	TargetGroup  = pevent.TargetGroup
	TargetGate   = pevent.TargetGate
)

// 函数/方法变量重导出
var (
	WithTraceID            = pevent.WithTraceID
	WithSource             = pevent.WithSource
	WithConnID             = pevent.WithConnID
	WithMsgID              = pevent.WithMsgID
	WithUID                = pevent.WithUID
	WithCtx                = pevent.WithCtx
	GenID                  = pevent.GenID
	NewClientRequestEvent  = pevent.NewClientRequestEvent
	NewServerNotifyEvent   = pevent.NewServerNotifyEvent
	NewInternalServerEvent = pevent.NewInternalServerEvent
	NewEvent               = pevent.NewEvent
	NewBus                 = pevent.NewBus
	WithAsync              = pevent.WithAsync
	MatchPattern           = pevent.MatchPattern
)
