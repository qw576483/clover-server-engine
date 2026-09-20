// Package room 提供房间子系统的公开门面（类型真身 + 接口）。
//
// 依赖方向（见 结构规则.md 5.1）：
//   - 类型 / 接口真身在本包；
//   - 实现在 internal/domain/room，反向 import 本包，
//     并在 init() 里通过 RegisterXxxFactory 注册构造入口（P3 注册钩子）；
//   - 本包**禁止** import internal。
package room

import (
	"clover-server-engine/pkg/domain/room/frame"
	"clover-server-engine/pkg/transport/event"
)

// MasterCaller 描述 room 对游戏服节点能力的最小需求：调用 master + 切换上游。
// app.Game 方法集天然满足，业务无需适配；pkg 不依赖 internal 的接口定义。
type MasterCaller interface {
	// CallMaster 跨节点调用 master。
	CallMaster(msgID uint32, req, resp any) error
	// SwitchUpstream 切换连接上游节点。
	SwitchUpstream(connID, targetAddr string) error
}

// Config 是 Module 的构造参数。业务层按需填充。
type Config struct {
	// MasterCaller 跨节点调用 master（Owner 路由 / Takeover 注册）。
	MasterCaller MasterCaller
	// Pusher 向指定玩家推送消息（与 frame.Broadcaster 签名一致）。
	Pusher func(playerID string, msgID uint32, v any) error
	// NodeAddr 本进程网络地址（Owner 注册 / Takeover 所有权判断）。
	NodeAddr string

	// Kernel 业务自写的房间内核（例如状态同步房间）。非 nil 时优先使用，
	// 下面的 FrameCfg / FrameSvc / FrameSvcOpts 会被忽略。
	Kernel Kernel

	// FrameCfg 挂载引擎内置帧同步内核时的「房间默认配置」（TargetFPS / 快照频率等）。
	// 语义是逐项覆盖 DefaultConfig()：零值字段不覆盖（详见 frame.WithDefaultRoomConfig）。
	// 与 Kernel 二选一：不传 Kernel 时，传它（或 FrameSvc / FrameSvcOpts）即挂帧同步内核。
	FrameCfg *frame.Config
	// FrameSvc 可选的预构造帧服务（nil 表示由实现内部根据 FrameCfg 构造）。
	FrameSvc frame.Service
	// FrameSvcOpts 附加的 ServiceOption（业务侧可传入 InputApplier 等）。
	FrameSvcOpts []frame.ServiceOption
}

// Module 是「房间外壳」：所有权路由 + 跨服接管 + 房间生命周期，内核可插拔。
//
// 外壳决定「房间归谁、挂了怎么搬」；内核决定「房间内部怎么同步」：
//   - 引擎内置帧同步内核：Config 传 FrameCfg / FrameSvc / FrameSvcOpts；
//   - 业务自写内核（如状态同步）：实现 Kernel 接口后经 Config.Kernel 传入。
//
// 两种内核共用同一套外壳 API（EnsureRoom / JoinRoom / LeaveRoom / DestroyRoom）。
type Module interface {
	// EnsureRoom 确保房间存在（按挂载的内核创建），并处理接管激活。
	EnsureRoom(roomID string) error
	// JoinRoom 把玩家路由到房间 owner 节点并进房。
	// 返回 (owner 节点地址, 是否发生了连接切换, error)。
	JoinRoom(connID, roomID, playerID string) (owner string, switched bool, err error)
	// LeaveRoom 玩家离房。
	LeaveRoom(roomID, playerID string) error
	// DestroyRoom 销毁房间。
	DestroyRoom(roomID string) error
	// Frame 返回帧同步服务；未挂载帧同步内核时返回 nil。
	Frame() frame.Service
	// Close 关闭房间子系统。
	Close()
}

// MasterRegistry master 侧房间 owner 注册表（业务可达的注册/查询/接管操作）。
type MasterRegistry interface {
	// Register 注册房间 owner 节点地址。
	Register(roomID, nodeAddr string) bool
	// Unregister 注销房间 owner。
	Unregister(roomID, nodeAddr string) bool
	// Find 查询房间 owner 节点地址。
	Find(roomID string) string
	// Reassign 重新分配房间 owner（支持 CAS 语义）。
	Reassign(roomID, oldNodeAddr, newNodeAddr string) bool
	// SaveTakeoverState 保存接管状态。
	SaveTakeoverState(roomID string, state any)
	// PopTakeoverState 获取并清除接管状态。
	PopTakeoverState(roomID string) (any, bool)
	// ClaimWithState 原子地注册房间 owner 并获取接管状态。
	ClaimWithState(roomID, oldNodeAddr, newNodeAddr string) (bool, any)
}

// MasterHandlerGame master 侧房间 handler 所需的接口。
// app.MasterGame 自动实现此接口，业务层无需额外操作。
type MasterHandlerGame interface {
	OnMsg(msgID uint32, handler event.Handler)
	Reply(c event.Ctx, v interface{})
	MasterRegistry() MasterRegistry
}

// MasterHandlers master 侧房间协议 handler 管理器。
type MasterHandlers interface {
	// Register 向 master 注册内核内置的 room owner 消息处理器。
	Register()
}

// ====================================================================
//  P3 注册钩子：构造入口的实现由 internal 装配层注册
//  （database/sql 式；pkg 侧不出现任何 internal 符号）
// ====================================================================

// ModuleFactory 构造 Module 实现。
type ModuleFactory func(Config) Module

// MasterRegistryFactory 构造 MasterRegistry 实现。
type MasterRegistryFactory func() MasterRegistry

// MasterHandlersFactory 构造 MasterHandlers 实现。
type MasterHandlersFactory func(MasterHandlerGame) MasterHandlers

var (
	moduleFactory         ModuleFactory
	masterRegistryFactory MasterRegistryFactory
	masterHandlersFactory MasterHandlersFactory
)

// RegisterModuleFactory 注册 Module 构造实现（由 internal/domain/room 的 init() 调用）。
// 注册 nil 视为装配错误：与「未注册」无法区分，会让守卫（只判 nil 返回值）失去意义。
func RegisterModuleFactory(f ModuleFactory) {
	if f == nil {
		panic("room: RegisterModuleFactory: nil factory")
	}
	moduleFactory = f
}

// RegisterMasterRegistryFactory 注册 MasterRegistry 构造实现（由 internal 的 init() 调用；nil 视为装配错误）。
func RegisterMasterRegistryFactory(f MasterRegistryFactory) {
	if f == nil {
		panic("room: RegisterMasterRegistryFactory: nil factory")
	}
	masterRegistryFactory = f
}

// RegisterMasterHandlersFactory 注册 MasterHandlers 构造实现（由 internal 的 init() 调用；nil 视为装配错误）。
func RegisterMasterHandlersFactory(f MasterHandlersFactory) {
	if f == nil {
		panic("room: RegisterMasterHandlersFactory: nil factory")
	}
	masterHandlersFactory = f
}

// NewModule 构造房间子系统聚合体。
// 实现由引擎装配层注册；未注册说明 internal/domain/room 未被链接。
func NewModule(cfg Config) Module {
	if moduleFactory == nil {
		panic("room: module factory not registered (internal/domain/room not linked?)")
	}
	return moduleFactory(cfg)
}

// NewMasterRegistry 创建 master 侧房间 owner 注册表。
func NewMasterRegistry() MasterRegistry {
	if masterRegistryFactory == nil {
		panic("room: master registry factory not registered")
	}
	return masterRegistryFactory()
}

// NewMasterHandlers 创建 master 侧房间协议 handler（game↔master 通信）。
//
//	mh := room.NewMasterHandlers(mg)
func NewMasterHandlers(mg MasterHandlerGame) MasterHandlers {
	if masterHandlersFactory == nil {
		panic("room: master handlers factory not registered")
	}
	return masterHandlersFactory(mg)
}

// MasterRegistryWrapper 把引擎内部的 owner 注册表包装成门面接口。
// 入参为 any：pkg 侧不出现 internal 类型名，具体转换由 internal 注册的实现完成。
type MasterRegistryWrapper func(inner any) MasterRegistry

var masterRegistryWrapper MasterRegistryWrapper

// RegisterMasterRegistryWrapper 注册包装实现（由 internal/domain/room 的 init() 调用）。
func RegisterMasterRegistryWrapper(f MasterRegistryWrapper) { masterRegistryWrapper = f }

// WrapMasterRegistry 把内部 registry 包成门面接口；未注册或入参为空时返回 nil。
func WrapMasterRegistry(inner any) MasterRegistry {
	if masterRegistryWrapper == nil || inner == nil {
		return nil
	}
	return masterRegistryWrapper(inner)
}
