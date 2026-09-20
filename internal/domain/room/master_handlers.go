// Package room 提供引擎内置的房间管理能力，包括 FrameRoom 生命周期、owner 注册、跨节点接管。
//
// 本文件暴露给 master 侧的 master room handler：
//   - NewMasterHandlers(mg) — 在 master 启动后由业务层显式调用，注册 game↔master 房间协议 handler。
//   - 不会在引擎启动时自动执行，业务按需挂载。
package room

import (
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/internal/transport/event"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// MasterHandlerGame 是 master 侧房间 handler 挂载所需的接口。
// app.MasterGame 自动实现此接口，业务层无需额外操作。
type MasterHandlerGame interface {
	OnMsg(msgID uint32, handler func(*event.Ctx) error)
	// InternalOnMsg 注册**引擎内部保留消息号**（≤ proto.InternalMsgMax）的 handler。
	// 本包注册的 4 条 master↔game 房间协议号（EMasterRoomRegister..TakeoverClaim = 6001..6004）
	// 是引擎内建的，必须走内部路径：OnMsg 那条业务路径有「msgID 必须 > 10000」的守卫
	// （防止业务号撞上引擎内建 handler），对业务不放宽。
	InternalOnMsg(msgID uint32, handler func(*event.Ctx) error)
	Reply(c *event.Ctx, v interface{})
	MasterRegistry() *MasterRegistry
}

// NewMasterHandlers 创建并注册 master 侧房间协议 handler（game↔master 通信）。
// 业务层在 master 启动后按需显式调用：
//
//	mh := room.NewMasterHandlers(mg)
func NewMasterHandlers(mg MasterHandlerGame) *MasterHandlers {
	mh := &MasterHandlers{mg: mg}
	mh.Register()
	return mh
}

// MasterHandlers 管理 master 侧房间协议 handler。
type MasterHandlers struct {
	mg MasterHandlerGame
}

// Register 向 master 注册内核内置的 4 条 room owner 消息处理器。
func (mh *MasterHandlers) Register() {
	registerRoomMsg(mh.mg, proto.EMasterRoomRegister,
		func(c *event.Ctx) error { return onRoomOwnerRegister(mh.mg, c) })
	registerRoomMsg(mh.mg, proto.EMasterRoomUnregister,
		func(c *event.Ctx) error { return onRoomOwnerUnregister(mh.mg, c) })
	registerRoomMsg(mh.mg, proto.EMasterRoomFind,
		func(c *event.Ctx) error { return onRoomOwnerFind(mh.mg, c) })
	registerRoomMsg(mh.mg, proto.EMasterRoomTakeoverClaim,
		func(c *event.Ctx) error { return onRoomTakeoverClaim(mh.mg, c) })
}

// registerRoomMsg 注册一条引擎内建的 master 房间协议 handler。
// 走 InternalOnMsg（保留号内部路径），不是 OnMsg —— 6001..6004 是引擎内建号，
// 经业务路径会被「业务消息号必须 > 10000」的守卫拒绝。
func registerRoomMsg(mg MasterHandlerGame, msgID uint32, handler func(*event.Ctx) error) {
	mg.InternalOnMsg(msgID, handler)
}

func onRoomOwnerRegister(mg MasterHandlerGame, c *event.Ctx) error {
	var req OwnerRegisterReq
	if err := c.BindMsg(&req); err != nil {
		return fmt.Errorf("room owner register decode: %w", err)
	}
	if req.RoomID == "" || req.NodeAddr == "" {
		return fmt.Errorf("room_id/node_addr required")
	}
	ok := mg.MasterRegistry().Register(req.RoomID, req.NodeAddr)
	mg.Reply(c, OwnerResp{OK: ok, RoomID: req.RoomID, NodeAddr: req.NodeAddr})
	return nil
}

func onRoomOwnerUnregister(mg MasterHandlerGame, c *event.Ctx) error {
	var req OwnerUnregisterReq
	if err := c.BindMsg(&req); err != nil {
		return fmt.Errorf("room owner unregister decode: %w", err)
	}
	if req.RoomID == "" {
		return fmt.Errorf("room_id required")
	}
	if req.TakeOverAddr == "" {
		ok := mg.MasterRegistry().Unregister(req.RoomID, req.NodeAddr)
		if !ok {
			// 非预期分支必须留痕：通常是房间 owner 已被别人接管/改派，本次注销未生效。
			logger.Warnf("room: master unregister rejected: room=%s node=%s owner mismatch or not found",
				req.RoomID, req.NodeAddr)
		}
		mg.Reply(c, OwnerResp{OK: ok, RoomID: req.RoomID})
		return nil
	}
	// 先保存接管状态，再执行 Reassign，确保状态先于所有权变更持久化，
	// 避免 Reassign 成功但 SaveTakeoverState 失败导致状态不一致。
	// 恢复包由发起迁移的节点（其内核）提供：master 不解析状态内容，因此不再自行派生。
	mg.MasterRegistry().SaveTakeoverState(req.RoomID, TakeoverState{
		State:    req.State,
		Recovery: req.Recovery,
		Frame:    req.Frame,
		Hash:     req.Hash,
	})
	ok := mg.MasterRegistry().Reassign(req.RoomID, req.NodeAddr, req.TakeOverAddr)
	mg.Reply(c, OwnerResp{OK: ok, RoomID: req.RoomID})
	return nil
}

func onRoomOwnerFind(mg MasterHandlerGame, c *event.Ctx) error {
	var req OwnerFindReq
	if err := c.BindMsg(&req); err != nil {
		return fmt.Errorf("room owner find decode: %w", err)
	}
	if req.RoomID == "" {
		return fmt.Errorf("room_id required")
	}
	owner := mg.MasterRegistry().Find(req.RoomID)
	mg.Reply(c, OwnerResp{OK: owner != "", RoomID: req.RoomID, NodeAddr: owner})
	return nil
}

func onRoomTakeoverClaim(mg MasterHandlerGame, c *event.Ctx) error {
	var req TakeoverClaimReq
	if err := c.BindMsg(&req); err != nil {
		return fmt.Errorf("room takeover claim decode: %w", err)
	}
	if req.RoomID == "" || req.NodeAddr == "" {
		return fmt.Errorf("room_id/node_addr required")
	}
	// 使用 ClaimWithState 原子地注册 owner 并获取接管状态，防止并发双认领。
	// oldNodeAddr 必须传 req.NodeAddr（认领者自己）：传空串会让 CAS 前置条件
	// （exists && oldNodeAddr != ""）恒为假 ⇒ 任何节点都能无条件抢占 owner，
	// 与「防止并发双认领」的注释矛盾。正常接管流程里 owner 已由 Reassign 指到本节点。
	ok, state := mg.MasterRegistry().ClaimWithState(req.RoomID, req.NodeAddr, req.NodeAddr)
	resp := OwnerResp{OK: ok, RoomID: req.RoomID, NodeAddr: req.NodeAddr}
	if ok && state != nil {
		if rs, ok2 := state.(TakeoverState); ok2 {
			resp.State = rs.State
			resp.Recovery = rs.Recovery
			resp.Frame = rs.Frame
			resp.Hash = rs.Hash
		} else {
			logger.Warnf("room: takeover claim room=%s got unexpected stored state type %T, dropped", req.RoomID, state)
		}
	}
	mg.Reply(c, resp)
	return nil
}
