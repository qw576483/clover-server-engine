package room

import (
	"encoding/json"

	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	proom "github.com/qw576483/clover-server-engine/pkg/domain/room"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// MasterCaller 描述 room 对游戏服节点能力的最小需求：调用 master + 切换上游。
// 与 app.MasterCaller 同构，app.Game 自动满足；room 不反向依赖 app，避免循环依赖。
type MasterCaller interface {
	CallMaster(msgID uint32, req, resp any) error
	SwitchUpstream(connID, targetAddr string) error
}

// OwnerClient 提供房间 owner 注册/查询/注销与切换辅助。
// 内部直接使用引擎保留段消息号，无需业务侧传入。
type OwnerClient struct {
	caller MasterCaller
}

// OwnerResp 是 room owner/master 协议的通用响应。
//
// 状态字段（State / Recovery / Frame / Hash）是「内核导出态」的透传载体：
// 外壳不解析其内容，内核是帧同步还是状态同步都能走同一套协议。
type OwnerResp struct {
	OK       bool            `json:"ok"`
	RoomID   string          `json:"room_id,omitempty"`
	NodeAddr string          `json:"node_addr,omitempty"`
	State    json.RawMessage `json:"state,omitempty"`
	Recovery json.RawMessage `json:"recovery,omitempty"`
	Frame    int64           `json:"frame,omitempty"`
	Hash     uint64          `json:"hash,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// OwnerRegisterReq 注册房间 owner。
type OwnerRegisterReq struct {
	RoomID   string `json:"room_id"`
	NodeAddr string `json:"node_addr"`
}

// OwnerUnregisterReq 注销房间 owner，必要时附带 takeover 目标与内核导出的可迁移态。
type OwnerUnregisterReq struct {
	RoomID       string          `json:"room_id"`
	NodeAddr     string          `json:"node_addr,omitempty"`
	TakeOverAddr string          `json:"takeover_addr,omitempty"`
	State        json.RawMessage `json:"state,omitempty"`
	Recovery     json.RawMessage `json:"recovery,omitempty"`
	Frame        int64           `json:"frame,omitempty"`
	Hash         uint64          `json:"hash,omitempty"`
}

// OwnerFindReq 查询房间 owner。
type OwnerFindReq struct {
	RoomID string `json:"room_id"`
}

// TakeoverClaimReq 用于新 owner 向 master 认领暂存的 takeover state。
type TakeoverClaimReq struct {
	RoomID   string `json:"room_id"`
	NodeAddr string `json:"node_addr"`
}

// TakeoverState 表示 master 暂存的房间迁移结果：内核导出态的透传（外壳不解析）。
type TakeoverState struct {
	State    json.RawMessage `json:"state,omitempty"`
	Recovery json.RawMessage `json:"recovery,omitempty"`
	Frame    int64           `json:"frame,omitempty"`
	Hash     uint64          `json:"hash,omitempty"`
}

// NewOwnerClient 创建房间 owner 客户端。
// 消息号使用引擎内建常量，业务侧无需分配。
func NewOwnerClient(caller MasterCaller) *OwnerClient {
	return &OwnerClient{caller: caller}
}

// RegisterRoom 向 master 注册房间 owner。
func (c *OwnerClient) RegisterRoom(roomID, nodeAddr string) bool {
	if c == nil || c.caller == nil || roomID == "" || nodeAddr == "" {
		return false
	}
	var resp OwnerResp
	if err := c.caller.CallMaster(proto.EMasterRoomRegister, &OwnerRegisterReq{RoomID: roomID, NodeAddr: nodeAddr}, &resp); err != nil {
		return false
	}
	return resp.OK
}

// UnregisterRoom 从 master 注销房间 owner。等同于 TransferRoom 不传入 takeover 地址。
func (c *OwnerClient) UnregisterRoom(roomID, nodeAddr string) bool {
	return c.TransferRoom(roomID, nodeAddr, "", proom.ExportPack{})
}

// TransferRoom 注销房间 owner 并指定 takeover 接管地址，同时携带内核导出的可迁移态。
// 用于节点迁移时移交：master 将暂存 pack，等待新节点通过 TakeoverClaim 认领。
// pack 的内容由内核决定（帧同步 / 状态同步各自不同），外壳只负责搬运。
func (c *OwnerClient) TransferRoom(roomID, nodeAddr, takeOverAddr string, pack proom.ExportPack) bool {
	if c == nil || c.caller == nil || roomID == "" {
		logger.Warnf("room: transfer rejected: room=%q node=%q (client/caller nil or empty room id)", roomID, nodeAddr)
		return false
	}
	var resp OwnerResp
	err := c.caller.CallMaster(proto.EMasterRoomUnregister, &OwnerUnregisterReq{
		RoomID:       roomID,
		NodeAddr:     nodeAddr,
		TakeOverAddr: takeOverAddr,
		State:        pack.State,
		Recovery:     pack.Recovery,
		Frame:        pack.Frame,
		Hash:         pack.Hash,
	}, &resp)
	if err != nil {
		logger.Warnf("room: transfer room=%s node=%s takeover=%s failed: %v", roomID, nodeAddr, takeOverAddr, err)
		return false
	}
	if !resp.OK {
		logger.Warnf("room: transfer rejected by master: room=%s node=%s takeover=%s", roomID, nodeAddr, takeOverAddr)
	}
	return resp.OK
}

// FindRoomOwner 向 master 查询房间 owner 节点地址。返回空字符串表示未找到。
func (c *OwnerClient) FindRoomOwner(roomID string) string {
	if c == nil || c.caller == nil || roomID == "" {
		return ""
	}
	var resp OwnerResp
	if err := c.caller.CallMaster(proto.EMasterRoomFind, &OwnerFindReq{RoomID: roomID}, &resp); err != nil || !resp.OK {
		return ""
	}
	return resp.NodeAddr
}

// EnsureOwner 确保当前节点是房间 owner：先查询，若已存在其他 owner 则切换连接；若不存在则注册自己。
// 返回 (owner地址, 是否发生了连接切换, error)。
func (c *OwnerClient) EnsureOwner(connID, roomID, localAddr string) (string, bool, error) {
	owner := c.FindRoomOwner(roomID)
	if owner == "" {
		// 无 owner：尝试注册自己。master 侧 Register 不覆盖已有 owner，
		// 并发注册时只有一个节点成功，其余必须相信回查结果而不是假定自己是 owner。
		if ok := c.RegisterRoom(roomID, localAddr); !ok {
			// 注册失败必须留痕：可能是并发被抢先（房间已归别的节点），也可能是 master 调用失败。
			logger.Warnf("room: ensure owner room=%s node=%s register failed, will re-query owner", roomID, localAddr)
		}
		// 注册后重新查询确认，避免并发双注册时使用错误的 owner。
		owner = c.FindRoomOwner(roomID)
		if owner == "" {
			// 注册失败且回查仍为空（master 调用异常）：退回本节点处理，避免直接失败。
			logger.Warnf("room: ensure owner room=%s node=%s owner still unknown after register; fallback to local node", roomID, localAddr)
			owner = localAddr
		}
	}
	if owner != localAddr {
		// owner 是别的节点（并发注册被抢先，或房间本来就有 owner）：把连接切到 owner 节点，
		// 保证客户端后续请求都落到真正的 owner —— 否则本节点会「自认 owner」并与对方双写分裂。
		if err := c.caller.SwitchUpstream(connID, owner); err != nil {
			return "", false, err
		}
		return owner, true, nil
	}
	return owner, false, nil
}
