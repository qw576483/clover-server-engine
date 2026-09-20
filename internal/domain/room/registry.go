package room

import "sync"

// MasterRegistry 是引擎内置的房间房主注册表，供逻辑服通过 CallMaster 统一定位房间 owner 节点。
type MasterRegistry struct {
	mu            sync.RWMutex
	rooms         map[string]string
	takeoverState map[string]any
}

// NewMasterRegistry 创建主人注册表。
func NewMasterRegistry() *MasterRegistry {
	return &MasterRegistry{rooms: make(map[string]string), takeoverState: make(map[string]any)}
}

// Register 注册房间 owner 节点地址。
//
// 已有 owner 时**不覆盖**：多节点并发创建同一个房间时，后到的无条件覆盖会让两个节点
// 都认为自己持有该房间，客户端随机落到其中一个，房间状态从此分裂。
// 同一节点重复注册视为幂等成功（节点重连、消息重投都会走到这里）。
// 需要抢 owner 请走 Reassign / ClaimWithState —— 那两条路径带明确的 CAS 前置条件。
func (r *MasterRegistry) Register(roomID, nodeAddr string) bool {
	if r == nil || roomID == "" || nodeAddr == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner, exists := r.rooms[roomID]; exists {
		return owner == nodeAddr
	}
	r.rooms[roomID] = nodeAddr
	return true
}

// Unregister 注销房间 owner。
//
// 返回值语义 = 「注销是否生效」：房间本就无 owner 视为幂等成功（true）；
// nodeAddr 非空且与当前 owner 不匹配时不做改动并返回 false —— 此时房间已转移给别人，
// 注销方无权删除记录，返回 true 会让调用方误判注销成功（进而漏掉真正的 owner 清理）。
func (r *MasterRegistry) Unregister(roomID, nodeAddr string) bool {
	if r == nil || roomID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.rooms[roomID]
	if !ok {
		return true
	}
	if nodeAddr == "" || owner == nodeAddr {
		delete(r.rooms, roomID)
		return true
	}
	return false
}

// Find 查询房间 owner 节点地址。
func (r *MasterRegistry) Find(roomID string) string {
	if r == nil || roomID == "" {
		return ""
	}
	r.mu.RLock()
	owner := r.rooms[roomID]
	r.mu.RUnlock()
	return owner
}

// Reassign 重新分配房间 owner。
// oldNodeAddr 非空时仅当当前 owner 匹配才覆盖（CAS 语义）；
// oldNodeAddr 为空时仅当房间无 owner 才注册，防止并发覆盖已有 owner。
func (r *MasterRegistry) Reassign(roomID, oldNodeAddr, newNodeAddr string) bool {
	if r == nil || roomID == "" || newNodeAddr == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.rooms[roomID]
	if !ok {
		r.rooms[roomID] = newNodeAddr
		return true
	}
	if oldNodeAddr != "" && owner == oldNodeAddr {
		r.rooms[roomID] = newNodeAddr
		return true
	}
	return false
}

// SaveTakeoverState 保存接管状态。
func (r *MasterRegistry) SaveTakeoverState(roomID string, state any) {
	if r == nil || roomID == "" {
		return
	}
	r.mu.Lock()
	r.takeoverState[roomID] = state
	r.mu.Unlock()
}

// PopTakeoverState 获取并清除接管状态。
func (r *MasterRegistry) PopTakeoverState(roomID string) (any, bool) {
	if r == nil || roomID == "" {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.takeoverState[roomID]
	if ok {
		delete(r.takeoverState, roomID)
	}
	return state, ok
}

// ClaimWithState 原子地注册房间 owner 并获取接管状态。
// 仅当当前 owner 为 oldNodeAddr（或无 owner）时才注册成功，防止并发双认领。
func (r *MasterRegistry) ClaimWithState(roomID, oldNodeAddr, newNodeAddr string) (bool, any) {
	if r == nil || roomID == "" || newNodeAddr == "" {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, exists := r.rooms[roomID]
	if exists && oldNodeAddr != "" && owner != oldNodeAddr {
		return false, nil
	}
	r.rooms[roomID] = newNodeAddr
	state, ok := r.takeoverState[roomID]
	if ok {
		delete(r.takeoverState, roomID)
	}
	return true, state
}
