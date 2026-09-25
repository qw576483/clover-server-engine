package room

import (
	"sync"
	"time"
)

const (
	// takeoverStateTTL 接管状态的存活上限。
	//
	// 接管状态的生命周期天然是「保存 → 目标节点上线认领」。若目标节点一直不上线
	// （崩溃 / 迁移取消 / 客户端再也不进这个房），这条记录会一直留下：
	// roomID 是动态值时，这张表只增不减，最终把 master 内存吃光。
	// 超过 TTL 仍未认领的一律视为过期丢弃（此时房间状态本也已无人继承）。
	takeoverStateTTL = 5 * time.Minute

	// takeoverPurgeEvery 摊还清扫节奏：每 N 次写入扫一遍全表。
	// 逐次全表扫描会让写入变成 O(表大小)，因此按固定次数摊还。
	takeoverPurgeEvery = 256
)

// takeoverEntry 一条接管状态：值 + 写入时间（TTL 判定的依据）。
type takeoverEntry struct {
	state any
	at    time.Time
}

// MasterRegistry 是引擎内置的房间房主注册表，供逻辑服通过 CallMaster 统一定位房间 owner 节点。
type MasterRegistry struct {
	mu            sync.RWMutex
	rooms         map[string]string
	takeoverState map[string]takeoverEntry
	// takeoverWrites 累计写入次数，用于触发摊还清扫。
	takeoverWrites uint64
}

// NewMasterRegistry 创建主人注册表。
func NewMasterRegistry() *MasterRegistry {
	return &MasterRegistry{rooms: make(map[string]string), takeoverState: make(map[string]takeoverEntry)}
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
		// 房间 owner 已注销 = 这个房的接管状态再也不会有人认领，顺手清掉，
		// 不必等 TTL 到点（TTL 只兜「对端压根没上线」那条路径）。
		delete(r.takeoverState, roomID)
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
// 写入按摊还节奏清扫过期条目（见 takeoverStateTTL），保证这张表有界。
func (r *MasterRegistry) SaveTakeoverState(roomID string, state any) {
	if r == nil || roomID == "" {
		return
	}
	r.mu.Lock()
	r.takeoverState[roomID] = takeoverEntry{state: state, at: time.Now()}
	r.takeoverWrites++
	if r.takeoverWrites%takeoverPurgeEvery == 0 {
		r.purgeTakeoverLocked(time.Now())
	}
	r.mu.Unlock()
}

// PopTakeoverState 获取并清除接管状态。
// 已超 TTL 的条目按「不存在」处理并顺手删除。
func (r *MasterRegistry) PopTakeoverState(roomID string) (any, bool) {
	if r == nil || roomID == "" {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.takeoverState[roomID]
	if !ok {
		return nil, false
	}
	delete(r.takeoverState, roomID)
	if time.Since(entry.at) > takeoverStateTTL {
		return nil, false
	}
	return entry.state, true
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
	entry, ok := r.takeoverState[roomID]
	if ok {
		delete(r.takeoverState, roomID)
	}
	if !ok || time.Since(entry.at) > takeoverStateTTL {
		return true, nil
	}
	return true, entry.state
}

// purgeTakeoverLocked 删除全部过期接管状态。调用方必须持有 r.mu。
func (r *MasterRegistry) purgeTakeoverLocked(now time.Time) {
	for roomID, entry := range r.takeoverState {
		if now.Sub(entry.at) > takeoverStateTTL {
			delete(r.takeoverState, roomID)
		}
	}
}

// TakeoverStateLen 返回当前接管状态条目数（含未过期前的暂存），供运维/测试观察有界性。
func (r *MasterRegistry) TakeoverStateLen() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.takeoverState)
}
