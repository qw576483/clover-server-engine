package room

import (
	"fmt"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	proom "github.com/qw576483/clover-server-engine/pkg/domain/room"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// 接管推送载体已收归 proto.ERoomTakeoverNotify：
// 统一 *Notify 命名（对齐 EAlertNotify/ESceneInfoNotify），并让 proto 不反向依赖 domain/room/frame。

// TakeoverDeps 注入 RoomTakeoverManager 所需的基础能力。
// 调用方（app 层）负责构造此 struct，Manager 不持有对任何上层对象的引用。
type TakeoverDeps struct {
	// Kernel 返回当前挂载的房间内核。外壳只通过它导出/导入状态，不关心同步方式
	// （帧同步、状态同步都用同一套接管流程）。
	Kernel     func() proom.Kernel
	CallMaster func(msgID uint32, req, resp interface{}) error // 调用 master
	Pusher     func(playerID string, msgID uint32, payload interface{}) error
	NodeAddr   string       // 本节点地址
	Owner      *OwnerClient // 房间所有权管理
}

// pendingImport 是一个待接管的导入项。
//
// imported 记录「内核态是否已导入成功」：导入成功后若注册 owner 失败，只重试注册而**不重复导入**
// —— 重新导入会把房间回退到导出时刻的旧帧（客户端已推进的帧被抹掉）。
type pendingImport struct {
	pack     proom.ExportPack
	imported bool
}

// RoomTakeoverManager 封装 game 侧通用的 room takeover 激活流程。
// 它负责 claim/import pending takeover state，完成恢复后自动向玩家推送恢复包。
// 设计为 room 包的公共组件，不持有对 app 层的任何引用，仅通过 TakeoverDeps 注入。
//
// 锁纪律：m.mu 只保护 pendingState / importing 两个内存 map；
// master 往返（FindRoomOwner / CallMaster）、内核导入与推送一律在锁外进行 ——
// 持锁做网络 IO 会让一个节点的慢网络串行阻塞所有房间的接管激活。
type RoomTakeoverManager struct {
	deps         TakeoverDeps
	mu           sync.Mutex
	pendingState map[string]*pendingImport
	importing    map[string]struct{} // 正在执行导入流程的房间（单飞，防止重复导入/重复推送）
}

// NewRoomTakeoverManager 创建一个通用的 room takeover 管理器。
func NewRoomTakeoverManager(deps TakeoverDeps) *RoomTakeoverManager {
	return &RoomTakeoverManager{
		deps:         deps,
		pendingState: make(map[string]*pendingImport),
		importing:    make(map[string]struct{}),
	}
}

// ExportState 导出房间可迁移态，供 owner transfer 使用。
// 状态内容由内核决定，外壳不解析。
func (m *RoomTakeoverManager) ExportState(roomID string) (proom.ExportPack, bool) {
	if m == nil || m.deps.Kernel == nil || m.deps.Kernel() == nil {
		logger.Warnf("room: takeover export room=%s rejected: kernel not mounted", roomID)
		return proom.ExportPack{}, false
	}
	pack, err := m.deps.Kernel().ExportState(roomID)
	if err != nil {
		logger.Warnf("room: takeover export room=%s failed: %v", roomID, err)
		return proom.ExportPack{}, false
	}
	return pack, true
}

// Activate 在房间入口前尝试激活 takeover。
// 若当前节点已成为 owner，会自动 claim + import，避免业务层重复写样板代码。
func (m *RoomTakeoverManager) Activate(roomID string) error {
	if m == nil || m.deps.Kernel == nil || m.deps.Kernel() == nil || m.deps.Owner == nil {
		// 未启用接管（或内核未挂载）：属正常跳过，不视为错误。
		return nil
	}
	if roomID == "" {
		logger.Warnf("room: takeover activate rejected: empty room id")
		return fmt.Errorf("room: 房间 ID 不能为空")
	}
	// 单飞：同一房间同一时刻只允许一个导入流程在跑；后来者直接跳过，
	// 由在飞的那次完成导入 + 注册 + 推送（并发重复导入会把房间回退到旧帧）。
	if !m.acquireImport(roomID) {
		return nil
	}
	defer m.releaseImport(roomID)

	if m.hasPending(roomID) {
		return m.importPending(roomID)
	}
	if m.deps.Owner.FindRoomOwner(roomID) != m.deps.NodeAddr {
		return nil
	}
	if err := m.claim(roomID, m.deps.NodeAddr); err != nil {
		return err
	}
	return m.importPending(roomID)
}

// acquireImport 尝试占住某房间的导入流程；已有 goroutine 在跑则返回 false。
func (m *RoomTakeoverManager) acquireImport(roomID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, busy := m.importing[roomID]; busy {
		return false
	}
	m.importing[roomID] = struct{}{}
	return true
}

// releaseImport 释放某房间的导入流程占用。
func (m *RoomTakeoverManager) releaseImport(roomID string) {
	m.mu.Lock()
	delete(m.importing, roomID)
	m.mu.Unlock()
}

// hasPending 是否存在待导入的接管状态（纯内存查询，无 IO）。
func (m *RoomTakeoverManager) hasPending(roomID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.pendingState[roomID]
	return ok
}

// claim 向 master 认领房间 owner，并把 master 返回的接管状态记为待导入。
// 网络调用在锁外进行。
func (m *RoomTakeoverManager) claim(roomID, nodeAddr string) error {
	if roomID == "" || nodeAddr == "" {
		logger.Warnf("room: takeover claim rejected: room=%q node=%q", roomID, nodeAddr)
		return fmt.Errorf("room: takeover claim 参数为空")
	}
	var resp OwnerResp
	if err := m.deps.CallMaster(proto.EMasterRoomTakeoverClaim, &TakeoverClaimReq{RoomID: roomID, NodeAddr: nodeAddr}, &resp); err != nil {
		logger.Warnf("room: takeover claim room=%s node=%s failed: %v", roomID, nodeAddr, err)
		return err
	}
	if !resp.OK {
		logger.Warnf("room: takeover claim rejected by master: room=%s node=%s", roomID, nodeAddr)
		return fmt.Errorf("takeover claim rejected for room %s", roomID)
	}
	if len(resp.State) > 0 || len(resp.Recovery) > 0 {
		m.mu.Lock()
		// 已有同房间待导入项时不覆盖：在飞的那次可能已把 imported 置位，
		// 覆盖会退回「未导入」，导致重复导入（旧状态回退）。
		if _, ok := m.pendingState[roomID]; !ok {
			m.pendingState[roomID] = &pendingImport{pack: proom.ExportPack{
				State:    resp.State,
				Recovery: resp.Recovery,
				Frame:    resp.Frame,
				Hash:     resp.Hash,
			}}
		}
		m.mu.Unlock()
	}
	return nil
}

// importPending 完成一次接管导入：导入内核态 → 注册 owner → 向房间玩家推送恢复包。
// 除 map 读写外全部在锁外执行；已导入过的项只重试注册/推送，不重复导入。
// 导入失败时保留 pendingState 等下次 Activate 重试（不丢状态）。
func (m *RoomTakeoverManager) importPending(roomID string) error {
	if roomID == "" {
		logger.Warnf("room: takeover import rejected: empty room id")
		return nil
	}
	m.mu.Lock()
	entry, ok := m.pendingState[roomID]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	kernel := m.deps.Kernel()
	if kernel == nil {
		logger.Warnf("room: takeover import room=%s rejected: kernel not mounted", roomID)
		return nil
	}
	if !entry.imported {
		if len(entry.pack.State) > 0 {
			if err := kernel.ImportState(roomID, entry.pack.State); err != nil {
				logger.Errorf("room: takeover import room=%s failed: %v", roomID, err)
				return err
			}
		} else {
			// 只有 Recovery 没有 State：本节点没有可恢复的运行态。
			// 照旧拿空 State 调 ImportState 会被内核拒绝（frame_kernel.go 拒绝空状态），
			// 结果是每次 Activate 反复失败、pendingState 永久滞留；此处跳过导入，
			// 但仍要注册 owner 并下发恢复包，接管流程才能走完。
			logger.Warnf("room: takeover import room=%s skipped: empty state (recovery only)", roomID)
		}
		m.mu.Lock()
		if cur, ok := m.pendingState[roomID]; ok {
			cur.imported = true
		}
		m.mu.Unlock()
	}
	if m.deps.Owner != nil {
		if ok := m.deps.Owner.RegisterRoom(roomID, m.deps.NodeAddr); !ok {
			// 注册失败：保留 pendingState（下次 Activate 只重试注册；状态已导入，不再重复导入）。
			logger.Warnf("room: takeover register owner room=%s node=%s failed (will retry on next activate)", roomID, m.deps.NodeAddr)
			return nil
		}
	}
	m.mu.Lock()
	delete(m.pendingState, roomID)
	m.mu.Unlock()
	// 接管完成后向房间内玩家推送恢复包。收件人由内核提供——外壳不解析状态内容。
	if len(entry.pack.Recovery) == 0 {
		return nil
	}
	if m.deps.Pusher == nil {
		// Pusher 未配置：恢复包无法下发。
		logger.Warnf("room: takeover recovery room=%s skipped: pusher not configured (recovery dropped)", roomID)
		return nil
	}
	recipients := kernel.Players(roomID)
	if len(recipients) == 0 {
		logger.Warnf("room: takeover recovery room=%s skipped: no player in room (recovery dropped)", roomID)
		return nil
	}
	push := proto.ERoomTakeoverNotify{
		RoomID:   roomID,
		NodeAddr: m.deps.NodeAddr,
		Frame:    entry.pack.Frame,
		Hash:     entry.pack.Hash,
		Recovery: entry.pack.Recovery,
	}
	for _, pid := range recipients {
		if err := m.deps.Pusher(pid, proto.EPushRoomTakeover, push); err != nil {
			logger.Warnf("room: push takeover to %s failed: %v", pid, err)
		}
	}
	return nil
}
