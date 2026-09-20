// 连接生命周期管理器（connBags / pendingBags / reconnect grace）。
package app

import (
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionManager 管理玩家连接状态。
// 持有活跃连接数据（connBags）、断线暂存（pendingBags）和重连宽限期。
// 不持有 Logic / Engine 引用，仅做纯数据管理，事件发射由 Game 层完成。
type ConnectionManager struct {
	connBags    sync.Map // connID → *sync.Map
	pendingBags sync.Map // owner/connID → *pendingBag
	grace       time.Duration
	kickFn      atomic.Pointer[func(connID string) bool]
}

// PendingBag 断线暂存数据，等待重连或超时清理。
type PendingBag struct {
	Bag    *sync.Map
	ConnID string
	Timer  *time.Timer
	// settled 标记该 pending 已被 RestoreBag / DeleteBag 处理。
	// ParkBag 是「先发布、后赋 Timer」两步，两步之间被并发清理的 pb 其 Timer 为 nil，
	// 清理方 Stop 不到随后才创建的定时器（得多持有到宽限到期）。
	// 用 settled 闭环：ParkBag 赋值 Timer 后复查 settled，已 settled 就立即 Stop。
	settled atomic.Bool
}

// NewConnectionManager 创建连接管理器。
func NewConnectionManager(reconnectGrace time.Duration) *ConnectionManager {
	return &ConnectionManager{grace: reconnectGrace}
}

// EnsureBag 获取或创建 connID 对应的 connBag。
func (m *ConnectionManager) EnsureBag(connID string) *sync.Map {
	if v, ok := m.connBags.Load(connID); ok {
		if bag, ok := v.(*sync.Map); ok {
			return bag
		}
	}
	bag := &sync.Map{}
	actual, _ := m.connBags.LoadOrStore(connID, bag)
	if bag, ok := actual.(*sync.Map); ok {
		return bag
	}
	return bag
}

// DeleteBag 删除指定连接。
func (m *ConnectionManager) DeleteBag(connID string) {
	if connID == "" {
		return
	}
	m.connBags.Delete(connID)
	// 同时清理 pending 中同名条目。不能命中第一条就停：
	// 同一 connID 可能挂在多个 pending 记录下（不同 owner key），漏掉的条目
	// 会残留到宽限到期才被 CompareAndDelete 移除。
	m.pendingBags.Range(func(key, value any) bool {
		pb, ok := value.(*PendingBag)
		if !ok {
			return true
		}
		if pb.ConnID == connID {
			pb.settled.Store(true)
			if pb.Timer != nil {
				pb.Timer.Stop()
			}
			m.pendingBags.Delete(key)
			return true
		}
		return true
	})
}

// ClearByOwner 按 owner（account）清理 pending 记录。
// 返回被清理的 connID，用于后续 event 发射等。
func (m *ConnectionManager) ClearByOwner(owner string) string {
	if owner == "" {
		return ""
	}
	v, ok := m.pendingBags.LoadAndDelete(owner)
	if !ok {
		return ""
	}
	pb, ok := v.(*PendingBag)
	if !ok {
		return ""
	}
	if pb.Timer != nil {
		pb.Timer.Stop()
	}
	if pb.ConnID != "" {
		m.connBags.Delete(pb.ConnID)
		return pb.ConnID
	}
	return ""
}

// Kick 踢下线并清理连接数据。
func (m *ConnectionManager) Kick(connID string) bool {
	if connID == "" {
		return false
	}
	m.DeleteBag(connID)
	if fn := m.kickFn.Load(); fn != nil {
		return (*fn)(connID)
	}
	return false
}

// SetKicker 设置踢下线回调函数。
func (m *ConnectionManager) SetKicker(fn func(connID string) bool) {
	if fn == nil {
		m.kickFn.Store(nil)
		return
	}
	m.kickFn.Store(&fn)
}

// ParkBag 断线时暂存 connBag，启动宽限计时器。
// 返回暂存的 PendingBag 引用，供外部完成 event 发射。
func (m *ConnectionManager) ParkBag(connID, owner string) *PendingBag {
	v, ok := m.connBags.LoadAndDelete(connID)
	if !ok {
		return nil
	}
	bag, ok := v.(*sync.Map)
	if !ok {
		return nil
	}
	pkey := owner
	if pkey == "" {
		pkey = connID
	}
	pb := &PendingBag{Bag: bag, ConnID: connID}
	if prev, loaded := m.pendingBags.Swap(pkey, pb); loaded {
		if old, ok := prev.(*PendingBag); ok && old.Timer != nil {
			old.Timer.Stop()
		}
	}
	pb.Timer = time.AfterFunc(m.grace, func() {
		m.pendingBags.CompareAndDelete(pkey, pb)
	})
	// 与 RestoreBag / DeleteBag 的 settled 协议见 PendingBag 注释：
	// 若本 pb 在发布后、Timer 赋值前已被清理，这里补一次 Stop，
	// 避免定时器陪跑到宽限到期才回收。
	if pb.settled.Load() {
		pb.Timer.Stop()
	}
	return pb
}

// RestoreBag 重连时从 pending 恢复到活跃连接。
// 返回恢复的 bag（or nil 表示无 pending 记录）。
func (m *ConnectionManager) RestoreBag(owner, newConnID string) *sync.Map {
	v, ok := m.pendingBags.LoadAndDelete(owner)
	if !ok {
		return nil
	}
	pb, ok := v.(*PendingBag)
	if !ok {
		return nil
	}
	// 先置 settled（见 PendingBag 注释）：即使此刻 Timer 尚未赋值，
	// ParkBag 的收尾复查也会 Stop 掉随后创建的定时器。
	pb.settled.Store(true)
	if pb.Timer != nil {
		pb.Timer.Stop()
	}
	m.connBags.Store(newConnID, pb.Bag)
	return pb.Bag
}

// ListConnIDs 返回当前活跃 connID 快照。
// 供灰度下线编排（drain）枚举待迁移 / 待踢的连接，顺序不稳定。
func (m *ConnectionManager) ListConnIDs() []string {
	ids := make([]string, 0, 64)
	m.connBags.Range(func(key, value any) bool {
		if id, ok := key.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}

// RangeConns 遍历所有活跃 connID；fn 返回 false 时提前停止。
func (m *ConnectionManager) RangeConns(fn func(connID string) bool) {
	if fn == nil {
		return
	}
	m.connBags.Range(func(key, value any) bool {
		id, ok := key.(string)
		if !ok {
			return true
		}
		return fn(id)
	})
}

// CountActive 返回当前活跃连接数。
func (m *ConnectionManager) CountActive() int {
	n := 0
	m.connBags.Range(func(key, value any) bool {
		if _, ok := value.(*sync.Map); ok {
			n++
		}
		return true
	})
	return n
}

// GetPlayerID 从指定连接中读取 player_id。
func (m *ConnectionManager) GetPlayerID(connID string) string {
	v, ok := m.connBags.Load(connID)
	if !ok {
		return ""
	}
	bag, ok := v.(*sync.Map)
	if !ok {
		return ""
	}
	pid, ok := bag.Load("player_id")
	if !ok {
		return ""
	}
	s, _ := pid.(string)
	return s
}

// RestorePlayerID 从 connBag 恢复 player_id 到 ctx。
func RestorePlayerID(bag *sync.Map, setPlayerID func(string)) {
	pid, ok := bag.Load("player_id")
	if !ok {
		return
	}
	s, ok := pid.(string)
	if !ok || s == "" {
		return
	}
	setPlayerID(s)
}
