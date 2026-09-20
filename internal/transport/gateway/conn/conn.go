// Package conn 提供网关统一连接抽象：在 net 层统一会话（session.Session，
// 兼容 TCP / WS）之上附加网关层元数据能力（uid、路由标签等），并保持与底层协议解耦。
//
// 网关内部连接封装，仅供引擎内部（gwcore）使用，未对 pkg 公开。
package conn

import (
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/shared/safe"
)

// metaKeyPlayerID 连接级元数据中存放已绑定玩家 UID 的键名。
const metaKeyPlayerID = "uid"

// Conn 网关统一连接抽象：在 net.Session 之上扩展网关层元数据能力。
// 任意实现了 session.Session 的连接（TCP/WS）皆可 Wrap 为 Conn，从而兼容两种协议。
type Conn interface {
	session.Session
	// SetMeta 设置连接级元数据（如 uid、路由标签）。
	SetMeta(key string, val any)
	// GetMeta 读取连接级元数据。
	GetMeta(key string) (any, bool)
	// DelMeta 删除元数据。
	DelMeta(key string)
	// Meta 返回元数据只读快照。
	Meta() map[string]any

	// BindPlayer 将本连接绑定到玩家 UID（在登录/鉴权成功后调用）。
	// 绑定后可通过 PlayerID 取回，用于下行投递、会话管理等。
	BindPlayer(uid string)
	// PlayerID 返回已绑定的玩家 UID；未绑定时 ok=false。
	PlayerID() (string, bool)
	// UnbindPlayer 解除本连接的玩家绑定（登出/掉线时调用）。
	UnbindPlayer()
	// ClosedCh 返回连接关闭通知 channel（仅关闭一次），供网关检测客户端断线。
	ClosedCh() <-chan struct{}
}

// wrapped 包装任意 session.Session，附加网关元数据。
type wrapped struct {
	session.Session
	mu      sync.RWMutex
	meta    map[string]any
	closeCh chan struct{}
	closed  atomic.Bool
}

// Wrap 将底层网络会话包装为带元数据的网关连接，统一兼容 TCP/WS。
// 若底层实现了 ClosedCh()（返回 channel，关闭即代表连接断开），直接监听该 channel 关闭
// 自动触发 closeSelf；否则通过周期性 IsClosed 轮询兜底感知连接断开。
func Wrap(s session.Session) Conn {
	w := &wrapped{Session: s, meta: make(map[string]any), closeCh: make(chan struct{})}
	if cw, ok := s.(interface{ ClosedCh() <-chan struct{} }); ok {
		safe.GoSafe(func() {
			<-cw.ClosedCh()
			w.closeSelf()
		})
	} else {
		// 底层未提供 ClosedCh 时，通过周期性 IsClosed 兜底感知，
		// 确保 closeCh 最终关闭（即使 Close 未被外部显式调用）。
		safe.GoSafe(func() {
			defer w.closeSelf()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				if s.IsClosed() {
					return
				}
			}
		})
	}
	return w
}

// closeSelf 原子地首次关闭 closeCh（幂等）。
func (w *wrapped) closeSelf() {
	if w.closed.CompareAndSwap(false, true) {
		close(w.closeCh)
	}
}

// ClosedCh 返回连接关闭通知 channel（仅关闭一次）。
func (w *wrapped) ClosedCh() <-chan struct{} { return w.closeCh }

func (w *wrapped) SetMeta(key string, val any) {
	w.mu.Lock()
	w.meta[key] = val
	w.mu.Unlock()
}

func (w *wrapped) GetMeta(key string) (any, bool) {
	w.mu.RLock()
	v, ok := w.meta[key]
	w.mu.RUnlock()
	return v, ok
}

func (w *wrapped) DelMeta(key string) {
	w.mu.Lock()
	delete(w.meta, key)
	w.mu.Unlock()
}

func (w *wrapped) Meta() map[string]any {
	w.mu.RLock()
	defer w.mu.RUnlock()
	cp := make(map[string]any, len(w.meta))
	for k, v := range w.meta {
		cp[k] = v
	}
	return cp
}

// BindPlayer 将本连接绑定到玩家 UID（登录/鉴权成功后调用）。
func (w *wrapped) BindPlayer(uid string) { w.SetMeta(metaKeyPlayerID, uid) }

// PlayerID 返回已绑定的玩家 UID；未绑定时 ok=false。
func (w *wrapped) PlayerID() (string, bool) {
	v, ok := w.GetMeta(metaKeyPlayerID)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// UnbindPlayer 解除本连接的玩家绑定（登出/掉线时调用）。
func (w *wrapped) UnbindPlayer() { w.DelMeta(metaKeyPlayerID) }

// Close 关闭底层连接并关闭 closeCh（幂等，供网关会话清理）。
// defer closeSelf 保证无论 Session.Close 成功与否，closeCh 都会关闭。
func (w *wrapped) Close() error {
	defer w.closeSelf()
	return w.Session.Close()
}

// Manager 网关连接管理器（语义等同 session.Manager，统一命名以便业务引用）。
type Manager = session.Manager

// NewManager 创建网关连接管理器。
func NewManager() *Manager { return session.NewManager() }

// 编译期断言：wrapped 满足 Conn 接口。
var _ Conn = (*wrapped)(nil)
