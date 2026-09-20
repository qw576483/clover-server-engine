// package session 提供统一的会话抽象，是 TCP / WS / UDP / QUIC / WT 五种网络连接的公共底座。
//
// 传输层统一会话抽象，仅供引擎内部（net/* 传输实现）使用，未对 pkg 公开。
package session

import (
	"errors"
	"sync"

	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed 连接已关闭。
	ErrClosed = errors.New("session_base: connection closed")
	// ErrServerClosed 服务器已停止。
	ErrServerClosed = errors.New("session_base: server closed")
	// ErrUnreliableNotSupported 连接不支持不可靠传输。
	ErrUnreliableNotSupported = errors.New("session: unreliable transport not supported")
	// ErrSendTimeout 发送队列满且等待超时（连接本身**未**关闭）。
	//
	// 只此一份：tcp 与 ws 连接层此前各定义一个同义 sentinel（各带包名前缀），
	// 调用方要按传输类型分别判断。统一后一次 errors.Is 即可覆盖两种传输，
	// 并与 ErrClosed 明确区分——避免把「对端太慢」误判成断线而触发清理/重连（假断线）。
	ErrSendTimeout = errors.New("session: send timeout (send buffer full)")
)

// 连接写入相关的默认超时：tcp / ws 连接层共用（此前两边各写一份同名常量，值会漂移）。
const (
	// DefaultWriteTimeout 单次写操作的兜底超时（心跳开启时以心跳间隔为限）。
	DefaultWriteTimeout = 30 * time.Second
	// DefaultSendTimeout Send 写入队列满时的等待上限，超时返回 ErrSendTimeout。
	DefaultSendTimeout = 5 * time.Second
)

// Session 统一会话接口。所有网络层连接（TCP/WS/UDP/QUIC/WT）均实现该接口，
// 以便网关把不同协议的连接放入同一个 Manager 统一下发。
type Session interface {
	// ConnID 返回连接唯一标识。
	ConnID() string
	// Send 向对端发送可靠数据；连接已关闭时返回 ErrClosed。
	Send(data []byte) error
	// SendUnreliable 向对端发送不可靠数据（Datagram/裸UDP）。
	// 不支持不可靠传输的连接（TCP/WS）降级为 Send。
	SendUnreliable(data []byte) error
	// Close 关闭连接（幂等）。
	Close() error
	// RemoteAddr 对端地址（字符串形式）。
	RemoteAddr() string
	// IsClosed 是否已关闭。
	IsClosed() bool
	// Capabilities 返回连接能力（支持的传输模式）。
	Capabilities() ConnCapabilities
}

// ConnCapabilities 描述连接支持的传输能力。
type ConnCapabilities struct {
	// Reliable 可靠传输（Stream/TCP/WS）。
	Reliable bool
	// Unreliable 不可靠传输（Datagram/裸UDP）。
	Unreliable bool
	// UnreliableViaDatagram 通过 QUIC/WT Datagram 实现不可靠传输。
	UnreliableViaDatagram bool
	// UnreliableViaRawUDP 通过裸 UDP 实现不可靠传输（QUIC+UDP 模式）。
	UnreliableViaRawUDP bool
}

var connSeq atomic.Uint64

// NewConnID 生成全局唯一连接 ID（"c-<序号>"）。
func NewConnID() string {
	return "c-" + conv.FormatUint(connSeq.Add(1))
}

// BaseConn 连接公共骨架，供各网络层 Conn 内嵌，提供：
//   - 原子幂等的关闭状态（closed / closeCh）
//   - ConnID / RemoteAddr / IsClosed 默认实现
//   - markClosed 首次关闭时关闭通知 channel
//   - SendUnreliable 默认返回 ErrUnreliableNotSupported（具体 Conn 覆写为降级或 Datagram 发送）
//   - Capabilities 默认返回仅支持可靠传输
//
// 具体网络层需自行实现 Send / Close（发送逻辑与底层连接耦合）。
type BaseConn struct {
	id      string
	remote  string
	closed  atomic.Bool
	closeCh chan struct{}
}

// NewBaseConn 构造骨架。closeCh 仅在首次 Close 时关闭。
func NewBaseConn(id, remote string) BaseConn {
	return BaseConn{id: id, remote: remote, closeCh: make(chan struct{})}
}

// ConnID 返回连接 ID。
func (b *BaseConn) ConnID() string { return b.id }

// RemoteAddr 返回对端地址。
func (b *BaseConn) RemoteAddr() string { return b.remote }

// IsClosed 是否已关闭。
func (b *BaseConn) IsClosed() bool { return b.closed.Load() }

// ClosedCh 返回关闭通知 channel（仅关闭一次）。
func (b *BaseConn) ClosedCh() <-chan struct{} { return b.closeCh }

// MarkClosed 原子标记关闭，并在首次关闭时关闭 closeCh。返回是否本次为首次关闭。
// 各网络层 Conn 在自身 Close() 中调用它（跨包内嵌亦可访问，因其为导出方法）。
func (b *BaseConn) MarkClosed() (first bool) {
	if b.closed.Swap(true) {
		return false
	}
	close(b.closeCh)
	return true
}

// SendUnreliable 默认实现：不降级，返回 ErrUnreliableNotSupported。
// 由于 BaseConn 不包含 Send 的具体实现，这里返回 ErrUnreliableNotSupported。
// 具体 Conn 类型（如 tcp.Conn / ws.Conn）会覆写为 Send 降级；quic.Conn / wt.Conn 会覆写为真正的 Datagram 发送。
func (b *BaseConn) SendUnreliable(data []byte) error {
	return ErrUnreliableNotSupported
}

// Capabilities 默认实现：仅支持可靠传输。
func (b *BaseConn) Capabilities() ConnCapabilities {
	return ConnCapabilities{Reliable: true}
}

// Manager 会话管理器：以 ConnID 为键维护在线会话，支持单发 / 广播 / 遍历 / 全关。
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]Session
}

// NewManager 创建空会话管理器。
func NewManager() *Manager {
	return &Manager{sessions: make(map[string]Session)}
}

// Add 注册会话。已存在相同 ConnID 时覆盖（并关闭旧会话）。
// 旧连接 Close 可能持有其他锁（如 UDP srv.mu），锁内调用会形成锁顺序反转死锁。
// 因此取 old 后释放本锁再 Close。
func (m *Manager) Add(s Session) {
	m.mu.Lock()
	old, ok := m.sessions[s.ConnID()]
	if ok && old == s {
		m.mu.Unlock()
		return
	}
	m.sessions[s.ConnID()] = s
	m.mu.Unlock()
	if ok {
		_ = old.Close()
	}
}

// Remove 移除指定 ConnID 会话。
func (m *Manager) Remove(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

// Get 按 ConnID 取会话。
func (m *Manager) Get(id string) (Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

// Range 遍历所有会话（回调返回 false 可提前结束）。
func (m *Manager) Range(fn func(s Session) bool) {
	m.mu.RLock()
	list := make([]Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		list = append(list, s)
	}
	m.mu.RUnlock()
	for _, s := range list {
		if !fn(s) {
			return
		}
	}
}

// Broadcast 向所有在线会话发送数据（忽略单条发送失败）。
func (m *Manager) Broadcast(data []byte) {
	m.Range(func(s Session) bool {
		_ = s.Send(data)
		return true
	})
}

// Len 返回在线会话数。
func (m *Manager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// CloseAll 关闭并移除所有会话。
func (m *Manager) CloseAll() {
	m.mu.Lock()
	list := m.sessions
	m.sessions = make(map[string]Session)
	m.mu.Unlock()
	for _, s := range list {
		_ = s.Close()
	}
}
