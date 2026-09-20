package tcp

import (
	"errors"
	"sync"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// Pool 网关 ↔ 逻辑服直连 TCP 连接池：维护常驻连接，支持预热、上限、健康复用、自动剔除死连接。
type Pool struct {
	cfg   PoolConfig
	mu    sync.Mutex
	conns []*Conn            // 所有连接（含在用）
	idle  []*Conn            // 空闲连接
	owned map[*Conn]struct{} // 所有权集合，O(1) 校验 Put 归还的连接是否属于本池
	// reserving 拨号中预留的槽位数（计数式预留）。
	reserving int
	// closed 池是否已关闭。Close 会把 owned/conns/idle 清空，
	// Get 的拨号慢路径必须在回写前检查该标志（否则向已置 nil 的 owned 写入即 panic）。
	closed bool
}

// NewPool 创建连接池并预热 MinConns 条连接。
func NewPool(cfg PoolConfig) (*Pool, error) {
	// 先 normalize 补默认值再校验语义（MinConns>MaxConns 属配置错误）。
	// 先 normalize 是为了让 MaxConns<=0 时的默认值也纳入校验，避免 MinConns 超过默认上限。
	c := cfg.normalize()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	p := &Pool{cfg: c, owned: make(map[*Conn]struct{}, c.MaxConns)}
	for i := 0; i < c.MinConns; i++ {
		conn, err := Dial(ClientConfig{
			Address:           c.Address,
			HeartbeatInterval: c.HeartbeatInterval,
			DialTimeout:       c.DialTimeout,
			MaxMsgSize:        c.MaxMsgSize,
			SendBufferSize:    c.SendBufferSize,
		}, c.Handler)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.conns = append(p.conns, conn)
		p.idle = append(p.idle, conn)
		p.owned[conn] = struct{}{}
	}
	return p, nil
}

// Get 取一条可用连接：优先复用空闲连接，否则在 MaxConns 内惰性新建。
// 持锁预留槽位后再拨号，避免并发 Get 双双越过 MaxConns 检查。
func (p *Pool) Get() (*Conn, error) {
	p.mu.Lock()
	// 已关闭：拒绝服务（而不是继续拨号/复用已清空的池）。
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("tcp pool: closed")
	}
	// 循环剔除栈顶死连接（不递归：idle 大量死连接时递归深度=死连接数）。
	for n := len(p.idle); n > 0; n = len(p.idle) {
		conn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		if conn.IsClosed() {
			p.removeClosedLocked(conn)
			continue
		}
		p.mu.Unlock()
		return conn, nil
	}
	if len(p.conns)+p.reserving >= p.cfg.MaxConns {
		// 达到上限前先清理已关闭连接，回收「漏 Put 但底层已断」占用的槽位，
		// 避免死连接永久占位导致池提前耗尽。
		p.pruneClosedLocked()
		if len(p.conns)+p.reserving >= p.cfg.MaxConns {
			p.mu.Unlock()
			return nil, errors.New("tcp pool: exhausted")
		}
	}
	// 计数式预留防止并发超限（不在 conns 中放 nil 占位符）。
	p.reserving++
	p.mu.Unlock()

	conn, err := Dial(ClientConfig{
		Address:           p.cfg.Address,
		HeartbeatInterval: p.cfg.HeartbeatInterval,
		DialTimeout:       p.cfg.DialTimeout,
		MaxMsgSize:        p.cfg.MaxMsgSize,
		SendBufferSize:    p.cfg.SendBufferSize,
	}, p.cfg.Handler)
	p.mu.Lock()
	p.reserving--
	// 拨号期间池可能已被 Close：此时 owned 已置 nil，直接写入会 panic。
	// 关闭新连接并把错误返回给调用方（拨号本身成功，但池已不再接受连接）。
	if p.closed {
		p.mu.Unlock()
		if err == nil {
			_ = conn.Close()
		}
		return nil, errors.New("tcp pool: closed")
	}
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.conns = append(p.conns, conn)
	p.owned[conn] = struct{}{}
	p.mu.Unlock()
	return conn, nil
}

// Put 归还连接到空闲池。只接收本池创建的连接，忽略外部连接避免 Get 返回错连。
func (p *Pool) Put(conn *Conn) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	// O(1) 所有权检查：通过 owned map 快速判断是否为池内连接。
	if _, ok := p.owned[conn]; !ok {
		p.mu.Unlock()
		return
	}
	// 已断开的连接不入空闲池：直接从 conns 剔除释放槽位，
	// 避免死连接占用 idle 待 Get 时才发现（白耗一次取用）。
	if conn.IsClosed() {
		p.removeClosedLocked(conn)
		p.mu.Unlock()
		return
	}
	// 检查是否已在 idle 列表中（防重复归还）。
	for _, c := range p.idle {
		if c == conn {
			p.mu.Unlock()
			return
		}
	}
	p.idle = append(p.idle, conn)
	p.mu.Unlock()
}

// pruneClosedLocked 移除 conns 中所有已关闭的连接（调用方须持 p.mu）。
// 同步从 idle 中剔除，防止 Get 复用到已关闭连接。回收漏 Put 且底层已断的槽位。
//
// nil 连接与已关闭连接一并剔除：nil 会被误判为存活，永久占住 conns/idle 槽位。
func (p *Pool) pruneClosedLocked() {
	// 一次遍历同时构建 conns 和 idle 的存活列表，避免两次 O(N) 扫描。
	aliveConns := p.conns[:0]
	aliveSet := make(map[*Conn]struct{}, len(p.conns))
	for _, c := range p.conns {
		if c == nil || c.IsClosed() {
			delete(p.owned, c)
			continue
		}
		aliveConns = append(aliveConns, c)
		aliveSet[c] = struct{}{}
	}
	p.conns = aliveConns
	if len(p.idle) > 0 {
		aliveIdle := p.idle[:0]
		for _, c := range p.idle {
			if c == nil || c.IsClosed() {
				continue
			}
			if _, ok := aliveSet[c]; ok {
				aliveIdle = append(aliveIdle, c)
			}
		}
		p.idle = aliveIdle
	}
}

// removeClosedLocked 从 conns 中移除指定连接（调用方须持 p.mu）。
func (p *Pool) removeClosedLocked(conn *Conn) {
	for i, c := range p.conns {
		if c == conn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			delete(p.owned, conn)
			break
		}
	}
}

// Close 关闭所有连接。
func (p *Pool) Close() {
	p.mu.Lock()
	// 置 closed 后再清空：Get 的慢路径据此拒绝向已置 nil 的 owned 写入（并发窗口下的 panic）。
	p.closed = true
	conns := p.conns
	p.conns = nil
	p.idle = nil
	p.owned = nil
	p.mu.Unlock()
	for _, c := range conns {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			logger.Errorf("tcp pool close: %v", err)
		}
	}
}
