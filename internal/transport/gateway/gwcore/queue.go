package gwcore

import (
	"sync"
	"time"

	isession "clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/shared/safe"
)

// queuePositionRefreshInterval 排队位置刷新的固定间隔。
//
// 为什么需要刷新：位置只在入队那一刻算一次的话，队伍前进了 500 人客户端仍显示「前面还有 5000 人」。
// 为什么是固定常量而不是配置项：刷新只做「位置变了才发」，代价随队列长度线性（每间隔 ≤ N 条帧），
// 而排队本身就是稀缺路径（正常容量下队列恒空、一条不发），不值得再增加一个配置面。
const queuePositionRefreshInterval = 3 * time.Second

// pendingConn 等候队列中的一个挂起连接：已接受客户端连接但尚未建会话
// （受限流 / 满载拦截）。首帧（如登录请求）一并缓冲，放行时重放。
type pendingConn struct {
	raw       isession.Session
	payload   []byte // 首帧缓冲，放行时重放
	enqueueAt time.Time
	timer     *time.Timer // 排队超时定时器（可选，QueueTimeout>0 时设置）
	// ticket 排队编号（入队序号，单调递增），随每次位置通知下发，供展示与排障。
	// 不参与排队逻辑（顺序只由 items 切片决定）。
	ticket int64
	// sentAhead 最近一次已通知客户端的「前面还有多少人」；-1 = 尚未通知（待下发）。
	// 由队列锁保护：刷新时先比对该值，位置没变就不发（避免每轮刷屏）。
	sentAhead int
}

// connQueue 连接等候队列（排队放行）。当网关限流 / 满载时，新连接不直接拒绝，
// 而是进入本队列；release 回调按固定速率把队首连接「放行」，由 Gateway 建会话并重放首帧。
//
// 设计要点：
// - 队列容量由 QueueCap 控制；满则新连接被拒绝（或继续受限流约束）。
// - 放行速率由 QueueReleasePerSec 控制（时间窗口放行，避免登录风暴）；
// 该速率与 MaxConns（活跃会话上限）共同形成背压：仍满载时放回队首等待。
// - 每个挂起连接可设 QueueTimeout，超时仍未放行则关闭客户端连接。
// - 位置通知：入队时下发一次，之后每 queuePositionRefreshInterval 检查一次位置变化并下发
// （notify 回调由 Gateway 注入；排队中连接尚未建会话，只能由网关直发帧）。
type connQueue struct {
	mu      sync.Mutex
	cap     int
	rate    int // 每秒放行数；<=0 表示尽快放行进队者
	items   []*pendingConn
	pending map[string]*pendingConn // connID -> pc，O(1) 判重（isPending）
	release func(*pendingConn)
	notify  func(pc *pendingConn, ahead, total int) // 位置下发回调（变位置才调用；网络 I/O 在锁外）
	seq     int64                                   // 入队序号（生成 ticket）
	stopCh  chan struct{}
	stopped bool
}

// newConnQueue 构造等候队列；capacity<=0 时返回 nil（不启用排队）。
// notify 可为 nil（不启用位置通知）：此时队列行为与加此能力前完全一致。
func newConnQueue(capacity, releasePerSec int, release func(*pendingConn), notify func(pc *pendingConn, ahead, total int)) *connQueue {
	if capacity <= 0 {
		return nil
	}
	return &connQueue{
		cap:     capacity,
		rate:    releasePerSec,
		pending: make(map[string]*pendingConn),
		release: release,
		notify:  notify,
		stopCh:  make(chan struct{}),
	}
}

// offer 尝试入队；队列已满返回 ok=false。
// ahead/total 为入队时刻的位置快照（ahead = 前面还有多少人，total = 队列总人数含自己），
// 与入队在同一个临界区内算出——调用方持此结果在锁外下发通知，避免与放行循环竞态后发错位置。
func (q *connQueue) offer(pc *pendingConn) (ahead, total int, ok bool) {
	if q == nil {
		return 0, 0, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	// 已停止：q.pending 已在 stop() 中置 nil，继续写入会向 nil map 赋值 panic。
	if q.stopped {
		return 0, 0, false
	}
	if len(q.items) >= q.cap {
		return 0, 0, false
	}
	q.items = append(q.items, pc)
	q.pending[pc.raw.ConnID()] = pc
	q.seq++
	pc.ticket = q.seq
	pc.sentAhead = -1 // 待首次下发
	return len(q.items) - 1, len(q.items), true
}

// dequeue 取出队首；空队列返回 nil。
func (q *connQueue) dequeue() *pendingConn {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	pc := q.items[0]
	// 先清首位再切片：底层数组会复用，不清会把已出队项（含 raw/timer）一直挂在数组头上。
	q.items[0] = nil
	q.items = q.items[1:]
	delete(q.pending, pc.raw.ConnID())
	return pc
}

// requeueHead 把未能放行的挂起连接放回队首（如仍满载或上游暂不可用）。
// 先移除既有副本（若仍在队列中），保证幂等、不重复入队。
func (q *connQueue) requeueHead(pc *pendingConn) {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		// 停机中队列不再收留挂起连接：直接关闭，避免它脱离一切回收路径
		// （stop 只关闭仍在 items 里的连接，本连接已 dequeue、不在其中）。
		_ = pc.raw.Close()
		return
	}
	defer q.mu.Unlock()
	for i, it := range q.items {
		if it == pc {
			q.items = append(q.items[:i], q.items[i+1:]...)
			q.items[len(q.items)-1] = nil // 清尾：避免底层数组残留被删项的引用
			break
		}
	}
	q.items = append([]*pendingConn{pc}, q.items...)
	q.pending[pc.raw.ConnID()] = pc
	// 本次放行未成功（仍满载 / 上游不可用），位置回到队首：标记待通知，
	// 让刷新循环告客户端「前面还有 0 人」，否则客户端会停在放行前看到的旧数字上。
	pc.sentAhead = -1
}

// refreshPositions 按位置变化下发排队位置通知（放行循环按 queuePositionRefreshInterval 调用）。
//
// 位置没变的连接不发（sentAhead 比对），因此稳态下每轮只对「确实前进了」的连接发一条；
// 通知在锁外发出（网络 I/O 不持队列锁，与 release 同款约束）。
func (q *connQueue) refreshPositions() {
	if q == nil || q.notify == nil {
		return
	}
	type update struct {
		pc    *pendingConn
		ahead int
		total int
	}
	var updates []update
	q.mu.Lock()
	total := len(q.items)
	for i, pc := range q.items {
		if pc.sentAhead == i {
			continue
		}
		pc.sentAhead = i
		updates = append(updates, update{pc: pc, ahead: i, total: total})
	}
	q.mu.Unlock()
	for _, u := range updates {
		q.notify(u.pc, u.ahead, u.total)
	}
}

// markNotified 记录某挂起连接最近一次「已实际下发」的位置（由 notifyQueued 在下发成功后调用）。
//
// offer / requeueHead 会把 sentAhead 置 -1（待通知），若下发成功后不回写实际位置，
// refreshPositions 会因 -1 != i 判定「未通知」而把同一条位置重复下发一次（队首连接必然命中）。
// refreshPositions 自身在锁内先回写再下发，重复调用本方法写入同值无害。
func (q *connQueue) markNotified(pc *pendingConn, ahead int) {
	if q == nil {
		return
	}
	q.mu.Lock()
	pc.sentAhead = ahead
	q.mu.Unlock()
}

// remove 从队列中移除指定挂起连接；返回是否真正移除（已不在队列中则 false）。
func (q *connQueue) remove(pc *pendingConn) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.pending[pc.raw.ConnID()]; !ok {
		return false
	}
	for i, it := range q.items {
		if it == pc {
			q.items = append(q.items[:i], q.items[i+1:]...)
			q.items[len(q.items)-1] = nil // 清尾：避免底层数组残留被删项（含 timer）的引用
			break
		}
	}
	delete(q.pending, pc.raw.ConnID())
	return true
}

// contains 判断某 connID 是否仍在排队中（用于 handleClient 丢弃排队期间的后续帧）。
func (q *connQueue) contains(connID string) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.pending[connID]
	return ok
}

// len 当前排队长度。
func (q *connQueue) len() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// start 启动放行循环与位置刷新循环（在 Gateway.Start 中调用）。
func (q *connQueue) start() {
	if q == nil {
		return
	}
	interval := 10 * time.Millisecond // 未显式限速时尽快放行进队者
	if q.rate > 0 {
		interval = time.Duration(int64(time.Second) / int64(q.rate))
		// rate 超过每秒 1e9 时整除结果为 0（误配的超大值），给 NewTicker 保底 1ns 防 panic。
		if interval <= 0 {
			interval = time.Nanosecond
		}
	}
	safe.GoSafe(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-q.stopCh:
				return
			case <-ticker.C:
				pc := q.dequeue()
				if pc == nil {
					continue
				}
				if q.release != nil {
					q.release(pc)
				}
			}
		}
	})
	// 位置刷新**独立成一条 goroutine**，不进上面的放行循环：单个客户端不读（半开连接）
	// 时 Send 会一直阻塞到发送超时，若与放行同循环，一次阻塞就拖慢整队的放行吞吐。
	// Ticker 在消费不及时会丢 tick，因此这里不会堆积待执行的刷新轮次。
	// 未启用位置通知时不启动（零开销）；停止时随 stopCh 退出，与放行循环同生命周期。
	if q.notify != nil {
		safe.GoSafe(func() {
			ticker := time.NewTicker(queuePositionRefreshInterval)
			defer ticker.Stop()
			for {
				select {
				case <-q.stopCh:
					return
				case <-ticker.C:
					q.refreshPositions()
				}
			}
		})
	}
}

// stop 停止放行循环（在 Gateway.Stop 中调用）。
// 先检查 stopped 标志，避免重复 close(stopCh) 导致 panic。
// 停机时清空队列中所有挂起连接并关闭其底层连接：否则 QueueTimeout==0 时这些连接
// 永远不会被超时关闭，导致客户端连接与 tryQueue 监听 goroutine 泄漏直到客户端自行断开。
func (q *connQueue) stop() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	items := q.items
	q.items = nil
	q.pending = nil
	q.mu.Unlock()
	close(q.stopCh)
	// 关闭连接（网络 I/O）放在锁外：任一连接 Close 阻塞时不得持队列锁，
	// 否则 offer/contains 等调用方（传输层读循环）会被一起卡住。
	for _, pc := range items {
		if pc.timer != nil {
			pc.timer.Stop()
		}
		_ = pc.raw.Close()
	}
}
