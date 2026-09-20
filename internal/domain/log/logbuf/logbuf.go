// #nosec G115 -- 长度为本地缓冲切片长度（已由 append/cap 保证），无外部可控的超大值。

// Package logbuf 提供业务日志缓冲器。
//
// 用 ringbuf 攒积高频业务日志（多 goroutine 并发 Emit 无锁竞争），
// 由后台 goroutine 定时 Drain 并一次性批量上报到 log 服，避免每条日志都走一次网络 RPC。
//
// 多实例与负载均衡：批次按轮询分发到不同 log 实例，每个打到的实例维持一条长连接。
// 分发目标由 app 层注入的 Dialer 提供（etcd 服务发现 + 轮询选实例）；
// 未注入 Dialer、或当前发现不到实例时，回退到配置里的静态 Addr。
//
// 与旧实现的差别（旧实现的问题：启动时连不上就永久静默丢弃全部业务日志）：
//   - 建连懒加载：New 不再因 log 服暂时不可达而失败，后续批次自动重试；
//   - 写入失败只影响本片：弃用该实例的连接（下批自动重连），并计入丢弃统计 + 告警；
//   - 不再静默：无实例 / 建连失败 / 写入失败都会打日志（5s 节流）并可经 Stats 观测。
package logbuf

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/internal/domain/log/client"
	"clover-server-engine/internal/domain/log/state"
	plogbuf "clover-server-engine/pkg/foundation/logbuf"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/ringbuf"
)

// LogEntry 单条业务日志，真身定义在 pkg/foundation/logbuf。
type LogEntry = plogbuf.LogEntry

// Option 业务日志选填项，真身定义在 pkg/foundation/logbuf。
type Option = plogbuf.Option

// Dialer 按负载均衡策略挑选一个可用的 log 实例地址。
// 由 app 层注入（内部是 etcd 服务发现 + 轮询）；为 nil 时退化为静态 Addr。
type Dialer interface {
	// Pick 返回一个实例地址；返回空串表示当前无可用实例。
	Pick() string
}

// Writer 日志发送连接的最小能力。*client.Client 实现之；测试可注入假连接。
type Writer interface {
	// WriteBatch 批量上报日志，返回实际写入条数。
	WriteBatch(source string, entries []state.LogEntry) (int, error)
	// Close 关闭连接。
	Close() error
}

// DialFunc 按地址建立一条 log 服连接（默认 client.Dial；测试可替换）。
type DialFunc func(addr string) (Writer, error)

// Options 缓冲器配置。
type Options struct {
	Addr          string        // 静态回退地址（未接入发现 / 发现列表为空时使用）
	Source        string        // 来源节点标识（nodeID）
	Capacity      int           // 环形缓冲容量；0=默认 4096
	FlushInterval time.Duration // 定时上报间隔；0=默认 1s
	MaxBatch      int           // 单批最大条数；0=默认 512

	// Dialer 负载均衡选择器（选填）：非 nil 时每片日志轮询选实例。
	Dialer Dialer
	// Dial 建连函数（选填）：默认 client.Dial。
	Dial DialFunc
}

// dropLogInterval 连续丢弃时打告警日志的最小间隔，避免实例全挂时刷屏。
const dropLogInterval = 5 * time.Second

// connIdleTTL 长连接空闲回收阈值：实例从服务发现列表消失（地址漂移）后
// 其连接不再有批次使用，必须回收（TCP 连接 + 其读/心跳 goroutine），
// 否则 conns 无上限增长。
const connIdleTTL = 30 * time.Minute

// maxBatchBytes 单批上报的最大估算字节：对端默认单帧上限 256KB，
// 留出余量避免整批被拒并断连（分片只按条数切分时，单批 JSON 可能超限）。
const maxBatchBytes = 192 << 10

// connEntry 一条长连接 + 最近使用时间（供空闲回收）。
type connEntry struct {
	w        Writer
	lastUsed atomic.Int64 // Unix 毫秒
}

// Buffer 业务日志缓冲器。
type Buffer struct {
	ring   *ringbuf.MpmcRing[state.LogEntry]
	source string

	dialer Dialer
	static string
	dial   DialFunc

	// conns：每个 log 实例一条长连接。懒建 + 失败即弃，故无需单独的重连器。
	mu    sync.Mutex
	conns map[string]*connEntry

	sent      atomic.Uint64
	dropped   atomic.Uint64
	reconnect atomic.Uint64

	lastDropLog atomic.Int64 // 上次「丢弃」告警的 Unix 毫秒（节流用）

	flushInterval time.Duration
	maxBatch      int

	closeOnce sync.Once
	closed    atomic.Bool // Close 后拒绝入队（无人消费，静默消失）

	stopCh chan struct{}
	done   chan struct{}
}

// Stats 上报统计快照（供 admin / metrics 观测）。
type Stats struct {
	Sent      uint64 // 成功上报条数
	Dropped   uint64 // 丢弃条数（无实例 / 建连失败 / 写入失败）
	Reconnect uint64 // 因写入失败而弃用连接、待重连的次数
	Conns     int    // 当前保持的长连接数（即已打到的实例数）
}

// New 创建并启动缓冲器（后台定时上报）。
//
// 建连是懒加载的：log 服暂时不可达不会让 New 失败，后续批次会自动重试。
func New(opts Options) (*Buffer, error) {
	capacity := opts.Capacity
	if capacity <= 0 {
		capacity = 4096
	}
	interval := opts.FlushInterval
	if interval <= 0 {
		interval = time.Second
	}
	maxBatch := opts.MaxBatch
	if maxBatch <= 0 {
		maxBatch = 512
	}
	dial := opts.Dial
	if dial == nil {
		dial = func(addr string) (Writer, error) { return client.Dial(addr) }
	}

	b := &Buffer{
		ring:          ringbuf.NewMpmc[state.LogEntry](capacity),
		source:        opts.Source,
		dialer:        opts.Dialer,
		static:        opts.Addr,
		dial:          dial,
		conns:         make(map[string]*connEntry),
		flushInterval: interval,
		maxBatch:      maxBatch,
		stopCh:        make(chan struct{}),
		done:          make(chan struct{}),
	}
	if b.dialer == nil && b.static == "" {
		return nil, errors.New("logbuf: neither Dialer nor Addr is set, no log instance to report to")
	}
	go b.loop()
	return b, nil
}

// AddLog 写入一条业务日志（必填字段 + option 选填）。
// 缓冲满时丢弃（返回 false），不阻塞调用方。
func (b *Buffer) AddLog(ownerType, ownerID, typ, info string, opts ...Option) bool {
	if b.closed.Load() {
		// Close 后 loop 已退出：入队的日志无人消费会静默消失，直接拒绝并计数。
		b.drop(1, "AddLog after Close (buffer shut down)")
		return false
	}
	e := state.LogEntry{
		Time:      time.Now().UnixMilli(),
		Source:    b.source,
		OwnerType: ownerType,
		OwnerID:   ownerID,
		Type:      typ,
		Info:      info,
	}
	for _, o := range opts {
		o(&e)
	}
	if !b.ring.Push(e) {
		// 环形缓冲满：必须计数 + 告警（5s 节流），否则缓冲满导致的丢包完全不可观测。
		b.drop(1, "ring buffer full (capacity reached)")
		return false
	}
	return true
}

// Emit 是 AddLog 的别名。
func (b *Buffer) Emit(ownerType, ownerID, typ, info string, opts ...Option) bool {
	return b.AddLog(ownerType, ownerID, typ, info, opts...)
}

// Len 返回当前缓冲条数。
func (b *Buffer) Len() int { return b.ring.Len() }

// Stats 返回上报统计快照。
func (b *Buffer) Stats() Stats {
	b.mu.Lock()
	conns := len(b.conns)
	b.mu.Unlock()
	return Stats{
		Sent:      b.sent.Load(),
		Dropped:   b.dropped.Load(),
		Reconnect: b.reconnect.Load(),
		Conns:     conns,
	}
}

// Flush 立即 Drain 并批量上报（同步）。
func (b *Buffer) Flush() {
	entries := b.ring.Drain()
	if len(entries) == 0 {
		return
	}
	b.send(entries)
}

// Close 停止后台上报、刷完剩余日志并关闭全部连接。
// 幂等：重复调用不再会二次 close(stopCh) 直接 panic。
func (b *Buffer) Close() {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.stopCh)
		<-b.done
		b.Flush() // 关闭前把剩余日志刷完
		b.mu.Lock()
		conns := b.conns
		b.conns = make(map[string]*connEntry)
		b.mu.Unlock()
		for addr, c := range conns {
			_ = c.w.Close()
			logger.Infof("logbuf: closed connection to %s on shutdown", addr)
		}
	})
}

// loop 后台定时上报。
func (b *Buffer) loop() {
	defer close(b.done)
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.sweepIdleConns(time.Now()) // 回收空闲连接（地址漂移后不再使用的实例）
			b.Flush()
		}
	}
}

// entrySize 估算单条日志的 JSON 字节数（真实序列化前的近似值，宁大勿小）。
func entrySize(e state.LogEntry) int {
	const overhead = 128 // 字段名、引号、转义等固定开销的保守估计
	return overhead + len(e.Source) + len(e.OwnerType) + len(e.OwnerID) +
		len(e.Type) + len(e.Info) + len(e.SubType) + len(e.Reason) +
		len(e.SubReason) + len(e.Level) + len(e.TraceID)
}

// send 把一批日志上报到 log 服，按 maxBatch（条数）与 maxBatchBytes（估算字节）
// 双重切分；每片独立选实例。分片即分发粒度：一片走一个实例，多实例下各片自然分散。
func (b *Buffer) send(entries []state.LogEntry) {
	for len(entries) > 0 {
		n := len(entries)
		if b.maxBatch > 0 && n > b.maxBatch {
			n = b.maxBatch
		}
		// 按字节预算再收窄：避免单批 JSON 超过对端单帧上限被整批丢弃并断连。
		// 单条自身超预算时至少发 1 条（发送失败会走丢弃/重连路径，不会静默）。
		bytes := 0
		for i := 0; i < n; i++ {
			bytes += entrySize(entries[i])
			if bytes > maxBatchBytes && i > 0 {
				n = i
				break
			}
		}
		b.sendBatch(entries[:n])
		entries = entries[n:]
	}
}

// sendBatch 选一个实例把这一片发出去。
// 失败只影响本片：连接被弃用（下批自动重连），本片计入丢弃并告警。
func (b *Buffer) sendBatch(batch []state.LogEntry) {
	addr := b.pickAddr()
	if addr == "" {
		b.drop(len(batch), "no log instance available (discovery empty and no static log_addr)")
		return
	}
	cli := b.conn(addr)
	if cli == nil {
		b.drop(len(batch), "dial log instance "+addr+" failed")
		return
	}
	n, err := cli.WriteBatch(b.source, batch)
	if err != nil {
		b.discard(addr)
		b.reconnect.Add(1)
		b.drop(len(batch), "write to "+addr+" failed: "+err.Error())
		return
	}
	if n < len(batch) {
		// 必须消费服务端返回的 written：ok=true 但 written < len(batch)（少写）时
		// 此前被一律记为全部成功——丢日志无感知、sent 统计虚高。
		if n < 0 {
			n = 0
		}
		b.sent.Add(uint64(n))
		b.drop(len(batch)-n, fmt.Sprintf("server wrote %d/%d entries", n, len(batch)))
		return
	}
	b.sent.Add(uint64(len(batch)))
}

// pickAddr 选一个实例地址：优先服务发现（多实例轮询），其次静态地址。
func (b *Buffer) pickAddr() string {
	if b.dialer != nil {
		if a := b.dialer.Pick(); a != "" {
			return a
		}
	}
	return b.static
}

// conn 取该实例的长连接，不存在则建一条。建连失败返回 nil（下批自动重试）。
// TCP 拨号（默认 5s 超时）必须在锁外执行：持锁 dial 会在 log 服不可达时
// 串行阻塞 Stats/discard/其它分片的发送。
func (b *Buffer) conn(addr string) Writer {
	b.mu.Lock()
	if c := b.conns[addr]; c != nil {
		c.lastUsed.Store(time.Now().UnixMilli())
		w := c.w
		b.mu.Unlock()
		return w
	}
	b.mu.Unlock()

	c, err := b.dial(addr)
	if err != nil {
		logger.Warnf("logbuf: dial log instance %s failed: %v", addr, err)
		return nil
	}
	b.mu.Lock()
	if existing := b.conns[addr]; existing != nil { // 并发建连：保留先到的，丢弃本次
		existing.lastUsed.Store(time.Now().UnixMilli())
		b.mu.Unlock()
		_ = c.Close()
		return existing.w
	}
	e := &connEntry{w: c}
	e.lastUsed.Store(time.Now().UnixMilli())
	b.conns[addr] = e
	n := len(b.conns)
	b.mu.Unlock()
	logger.Infof("logbuf: connected to log instance %s (live connections=%d)", addr, n)
	return c
}

// discard 弃用某实例的连接（写入失败即认为连接已坏），下批自动重连。
func (b *Buffer) discard(addr string) {
	b.mu.Lock()
	c := b.conns[addr]
	delete(b.conns, addr)
	b.mu.Unlock()
	if c != nil {
		_ = c.w.Close()
	}
	logger.Warnf("logbuf: dropped connection to log instance %s, will reconnect on next batch", addr)
}

// sweepIdleConns 回收长时间未发送过批次的连接：实例从服务发现列表消失后
// （地址漂移），其长连接不再有引用可释放，不回收则 conns 无上限增长。
func (b *Buffer) sweepIdleConns(now time.Time) {
	var idle []*connEntry
	b.mu.Lock()
	for addr, c := range b.conns {
		if now.UnixMilli()-c.lastUsed.Load() > connIdleTTL.Milliseconds() {
			idle = append(idle, c)
			delete(b.conns, addr)
		}
	}
	b.mu.Unlock()
	for _, c := range idle {
		_ = c.w.Close()
	}
	if len(idle) > 0 {
		logger.Infof("logbuf: closed %d idle log connections (idle > %s)", len(idle), connIdleTTL)
	}
}

// drop 记录丢弃并告警（5s 节流，避免实例全挂时刷屏）。
func (b *Buffer) drop(n int, reason string) {
	total := b.dropped.Add(uint64(n))
	now := time.Now().UnixMilli()
	last := b.lastDropLog.Load()
	if now-last < dropLogInterval.Milliseconds() {
		return
	}
	if b.lastDropLog.CompareAndSwap(last, now) {
		logger.Warnf("logbuf: dropped %d log entries (total dropped=%d): %s", n, total, reason)
	}
}
