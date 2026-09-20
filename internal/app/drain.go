// 灰度下线（drain）编排：把「摘流量 → 迁移/等待 → 强踢 → 排水 → 退出」串成一条命令。

// 适用场景：滚动重启热更。新版本进程在备用端口就绪后，先让网关把**新连接**导向新进程，
// 再对旧进程调用 Drain 处理**存量连接**，旧进程归零后自行退出，全程无业务停机。

// 三种存量处理模式见 DrainMode。核心安全性质：
//   - 迁移走 GWControlSwitchUpstream：网关先 dial 新上游、成功后再原子替换，
//     客户端 TCP 连接不断开，玩家零感知；
//   - 强踢只在宽限期结束后对「仍未迁走」的连接执行，客户端重连即落到新进程；
//   - 收尾复用 Game.Stop 的既有语义（stopped 拦新请求 → inflight.Wait 排在途 → closeBackends）。
package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"clover-server-engine/internal/domain/master/state"
	"clover-server-engine/pkg/foundation/logger"
)

// ErrDraining 灰度下线期间新连接被拒绝时返回给客户端。
// 客户端收到后应重连（网关的默认上游此时已切到新版本进程）。
var ErrDraining = errors.New("game draining, please reconnect")

// DrainMode 存量连接的处理方式。
type DrainMode string

const (
	// DrainGrace 只等玩家自然退出，宽限期到后分批强踢。
	// 不迁移，故不要求 target；但必须先把网关 upstream 切走，否则新连接仍会进来。
	DrainGrace DrainMode = "grace"
	// DrainMigrate 逐连接 SwitchUpstream 到新进程，客户端无感。
	// 宽限期到后不再强踢（宁可留下，也不能踢掉还没迁完的玩家）。
	DrainMigrate DrainMode = "migrate"
	// DrainHybrid 先迁移，宽限期到后对剩余连接分批强踢。滚动重启的推荐模式。
	DrainHybrid DrainMode = "hybrid"
)

// DrainOptions 一次灰度下线的参数。零值回落到下列默认值。
type DrainOptions struct {
	Mode         DrainMode     // 默认 hybrid
	Target       string        // 新进程逻辑服地址（g.Addr() 的返回值）；migrate/hybrid 必填
	Grace        time.Duration // 迁移 / 等待自然退出的总时长，默认 5m
	HardTimeout  time.Duration // 进入强踢阶段后的硬上限，默认 2m；到点无论剩多少都收尾
	MigrateBatch int           // 每个 tick 迁移的连接数，默认 128
	MigrateTick  time.Duration // 迁移批间隔，默认 200ms
	KickBatch    int           // 强踢每批条数，默认 32
	KickInterval time.Duration // 强踢批间隔，默认 1s
	StopAfter    bool          // 归零后是否自动停机（滚动重启置 true）
}

func (o *DrainOptions) normalize() {
	if o.Mode == "" {
		o.Mode = DrainHybrid
	}
	if o.Grace <= 0 {
		o.Grace = 5 * time.Minute
	}
	if o.HardTimeout <= 0 {
		o.HardTimeout = 2 * time.Minute
	}
	if o.MigrateBatch <= 0 {
		o.MigrateBatch = 128
	}
	if o.MigrateTick <= 0 {
		o.MigrateTick = 200 * time.Millisecond
	}
	if o.KickBatch <= 0 {
		o.KickBatch = 32
	}
	if o.KickInterval <= 0 {
		o.KickInterval = time.Second
	}
}

// DrainStatus 一次灰度下线的实时状态（admin 端点直接序列化返回）。
type DrainStatus struct {
	Draining  bool      `json:"draining"`
	Mode      string    `json:"mode"`
	Target    string    `json:"target"`
	Phase     string    `json:"phase"` // migrate | kicking | done
	Remaining int       `json:"remaining"`
	Migrated  int       `json:"migrated"`
	Kicked    int       `json:"kicked"`
	StartedAt time.Time `json:"started_at"`
	Deadline  time.Time `json:"deadline"`
}

// Drainer 单节点灰度下线编排器。一个 Game 实例至多一个，重复 Drain 只更新参数。
type Drainer struct {
	g  *Game
	mu sync.Mutex
	// flowMu 串行化「摘流量」（Drain：停心跳 + 摘节点）与「恢复流量」
	//（CancelDrain / finish(stop=false)：重注册 + 重启心跳）两组动作，
	// 保证与取消并发时最终收敛到正确顺序（要么未摘，要么已恢复），
	// 不会停在「心跳停 + 未注册」的半死态。
	flowMu  sync.Mutex
	opts    DrainOptions
	mig     map[string]bool // 已发起迁移的 connID
	running bool
	st      DrainStatus
	cancel  chan struct{}
}

// Draining 返回本节点是否处于灰度下线中。
// 读 atomic 镜像，不取锁——BeforeDispatch 每帧调用它，必须保持无锁。
func (g *Game) Draining() bool { return g.draining.Load() }

// Drain 启动一次灰度下线。

// 若已在编排中，只更新 mode / target / deadline，不会起第二个循环（幂等）。
// 编排在后台 goroutine 执行，本方法立即返回。
func (g *Game) Drain(opts DrainOptions) error {
	opts.normalize()
	if opts.Mode != DrainGrace && opts.Target == "" {
		return errors.New("drain: target required for mode " + string(opts.Mode))
	}
	if opts.Target == g.Addr() {
		return errors.New("drain: target must not be self")
	}

	g.drainMu.Lock()
	if g.drainer == nil {
		g.drainer = &Drainer{g: g, mig: make(map[string]bool), cancel: make(chan struct{})}
	}
	d := g.drainer
	g.drainMu.Unlock()

	d.mu.Lock()
	d.opts = opts
	if d.running {
		// 已在跑：只刷新目标与截止时间，供运维中途改主意（如换一个新进程地址）。
		d.st.Mode = string(opts.Mode)
		d.st.Target = opts.Target
		d.st.Deadline = time.Now().Add(opts.Grace)
		d.mu.Unlock()
		logger.Infof("drain: updated (mode=%s target=%s grace=%v)", opts.Mode, opts.Target, opts.Grace)
		return nil
	}
	d.running = true
	// 上一轮（含被 CancelDrain 中断的）留下的已迁出记录不能带入本轮：
	// remainingLocked 按 total − len(mig) 算剩余数，残留键会把剩余虚减，
	// 可能提前判 0 触发 finish 强断尚未处理的连接。
	d.mig = make(map[string]bool)
	// 上一轮 CancelDrain 可能已经 close 过 cancel：必须换一个新的再开跑，
	// 否则新起的编排 goroutine 会在 <-d.cancel 上立刻返回 —— 表现是「点了 Drain 却什么都没做」。
	select {
	case <-d.cancel:
		d.cancel = make(chan struct{})
	default:
		// 未关闭：沿用同一个（本轮的 CancelDrain 关的就是它）。
	}
	d.st = DrainStatus{
		Draining:  true,
		Mode:      string(opts.Mode),
		Target:    opts.Target,
		Phase:     "migrate",
		StartedAt: time.Now(),
		Deadline:  time.Now().Add(opts.Grace),
	}
	// 置 draining 必须与 running=true 同处锁内：否则 CancelDrain 可在解锁窗口内
	// 完成取消（running=false + draining=false + 恢复流量），随后本行的 Store(true)
	// 会把 Draining() 永久钉在 true —— 节点永久拒客。
	g.draining.Store(true) // 无锁镜像，供 BeforeDispatch 每帧读取
	d.mu.Unlock()

	// 摘流量第一步：停止心跳上报 + 主动摘除节点，
	// 让 master 立刻不再把跨节点事件路由到本节点（不必等心跳超时判死）。
	// flowMu 与 CancelDrain / finish 的「恢复流量」段互斥，避免摘除动作穿插到恢复之后。
	d.flowMu.Lock()
	d.mu.Lock()
	running := d.running
	d.mu.Unlock()
	if !running {
		// 解锁窗口内已被 CancelDrain 取消：恢复流量由它负责，这里不能再摘。
		d.flowMu.Unlock()
		return nil
	}
	if g.heartbeat != nil {
		g.heartbeat.Stop()
	}
	g.deregisterNode()
	d.flowMu.Unlock()

	logger.Infof("drain: started (mode=%s target=%s grace=%v stop_after=%v)",
		opts.Mode, opts.Target, opts.Grace, opts.StopAfter)
	go d.run()
	return nil
}

// CancelDrain 取消灰度下线（回滚用）。

// 已迁移出去的连接不会自动搬回——回滚的正确做法是：先 CancelDrain 让本节点恢复接客，
// 再由运维把网关 upstream 切回本节点，玩家重连时自然回来。
func (g *Game) CancelDrain() {
	g.drainMu.Lock()
	d := g.drainer
	g.drainMu.Unlock()
	if d == nil {
		return
	}
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.running = false
	d.st.Phase = "done"
	d.st.Draining = false
	d.mu.Unlock()
	g.draining.Store(false)
	select {
	case <-d.cancel:
	default:
		close(d.cancel)
	}
	// 恢复接客：重新注册节点 + 重启心跳。
	// flowMu 与 Drain / finish 的摘流量段互斥：若摘除动作正在执行，等它结束后再恢复，
	// 保证「摘 → 恢复」的最终顺序正确（反序会让节点停在未注册 + 心跳停的半死态）。
	d.flowMu.Lock()
	g.registerNode()
	if g.heartbeat != nil {
		g.heartbeat.Restart()
	}
	d.flowMu.Unlock()
	logger.Infof("drain: cancelled")
}

// DrainStatus 返回当前编排状态（未开始过则 draining=false）。
func (g *Game) DrainStatus() DrainStatus {
	g.drainMu.RLock()
	d := g.drainer
	g.drainMu.RUnlock()
	if d == nil {
		return DrainStatus{}
	}
	total := d.connTotal() // 锁外取连接总数，见 connTotal 注释
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.st
	st.Remaining = d.remainingLocked(total)
	return st
}

// drainTarget 返回当前迁移目标；未编排或模式为 grace 时返回空串。
func (g *Game) drainTarget() string {
	g.drainMu.RLock()
	d := g.drainer
	g.drainMu.RUnlock()
	if d == nil {
		return ""
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.running || d.opts.Mode == DrainGrace {
		return ""
	}
	return d.opts.Target
}

// markMigrated 标记该连接已迁出（供 BeforeDispatch 就地迁移新连接时记账）。
func (g *Game) markMigrated(connID string) {
	if connID == "" {
		return
	}
	g.drainMu.RLock()
	d := g.drainer
	g.drainMu.RUnlock()
	if d == nil {
		return
	}
	d.mu.Lock()
	if !d.mig[connID] {
		d.mig[connID] = true
		d.st.Migrated++
	}
	d.mu.Unlock()
}

// 编排循环
func (d *Drainer) run() {
	opts := d.currentOpts()
	tk := time.NewTicker(opts.MigrateTick)
	defer tk.Stop()

	// 阶段一：迁移（或等待自然退出），直到归零或宽限期到。
	for {
		select {
		case <-d.cancel:
			return
		case <-tk.C:
		}
		cur := d.currentOpts()
		if cur.Target != "" && cur.Mode != DrainGrace {
			d.migrateBatch(cur)
		}
		if d.remaining() == 0 {
			d.finish(cur.StopAfter)
			return
		}
		d.mu.Lock()
		deadline := d.st.Deadline
		d.mu.Unlock()
		if time.Now().After(deadline) {
			break
		}
	}

	// 阶段二：强踢剩余连接。migrate 模式不踢——宁可留下，也不打断尚未迁完的玩家。
	cur := d.currentOpts()
	if cur.Mode == DrainMigrate {
		logger.Warnf("drain: grace expired with %d conn(s) left, migrate mode keeps them", d.remaining())
		d.finish(cur.StopAfter)
		return
	}
	d.setPhase("kicking")
	logger.Infof("drain: grace expired, kicking %d conn(s) in batches", d.remaining())

	hard := time.After(cur.HardTimeout)
	// KickInterval 的计时器在循环外创建：原实现每轮迭代都 time.After 一个新 Timer，
	// 被 cancel / hard 抢先返回时当轮 Timer 无人 Stop，要残留到 KickInterval 到期。
	kick := time.NewTicker(cur.KickInterval)
	defer kick.Stop()
	for {
		if d.remaining() == 0 {
			break
		}
		select {
		case <-d.cancel:
			return
		case <-hard:
			logger.Errorf("drain: hard timeout reached, %d conn(s) left, finishing anyway", d.remaining())
			d.finish(cur.StopAfter)
			return
		case <-kick.C:
		}
		for _, id := range d.pending(cur.KickBatch) {
			// KickConn 会把连接从 connMgr 摘除（connTotal 随之 −1）。
			// 这里**不能**再写 d.mig：remainingLocked 按 total − len(mig) 计算，
			// 对已摘除的连接再记一次会让剩余数一次掉 2、提前收尾强断未处理的连接。
			if !d.g.KickConn(id) { // all 模式直连网关；分离部署经 NATS 控制指令
				logger.Warnf("drain: kick conn %s failed (connection data already cleared)", id)
			}
			d.mu.Lock()
			d.st.Kicked++
			d.mu.Unlock()
		}
	}
	d.finish(cur.StopAfter)
}

// migrateBatch 迁移一批尚未迁出的连接。
func (d *Drainer) migrateBatch(opts DrainOptions) {
	n := 0
	for _, id := range d.pending(opts.MigrateBatch) {
		if err := d.g.SwitchUpstream(id, opts.Target); err != nil {
			logger.Warnf("drain: switch upstream %s -> %s: %v", id, opts.Target, err)
			continue
		}
		d.mu.Lock()
		d.mig[id] = true
		d.st.Migrated++
		d.mu.Unlock()
		n++
	}
	if n > 0 {
		logger.Infof("drain: migrated %d conn(s) to %s", n, opts.Target)
	}
}

// finish 收尾。stop 为真时：等排空 → Game.Stop → 通知进程退出。
func (d *Drainer) finish(stop bool) {
	g := d.g
	total := d.connTotal() // 锁外取连接总数，见 connTotal 注释
	d.mu.Lock()
	d.running = false
	d.st.Draining = false
	d.st.Phase = "done"
	d.st.Remaining = d.remainingLocked(total)
	mig, kick, left := d.st.Migrated, d.st.Kicked, d.st.Remaining
	d.mu.Unlock()
	g.draining.Store(false)
	logger.Infof("drain: finished (migrated=%d kicked=%d remaining=%d)", mig, kick, left)
	if !stop {
		// 非停机收尾（stop_after=false）：节点继续接客，必须恢复心跳与节点注册——
		// 否则停在「Draining() 已放行新请求、但 master / 跨节点看不见本节点」的半死态
		//（原实现只在 CancelDrain 里恢复）。与 Drain / CancelDrain 共用 flowMu 串行；
		// 若解锁窗口内已开启新一轮 Drain，则交给新一轮的摘除逻辑管理，不恢复。
		d.flowMu.Lock()
		d.mu.Lock()
		running := d.running
		d.mu.Unlock()
		if !running {
			g.registerNode()
			if g.heartbeat != nil {
				g.heartbeat.Restart()
			}
		}
		d.flowMu.Unlock()
		return
	}
	// 不在这里单独 inflight.Wait()：此刻 stopped 尚未置位、handler 仍在进入，
	// 独立 Wait 与新 handler 的 Add 并发会命中 WaitGroup「Add/Wait 并发」误用。
	// Game.Stop() 内部先关 handler 闸门再 Wait，语义等价且安全。
	g.Stop()
	requestShutdown()
}

// 内部
func (d *Drainer) currentOpts() DrainOptions {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opts
}

func (d *Drainer) setPhase(p string) {
	d.mu.Lock()
	d.st.Phase = p
	d.mu.Unlock()
}

// pending 返回尚未迁出的 connID，最多 n 条。
//
// 连接表遍历（sync.Map Range）在 d.mu **之外**做，锁内只做「排除已迁出 + 截断」，
// 锁内工作量与返回条数相关，不再与连接总数相关（否则每 200ms 一次的迁移批次
// 会各自持锁扫全表，把每帧 BeforeDispatch 的 drainTarget 一起堵住）。
func (d *Drainer) pending(n int) []string {
	all := make([]string, 0, 256)
	d.g.connMgr.RangeConns(func(id string) bool {
		all = append(all, id)
		return true
	})
	out := make([]string, 0, n)
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range all {
		if d.mig[id] {
			continue
		}
		out = append(out, id)
		if len(out) >= n {
			break
		}
	}
	return out
}

func (d *Drainer) remaining() int {
	total := d.connTotal()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.remainingLocked(total)
}

// connTotal 当前活跃连接数。
//
// 全表遍历必须在 d.mu **之外**：d.mu 同时被 drainTarget（每帧 BeforeDispatch 调用）、
// 迁移批次、状态查询争用，把万级连接的扫描放进去，等于让每条消息的派发排队等它。
func (d *Drainer) connTotal() int {
	return len(d.g.connMgr.ListConnIDs())
}

// remainingLocked 剩余待处理连接数：活跃连接 − 已迁出/已踢。
//
// total 由调用方在锁外取好（见 connTotal），本函数只做减法、不碰连接表；
// 调用方须持有 d.mu。
func (d *Drainer) remainingLocked(total int) int {
	n := total - len(d.mig)
	if n < 0 {
		return 0
	}
	return n
}

// deregisterNode 从 master 摘除本节点（跨节点事件不再路由过来）。
func (g *Game) deregisterNode() {
	if g.crossNodeBus == nil {
		return
	}
	ac := g.crossNodeBus.AuthorityClient()
	if ac == nil {
		return
	}
	id := g.Addr()
	if id == "" {
		return
	}
	if err := ac.RemoveNode(context.Background(), id); err != nil {
		logger.Warnf("drain: remove node %s from master: %v", id, err)
		return
	}
	logger.Infof("drain: deregistered node %s from master", id)
}

// registerNode 向 master 重新注册本节点（CancelDrain 回滚用）。
func (g *Game) registerNode() {
	if g.crossNodeBus == nil {
		return
	}
	ac := g.crossNodeBus.AuthorityClient()
	if ac == nil {
		return
	}
	id := g.Addr()
	if id == "" {
		return
	}
	node := state.Node{
		ID:   id,
		Addr: id,
		Type: state.NodeTypeGame,
		Tags: g.nodeTags,
	}
	if err := ac.RegisterNode(context.Background(), node); err != nil {
		logger.Warnf("drain: re-register node %s to master: %v", id, err)
		return
	}
	logger.Infof("drain: re-registered node %s to master", id)
}
