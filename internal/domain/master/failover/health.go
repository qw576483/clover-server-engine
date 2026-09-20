// Package failover 提供 master 侧的节点健康探测。
//
// 职责：
//   - Detector：维护每个注册节点的最后心跳时间，周期扫描并驱动
//     Alive → Suspect → Dead 状态机。Dead 时触发 OnNodeDown 回调，
//     由上层（app 层装配的回调）摘除死节点（节点上承载的场景由业务侧决定去向）。
package failover

import (
	"sort"
	"sync"
	"time"

	"clover-server-engine/internal/domain/master"
	"clover-server-engine/internal/domain/master/metrics"
	"clover-server-engine/pkg/foundation/logger"
)

// Health 是节点的健康状态。
type Health string

const (
	// HealthAlive 心跳正常。
	HealthAlive Health = "alive"
	// HealthSuspect 心跳静默超过 SuspectTimeout，疑似失联但尚未摘除。
	HealthSuspect Health = "suspect"
	// HealthDead 心跳静默超过 DeadTimeout，已判定死亡（由上层回调摘除）。
	HealthDead Health = "dead"
)

// NodeHealth 是探测器对单个节点的健康视图。
type NodeHealth struct {
	NodeID string
	Health Health
	// LastHeartbeat 最近一次心跳时间。
	LastHeartbeat time.Time
	// Silence 距上次心跳的静默时长（扫描时刻计算）。
	Silence time.Duration
}

// Detector 是节点健康探测器。
//
// 使用方式：
//
//	d := failover.NewDetector(cfg)
//	d.OnNodeDown(func(id string) { ... })  // Dead：摘除节点
//	d.OnNodeUp(func(id string) { ... })    // 恢复：重新加入可用列表
//	d.Start(stop)                          // stop 关闭或 d.Stop() 时退出（不是 ctx）
//	d.Heartbeat("node-1")                  // server 收到心跳时调用
//
// 并发安全。
type Detector struct {
	cfg master.HealthConfig

	mu    sync.RWMutex
	nodes map[string]*nodeEntry

	onDown    []func(nodeID string)
	onUp      []func(nodeID string)
	onSuspect []func(nodeID string)

	// now 与 ticker 是测试注入点：默认使用真实时钟。
	now       func() time.Time
	tickerFor func(time.Duration) (<-chan time.Time, func())

	startOnce sync.Once
	stopOnce  sync.Once
	stopped   chan struct{}
}

type nodeEntry struct {
	id     string
	last   time.Time
	health Health
}

// NewDetector 创建健康探测器，配置零值自动回落到默认值。
func NewDetector(cfg master.HealthConfig) *Detector {
	return &Detector{
		cfg:     cfg.Normalize(),
		nodes:   make(map[string]*nodeEntry),
		now:     time.Now,
		stopped: make(chan struct{}),
	}
}

// Config 返回归一化后的配置，供 server 下发心跳间隔给节点。
func (d *Detector) Config() master.HealthConfig { return d.cfg }

// OnNodeDown 注册节点判定 Dead（摘除）时的回调。
func (d *Detector) OnNodeDown(fn func(nodeID string)) {
	if fn == nil {
		return
	}
	d.mu.Lock()
	d.onDown = append(d.onDown, fn)
	d.mu.Unlock()
}

// OnNodeUp 注册节点恢复（重新上报心跳）时的回调。
func (d *Detector) OnNodeUp(fn func(nodeID string)) {
	if fn == nil {
		return
	}
	d.mu.Lock()
	d.onUp = append(d.onUp, fn)
	d.mu.Unlock()
}

// OnNodeSuspect 注册节点进入 Suspect 时的回调（仅告警，不摘除）。
func (d *Detector) OnNodeSuspect(fn func(nodeID string)) {
	if fn == nil {
		return
	}
	d.mu.Lock()
	d.onSuspect = append(d.onSuspect, fn)
	d.mu.Unlock()
}

// Track 开始跟踪一个节点（节点注册时调用），初始状态为 Alive。
func (d *Detector) Track(nodeID string) {
	if nodeID == "" {
		return
	}
	d.mu.Lock()
	if _, ok := d.nodes[nodeID]; !ok {
		d.nodes[nodeID] = &nodeEntry{id: nodeID, last: d.now(), health: HealthAlive}
		logger.Infof("master/failover: tracking node %s (heartbeat=%s suspect=%s dead=%s)",
			nodeID, d.cfg.HeartbeatInterval, d.cfg.SuspectTimeout, d.cfg.DeadTimeout)
	}
	d.mu.Unlock()
}

// Untrack 停止跟踪一个节点（节点主动注销时调用），不触发 OnNodeDown。
func (d *Detector) Untrack(nodeID string) {
	d.mu.Lock()
	delete(d.nodes, nodeID)
	d.mu.Unlock()
}

// Heartbeat 记录一次节点心跳。
// 若该节点此前非 Alive（Dead 或 Suspect），会触发 OnNodeUp 让上层重新纳入可用列表。
func (d *Detector) Heartbeat(nodeID string) {
	if nodeID == "" {
		return
	}
	metrics.Heartbeat()
	d.mu.Lock()
	e, ok := d.nodes[nodeID]
	if !ok {
		// 未跟踪的节点直接补登记（可能是 master 重启后节点先发的心跳）。
		// 必须留日志：这类节点可能不在上层 state 中，Scan 判 Dead 后
		// RemoveNodeWithReason 会返回 ErrNodeNotFound 而不触发 Untrack，
		// 让 d.nodes 永久滞留、nodes_dead Gauge 虚高——有日志才能定位来源。
		logger.Warnf("master/failover: heartbeat from untracked node %s, auto-tracking as alive", nodeID)
		d.nodes[nodeID] = &nodeEntry{id: nodeID, last: d.now(), health: HealthAlive}
		d.mu.Unlock()
		return
	}
	prev := e.health
	e.last = d.now()
	e.health = HealthAlive
	var ups []func(string)
	if prev != HealthAlive {
		ups = append([]func(string){}, d.onUp...)
	}
	d.mu.Unlock()

	if prev == HealthDead {
		metrics.NodeRecovered()
		logger.Infof("master/failover: node %s recovered (was dead), rejoining", nodeID)
	} else if prev == HealthSuspect {
		logger.Infof("master/failover: node %s recovered from suspect", nodeID)
	}
	for _, fn := range ups {
		fn(nodeID)
	}
}

// Status 返回单个节点的健康视图。
func (d *Detector) Status(nodeID string) (NodeHealth, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	e, ok := d.nodes[nodeID]
	if !ok {
		return NodeHealth{}, false
	}
	return NodeHealth{
		NodeID:        e.id,
		Health:        e.health,
		LastHeartbeat: e.last,
		Silence:       d.now().Sub(e.last),
	}, true
}

// Snapshot 返回全部节点的健康视图，按节点 ID 升序，便于稳定展示与快照。
func (d *Detector) Snapshot() []NodeHealth {
	d.mu.RLock()
	now := d.now()
	out := make([]NodeHealth, 0, len(d.nodes))
	for _, e := range d.nodes {
		out = append(out, NodeHealth{
			NodeID:        e.id,
			Health:        e.health,
			LastHeartbeat: e.last,
			Silence:       now.Sub(e.last),
		})
	}
	d.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// AliveNodes 返回当前健康（非 Dead）的节点 ID 列表，按 ID 升序。
func (d *Detector) AliveNodes() []string {
	d.mu.RLock()
	out := make([]string, 0, len(d.nodes))
	for id, e := range d.nodes {
		if e.health != HealthDead {
			out = append(out, id)
		}
	}
	d.mu.RUnlock()
	sort.Strings(out)
	return out
}

// Start 启动周期扫描。多次调用只生效一次；stop 关闭或 Stop 时退出。
func (d *Detector) Start(stop <-chan struct{}) {
	d.startOnce.Do(func() {
		go d.loop(stop)
	})
}

// Stop 停止扫描。
func (d *Detector) Stop() {
	d.stopOnce.Do(func() { close(d.stopped) })
}

func (d *Detector) loop(stop <-chan struct{}) {
	// 双保险：Normalize 已把非正周期收敛为默认值，但 NewDetector 之外的注入路径
	// （如测试直接改 cfg）若把非正值传进来，time.NewTicker 会直接 panic（且本 goroutine
	// 无 recover → master 进程崩溃），这里再挡一次。
	interval := d.cfg.ProbeInterval
	if interval <= 0 {
		logger.Errorf("master/failover: non-positive probe interval %v, fallback to %v", interval, master.DefaultProbeInterval)
		interval = master.DefaultProbeInterval
	}
	var tick <-chan time.Time
	var cancel func()
	if d.tickerFor != nil {
		tick, cancel = d.tickerFor(interval)
	} else {
		t := time.NewTicker(interval)
		tick, cancel = t.C, t.Stop
	}
	defer cancel()

	for {
		select {
		case <-d.stopped:
			return
		case <-stop:
			return
		case <-tick:
			d.Scan()
		}
	}
}

// Scan 执行一轮健康判定。导出以便测试直接驱动，无需等待真实定时器。
//
// 判定顺序（Dead 优先判定：静默达到 DeadTimeout 时直接判死，不保证经过 Suspect）：
//
//	静默 >= DeadTimeout    → Dead，触发 OnNodeDown（摘除死节点）
//	静默 >= SuspectTimeout → Suspect，触发 OnNodeSuspect（仅告警）
func (d *Detector) Scan() {
	now := d.now()

	type transition struct {
		id string
		to Health
	}
	var trans []transition

	d.mu.Lock()
	for _, e := range d.nodes {
		silence := now.Sub(e.last)
		switch {
		case silence >= d.cfg.DeadTimeout:
			if e.health != HealthDead {
				e.health = HealthDead
				trans = append(trans, transition{e.id, HealthDead})
			}
		case silence >= d.cfg.SuspectTimeout:
			if e.health == HealthAlive {
				e.health = HealthSuspect
				trans = append(trans, transition{e.id, HealthSuspect})
			}
		}
	}
	downs := append([]func(string){}, d.onDown...)
	suspects := append([]func(string){}, d.onSuspect...)
	d.mu.Unlock()

	// 回调在锁外触发，避免上层回调重入 Detector 造成死锁。
	// 按节点 ID 排序，让多节点同时超时的处理顺序可预期，便于测试断言。
	sort.Slice(trans, func(i, j int) bool { return trans[i].id < trans[j].id })
	for _, t := range trans {
		switch t.to {
		case HealthSuspect:
			metrics.HeartbeatTimeout(metrics.TimeoutSuspect)
			logger.Warnf("master/failover: node %s SUSPECT (silence>=%s)", t.id, d.cfg.SuspectTimeout)
			for _, fn := range suspects {
				fn(t.id)
			}
		case HealthDead:
			metrics.HeartbeatTimeout(metrics.TimeoutDead)
			logger.Errorf("master/failover: node %s DEAD (silence>=%s), evicting", t.id, d.cfg.DeadTimeout)
			for _, fn := range downs {
				fn(t.id)
			}
		}
	}

	// 扫描结束后整体刷新各健康状态的节点计数 Gauge。
	metrics.SetNodeHealth(d.countByHealth())
}

// countByHealth 统计当前各健康状态的节点数，供 SetNodeHealth 刷新 Gauge。
func (d *Detector) countByHealth() (alive, suspect, dead int) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, e := range d.nodes {
		switch e.health {
		case HealthAlive:
			alive++
		case HealthSuspect:
			suspect++
		case HealthDead:
			dead++
		}
	}
	return
}
