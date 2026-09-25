package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// defaultHeartbeatInterval 心跳上报默认周期，取自协议定义（state）——
// client 不能引用 master 根包的常量（master import client，会成环），
// 所以两边共用的默认值放在 state，避免各写一个 3s 后漂移。
const defaultHeartbeatInterval = state.DefaultHeartbeatInterval

// ——— HeartbeatClient ——
//
// HeartbeatClient 心跳管理远程客户端。
// 由调用方自己组装：hc := NewHeartbeatClient(c)。
type HeartbeatClient struct{ cli *Client }

// NewHeartbeatClient 从原始 master TCP 客户端创建心跳客户端。
func NewHeartbeatClient(c *Client) *HeartbeatClient { return &HeartbeatClient{cli: c} }

// Heartbeat 主动向 master 上报一次心跳。
func (h *HeartbeatClient) Heartbeat(ctx context.Context, nodeID string, load int) (*state.HeartbeatResp, error) {
	var resp state.HeartbeatResp
	if err := h.cli.Call(ctx, state.MsgHeartbeat, state.HeartbeatReq{NodeID: nodeID, Load: load}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return &resp, fmt.Errorf("master: heartbeat %s: %s", nodeID, resp.Error)
	}
	return &resp, nil
}

// NodeHealth 查询全部节点的健康视图。
func (h *HeartbeatClient) NodeHealth(ctx context.Context) ([]state.NodeHealthEntry, error) {
	var resp state.NodeHealthResp
	if err := h.cli.Call(ctx, state.MsgNodeHealth, struct{}{}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("master: node health: %s", resp.Error)
	}
	return resp.Nodes, nil
}

// ——— HeartbeatReporter ——
//
// HeartbeatReporter 是节点侧的心跳上报器。
//
// 它在后台以固定间隔向 master 上报心跳；若 master 在响应中下发了不同的
// 间隔（服务端集中配置），会自动跟随调整，避免节点与 master 阈值不一致。
//
// 上报失败不会中断循环：master 短暂不可达时持续重试，
// 恢复后 master 侧的 Detector 会自动把节点从 Dead 拉回 Alive。
//
// 实际 RPC 复用 HeartbeatClient，本类型只负责「定时 + 失败容忍 + 跟随服务端间隔」。
type HeartbeatReporter struct {
	hc     *HeartbeatClient
	nodeID string

	// fnMu 保护 loadFn / onStale：loop 每周期读、SetLoadFn/OnStale 可在启动后调用，
	// 裸读写构成数据竞争。
	fnMu sync.RWMutex
	// loadFn 返回节点当前负载，可为 nil。
	loadFn func() int
	// onStale 在 master 报告"不认识该节点"时触发，调用方应重新注册。
	onStale func()

	interval atomic.Int64 // 当前上报间隔（纳秒），可被服务端动态调整
	started  atomic.Bool  // 是否已启动 loop（Stop 未启动时不必等 doneCh）

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewHeartbeatReporter 创建心跳上报器。interval<=0 时使用默认值 3s。
func NewHeartbeatReporter(cli *Client, nodeID string, interval time.Duration) *HeartbeatReporter {
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	r := &HeartbeatReporter{
		hc:     NewHeartbeatClient(cli),
		nodeID: nodeID,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	r.interval.Store(int64(interval))
	return r
}

// SetLoadFn 设置负载采集函数，心跳会顺带上报当前负载。
func (r *HeartbeatReporter) SetLoadFn(fn func() int) {
	r.fnMu.Lock()
	r.loadFn = fn
	r.fnMu.Unlock()
}

// OnStale 设置"master 不认识本节点"时的回调，通常用于重新注册。
func (r *HeartbeatReporter) OnStale(fn func()) {
	r.fnMu.Lock()
	r.onStale = fn
	r.fnMu.Unlock()
}

// Interval 返回当前上报间隔。
func (r *HeartbeatReporter) Interval() time.Duration {
	return time.Duration(r.interval.Load())
}

// Start 启动后台上报循环。多次调用只生效一次。
func (r *HeartbeatReporter) Start(ctx context.Context) {
	if r == nil || r.hc == nil || r.hc.cli == nil || r.nodeID == "" {
		return
	}
	r.startOnce.Do(func() {
		r.started.Store(true)
		go r.loop(ctx)
	})
}

// Stop 停止上报并等待循环退出。未 Start 过的实例立即返回（若固定阻塞 2 秒，
// doneCh 永不关闭，只能靠超时分支兜底）。
func (r *HeartbeatReporter) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.stopCh)
		if !r.started.Load() {
			return // 从未启动：无 loop 可等，无需等 doneCh
		}
		select {
		case <-r.doneCh:
		case <-time.After(2 * time.Second):
			// 上报可能卡在网络 IO 上，超时后不再等待，避免拖慢进程退出。
		}
	})
}

func (r *HeartbeatReporter) heartbeat(ctx context.Context, nodeID string, load int) (*state.HeartbeatResp, error) {
	return r.hc.Heartbeat(ctx, nodeID, load)
}

func (r *HeartbeatReporter) loop(ctx context.Context) {
	defer close(r.doneCh)

	logger.Infof("master/client: heartbeat reporter started for %s (interval=%s)",
		r.nodeID, r.Interval())

	t := time.NewTimer(r.Interval())
	defer t.Stop()

	var failures int
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-t.C:
		}

		load := 0
		r.fnMu.RLock()
		loadFn := r.loadFn
		r.fnMu.RUnlock()
		if loadFn != nil {
			load = loadFn()
		}

		// 单次上报设超时，避免 master 假死时把上报循环永久卡住。
		callCtx, cancel := context.WithTimeout(ctx, r.Interval())
		resp, err := r.heartbeat(callCtx, r.nodeID, load)
		cancel()

		switch {
		case err != nil:
			failures++
			if failures == 1 || failures%10 == 0 {
				logger.Warnf("master/client: heartbeat %s failed (%d times): %v", r.nodeID, failures, err)
			}
		default:
			if failures > 0 {
				logger.Infof("master/client: heartbeat %s recovered after %d failures", r.nodeID, failures)
				failures = 0
			}
			if resp != nil && resp.IntervalMS > 0 {
				want := time.Duration(resp.IntervalMS) * time.Millisecond
				if want != r.Interval() {
					logger.Infof("master/client: heartbeat interval %s -> %s (server directed)",
						r.Interval(), want)
					r.interval.Store(int64(want))
				}
			}
			r.fnMu.RLock()
			onStale := r.onStale
			r.fnMu.RUnlock()
			if resp != nil && !resp.Known && onStale != nil {
				logger.Warnf("master/client: master does not know node %s, re-registering", r.nodeID)
				onStale()
			}
		}

		t.Reset(r.Interval())
	}
}
