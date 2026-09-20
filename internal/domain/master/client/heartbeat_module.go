// Package client 提供节点心跳模块（存活/负载上报到 master）。
//
// 设计要点：
//   - 引擎在 app 层自动启动（见 bootstrap）；业务也可通过 NewHeartbeatModule 显式创建。
//   - 不入 field 到 app.Game 避免耦合；调用方自行持有生命周期（defer Stop）。
//   - HeartbeatConfig 纯数据无行为，LoadFn 由调用方注入（如 g.OnlineCount() 作为负载指标）。
//   - 支持 Stop → Restart：灰度下线（drain）时停心跳让 master 摘流，
//     drain 取消后必须能重新上报存活，否则节点会被 master 永久判死。
package client

import (
	"context"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// HeartbeatModule 封装心跳上报器生命周期。
//
// 保存 cli 与 cfg 是为了支持 Restart：HeartbeatReporter 内部的 startOnce /
// stopOnce 都是一次性开关，停止后的实例无法复用，只能整体重建。
type HeartbeatModule struct {
	mu       sync.Mutex
	cli      *Client
	cfg      HeartbeatConfig
	reporter *HeartbeatReporter
	running  bool
}

// HeartbeatConfig 心跳模块配置。
type HeartbeatConfig struct {
	NodeID   string        // 节点标识
	Interval time.Duration // 上报间隔
	LoadFn   func() int    // 负载函数（如在线人数）
}

// NewHeartbeatModule 创建并启动心跳模块。
// mc 为 master 客户端，nil 时跳过创建。
func NewHeartbeatModule(mc *Client, cfg HeartbeatConfig) *HeartbeatModule {
	if mc == nil || cfg.NodeID == "" || cfg.Interval <= 0 {
		// 不能静默返回 nil：调用方只看到「没有心跳模块」，节点永远不上报心跳
		// 而被 master 判 Dead——现象与配置错误无从关联（此前无任何日志）。
		logger.Warnf("master/client: heartbeat module not started (mc_nil=%v node_id=%q interval=%s)",
			mc == nil, cfg.NodeID, cfg.Interval)
		return nil
	}
	h := &HeartbeatModule{cli: mc, cfg: cfg}
	h.spawn()
	return h
}

// spawn 新建并启动一个上报器，标记 running。调用方须持有 mu。
func (h *HeartbeatModule) spawn() {
	if h == nil || h.cli == nil {
		return
	}
	r := NewHeartbeatReporter(h.cli, h.cfg.NodeID, h.cfg.Interval)
	if h.cfg.LoadFn != nil {
		r.SetLoadFn(h.cfg.LoadFn)
	}
	r.Start(context.Background())
	h.reporter = r
	h.running = true
}

// Stop 停止心跳上报。幂等；nil 接收者安全。
func (h *HeartbeatModule) Stop() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if !h.running {
		h.mu.Unlock()
		return
	}
	r := h.reporter
	h.reporter = nil
	h.running = false
	h.mu.Unlock()
	// reporter.Stop 最长等 2s（网络 IO 兜底）：必须在锁外执行，
	// 否则期间 Restart/Running 全被阻塞。
	if r != nil {
		r.Stop()
	}
}

// Restart 在 Stop 之后重建上报循环（drain 取消 → 节点重新摘流回服务）。
// 已在运行时为空操作；nil 接收者安全。
func (h *HeartbeatModule) Restart() {
	if h == nil || h.cli == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.running {
		return
	}
	h.spawn()
}

// Running 返回心跳是否正在上报；nil 接收者返回 false。
func (h *HeartbeatModule) Running() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}
