// 节点目录读取侧（步骤文档 S4b）：本地缓存 etcd 的 `clover/nodes/` 全量节点。
//
// 取代「向 master 查节点表」的两条读路径：
//   - crossnode 的存活节点集合（原每 5s 轮询 master 的 MsgNodesByType）
//   - 业务按 tag 查节点（原 MsgNodesByTag）
//
// 存活语义与 master 一致但更快：etcd 租约到期即摘除（无需等心跳超时判定），
// 且 watch 推送是事件驱动，没有 5s 轮询窗口。
//
// 未配 etcd（ec == nil）时 Enabled() 为 false，调用方回落到 master 读路径，
// 行为与未接入节点目录时完全一致。
package app

import (
	"context"
	"sort"
	"strings"
	"sync"

	"clover-server-engine/internal/transport/etcd"
	"clover-server-engine/pkg/foundation/logger"
	ujson "clover-server-engine/pkg/shared/json"
)

// nodeDirectory 节点目录的本地视图（watch 增量维护）。
type nodeDirectory struct {
	ec *etcd.Client

	mu    sync.RWMutex
	nodes map[string]nodeInfo // nodeID → info
}

// newNodeDirectory 构造节点目录视图。ec 为 nil 时 Enabled() 为 false。
func newNodeDirectory(ec *etcd.Client) *nodeDirectory {
	return &nodeDirectory{ec: ec, nodes: make(map[string]nodeInfo)}
}

// Enabled 报告节点目录是否可用（配了 etcd）。
func (d *nodeDirectory) Enabled() bool { return d != nil && d.ec != nil }

// start 首次拉取全量节点并注册前缀监听（节点上下线自动刷新）。
func (d *nodeDirectory) start(ctx context.Context) {
	if !d.Enabled() {
		return
	}
	d.refresh(ctx)
	if err := d.ec.WatchPrefix(ctx, nodeKeyRoot, func() { d.refresh(context.WithoutCancel(ctx)) }); err != nil {
		// 监听失败只降级为「启动时拉一次」，不阻断启动。
		logger.Warnf("discovery: watch node directory %s: %v", nodeKeyRoot, err)
	}
}

// refresh 重新拉取全量节点；失败保留旧值（宁可短暂过期，也不要让查询直接失败）。
func (d *nodeDirectory) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, defaultDiscoveryTimeout)
	defer cancel()
	m, err := d.ec.GetPrefix(ctx, nodeKeyRoot)
	if err != nil {
		d.mu.RLock()
		n := len(d.nodes)
		d.mu.RUnlock()
		logger.Warnf("discovery: list node directory failed: %v (keep %d cached)", err, n)
		return
	}
	nodes := make(map[string]nodeInfo, len(m))
	for k, raw := range m {
		nodeID := strings.TrimPrefix(k, nodeKeyRoot)
		if nodeID == "" {
			continue
		}
		var info nodeInfo
		if jsonErr := ujson.Unmarshal([]byte(raw), &info); jsonErr != nil {
			logger.Warnf("discovery: node %s value %q invalid, ignored", nodeID, raw)
			continue
		}
		nodes[nodeID] = info
	}
	d.mu.Lock()
	// 变更判定不能只看数量：节点集合「数量不变但成员 / 内容变化」
	//（如某节点换 tag、换 admin 地址、一个下线一个上线）同样要留更新日志。
	changed := !sameNodeInfos(nodes, d.nodes)
	d.nodes = nodes
	d.mu.Unlock()
	if changed {
		logger.Infof("discovery: node directory updated: %d nodes", len(nodes))
	}
}

// sameNodeInfos 比较两份节点目录是否等价（成员、类型、标签与 admin 地址都一致）。
func sameNodeInfos(a, b map[string]nodeInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for id, ia := range a {
		ib, ok := b[id]
		if !ok || ia.Type != ib.Type || ia.Admin != ib.Admin || len(ia.Tags) != len(ib.Tags) {
			return false
		}
		for i := range ia.Tags {
			if ia.Tags[i] != ib.Tags[i] {
				return false
			}
		}
	}
	return true
}

// ByType 返回指定类型的存活节点 ID 列表（升序，便于排障与可预期）。
func (d *nodeDirectory) ByType(typ string) []string {
	return d.filter(func(info nodeInfo) bool { return info.Type == typ })
}

// ByTag 返回含指定 tag 的存活节点 ID 列表。
func (d *nodeDirectory) ByTag(tag string) []string {
	return d.filter(func(info nodeInfo) bool {
		for _, t := range info.Tags {
			if t == tag {
				return true
			}
		}
		return false
	})
}

// filter 按谓词筛选节点 ID 并按字典序排序。
func (d *nodeDirectory) filter(match func(nodeInfo) bool) []string {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	out := make([]string, 0, len(d.nodes))
	for id, info := range d.nodes {
		if match(info) {
			out = append(out, id)
		}
	}
	d.mu.RUnlock()
	sort.Strings(out)
	return out
}
