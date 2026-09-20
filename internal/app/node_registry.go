// 节点目录（步骤文档 S4）：把本节点的「类型 + tags」登记到 etcd，作为节点表的权威来源。
//
// 与 discovery.go 的角色前缀（clover/services/<role>/...，用于「按角色找实例」）不同，
// 这里是「按节点 ID 找节点」：
//
//	clover/nodes/<nodeID> = {"type":"game","tags":["room","pvp"]}
//
// 关键约定：`type` 与 master `state.Node.Type` **同一套取值**（`state.NodeTypeGame` 等），
// 否则按类型查询（crossnode 找 game 节点）会查不到——这正是 S4b 的阻塞点之一。
//
// 存活判定交给 etcd 租约：进程崩溃 / 断网 → 租约到期自动摘除，不留僵尸节点。
// 动态负载（在线人数）不进节点目录：它变化频繁，写 etcd 会把租约续期变成高频写，
// 仍走 master 心跳链路。
package app

import (
	"context"
	"fmt"

	"clover-server-engine/internal/transport/etcd"
	"clover-server-engine/pkg/foundation/logger"
	ujson "clover-server-engine/pkg/shared/json"
)

// nodeKeyRoot 节点目录的 etcd 根前缀。
const nodeKeyRoot = "clover/nodes/"

// nodeInfo 节点目录中记录的信息（节点地址即 key 后缀，不重复存）。
type nodeInfo struct {
	// Type 节点类型，取值与 master state.Node.Type 一致（如 state.NodeTypeGame）。
	Type string `json:"type"`
	// Tags 节点标签，供按 tag 查询（如 "room" 房间服）。
	Tags []string `json:"tags,omitempty"`
	// Admin admin HTTP 控制面地址，供运维编排工具（clover-server-tools/manager）
	// 定位本节点后下发 drain / 切上游 / shutdown。原样上报配置里的 admin.listen_addr，
	// 不做主机名改造——回环 / 通配地址如何换算成可达地址，由消费方决定（见该工具 README）。
	// 跨机管理时须把 admin.listen_addr 配成内网可达地址，否则远端连不上（回环只对本机有效）。
	//
	// ⚠️ 安全前提：把监听地址配成非回环时，**必须同时配置 admin.token**
	// （否则 AdminConfig.Normalize 直接拒绝启动，见 pkg/app/types）。
	// 有 token 也仍建议由内网隔离 / 防火墙再兜一层：admin server 启动时对该情况
	// 会打印一条 Warn 提醒（internal/app/admin.go Start）。
	Admin string `json:"admin,omitempty"`
}

// adminListenAddr 返回用于上报节点目录的 admin 控制面地址。
// 只做「零值 → 默认监听地址」的归一化；不改造主机名，避免把「配置意图」与
// 「工具侧可达性换算」两件事耦合在一个地方。
//
// 归一化失败（未配 admin.token 却要绑非回环）时返回 error 并**原样透传**：
// 进程在 runApp 里已经因同一份配置被拒、根本走不到这里；保留 error 是为了让
// 「配置不合法」永远只有一个判定源（AdminConfig.Normalize），不在这里另立一套兜底。
func adminListenAddr(cfg AdminConfig) (string, error) {
	c := cfg
	if err := c.Normalize(); err != nil {
		return "", err
	}
	return c.ListenAddr, nil
}

// nodeKey 返回某节点在 etcd 节点目录中的键。
func nodeKey(nodeID string) string { return nodeKeyRoot + nodeID }

// registerNode 把本节点登记到 etcd 节点目录（租约 + 自动续租）。
//
// 返回的 unregister 在进程退出时调用（撤销注册、回收租约），可安全重复调用。
// ec 为 nil（未配 etcd）或 nodeID 为空时返回空操作——未接入 etcd 的部署行为不变。
func registerNode(ctx context.Context, ec *etcd.Client, nodeID string, info nodeInfo) (func(), error) {
	if ec == nil || nodeID == "" {
		return func() {}, nil
	}
	value, err := ujson.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("discovery: marshal node info: %w", err)
	}
	key := nodeKey(nodeID)
	ttl := ec.Config().RegisterTTL
	stop, err := ec.Register(ctx, key, string(value), ttl)
	if err != nil {
		return nil, fmt.Errorf("discovery: register node %s (key=%s) failed: %w", nodeID, key, err)
	}
	logger.Infof("discovery: node registered (key=%s type=%s tags=%v ttl=%s)", key, info.Type, info.Tags, ttl)
	return stop, nil
}
