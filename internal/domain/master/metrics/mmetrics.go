// Package metrics 集中定义 master 域的指标埋点。

// # 为什么单独开一个包而不是各子包各写一份

// master 域被拆成 state / failover / client / server 等多个平级子包，
// 它们都需要上报指标。若每个子包各自 var xxx = metrics.ForModule(...)，
// 会出现同一模块名被重复构造、指标命名各写各的问题。
// 抽到一个极薄的公共包里，既保证命名唯一，也让「master 有哪些指标」一目了然。

// 本包**只依赖 foundation/metrics**，不反向依赖任何 master 子包，
// 因此不会引入循环依赖。

// 命名遵循 clover_<模块>_<指标>_<单位> 规范，模块名固定为 master。

// 埋点清单：

//	clover_master_nodes{kind}                      Gauge   已注册节点数（kind=节点类型）
//	clover_master_node_registered_total            Counter 累计注册次数
//	clover_master_node_removed_total{reason}       Counter 累计摘除次数（按原因）
//	clover_master_heartbeats_received_total        Counter 收到的心跳数
//	clover_master_heartbeat_timeouts_total{reason} Counter 心跳超时（suspect/dead）
//	clover_master_players                         Gauge   已登记定位的玩家数

// label key 一律复用 metrics 包已有的通用常量（kind/reason/role），
// 不自造 type/level/to 等同义词。
package metrics

import (
	"clover-server-engine/pkg/foundation/metrics"
)

// m 是 master 模块的埋点句柄（进程级单例）。
var m = metrics.ForModule(metrics.ModuleMaster)

// 摘除原因 / 超时级别的标准取值，集中定义避免各处拼写漂移。
const (
	// RemoveReasonAPI 由上层显式调用 RemoveNode 摘除（正常下线）。
	RemoveReasonAPI = "api"
	// RemoveReasonDead 心跳超时判定 Dead 后自动摘除。
	RemoveReasonDead = "dead"
	// RemoveReasonRetag 同 nodeID 换 Type 重注册时，按旧类型配对递减 Gauge 用的原因
	// （不是真的摘除，单独取值以免混入 api/dead 的摘除计数）。
	RemoveReasonRetag = "retag"

	// TimeoutSuspect 静默超过 SuspectTimeout，疑似失联（尚未摘除）。
	TimeoutSuspect = "suspect"
	// TimeoutDead 静默超过 DeadTimeout，判定死亡并摘除。
	TimeoutDead = "dead"
)

// NodeRegistered 节点注册：累计数 +1，对应类型的在册数 +1。
func NodeRegistered(nodeType string) {
	m.Count("node_registered_total")
	m.Gauge("nodes", metrics.LabelKind, nodeType).Inc()
}

// NodeRemoved 节点摘除：累计数 +1，对应类型的在册数 -1。

// 必须与 NodeRegistered 严格配对调用，否则 Gauge 会漂移。
// 调用方需保证只在「确实存在并被删除」的分支上调用（幂等删除的重复调用不能计数）。
func NodeRemoved(nodeType, reason string) {
	m.Count("node_removed_total", metrics.LabelReason, reason)
	m.Gauge("nodes", metrics.LabelKind, nodeType).Dec()
}

// Heartbeat 收到一次节点心跳。

// 指标名用复数 heartbeats_received_total：计数器统计的是「事件发生了多少次」，
// 与 node_recoveries_total / snapshot_saves_total 保持同一构词法。
func Heartbeat() { m.Count("heartbeats_received_total") }

// HeartbeatReceived 是 Heartbeat 的语义化别名，与指标名同形，便于调用处自解释。
func HeartbeatReceived() { Heartbeat() }

// HeartbeatTimeout 心跳超时判定（level 取 TimeoutSuspect / TimeoutDead）。
func HeartbeatTimeout(level string) {
	m.Count("heartbeat_timeouts_total", metrics.LabelReason, level)
}

// SetPlayers 上报当前已登记定位的玩家数。
func SetPlayers(n int) { m.Gauge("players").Set(float64(n)) }

// 扩展埋点（覆盖 health/failover 全链路）

//	clover_master_nodes_alive                   Gauge   存活节点数
//	clover_master_nodes_suspect                 Gauge   疑似失联节点数
//	clover_master_nodes_dead                    Gauge   已判定死亡节点数
//	clover_master_node_recoveries_total         Counter 节点恢复次数

// 健康状态的标准取值。
const (
	// StatusAlive / StatusSuspect / StatusDead 对应健康状态枚举，作为 Gauge 维度不易串。
	StatusAlive   = "alive"
	StatusSuspect = "suspect"
	StatusDead    = "dead"
)

// SetNodeHealth 上报各健康状态的节点计数（瞬时值，每次扫描后整体刷新）。
func SetNodeHealth(alive, suspect, dead int) {
	m.Gauge("nodes_alive").Set(float64(alive))
	m.Gauge("nodes_suspect").Set(float64(suspect))
	m.Gauge("nodes_dead").Set(float64(dead))
}

// NodeRecovered 节点从死亡恢复为存活。
func NodeRecovered() { m.Count("node_recoveries_total") }
