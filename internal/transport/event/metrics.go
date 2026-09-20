package event

import (
	"time"

	"clover-server-engine/pkg/foundation/metrics"
)

// event 层指标埋点

// 命名遵循 metrics 包定义的 clover_<模块>_<指标>_<单位> 规范，模块名固定为 event。

// 埋点清单：

//	clover_event_publish_total{kind}              Counter 本地事件发布数（按事件类型）
//	clover_event_crossnode_sent_total             Counter 跨节点投递尝试数（含重投）
//	clover_event_crossnode_acked_total            Counter 收到 ACK 成功确认数
//	clover_event_crossnode_retried_total          Counter 重投次数
//	clover_event_crossnode_rejected_total         Counter 接收方明确拒绝数
//	clover_event_crossnode_duplicate_total        Counter 接收方判定重复、未再派发的事件数
//	clover_event_crossnode_dead_letter_total      Counter 进入死信队列的事件数
//	clover_event_crossnode_dropped_total{reason}  Counter 未投递即丢弃（节点不存活 / 在途表满）
//	clover_event_publish_duration_seconds{result} Histogram 跨节点投递端到端耗时
//	clover_event_pending                          Gauge   当前在途（等待 ACK）事件数
//	clover_event_received_total                   Counter 收到的远端事件数

// # 关于 label 基数

// publish_total 用 kind（事件类型）做 label：事件类型是代码里写死的有限枚举，基数可控。
// 复用通用的 LabelKind 而非自造 "type"，避免同义 label key 在不同模块间漂移。
// 但**跨节点系列刻意不带 kind 也不带 target_node**：
//   - target_node 随集群弹性伸缩变化，是半无界的；
//   - kind × node 叉乘会让序列数快速膨胀。

// 需要按节点定位问题时看日志里的 trace_id，指标只回答「整体健不健康」。

// evtMetrics 是 event 模块的埋点句柄（进程级单例）。
var evtMetrics = metrics.ForModule(metrics.ModuleEvent)

// 丢弃原因的标准取值。
const (
	dropReasonNodeDown = "node_not_alive" // 目标节点不在存活集合
	dropReasonPending  = "pending_full"   // 在途表已满
)

// metricEventReceived 收到一条远端事件。
func metricEventReceived() { evtMetrics.Count("received_total") }

// metricCrossSent 一次投递尝试（首投或重投都计）。
func metricCrossSent() { evtMetrics.Count("crossnode_sent_total") }

// metricCrossAcked 收到成功 ACK。
func metricCrossAcked() { evtMetrics.Count("crossnode_acked_total") }

// metricCrossRetried 一次重投。
func metricCrossRetried() { evtMetrics.Count("crossnode_retried_total") }

// metricCrossRejected 接收方明确拒绝（不可重试，也不入死信）。
func metricCrossRejected() { evtMetrics.Count("crossnode_rejected_total") }

// metricCrossDuplicate 接收方按 MsgID 判定重复，直接回 ACK、不再派发。
func metricCrossDuplicate() { evtMetrics.Count("crossnode_duplicate_total") }

// metricCrossDeadLetter 事件进入死信队列。
// 这是最需要告警的指标：非零即代表有事件永久丢失，必须人工介入。
func metricCrossDeadLetter() { evtMetrics.Count("crossnode_dead_letter_total") }

// metricCrossDropped 未进入投递流程即被丢弃。
func metricCrossDropped(reason string) {
	evtMetrics.Count("crossnode_dropped_total", metrics.LabelReason, reason)
}

// metricPending 上报当前在途（等待 ACK）事件数。
// 持续增长说明 ACK 回不来，是跨服链路劣化的先行信号。
func metricPending(n int) {
	evtMetrics.Gauge("pending").Set(float64(n))
}

// metricCrossDuration 记录跨节点投递端到端耗时（含全部重试）。
// 用 result=ok/error 区分，避免失败的长耗时污染成功链路的 P99。
func metricCrossDuration(start time.Time, err error) {
	evtMetrics.Observe("crossnode", start, err)
}
