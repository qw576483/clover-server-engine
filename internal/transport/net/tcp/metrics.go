package tcp

import (
	"clover-server-engine/pkg/foundation/metrics"
)

// net 层指标埋点

// 命名遵循 metrics 包定义的 clover_<模块>_<指标>_<单位> 规范，模块名固定为 net。

// 埋点清单：

//	clover_net_connections{role}            Gauge   当前存活连接数（server/client）
//	clover_net_connections_total{role}      Counter 累计接受/建立的连接数
//	clover_net_disconnections_total{role}   Counter 累计断开数
//	clover_net_read_errors_total{role}      Counter 读错误（含拆帧失败）
//	clover_net_write_errors_total{role}     Counter 写错误
//	clover_net_send_queue_length{role}      Gauge   发送队列积压长度
//	clover_net_send_timeouts_total{role}    Counter 发送队列满导致的超时丢弃
//	clover_net_received_bytes_total{role}   Counter 收到的业务帧字节数
//	clover_net_sent_bytes_total{role}       Counter 发出的业务帧字节数

// # 关于 label 基数

// 只用 role 这一个 label（取值仅 server/client 两种）。
// 刻意**不**用 conn_id / remote_addr 做 label —— 连接是无界的，
// 一旦作为 label 会让时间序列随在线人数线性膨胀，直接打爆 Prometheus。
// 单连接粒度的问题排查靠日志里的 trace_id，不靠指标。

// netMetrics 是 net 模块的埋点句柄（进程级单例，构造一次复用）。
var netMetrics = metrics.ForModule(metrics.ModuleNet)

// 连接角色 label 取值。
const (
	roleServer = "server" // 被动接受的入站连接
	roleClient = "client" // 主动发起的出站连接
)

// connRole 从 Conn 上读取角色；未显式标注时按 server 计（Server.handleConn 是主要来源）。
func connRole(c *Conn) string {
	if c == nil {
		return roleServer
	}
	if v, ok := c.Value(connRoleKey{}); ok {
		if s, _ := v.(string); s != "" {
			return s
		}
	}
	return roleServer
}

// connRoleKey 是存放角色的 userData key。用空结构体私有类型作 key，
// 避免与业务用字符串 key 撞车。
type connRoleKey struct{}

// markRole 标注连接角色，供指标 label 使用。
func (c *Conn) markRole(role string) { c.SetValue(connRoleKey{}, role) }

// 以下为各埋点的语义化包装，调用点只写一行，保持业务代码可读
// metricConnOpened 连接建立：累计数 +1，存活数 +1。
func metricConnOpened(role string) {
	netMetrics.Count("connections_total", metrics.LabelRole, role)
	netMetrics.Gauge("connections", metrics.LabelRole, role).Inc()
}

// metricConnClosed 连接断开：累计断开数 +1，存活数 -1。
func metricConnClosed(role string) {
	netMetrics.Count("disconnections_total", metrics.LabelRole, role)
	netMetrics.Gauge("connections", metrics.LabelRole, role).Dec()
}

// metricReadError 读错误（拆帧失败 / 对端断开 / 读超时）。
func metricReadError(role string) {
	netMetrics.Count("read_errors_total", metrics.LabelRole, role)
}

// metricWriteError 写错误。
func metricWriteError(role string) {
	netMetrics.Count("write_errors_total", metrics.LabelRole, role)
}

// metricSendTimeout 发送队列满且等待超时。
func metricSendTimeout(role string) {
	netMetrics.Count("send_timeouts_total", metrics.LabelRole, role)
}

// metricSendQueue 上报发送队列当前积压长度。
// 这是排查「写侧反压」最直接的指标：持续接近容量说明下游消费不过来。
func metricSendQueue(role string, n int) {
	netMetrics.Gauge("send_queue_length", metrics.LabelRole, role).Set(float64(n))
}

// metricRecvBytes 收到的业务帧字节数。
func metricRecvBytes(role string, n int) {
	netMetrics.Add("received_bytes_total", float64(n), metrics.LabelRole, role)
}

// metricSentBytes 发出的业务帧字节数。
func metricSentBytes(role string, n int) {
	netMetrics.Add("sent_bytes_total", float64(n), metrics.LabelRole, role)
}
