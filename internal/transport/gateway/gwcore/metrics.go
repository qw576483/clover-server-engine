package gwcore

import (
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/metrics"
)

// gateway 层指标埋点

// 命名遵循 metrics 包定义的 clover_<模块>_<指标>_<单位> 规范，模块名固定为 gateway。

// 埋点清单：

//	clover_gateway_connections                        Gauge   当前在线会话数
//	clover_gateway_connections_total                  Counter 累计建立的会话数
//	clover_gateway_disconnections_total               Counter 累计断开的会话数
//	clover_gateway_messages_total{dir}                Counter 收发消息数（in=上行 / out=下行）
//	clover_gateway_bytes_total{dir}                   Counter 收发字节数
//	clover_gateway_message_duration_seconds{dir}      Histogram 单帧处理耗时
//	clover_gateway_rejected_total{reason}             Counter 被拒绝的帧/连接（按原因分类）
//	clover_gateway_upstream_errors_total{kind}        Counter 上游相关错误（拨号/转发）

// # 关于 label 基数

// 只用 dir（in/out）、reason、kind 这类**有限枚举**做 label。
// 严禁把 connID / playerID / 远端地址做 label —— 它们是无界的，
// 会让时间序列随在线人数线性膨胀，直接打爆 Prometheus。

// msg_id 同样**未**纳入 label：虽然它是有限枚举，但游戏协议号常有数百个，
// 与 dir 叉乘后基数偏高；确有单消息号分析需求时应在 logic 层单独埋点。

// gwMetrics 是 gateway 模块的埋点句柄（进程级单例）。
var gwMetrics = metrics.ForModule(metrics.ModuleGateway)

// 拒绝原因 / 错误类型的标准取值，集中定义避免各处拼写漂移。
const (
	reasonFrameTooLarge = "frame_too_large" // 超过 MaxFrameSize
	reasonQueuedDup     = "queued_dup"      // 排队中重复帧
	reasonAdmission     = "admission"       // 限流 / 容量拒绝
	reasonRateLimit     = "rate_limit"      // 消息级限流
	reasonDecrypt       = "decrypt"         // 解密失败
	reasonDecode        = "decode"          // 客户端帧解码失败
	reasonEncode        = "encode"          // 内部信封编码失败
	reasonKeepalive     = "keepalive"       // 最小保活帧（msgID=0 空 body），不下发逻辑服
	reasonUnauth        = "unauthenticated" // 登录门禁：未绑定对象标识且不在免登录白名单

	upstreamKindDial    = "dial"    // 上游拨号失败
	upstreamKindForward = "forward" // 上行转发失败
)

// metricSessionOpened 会话建立：累计数 +1，在线数 +1。
func metricSessionOpened() {
	gwMetrics.Count("connections_total")
	gwMetrics.Gauge("connections").Inc()
}

// metricSessionClosed 会话断开：累计断开数 +1，在线数 -1。
func metricSessionClosed() {
	gwMetrics.Count("disconnections_total")
	gwMetrics.Gauge("connections").Dec()
}

// metricRecvFrame 收到一条上行帧（含字节数）。
func metricRecvFrame(n int) {
	gwMetrics.Count("messages_total", metrics.LabelDir, metrics.DirIn)
	gwMetrics.Add("bytes_total", float64(n), metrics.LabelDir, metrics.DirIn)
}

// metricSentFrame 发出一条下行帧（含字节数）。
func metricSentFrame(n int) {
	gwMetrics.Count("messages_total", metrics.LabelDir, metrics.DirOut)
	gwMetrics.Add("bytes_total", float64(n), metrics.LabelDir, metrics.DirOut)
}

// metricFrameDuration 记录单帧处理耗时。
// 用 FastDuration 分桶：网关转发通常在百微秒量级，
// 默认 DefBuckets 起点 5ms 会把所有样本挤进第一个桶，P99 完全失真。
func metricFrameDuration(dir string, start time.Time) {
	gwMetrics.FastDuration("message_duration_seconds", metrics.LabelDir, dir).
		Observe(time.Since(start).Seconds())
}

// metricRejected 按原因累加被拒绝的帧/连接。
func metricRejected(reason string) {
	gwMetrics.Count("rejected_total", metrics.LabelReason, reason)
}

// metricUpstreamError 按类型累加上游错误。
func metricUpstreamError(kind string) {
	gwMetrics.Count("upstream_errors_total", metrics.LabelKind, kind)
}
