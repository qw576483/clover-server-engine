package watchdog

import (
	"time"

	"clover-server-engine/pkg/foundation/metrics"
)

// 看门狗埋点。
//
// 命名遵循 clover_<模块>_<指标>_<单位> 规范，模块名固定为 watchdog（见 metrics.ModuleWatchdog）。
//
// 埋点清单：
//
//	clover_watchdog_rules                          Gauge     已注册规则数
//	clover_watchdog_checks_total{rule,status}      Counter   巡检执行数（status=ok/firing/skipped/panic）
//	clover_watchdog_check_duration_seconds{rule}   Histogram 单次巡检耗时
//	clover_watchdog_alerts_total{rule,level}       Counter   触发告警数（level 含 recovered）
//	clover_watchdog_alerts_dropped_total           Counter   告警出口队列满被丢弃数
//
// # 关于 label 基数
//
// `rule` 是注册时的固定规则名 —— 规则数就是基数（个位数到几十），不会随在线人数增长；
// 规则名里**禁止**拼 player_id / conn_id / 时间戳这类动态值（那会让时间序列爆炸）。
// `status` / `level` 都是有限枚举，取值集中在下方常量与 Level 常量里。
//
// 这五个指标回答的问题分别是：
//
//	rules                     —— 看门狗到底在巡什么（为 0 = 没人在看，比没指标更危险）
//	checks_total{status}      —— 规则有没有在跑（skipped 持续增长 = 某条规则卡住了）
//	check_duration_seconds    —— 规则本身有没有变成负担
//	alerts_total{level}       —— 触发过什么
//	alerts_dropped_total      —— 告警**没送出去**（非零说明出口通道有问题）
var m = metrics.ForModule(metrics.ModuleWatchdog)

// 巡检结果的标准取值（对应 clover_watchdog_checks_total 的 status label）。
const (
	statusOK      = "ok"      // 本轮正常
	statusFiring  = "firing"  // 本轮命中（未达连续门限 / 冷却中也会计）
	statusSkipped = "skipped" // 上一轮仍在跑，本轮跳过
	statusPanic   = "panic"   // 检查函数 panic 或返回非法级别
)

// metricRules 上报已注册规则数。
func metricRules(n int) { m.Gauge("rules").Set(float64(n)) }

// metricCheck 上报一次巡检执行。
func metricCheck(rule, status string) {
	m.Count("checks_total", metrics.LabelRule, rule, metrics.LabelStatus, status)
}

// metricCheckDuration 上报单次巡检耗时。
func metricCheckDuration(rule string, d time.Duration) {
	m.Duration("check_duration_seconds", metrics.LabelRule, rule).Observe(d.Seconds())
}

// metricAlert 上报一次「决定告警」（含 recovered）。
// 与 alerts_dropped_total 对照即可区分「要不要告警」与「有没有送出去」。
func metricAlert(rule string, level Level) {
	m.Count("alerts_total", metrics.LabelRule, rule, metrics.LabelLevel, string(level))
}

// metricAlertDropped 上报一次因队列满被丢弃的告警。
func metricAlertDropped() { m.Count("alerts_dropped_total") }
