package watchdog

import (
	"runtime/debug"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// Alert 一条告警（投给 Sink 的载荷，也是 /watchdog 快照之外的对外载体）。
type Alert struct {
	// Rule 规则名（注册时的固定名）。
	Rule string `json:"rule"`
	// Level 级别：warning / critical / recovered。
	Level Level `json:"level"`
	// Message 巡检函数给出的描述（recovered 时是"恢复正常（此前 …）"）。
	Message string `json:"message"`
	// At 告警产生时刻。
	At time.Time `json:"at"`
	// Consecutive 触发本次告警时已连续命中的轮数（recovered 时为 0）。
	Consecutive int `json:"consecutive,omitempty"`
}

// Sink 告警出口。
//
// 契约（实现方必须满足，否则会拖垮巡检）：
//
//   - Notify 由**单个 worker goroutine 串行**调用：实现不必自己加锁，但要能在非主 goroutine 里跑；
//   - **必须快速返回、不阻塞**：外部通道（webhook / IM）要用自己的超时与重试，
//     绝不能在 Notify 里长时间等待。队列满时引擎丢弃告警并计入
//     clover_watchdog_alerts_dropped_total，不会因此卡住巡检线程；
//   - panic 会被引擎捕获（只记 Error 日志），不会打死看门狗；
//   - 默认出口只保证「接口不为 nil」，日志留痕由 Watcher 统一负责（见 logAlert）；
//     真实通道（webhook / 邮件 / IM）由业务实现本接口注入：
//     watchdog.Install(watchdog.New(watchdog.Options{Sink: mySink}))。
type Sink interface {
	Notify(Alert)
}

// SinkFunc 把普通函数适配成 Sink。
type SinkFunc func(Alert)

// Notify 实现 Sink。
func (f SinkFunc) Notify(a Alert) { f(a) }

// noopSink 默认出口：什么都不做。
//
// 为什么默认出口是空实现而不是"把日志再打一遍"：告警的日志留痕由 Watcher.logAlert 统一保证
// （即使 Sink 全坏，日志里也有），默认出口再写一次只会产生重复日志。
// 它存在的意义是让 Options.Sink 永远不为 nil，业务替换它即可接上真实通道。
type noopSink struct{}

func (noopSink) Notify(Alert) {}

// dispatch 把告警投入投递队列。
//
// **非阻塞**：队列满就丢弃并计数（同时降频告警一条日志）——
// 巡检线程的优先级高于告警送达，绝不为一条送不出去的告警而阻塞。
func (w *Watcher) dispatch(a Alert) {
	select {
	case w.sinkCh <- a:
	default:
		w.dropped.Add(1)
		metricAlertDropped()
		if w.throttle.allow("drop:"+a.Rule, w.now()) {
			logger.Warnf("watchdog: alert queue full, dropping alert: rule=%s level=%s (同类日志已降频)", a.Rule, a.Level)
		}
	}
}

// sinkLoop 告警投递 worker：串行消费队列，停机时把队列里已排队的告警投完再退出。
func (w *Watcher) sinkLoop() {
	defer close(w.sinkDone)
	for {
		select {
		case a := <-w.sinkCh:
			w.notify(a)
		case <-w.stopCh:
			for {
				select {
				case a := <-w.sinkCh:
					w.notify(a)
				default:
					// 队列已排空（best-effort：不为了等一个慢 Sink 而拖住关停）。
					return
				}
			}
		}
	}
}

// notify 调用 Sink 投递一条告警，捕获 panic。
func (w *Watcher) notify(a Alert) {
	defer func() {
		if r := recover(); r != nil {
			// Sink 自己崩了不能连带打死看门狗（否则告警系统坏掉时连"告警系统坏了"都看不见）。
			// 堆栈必留：只打 %v 时告警系统自身的崩溃无从定位。
			logger.Errorf("watchdog: sink panic: rule=%s level=%s err=%v\n%s", a.Rule, a.Level, r, debug.Stack())
		}
	}()
	w.sink.Notify(a)
}
