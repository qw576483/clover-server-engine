// package mmo 的场景多级扫描定时器。
//
// 设计原则：把场景里不同频率的扫描任务分层。
// - 实时（Realtime）：每帧都跑，如关键输入/碰撞即时响应；
// - 快（Fast）：如 200ms 一次的 AI/状态刷新；
// - 慢（Slow）：如 1s 一次的持久化/统计。
//
// 主循环每帧调用一次 Tick(dt)；realtime 直接执行，fast/slow 按累计时间到周期才触发。
// 注册/取消可在任意线程调用（少量锁保护）；Tick 由单线程（主循环）驱动。
package mmo

import (
	"sync"
	"time"

	"clover-server-engine/pkg/foundation/logger"
)

// Tier 是扫描档位。
type Tier int

const (
	// TierRealtime 实时档：每帧执行。
	TierRealtime Tier = iota
	// TierFast 快档：按 periodFast 周期执行。
	TierFast
	// TierSlow 慢档：按 periodSlow 周期执行。
	TierSlow
)

// TaskFn 是扫描任务函数，dt 为本周期的累计时间。
type TaskFn func(dt time.Duration)

// task 是注册的内部任务节点。
type task struct {
	fn      TaskFn
	stopped bool
}

// SceneBeat 场景多级扫描定时器。
type SceneBeat struct {
	mu         sync.Mutex
	periodFast time.Duration
	periodSlow time.Duration
	realtime   []*task
	fast       []*task
	slow       []*task
	fastAcc    time.Duration
	slowAcc    time.Duration
	tickCnt    int // 每 100 tick 压缩 stopped 任务
}

// NewSceneBeat 构造 SceneBeat；realtime 每帧执行，fast/slow 分别按周期（0 表示每帧）。
func NewSceneBeat(periodFast, periodSlow time.Duration) *SceneBeat {
	return &SceneBeat{
		periodFast: periodFast,
		periodSlow: periodSlow,
	}
}

// Add 注册一个任务，返回取消函数。
func (b *SceneBeat) Add(tier Tier, fn TaskFn) (cancel func()) {
	b.mu.Lock()
	t := &task{fn: fn}
	switch tier {
	case TierFast:
		b.fast = append(b.fast, t)
	case TierSlow:
		b.slow = append(b.slow, t)
	case TierRealtime:
		b.realtime = append(b.realtime, t)
	default:
		// 未知档位静默按「每帧执行」调度会把慢任务变成热路径，性能语义错位，必须留痕。
		logger.Warnf("mmo: SceneBeat.Add 收到未知 tier=%d，按 realtime（每帧执行）调度", int(tier))
		b.realtime = append(b.realtime, t)
	}
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		t.stopped = true
		b.mu.Unlock()
	}
}

// Tick 主循环每帧调用：realtime 直接跑；fast/slow 累计 dt 到周期才触发。
// 任务在解锁后执行，避免回调内注册/取消造成死锁。
// slow 档与 fast 一致，可补偿多个周期。
// 每 100 tick 压缩一次 stopped 任务，防止内存泄漏。
func (b *SceneBeat) Tick(dt time.Duration) {
	b.mu.Lock()
	var rt []TaskFn
	for _, t := range b.realtime {
		if !t.stopped {
			rt = append(rt, t.fn)
		}
	}

	b.fastAcc += dt
	var fast []TaskFn
	// 周期 0 表示「每帧执行」（NewSceneBeat 注释的承诺）：不能让 periodFast>0 成为前置条件，
	// 否则 NewSceneBeat(0, x) 注册的 fast 任务永不执行、静默失效。
	fastDt := dt
	if b.periodFast > 0 {
		fastDt = 0
		if b.fastAcc >= b.periodFast {
			// 补偿周期数设上限，防止极端丢帧下长时间循环
			count := 0
			for b.fastAcc >= b.periodFast {
				b.fastAcc -= b.periodFast
				count++
				if count >= 200 { // 安全上限，防止无限循环
					b.fastAcc = 0
					break
				}
			}
			b.fastAcc = min(b.fastAcc, b.periodFast)
			// 传给回调的是「本周期累计时间」（TaskFn 注释的语义），不是帧 dt：
			// 多周期补偿时被补偿掉的那些时间必须对任务可见，否则
			// progress -= dt*rate 这类积分会少算。
			fastDt = b.periodFast * time.Duration(count)
			for _, t := range b.fast {
				if !t.stopped {
					fast = append(fast, t.fn)
				}
			}
		}
	} else {
		b.fastAcc = 0
		for _, t := range b.fast {
			if !t.stopped {
				fast = append(fast, t.fn)
			}
		}
	}

	b.slowAcc += dt
	var slow []TaskFn
	// slow 档支持多周期补偿。
	// 与 fast 一致设补偿上限：dt 异常巨大（进程长时间挂起后补帧）时，
	// 无上限的减法循环会让 tick 线程长时间自旋，拖垮整个场景。
	// 同理 periodSlow==0 也必须每帧执行。
	slowDt := dt
	if b.periodSlow > 0 {
		slowDt = 0
		if b.slowAcc >= b.periodSlow {
			count := 0
			for b.slowAcc >= b.periodSlow {
				b.slowAcc -= b.periodSlow
				count++
				if count >= 200 { // 安全上限，防止无限循环
					b.slowAcc = 0
					break
				}
			}
			b.slowAcc = min(b.slowAcc, b.periodSlow)
			slowDt = b.periodSlow * time.Duration(count)
			for _, t := range b.slow {
				if !t.stopped {
					slow = append(slow, t.fn)
				}
			}
		}
	} else {
		b.slowAcc = 0
		for _, t := range b.slow {
			if !t.stopped {
				slow = append(slow, t.fn)
			}
		}
	}

	// 每约 100 tick 压缩 stopped 任务
	b.tickCnt++
	if b.tickCnt >= 100 {
		b.tickCnt = 0
		b.realtime = compressTasks(b.realtime)
		b.fast = compressTasks(b.fast)
		b.slow = compressTasks(b.slow)
	}

	b.mu.Unlock()

	for _, fn := range rt {
		fn(dt)
	}
	for _, fn := range fast {
		fn(fastDt)
	}
	for _, fn := range slow {
		fn(slowDt)
	}
}

// compressTasks 移除 stopped 的任务，收缩切片。
func compressTasks(tasks []*task) []*task {
	n := 0
	for _, t := range tasks {
		if !t.stopped {
			tasks[n] = t
			n++
		}
	}
	// 尾部必须置 nil：否则底层数组仍持有已取消任务的指针（含 fn 闭包），
	// 被取消的任务要等下一次扩容才可能被 GC 回收。
	for i := n; i < len(tasks); i++ {
		tasks[i] = nil
	}
	return tasks[:n]
}
