// 本文件提供分级时间轮（HierarchicalWheel）：5 级轮盘覆盖 毫秒→秒→分钟→小时→天，
// 每 tick 推进指针，溢出降级到下一层。适合海量短延时任务的粗粒度调度。

// 使用示例：

// hw := timer.NewHierarchicalWheel()
// hw.Add(func() { fmt.Println("fired") }, 1500*time.Millisecond)
// hw.Start()
package timer

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/pkg/shared/safe"
)

// level 时间轮层级定义。
type level struct {
	name  string
	tick  time.Duration // 每 tick 间隔
	slots int           // 槽位数
}

var wheelLevels = [5]level{
	{"ms", time.Millisecond, 100},     // 100ms 一轮
	{"s", 100 * time.Millisecond, 60}, // 6s 一轮
	{"min", 6 * time.Second, 60},      // 6min 一轮
	{"hour", 6 * time.Minute, 60},     // 6h 一轮
	{"day", 6 * time.Hour, 24},        // 144h ≈ 6 天
}

// timerTask 时间轮任务。
type timerTask struct {
	id    uint64
	delay time.Duration
	fn    func()
	slot  int
}

// timeWheel 单层时间轮。
type timeWheel struct {
	lvl      level
	slots    []*list.List
	current  int
	taskCnt  atomic.Int64
	overflow *timeWheel // 下一层（溢出降级）
	mu       sync.Mutex
}

// newTimeWheel 创建单层时间轮。
func newTimeWheel(lvl level) *timeWheel {
	tw := &timeWheel{
		lvl:   lvl,
		slots: make([]*list.List, lvl.slots),
	}
	for i := range tw.slots {
		tw.slots[i] = list.New()
	}
	return tw
}

// calcSteps 计算 delay 需要多少个本层 tick 才能覆盖（向上取整）。
// 调用方须保证 delay > 0；返回值 >= 1 且不会因 delay+tick 相加而溢出。
func calcSteps(delay, tick time.Duration) int {
	steps := delay / tick
	if delay%tick != 0 {
		steps++
	}
	if steps < 1 {
		steps = 1
	}
	return int(steps)
}

// add 添加任务到该层（含溢出降级）。对外入口，自持锁。
func (tw *timeWheel) add(task *timerTask) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.addLocked(task)
}

// addLocked 把任务放入本层槽位或降级到 overflow；调用方须持锁。
//
// 时基约定：add 时把任务放到 `(current+steps)%slots` 槽，下一次 tick 先自增
// current 再处理槽位，因此该任务恰好经过 steps 个本层 tick 后被取出。
// steps ∈ [1, slots]（steps==slots 表示绕整圈），大于 slots 才降级。
// 放入槽位时先扣减 `steps*tick` 的等待量，任务被取出时 `delay <= 0` 即到点触发，
// 这保证任务**不会提前触发**（最多晚一个本层 tick 的粒度）。
func (tw *timeWheel) addLocked(task *timerTask) {
	steps := calcSteps(task.delay, tw.lvl.tick)
	if steps > tw.lvl.slots {
		if tw.overflow != nil {
			tw.overflow.add(task) // 降级到下一层（锁序恒为「由浅到深」，无环）
			return
		}
		// 顶层溢出（延时超过最大轮盘覆盖范围，如 > 144h）：钳制到整圈等待，
		// 到期后在 tick 中按剩余 delay 重新排入本层（不丢任务、不提前触发）。
		steps = tw.lvl.slots
	}
	task.delay -= time.Duration(steps) * tw.lvl.tick
	task.slot = (tw.current + steps) % tw.lvl.slots
	tw.slots[task.slot].PushBack(task)
	tw.taskCnt.Add(1)
}

// tick 推进一个 tick，返回触发的任务列表。
// current 先自增再读槽：与 addLocked 的 `(current+steps)%slots` 约定配对，
// 保证任务恰在其预计的 tick 数后从槽位取出（不提前、不滞后一圈）。
func (tw *timeWheel) tick() []func() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	var fns []func()
	tw.current = (tw.current + 1) % tw.lvl.slots

	// 取出当前槽位的所有任务
	slot := tw.slots[tw.current]
	var remaining []*timerTask

	for e := slot.Front(); e != nil; e = e.Next() {
		task := e.Value.(*timerTask)
		// 任务离开本层槽位：先把本层计数摘掉，避免多级重复计入 TaskCount。
		// 若任务被重新排入某层（remaining 降级/重排），由目标层 addLocked 重新 +1。
		tw.taskCnt.Add(-1)
		if task.delay <= 0 {
			fns = append(fns, task.fn)
		} else {
			remaining = append(remaining, task)
		}
	}

	slot.Init()

	// 残余任务（仅顶层钳制的超长延时任务会走这里）：按剩余 delay 在本层重排。
	// 不再直接丢给无锁的 overflow（旧实现里 add 会在 tick 持锁时二次加锁自死锁）。
	for _, task := range remaining {
		tw.addLocked(task)
	}

	return fns
}

// HierarchicalWheel 分级时间轮。
type HierarchicalWheel struct {
	wheels [5]*timeWheel
	// mu 串行化 Start/Stop 生命周期：保证 stopCh 只被关闭一次、重启前旧循环已退出。
	mu       sync.Mutex
	stopCh   chan struct{}
	loopDone chan struct{}
	running  atomic.Bool
	seq      atomic.Uint64
	// ticks 是基础 tick（= wheelLevels[0].tick）的累计计数，仅由调度循环 goroutine 读写，
	// 用于判断各层是否到达自己的推进节奏（见 advance）。
	ticks uint64
}

// NewHierarchicalWheel 创建分级时间轮（无参数）。
// 最底层每 tick 间隔固定取 wheelLevels[0].tick（time.Millisecond），不可配置。
func NewHierarchicalWheel() *HierarchicalWheel {
	hw := &HierarchicalWheel{
		stopCh: make(chan struct{}),
	}

	// 构建 5 层时间轮
	for i := 0; i < 5; i++ {
		hw.wheels[i] = newTimeWheel(wheelLevels[i])
		if i > 0 {
			hw.wheels[i-1].overflow = hw.wheels[i]
		}
	}
	return hw
}

// Add 添加一个定时任务。返回任务 id（0 表示参数非法未注册）。
// 注意：时间轮未 Start 时任务不会被推进（不会触发），Start 后才开始计时（粗细粒度见包注释）。
func (hw *HierarchicalWheel) Add(fn func(), delay time.Duration) uint64 {
	if fn == nil || delay <= 0 {
		return 0
	}

	id := hw.seq.Add(1)
	task := &timerTask{
		id:    id,
		delay: delay,
		fn:    fn,
	}

	// 从底层时间轮开始添加
	hw.wheels[0].add(task)
	return id
}

// Start 启动时间轮。可重复调用（幂等）；Stop 之后可再次 Start 重启。
func (hw *HierarchicalWheel) Start() {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	if hw.running.Load() {
		return
	}

	// 每次启动都使用全新的停止信号与退出信号：Stop 已关闭过的 channel 不能复用，
	// 否则二次 close 会 panic、复用已关闭的 stopCh 会让新循环立即退出（不可重启）。
	stopCh := make(chan struct{})
	done := make(chan struct{})
	hw.stopCh = stopCh
	hw.loopDone = done
	hw.running.Store(true)

	go func() {
		defer close(done)
		ticker := time.NewTicker(wheelLevels[0].tick)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				hw.advance()
			}
		}
	}()
}

// Stop 停止时间轮（幂等：未启动或已停止时调用无副作用）。
// 会等待调度循环真正退出后才返回，保证随后 Start 重启时不会出现新旧两个循环同时推进。
func (hw *HierarchicalWheel) Stop() {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	if !hw.running.Load() {
		return
	}
	hw.running.Store(false)
	close(hw.stopCh)
	<-hw.loopDone
}

// advance 推进各层级。由基础 ticker（wheelLevels[0].tick）驱动。
//
// 关键：每层只按**自己的槽宽**节奏推进一次，而不是每来一个基础 tick 就把
// 全部 5 层各推一格 —— 后者会让 level1 及以上层级比预期快 100~2e7 倍推进，
// 落在上层的任务被大幅提前触发（如 150ms 的任务约 1ms 就触发），定时语义整体失效。
func (hw *HierarchicalWheel) advance() {
	hw.ticks++
	for i := range hw.wheels {
		div := hw.wheels[i].lvl.tick / wheelLevels[0].tick
		if div < 1 {
			div = 1
		}
		if hw.ticks%uint64(div) != 0 {
			continue // 该层尚未走到自己的推进时刻
		}
		fns := hw.wheels[i].tick()
		for _, fn := range fns {
			safeCall(fn)
		}
	}
}

// TaskCount 返回总任务数（各层槽位内任务数之和；任务迁移到其它层时只计入一层，
// 不会在多级重复记账）。
func (hw *HierarchicalWheel) TaskCount() int64 {
	var total int64
	for i := 0; i < 5; i++ {
		total += hw.wheels[i].taskCnt.Load()
	}
	return total
}

// safeCall 安全调用任务函数，避免单个 panic 干扰其他任务。
func safeCall(fn func()) {
	safe.SafeRun(fn)
}
