// Package timer 提供进程内定时器（集中式调度器）。

// 设计目标：用单 goroutine + 截止时间最小堆驱动大量短生命周期定时任务，
// 适合每玩家心跳 / 超时、技能 CD、定时批量落盘等场景；零外部依赖，可独立单测。

// 三层定时能力：
//   - After(d, task)    一次性延迟任务
//   - Every(i, task)    固定间隔周期任务（首次在 i 后触发）
//   - Cron(spec, task)  类 crontab 周期任务（可选，最小粒度到分钟）

// 所有任务在独立 goroutine（safe.GoSafe）中执行，单个任务 panic 不影响调度器。

// 具名与作用域（供业务「挂在对象上」与「按名字关闭」）：
//   - AfterName / EveryName / CronName：注册时带 name（可 StopNamed 取消）与 scope（可 StopScope 批量取消）。
//   - Group(scope)：一组共享同一 scope 标签的定时任务（玩家 / 场景 / 虚拟服 …），Group.Stop 或
//     StopScope(scope) 可一次性取消该 scope 下全部任务（如玩家掉线时清理其所有定时任务）。

// 业务通常无需自行 NewScheduler：引擎已装配好共享调度器，经 g.Timer（app.TimeEvent）
// 提供 Every / After / ByTime / DailyAt / Cron / TimerGroup 等注册入口；
// 需要独立的调度域（如单元测试、独立子系统）时才直接调用 NewScheduler。
package timer

import (
	"container/heap"
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// ErrNoBackend 未配置持久化后端时调用 PersistScope/RestoreScope/ClearPersist 返回此错误。
var ErrNoBackend = errors.New("timer: persistence backend not configured")

// PersistBackend 定时器持久化后端接口。
// 实现方提供具体的存储逻辑（Redis、文件等），定时器包不依赖任何外部存储。
type PersistBackend interface {
	// Save 保存定时器状态。key 为持久化键（含 scope），data 为 DumpScope 产出的 JSON。
	Save(ctx context.Context, key string, data []byte) error
	// Load 加载定时器状态。key 不存在时应返回 nil, nil。
	Load(ctx context.Context, key string) ([]byte, error)
	// Delete 删除定时器状态。key 不存在时应静默返回 nil。
	Delete(ctx context.Context, key string) error
}

// Task 定时任务函数（无参、无返回值）。
type Task func()

// Timer 已注册任务句柄，调用 Stop 可取消。
type Timer interface {
	// Stop 取消任务：one-shot 未触发则不再触发；周期任务不再重复执行。
	Stop()
	// Active 报告任务是否仍在调度中（未取消、未触发完、未 Close）。
	Active() bool
	// Name 返回注册时的名字（匿名任务为空串）。
	Name() string
}

// SchedulerOption 调度器可选配置。
type SchedulerOption func(*Scheduler)

// WithGranularity 设置调度精度（默认 100ms）。
// 调度 goroutine 至少每 granularity 重新扫描一次堆；新插入更早的任务会通过内部通知立即唤醒。
func WithGranularity(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		if d > 0 {
			s.granularity = d
		}
	}
}

// WithLocation 设置调度器时区（默认 time.Local，即操作系统本地时区）。
// 影响 cron 的匹配时间计算以及 now() 的时区。
func WithLocation(loc *time.Location) SchedulerOption {
	return func(s *Scheduler) {
		if loc != nil {
			s.loc = loc
		}
	}
}

// WithPersistence 配置定时器持久化后端。设置后可调用 PersistScope / RestoreScope / ClearPersist。
func WithPersistence(backend PersistBackend) SchedulerOption {
	return func(s *Scheduler) {
		s.persist = backend
	}
}

// WithPersistPrefix 设置持久化键前缀，用于多个调度器共用同一存储时隔离。默认 "timer:"。
func WithPersistPrefix(prefix string) SchedulerOption {
	return func(s *Scheduler) {
		if prefix != "" {
			s.persistKey = prefix
		}
	}
}

// entry 堆中一个定时任务。
type entry struct {
	id       int64
	name     string        // 具名任务的名字（匿名为空）；用于 StopNamed
	scope    string        // 作用域标签（玩家/场景/虚拟服…）；用于 StopScope 批量取消
	next     time.Time     // 下次触发时间
	interval time.Duration // >0 为周期任务（固定间隔）；0 为一次性
	task     Task
	canceled bool
}

// scopeRef 记录一个具名任务在 byScope 中的位置，便于 StopNamed 同步摘除，
// 避免 byScope 残留已取消任务的 stopper 造成内存泄漏。
type scopeRef struct {
	scope string
	key   string
}

// timerHeap 按 next 升序的最小堆。
type timerHeap []*entry

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)        { *h = append(*h, x.(*entry)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// Scheduler 集中式定时器：单 goroutine 驱动，基于截止时间最小堆。
type Scheduler struct {
	mu          sync.Mutex
	heap        timerHeap
	entries     map[int64]*entry
	byName      map[string]func()            // name -> stopper（StopNamed 用）
	byScope     map[string]map[string]func() // scope -> (key -> stopper)（StopScope 批量取消）
	nameLoc     map[string]scopeRef          // name -> 其在 byScope 中的位置，供 StopNamed 同步清理
	taskReg     map[string]Task              // scope+"\x00"+name -> Task（迁移重建用，注册时存入，StopScope 时清理对应键）
	cronSpec    map[string]string            // scope+"\x00"+name -> cron 表达式（供 DumpScope 导出）
	regOwner    map[string]int64             // scope+"\x00"+name -> 注册该键的 entry id（防止后注册的同名任务被旧任务误删）
	granularity time.Duration
	loc         *time.Location
	tick        *time.Ticker
	notify      chan struct{}
	stopCh      chan struct{}
	closed      bool
	seq         int64
	wg          sync.WaitGroup
	persist     PersistBackend // 持久化后端（可选，nil 表示不持久化）
	persistKey  string         // 持久化键前缀（默认 "timer:"）
}

// NewScheduler 构造集中式定时器，并立即启动调度 goroutine。
func NewScheduler(opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		heap:        make(timerHeap, 0),
		entries:     make(map[int64]*entry),
		byName:      make(map[string]func()),
		byScope:     make(map[string]map[string]func()),
		nameLoc:     make(map[string]scopeRef),
		taskReg:     make(map[string]Task),
		cronSpec:    make(map[string]string),
		regOwner:    make(map[string]int64),
		granularity: 100 * time.Millisecond,
		loc:         time.Local,
		persistKey:  "timer:",
		notify:      make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	s.tick = time.NewTicker(s.granularity)
	s.wg.Add(1)
	go s.run()
	return s
}

// now 返回经时区校正的当前时间。
func (s *Scheduler) now() time.Time {
	return time.Now().In(s.loc)
}

// scopedKey 生成带 scope 前缀的复合键，避免跨 scope 同名覆盖。
func scopedKey(scope, name string) string {
	if scope == "" {
		return name
	}
	return scope + "\x00" + name
}

// After 注册一次性延迟任务（匿名，仅靠返回的 Timer 句柄取消）。
func (s *Scheduler) After(delay time.Duration, task Task) Timer {
	return s.add("", "", delay, 0, task)
}

// Every 注册固定间隔周期任务（匿名，首次在 interval 后触发，之后每 interval 重复）。
func (s *Scheduler) Every(interval time.Duration, task Task) Timer {
	if interval <= 0 {
		logger.Warnf("timer.Every: non-positive interval ignored")
		return &timerHandle{}
	}
	return s.add("", "", interval, interval, task)
}

// AfterName 具名一次性延迟任务：可用 StopNamed(name) 取消，或随 scope 由 StopScope 批量取消。
func (s *Scheduler) AfterName(name, scope string, delay time.Duration, task Task) Timer {
	return s.add(name, scope, delay, 0, task)
}

// EveryName 具名固定间隔周期任务。
func (s *Scheduler) EveryName(name, scope string, interval time.Duration, task Task) Timer {
	if interval <= 0 {
		logger.Warnf("timer.EveryName: non-positive interval ignored")
		return &timerHandle{}
	}
	return s.add(name, scope, interval, interval, task)
}

// CronName 具名 crontab 周期任务。
func (s *Scheduler) CronName(name, scope, spec string, task Task) (Timer, error) {
	ct, err := s.newCronTimer(spec, task)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.seq++
	ct.id = s.seq
	if name != "" {
		k := scopedKey(scope, name)
		s.taskReg[k] = task  // 注册任务供迁移重建，键含 scope 防跨域覆盖
		s.cronSpec[k] = spec // 记录 cron 表达式供 DumpScope 导出
		s.regOwner[k] = ct.id
	}
	s.mu.Unlock()
	ct.name = name
	ct.scope = scope
	if name != "" || scope != "" {
		key := conv.FormatInt(ct.id)
		s.registerNamed(name, scope, key, func() { ct.Stop() })
	}
	return ct, nil
}

// registerNamed 登记具名/作用域任务的取消器：同名旧任务会被先取消（重启语义），
// scope 下的全部任务可由 StopScope 一次性取消。
func (s *Scheduler) registerNamed(name, scope, key string, stop func()) {
	var old func()
	s.mu.Lock()
	if name != "" {
		old = s.byName[name]
		// 防跨 scope 同名干扰：如果旧任务是不同 scope 的，不取消它，
		// 否则 PlayerA 的 "heartbeat" 会被 PlayerB 的注册静默取消。
		if old != nil && scope != "" {
			if ref, ok := s.nameLoc[name]; ok && ref.scope != scope {
				old = nil
			}
		}
		s.byName[name] = stop
	}
	if scope != "" {
		if s.byScope[scope] == nil {
			s.byScope[scope] = map[string]func(){}
		}
		s.byScope[scope][key] = stop
		if name != "" {
			s.nameLoc[name] = scopeRef{scope: scope, key: key}
		}
	}
	s.mu.Unlock()
	if old != nil {
		old() // 取消同名旧任务（在锁外调用，避免重入）
	}
}

// add 内部注册入口：delay 为首次延迟，period 为周期（0=一次性）；name/scope 可空。
func (s *Scheduler) add(name, scope string, delay, period time.Duration, task Task) Timer {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return &timerHandle{}
	}
	s.seq++
	id := s.seq
	e := &entry{id: id, name: name, scope: scope, next: s.now().Add(delay), interval: period, task: task}
	heap.Push(&s.heap, e)
	s.entries[id] = e
	if name != "" {
		k := scopedKey(scope, name)
		s.taskReg[k] = task // 注册任务供迁移重建，键含 scope 防跨域覆盖
		s.regOwner[k] = id
	}
	s.mu.Unlock()
	if name != "" || scope != "" {
		key := conv.FormatInt(id)
		s.registerNamed(name, scope, key, func() { s.stop(id) })
	}
	s.signal()
	return &timerHandle{s: s, id: id, name: name}
}

// signal 非阻塞唤醒调度循环（新任务/更早截止）。
func (s *Scheduler) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// run 调度主循环：顺序触发所有到期任务，完成后按最近截止或 granularity 休眠。
func (s *Scheduler) run() {
	defer s.wg.Done()
	var wake *time.Timer
	defer func() {
		if wake != nil {
			wake.Stop()
		}
	}()
	for {
		s.mu.Lock()
		now := s.now()
		for s.heap.Len() > 0 {
			top := s.heap[0]
			if top.next.After(now) {
				break
			}
			e := heap.Pop(&s.heap).(*entry)
			if e.canceled {
				delete(s.entries, e.id)
				continue
			}
			if e.interval > 0 {
				e.next = e.next.Add(e.interval)
				// 防追赶风暴：进程挂起/长 GC 后 next 深陷过去，固定步进会以最快速度
				// 连发所有「欠账」触发（心跳类任务下是风暴级）。落后超过一个周期时
				// 直接对齐到 now+interval，丢弃欠账触发。
				if !e.next.After(now) {
					e.next = now.Add(e.interval)
				}
				heap.Push(&s.heap, e)
			} else {
				delete(s.entries, e.id)
				// 一次性任务自然触发后同样要回收注册项，否则闭包永久残留。
				s.unregisterRegLocked(e.scope, e.name, e.id)
			}
			s.mu.Unlock()
			s.fire(e)
			s.mu.Lock()
			now = s.now()
		}

		hasTask := s.heap.Len() > 0
		var sleep time.Duration
		if hasTask {
			if d := s.heap[0].next.Sub(now); d > 0 {
				sleep = d
			}
		}
		s.mu.Unlock()

		if !hasTask {
			select {
			case <-s.stopCh:
				return
			case <-s.notify:
			case <-s.tick.C:
			}
			continue
		}
		// 极小 interval 防护：sleep 为 0 或极短时不要忙循环，至少等待 1ms 或使用 granularity。
		if sleep <= 0 {
			sleep = time.Millisecond
		}
		if sleep < time.Millisecond {
			sleep = time.Millisecond
		}
		if wake == nil {
			wake = time.NewTimer(sleep)
		} else {
			if !wake.Stop() {
				select {
				case <-wake.C:
				default:
				}
			}
			wake.Reset(sleep)
		}
		select {
		case <-s.stopCh:
			return
		case <-s.notify:
		case <-s.tick.C:
		case <-wake.C:
		}
	}
}

// fire 在独立 goroutine 中执行任务（safe.GoSafe 兜底 panic）；
// 在 GoSafe 内部做第三次 canceled 检查：因为 stop(id) 可能在 unlock 与 GoSafe 执行之间
// 设置 canceled=true，若不做终检，已释放的 owner 对象可能仍被任务访问。
func (s *Scheduler) fire(e *entry) {
	s.mu.Lock()
	if e.canceled {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	safe.GoSafe(func() {
		s.mu.Lock()
		canceled := e.canceled
		s.mu.Unlock()
		if canceled {
			return
		}
		e.task()
	})
}

// unregisterRegLocked 回收 (scope,name) 在 taskReg/cronSpec 中的注册项。
//
// 背景（缺陷）：这两个表只在 StopScope/StopAll/Close 里清理，而 stop(id)/StopNamed 不清，
// 于是「注册→取消」往返后仍残留一个 Task 闭包，业务用「每玩家唯一名」注册时永久泄漏。
//
// 只有该键确由 id 注册时才删除：同名任务被「重启」时（旧 id 未取消、新 id 已写入 taskReg），
// 旧任务的 stop 不得把新任务的注册项删掉。调用方须持锁。
func (s *Scheduler) unregisterRegLocked(scope, name string, id int64) {
	if name == "" {
		return
	}
	k := scopedKey(scope, name)
	if owner, ok := s.regOwner[k]; !ok || owner != id {
		return
	}
	delete(s.taskReg, k)
	delete(s.cronSpec, k)
	delete(s.regOwner, k)
}

// stop 取消指定任务（标记 canceled 并从 entries 移除；堆中残留项在到期时被跳过）。
func (s *Scheduler) stop(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[id]; ok {
		e.canceled = true
		delete(s.entries, id)
		if e.name != "" {
			if ref, ok := s.nameLoc[e.name]; ok && ref.key == conv.FormatInt(id) {
				delete(s.nameLoc, e.name)
			}
			s.unregisterRegLocked(e.scope, e.name, id)
		}
	}
}

// unregisterCronReg 供 cronTimer.Stop 在锁外回收注册项（内部再取调度器锁）。
func (s *Scheduler) unregisterCronReg(scope, name string, id int64) {
	if name == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unregisterRegLocked(scope, name, id)
}

// active 报告任务是否仍在调度中。
func (s *Scheduler) active(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	return ok && !e.canceled
}

// StopNamed 按名字取消具名任务；返回是否找到该名字。同名任务被「重启」时旧任务会先被取消。
func (s *Scheduler) StopNamed(name string) bool {
	s.mu.Lock()
	stop, ok := s.byName[name]
	delete(s.byName, name)
	// 同步摘除 byScope 中的同一个 stopper，否则该闭包会一直挂在 scope 下，
	// 长生命周期 scope 反复注册同名定时器时会持续泄漏。
	s.dropScopeRefLocked(name)
	s.mu.Unlock()
	if ok {
		stop()
		return true
	}
	return false
}

// StopScope 取消某一作用域（玩家/场景/虚拟服…）下的全部定时任务。
// 同时清理 taskReg 和 cronSpec 中对应 scope 的条目，避免闭包引用阻止 GC。
func (s *Scheduler) StopScope(scope string) {
	s.mu.Lock()
	m := s.byScope[scope]
	delete(s.byScope, scope)
	for n, ref := range s.nameLoc {
		if ref.scope == scope {
			delete(s.nameLoc, n)
		}
	}
	// 清理 taskReg 中该 scope 的条目，释放闭包引用
	for key := range s.taskReg {
		if taskScope, _ := splitScopedKey(key); taskScope == scope {
			delete(s.taskReg, key)
			delete(s.regOwner, key)
		}
	}
	// 清理 cronSpec 中该 scope 的条目
	for key := range s.cronSpec {
		if cronScope, _ := splitScopedKey(key); cronScope == scope {
			delete(s.cronSpec, key)
			delete(s.regOwner, key)
		}
	}
	s.mu.Unlock()
	for _, stop := range m {
		stop()
	}
}

// StopAll 取消全部任务但保持调度 goroutine 运行（可继续注册新任务）。
func (s *Scheduler) StopAll() {
	s.mu.Lock()
	// 必须逐个标记 canceled：run() 可能已把某个 entry 弹出堆、正准备执行，
	// 仅清空 heap/entries 无法阻止它触发（fire 判定的是 e.canceled）。
	for _, e := range s.entries {
		e.canceled = true
	}
	for _, e := range s.heap {
		e.canceled = true
	}
	s.heap = s.heap[:0]
	s.entries = make(map[int64]*entry)
	s.byName = make(map[string]func())
	s.byScope = make(map[string]map[string]func())
	s.nameLoc = make(map[string]scopeRef)
	s.taskReg = make(map[string]Task)
	s.cronSpec = make(map[string]string)
	s.regOwner = make(map[string]int64)
	s.mu.Unlock()
	// 唤醒 run() goroutine：之前可能因最新任务较远而在 wake.C 上睡眠，
	// StopAll 后堆已空，需通知其立即重新计算 sleep=0 进入下一次 select。
	s.signal()
}

// Close 停止调度 goroutine 并清空所有任务；之后注册的任务返回非活跃句柄。
func (s *Scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	for _, e := range s.entries {
		e.canceled = true
	}
	for _, e := range s.heap {
		e.canceled = true
	}
	s.heap = s.heap[:0]
	s.entries = make(map[int64]*entry)
	s.byName = make(map[string]func())
	s.byScope = make(map[string]map[string]func())
	s.nameLoc = make(map[string]scopeRef)
	s.taskReg = make(map[string]Task)
	s.cronSpec = make(map[string]string)
	s.regOwner = make(map[string]int64)
	s.mu.Unlock()
	// close(stopCh) 停止：run() 在下一轮 select 迭代中检出并退出，
	// 但在此之前会完成当前循环中所有到期任务的派发。
	close(s.stopCh)
	s.wg.Wait()
	s.tick.Stop()
}

// Group 一组共享同一 scope 标签的定时任务（玩家 / 场景 / 虚拟服 …）。
// 经 Scheduler.Group(scope) 创建；其上注册的定时任务自动打 scope 标签，
// 调用 Group.Stop() 或 Scheduler.StopScope(scope) 可一次性取消该 scope 下全部任务。
type Group struct {
	s     *Scheduler
	scope string
}

// Group 创建带统一 scope 标签的定时任务组。scope 通常为 owner（玩家）/ 场景 ID / 虚拟服 ID。
func (s *Scheduler) Group(scope string) *Group {
	return &Group{s: s, scope: scope}
}

// Every 固定间隔周期任务（具名，挂在 Group 的 scope 上）。
func (g *Group) Every(name string, interval time.Duration, task Task) Timer {
	return g.s.EveryName(name, g.scope, interval, task)
}

// After 一次性延迟任务（具名，挂在 Group 的 scope 上）。
func (g *Group) After(name string, delay time.Duration, task Task) Timer {
	return g.s.AfterName(name, g.scope, delay, task)
}

// OnTimer 在指定时刻触发一次性任务（具名，挂在 Group 的 scope 上）。
// when 是触发的绝对时间，若已过去则立即触发。
func (g *Group) OnTimer(name string, when time.Time, task Task) Timer {
	delay := time.Until(when)
	if delay < 0 {
		delay = 0
	}
	return g.s.AfterName(name, g.scope, delay, task)
}

// Cron crontab 任务（具名，挂在 Group 的 scope 上）。
func (g *Group) Cron(name, spec string, task Task) (Timer, error) {
	return g.s.CronName(name, g.scope, spec, task)
}

// Stop 取消该 scope 下的全部定时任务。
func (g *Group) Stop() {
	g.s.StopScope(g.scope)
}

// DumpScope 导出该 Group 下所有具名定时器为 JSON（供跨节点迁移）。
func (g *Group) DumpScope() []byte {
	return g.s.DumpScope(g.scope)
}

// ImportScope 从迁移 JSON 恢复该 Group 下的定时器（在目标节点调用）。
func (g *Group) ImportScope(data []byte) {
	g.s.ImportScope(g.scope, data)
}

// Scope 返回该 Group 的 scope 标签。
func (g *Group) Scope() string {
	return g.scope
}

// timerHandle 普通任务（After/Every）的句柄。
type timerHandle struct {
	s    *Scheduler
	id   int64
	name string
}

func (h *timerHandle) Stop() {
	if h.s == nil {
		return
	}
	h.s.stop(h.id)
}

func (h *timerHandle) Active() bool {
	if h.s == nil {
		return false
	}
	return h.s.active(h.id)
}

func (h *timerHandle) Name() string { return h.name }

// TimerState 单个定时器的可序列化摘要（迁移协议用）。
// 匿名定时器（name==""）不支持迁移，DumpScope 自动跳过。
type TimerState struct {
	Name        string `json:"name"`
	RemainingMs int64  `json:"rem_ms"`         // 距离下次触发的毫秒数（相对导出时刻，跨停机不守恒）
	IntervalMs  int64  `json:"ivl_ms"`         // 0 = 一次性，>0 = 周期
	Spec        string `json:"spec,omitempty"` // cron 表达式（非空表示 crontab 任务）
}

// DumpScope 导出 scope 下所有具名定时器（含 cron 任务）为 JSON 字节。
// 匿名定时器自动跳过；cron 任务通过 cronSpec 注册表查找并计算下次触发剩余时间。
func (s *Scheduler) DumpScope(scope string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	list := s.entriesByScope(scope)
	now := s.now()
	states := make([]TimerState, 0, len(list))
	seen := make(map[string]bool, len(list))
	for _, e := range list {
		if e.name == "" {
			continue // 匿名定时器无法重建，跳过
		}
		rem := e.next.Sub(now)
		if rem < 0 {
			rem = 0
		}
		states = append(states, TimerState{
			Name:        e.name,
			RemainingMs: rem.Milliseconds(),
			IntervalMs:  e.interval.Milliseconds(),
		})
		seen[e.name] = true
	}
	// 追加 cron 任务：通过 cronSpec 注册表找出该 scope 下的 crontab 任务
	for key, spec := range s.cronSpec {
		// key 格式为 scope+"\x00"+name
		cronScope, cronName := splitScopedKey(key)
		if cronScope != scope {
			continue
		}
		if seen[cronName] {
			continue // 避免重复（理论上 cron 不在 entries 中，但防御）
		}
		// 计算 cron 下次触发的剩余时间
		sched, err := parseCron(spec)
		if err != nil {
			continue
		}
		next := sched.Next(now)
		rem := int64(0)
		if !next.IsZero() {
			if d := next.Sub(now); d > 0 {
				rem = d.Milliseconds()
			}
		}
		states = append(states, TimerState{
			Name:        cronName,
			RemainingMs: rem,
			Spec:        spec,
		})
	}
	if len(states) == 0 {
		return nil
	}
	data, _ := ujson.Marshal(states)
	return data
}

// splitScopedKey 反解 scopedKey(scope, name) 为 scope 和 name。
func splitScopedKey(key string) (scope, name string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '\x00' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}

// ImportScope 从迁移 JSON 恢复定时器。对每个状态查 taskReg 拿 Task，找不到则跳过。
// 若 TimerState.Spec 非空则重建为 cron 任务，否则按 IntervalMs 重建为 interval 任务。
func (s *Scheduler) ImportScope(scope string, data []byte) {
	if len(data) == 0 {
		return
	}
	var states []TimerState
	if err := ujson.Unmarshal(data, &states); err != nil || len(states) == 0 {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	now := s.now()
	for _, st := range states {
		task, ok := s.taskReg[scopedKey(scope, st.Name)]
		if !ok {
			continue
		}
		// Cron 任务：有 Spec 则走 CronName 路径重建
		if st.Spec != "" {
			s.mu.Unlock()
			ct, err := s.CronName(st.Name, scope, st.Spec, task)
			s.mu.Lock()
			if err != nil {
				logger.Warnf("timer.ImportScope: failed to restore cron %q for scope %q: %v", st.Name, scope, err)
			}
			_ = ct
			continue
		}
		rem := time.Duration(st.RemainingMs) * time.Millisecond
		ivl := time.Duration(st.IntervalMs) * time.Millisecond
		s.seq++
		id := s.seq
		e := &entry{
			id:       id,
			name:     st.Name,
			scope:    scope,
			next:     now.Add(rem),
			interval: ivl,
			task:     task,
		}
		heap.Push(&s.heap, e)
		s.entries[id] = e
		key := conv.FormatInt(id)
		s.registerNamedLocked(st.Name, scope, key, func() { s.stop(id) })
		// regOwner 必须与重建后的新 id 同步：否则之后 StopNamed→stop(newID)→
		// unregisterRegLocked 会因 owner != id 直接返回，taskReg/cronSpec 里的
		// Task 闭包永久无法回收（正是 unregisterRegLocked 要消除的那类泄漏）。
		if st.Name != "" {
			s.regOwner[scopedKey(scope, st.Name)] = id
		}
	}
	s.mu.Unlock()
	s.signal()
}

// entriesByScope 返回 scope 下所有活跃 entry 切片（调用方需持锁）。
func (s *Scheduler) entriesByScope(scope string) []*entry {
	var list []*entry
	for _, e := range s.entries {
		if e.scope == scope {
			list = append(list, e)
		}
	}
	return list
}

// registerNamedLocked 同 registerNamed 但调用方已持 mu。
// 调用方（ImportScope）已持mu，故不能调用 oldStopper()（内部需mu → 死锁）。
// 替代方案：直接标记旧 entry 为 canceled，旧 stopper 将被 run 循环的 fire 跳过。
func (s *Scheduler) registerNamedLocked(name, scope, key string, stop func()) {
	if name != "" {
		// 标记同名旧 entry 为 canceled（调用方持 mu，不能调 old() 否则死锁）。
		if oldRef, ok := s.nameLoc[name]; ok {
			if oldID, err := strconv.ParseInt(oldRef.key, 10, 64); err == nil {
				if old, exists := s.entries[oldID]; exists {
					old.canceled = true
					delete(s.entries, oldID)
				}
			}
			// 清理旧 byScope 中的残留（与 dropScopeRefLocked 逻辑一致）。
			if m := s.byScope[oldRef.scope]; m != nil {
				delete(m, oldRef.key)
				if len(m) == 0 {
					delete(s.byScope, oldRef.scope)
				}
			}
		}
		s.byName[name] = stop
	}
	if scope != "" {
		m, ok := s.byScope[scope]
		if !ok {
			m = make(map[string]func())
			s.byScope[scope] = m
		}
		m[key] = stop
		if name != "" {
			s.nameLoc[name] = scopeRef{scope: scope, key: key}
		}
	}
}

// dropScopeRefLocked 摘除具名任务在 byScope 中的残留 stopper（调用方需持锁）。
func (s *Scheduler) dropScopeRefLocked(name string) {
	ref, ok := s.nameLoc[name]
	if !ok {
		return
	}
	delete(s.nameLoc, name)
	if m := s.byScope[ref.scope]; m != nil {
		delete(m, ref.key)
		if len(m) == 0 {
			delete(s.byScope, ref.scope)
		}
	}
}

// 持久化
// persistKeyFor 生成 scope 对应的持久化键。
func (s *Scheduler) persistKeyFor(scope string) string {
	return s.persistKey + scope
}

// PersistScope 将 scope 下所有具名定时器持久化到后端存储。
// 调用前须通过 WithPersistence 注入后端，否则返回 ErrNoBackend。
// 通常在 scope 生命周期结束前（如玩家下线、场景销毁）调用。
//
// 语义边界：保存的是「相对剩余时间」而非绝对到期时刻（见 DumpScope/TimerState），
// 且 RestoreScope 不会补触发停机期间已到点的任务。因此「到点必须发生」的事
// （行军到达、建造/征兵完成、挂机产出等）应把绝对 deadline 写进业务数据、
// 在加载时重建定时器，不要依赖本方法兜底。
func (s *Scheduler) PersistScope(ctx context.Context, scope string) error {
	if s.persist == nil {
		return ErrNoBackend
	}
	data := s.DumpScope(scope)
	if len(data) == 0 {
		// scope 下无定时器，清除可能存在的旧持久化数据
		return s.persist.Delete(ctx, s.persistKeyFor(scope))
	}
	return s.persist.Save(ctx, s.persistKeyFor(scope), data)
}

// RestoreScope 从后端存储恢复 scope 下的定时器。
// 调用前须通过 WithPersistence 注入后端，否则返回 ErrNoBackend。
// 重要：业务层必须在调用 RestoreScope 之前重新注册所有 Task（通过 AfterName/EveryName/CronName），
// 因为 ImportScope 依赖 taskReg 中已注册的 Task 来重建定时器。
// 典型流程：注册 Task → RestoreScope → 定时器恢复到上次持久化时的状态。
//
// 语义边界：只按持久化的「相对剩余时间」重建（ImportScope 用 now.Add(rem) 重新入堆），
// 停机期间流逝的时间不会被补算 —— 例如导出时剩余 5 分钟、停机 2 小时，恢复后仍是「再过 5 分钟」触发。
// 「到点必须精确发生」的事请以业务数据里的绝对 deadline 为准重建定时器。
func (s *Scheduler) RestoreScope(ctx context.Context, scope string) error {
	if s.persist == nil {
		return ErrNoBackend
	}
	data, err := s.persist.Load(ctx, s.persistKeyFor(scope))
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil // 无持久化数据，静默返回
	}
	s.ImportScope(scope, data)
	return nil
}

// ClearPersist 清除 scope 的持久化数据。在 scope 彻底销毁时调用。
func (s *Scheduler) ClearPersist(ctx context.Context, scope string) error {
	if s.persist == nil {
		return ErrNoBackend
	}
	return s.persist.Delete(ctx, s.persistKeyFor(scope))
}
