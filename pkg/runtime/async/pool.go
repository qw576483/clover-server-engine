// 本文件提供异步任务执行器（Pool）：提交一段工作、拿到句柄、可等待/取消/按名查杀。
package async

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

var (
	// ErrPoolClosed 在 Pool 已 Stop 之后提交任务返回。
	ErrPoolClosed = errors.New("async: pool closed")
	// ErrPoolNotStarted 在 Pool 未 Start 时提交任务返回。
	ErrPoolNotStarted = errors.New("async: pool not started")
	// ErrDuplicateName 提交了与运行中任务同名的任务（且未开启 WithAllowDuplicate）。
	ErrDuplicateName = errors.New("async: duplicate task name")
	// ErrTimeout 仅作语义常量占位；任务超时时实际返回的是 ctx.Err()（context.DeadlineExceeded）。
	ErrTimeout = errors.New("async: task timeout")
)

// errOK 是 atomic.Value 的「成功」哨兵（atomic.Value 不能存 nil 接口）。
var errOK = struct{}{}

// TaskFunc 是异步任务的执行体。ctx 会在超时 / Kill / Pool 停止时被取消，
// 实现体应监听 ctx.Done() 以协作式中断。
type TaskFunc func(ctx context.Context) error

// Task 代表一个已提交的任务句柄，可等待结果或主动取消。
type Task struct {
	// Name 任务名（与 Submit 时一致；空名任务无法按名 Kill）。
	Name string

	cancel   context.CancelFunc
	ctx      context.Context // 任务的执行 context（供 runItem 检查取消）
	err      atomic.Value    // 存 error（nil 表示成功或无错误）
	done     chan struct{}
	finalize sync.Once // 保证 setErr+close(done) 只发生一次（防 worker 与 Stop/Submit 竞态 double-close）
}

// finish 幂等地写入结果并关闭 done：无论被 worker、Stop 排空还是 Submit 孤儿回收调用，
// 都只会真正执行一次，杜绝 double close(done) panic 与「任务入了已停摆的池导致 Wait 永久阻塞」。
func (t *Task) finish(e error) {
	t.finalize.Do(func() {
		t.setErr(e)
		close(t.done)
	})
}

// ctxFor 返回任务执行 context。
func (t *Task) ctxFor() context.Context { return t.ctx }

// ctxErr 返回任务 context 的当前错误（nil 表示未取消）。
func (t *Task) ctxErr() error { return t.ctx.Err() }

// ctxDone 返回任务 context 的取消信号 channel。
func (t *Task) ctxDone() <-chan struct{} { return t.ctx.Done() }

// Wait 阻塞直到任务结束，返回其最终 error（nil 表示成功）。
func (t *Task) Wait() error {
	<-t.done
	return t.loadErr()
}

// Err 非阻塞返回任务当前结果：未结束返回 nil，已结束返回成功(nil)或失败(error)。
func (t *Task) Err() error {
	select {
	case <-t.done:
		return t.loadErr()
	default:
		return nil
	}
}

// loadErr 把 atomic.Value 中存的成功哨兵还原为 nil，error 原样返回。
func (t *Task) loadErr() error {
	v := t.err.Load()
	if v == nil {
		return nil
	}
	if _, ok := v.(struct{}); ok {
		return nil
	}
	return v.(error)
}

// Cancel 主动取消该任务（等效于 Kill(Name)）。协作式：仅取消 ctx，任务函数需自行退出。
func (t *Task) Cancel() {
	if t.cancel != nil {
		t.cancel()
	}
}

// Done 返回一个在任务结束时关闭的 channel，便于 select。
func (t *Task) Done() <-chan struct{} {
	return t.done
}

func (t *Task) setErr(e error) {
	if e == nil {
		t.err.Store(errOK)
		return
	}
	t.err.Store(e)
}

type taskItem struct {
	name    string
	fn      TaskFunc
	parent  context.Context
	timeout time.Duration
	retries int
	backoff time.Duration

	task   *Task
	cancel context.CancelFunc
}

// TaskOption 定制单条任务的执行参数。
type TaskOption func(*taskItem)

// WithTimeout 设置任务整体超时（从开始执行计时）。超时触发后 ctx 被取消。
func WithTimeout(d time.Duration) TaskOption {
	return func(it *taskItem) { it.timeout = d }
}

// WithRetry 设置失败重试次数与每次重试前的退避间隔（退避期间若 ctx 被取消则中止重试）。
func WithRetry(n int, backoff time.Duration) TaskOption {
	return func(it *taskItem) { it.retries = n; it.backoff = backoff }
}

// WithParent 把任务绑定到一个父 context：父 context 结束时本任务一并取消（如一次请求的生命周期）。
func WithParent(ctx context.Context) TaskOption {
	return func(it *taskItem) { it.parent = ctx }
}

// // 工作池
// // Pool 一个固定 worker 数的异步任务执行器。

// 典型用法：

// p := async.New(async.WithWorkers(8))
// p.Start()
// t, _ := p.Submit("sync-1", func(ctx context.Context) error { ... }, async.WithTimeout(5*time.Second))
// go func() { if err := t.Wait(); err != nil { ... } }()
// // ... 需要时 p.Kill("sync-1") 或 p.Stop()
type Pool struct {
	name      string
	workers   int
	queueSize int
	allowDup  bool

	mu       sync.RWMutex
	started  bool
	closed   bool
	activeMu sync.Mutex // 仅保护 active（与 mu 分开，避免持 mu 入队时与 worker 的 unregister 抢锁）
	tasks    chan *taskItem
	active   map[string]*taskItem // name -> 运行中任务（用于 Kill）
	stopCh   chan struct{}

	poolCtx    context.Context
	poolCancel context.CancelFunc

	wg sync.WaitGroup
}

// Option 定制 Pool。
type Option func(*Pool)

// WithWorkers 设置 worker 数（并发执行上限），至少 1。
func WithWorkers(n int) Option {
	return func(p *Pool) { p.workers = n }
}

// WithQueueSize 设置等待队列容量（背压上限）。0 表示无缓冲（Submit 在队满时阻塞直到被取走）。
func WithQueueSize(n int) Option {
	return func(p *Pool) { p.queueSize = n }
}

// WithName 给 Pool 一个名字（仅用于日志/自省）。
func WithName(name string) Option {
	return func(p *Pool) { p.name = name }
}

// WithAllowDuplicate 允许提交同名任务（默认同名会被 ErrDuplicateName 拒绝）。
func WithAllowDuplicate() Option {
	return func(p *Pool) { p.allowDup = true }
}

// New 构造一个未启动的 Pool。默认 4 worker、128 队列。
func New(opts ...Option) *Pool {
	p := &Pool{workers: 4, queueSize: 128, poolCtx: context.Background()}
	for _, o := range opts {
		o(p)
	}
	if p.workers < 1 {
		p.workers = 1
	}
	if p.queueSize < 0 {
		p.queueSize = 0
	}
	return p
}

// Start 启动 worker 并开始消费任务。可重复调用（幂等）。
// 未 Start 时 Submit 返回 ErrPoolNotStarted。
func (p *Pool) Start() error {
	p.mu.Lock()
	if p.started && !p.closed {
		p.mu.Unlock()
		return nil
	}
	p.started = true
	// 允许 Stop 之后重启：重建全部通道/ctx/在册表并复位 closed。
	p.closed = false
	p.poolCtx, p.poolCancel = context.WithCancel(context.Background())
	p.tasks = make(chan *taskItem, p.queueSize)
	p.stopCh = make(chan struct{})
	p.active = make(map[string]*taskItem)
	p.mu.Unlock()

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.loop()
	}
	return nil
}

// Submit 提交一个命名任务。返回的 *Task 可用于 Wait/Cancel/Err。
// 未 Start → ErrPoolNotStarted；已 Stop → ErrPoolClosed；同名且未开 WithAllowDuplicate → ErrDuplicateName。

// 并发安全要点：判活（started/closed）与「入队」必须在同一把写锁（p.mu）下原子完成，
// 否则会出现「先通过 closed 检查，Stop 随后关池并排空，但本次仍把任务塞进已停摆的池」
// 导致 done 永不关闭、Wait 永久阻塞。这里持 p.mu 直到入队完成，Stop 无法在中间 set closed。
func (p *Pool) Submit(name string, fn TaskFunc, opts ...TaskOption) (*Task, error) {
	it := &taskItem{name: name, fn: fn}
	for _, o := range opts {
		o(it)
	}

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil, ErrPoolNotStarted
	}
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	poolCtx := p.poolCtx
	// tasks/stopCh 由 Start 在 p.mu 下赋值，这里必须在同一把锁内快照到局部变量。
	// 释放锁后再读 p.tasks/p.stopCh 属于对同一变量的无锁读写并发（data race），
	// 且与 Start 并发时可能读到新旧混用的 channel。
	tasks := p.tasks
	stopCh := p.stopCh
	p.mu.Unlock()

	ctx, cancel := newTaskContext(poolCtx, it.parent, it.timeout)
	task := &Task{Name: name, done: make(chan struct{}), cancel: cancel, ctx: ctx}
	it.task = task
	it.cancel = cancel

	if name != "" {
		p.activeMu.Lock()
		if _, dup := p.active[name]; dup && !p.allowDup {
			p.activeMu.Unlock()
			cancel()
			return nil, ErrDuplicateName
		}
		p.active[name] = it
		p.activeMu.Unlock()
	}

	select {
	case tasks <- it:
		return p.reclaimIfClosed(it, task, cancel)
	default:
		// tasks channel 满或 stopCh 已关闭：检查 close 状态
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			cancel()
			p.unregister(it)
			return nil, ErrPoolClosed
		}
		// channel 满但未关闭，阻塞等待
		select {
		case tasks <- it:
		case <-stopCh:
			cancel()
			p.unregister(it)
			return nil, ErrPoolClosed
		}
		return p.reclaimIfClosed(it, task, cancel)
	}
}

// reclaimIfClosed 处理「入队成功之后」与 Stop 的竞态。
// 若入队恰好发生在 Stop 关池并排空 tasks 之后，该任务会成为无人消费的孤儿，Wait 将永久阻塞。
// 这里在入队后再检查一次 closed；若已关闭，则幂等地 finish(ErrPoolClosed) 收尾（若 worker 已取走
// 会由 worker 先 finish，Task.finalize.Once 保证不会 double close），并返回 ErrPoolClosed。
func (p *Pool) reclaimIfClosed(it *taskItem, task *Task, cancel context.CancelFunc) (*Task, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		task.finish(context.Canceled)
		cancel()
		p.unregister(it)
		return nil, ErrPoolClosed
	}
	return task, nil
}

// Kill 按名取消一个正在运行的任务。返回是否找到了该任务（协作式取消，函数需自行退出）。
func (p *Pool) Kill(name string) bool {
	p.activeMu.Lock()
	it, ok := p.active[name]
	p.activeMu.Unlock()
	if !ok {
		return false
	}
	if it.cancel != nil {
		it.cancel()
	}
	return true
}

// Running 返回当前在册（运行中）的任务名快照（用于自省）。
func (p *Pool) Running() []string {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	out := make([]string, 0, len(p.active))
	for n := range p.active {
		out = append(out, n)
	}
	return out
}

// Stop 停止：拒绝新提交、取消仍在运行的任务(ctx)、等待在途任务结束，
// 并把未被 worker 取走的排队任务标记为已取消。可重复调用（幂等）。

// 注意：只有 Start 之后 stopCh/poolCancel/tasks 才被初始化；若 Start 从未调用，
// 直接 Stop 不应 panic。
func (p *Pool) Stop() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	started := p.started
	p.mu.Unlock()

	if !started {
		return nil
	}

	if p.poolCancel != nil {
		p.poolCancel() // 中断在途任务
	}
	close(p.stopCh)

	// 必须「先排空、再 wg.Wait」：worker 收到 stopCh 后立刻退出，不会再消费 tasks。
	// 若把排空放在 wg.Wait 之后，一个正阻塞在 `tasks <- it` 上的 Submit 在队列满时
	// 需要有人接收才能返回，而此刻已无消费者，wg.Wait 之前的窗口里它只能靠 stopCh 解除，
	// 但队列中残留任务的 done 无人关闭，等待它们的 Wait 会永久阻塞。
	// 这里在 wg.Wait 前先排空一轮，Wait 之后再排空至稳定，覆盖 worker 退出瞬间回灌的任务。
	p.drainTasks()
	p.wg.Wait()
	p.drainTasks()
	return nil
}

// drainTasks 非阻塞排空 tasks 队列，把未被 worker 取走的任务标记为已取消。
// 用幂等 finish 避免与 Submit 的孤儿回收（reclaimIfClosed）double close。
func (p *Pool) drainTasks() {
	for {
		select {
		case it := <-p.tasks:
			it.task.finish(context.Canceled)
			p.unregister(it)
		default:
			return
		}
	}
}

func (p *Pool) loop() {
	defer p.wg.Done()
	for {
		// select 在多个 case 同时就绪时是随机选取的，Stop 之后若队列非空，
		// worker 仍有一半概率取到 tasks 并启动新任务。先做一次非阻塞的 stopCh 探测，
		// 保证「已 Stop」优先于「队列还有活」，停机后不再启动新任务。
		select {
		case <-p.stopCh:
			return
		default:
		}
		select {
		case <-p.stopCh:
			return
		case it := <-p.tasks:
			p.runItem(it)
		}
	}
}

func (p *Pool) runItem(it *taskItem) {
	defer it.cancel() // 释放 timer 资源
	defer p.unregister(it)

	// panic recover：任务 panic 不应杀死 worker goroutine。
	// 必须同时留下日志与堆栈：只把 %v 塞进 finish 的 error 时，调用方若不打印，
	// 这次 panic 就彻底静默（无行号、无堆栈，线上无法定位）。
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("async: task %q panic: %v\n%s", it.name, r, debug.Stack())
			it.task.finish(fmt.Errorf("async: task %q panic: %v", it.name, r))
		}
	}()

	// 首次执行前也要检查 ctx：Submit 后立即 Kill / 父 ctx 已取消 / 池已停，
	if err := it.task.ctxErr(); err != nil {
		it.task.finish(err)
		return
	}

	var lastErr error
	attempts := it.retries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 && it.backoff > 0 {
			t := time.NewTimer(it.backoff)
			select {
			case <-it.task.ctxDone():
				t.Stop()
				it.task.finish(it.task.ctxErr())
				return
			case <-t.C:
			}
		}
		err := it.fn(it.task.ctxFor())
		if err == nil {
			it.task.finish(nil)
			return
		}
		lastErr = err
		// ctx 已取消（超时/Kill/停止）则不再重试。
		if it.task.ctxErr() != nil {
			break
		}
	}

	final := lastErr
	if final == nil {
		if e := it.task.ctxErr(); e != nil {
			final = e
		}
	}
	it.task.finish(final)
}

// unregister 任务结束后从 active 移除（仅当仍是同一实例）。
// 使用独立的 activeMu，避免与 Submit 持 p.mu 入队时抢锁导致死锁。
func (p *Pool) unregister(it *taskItem) {
	if it.name == "" {
		return
	}
	p.activeMu.Lock()
	if cur, ok := p.active[it.name]; ok && cur == it {
		delete(p.active, it.name)
	}
	p.activeMu.Unlock()
}

// newTaskContext 在 poolCtx 之上叠加父 context 取消与可选超时，返回 (ctx, cancel)。
func newTaskContext(poolCtx, parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(poolCtx)
	if timeout > 0 {
		// 超时层必须派生自上一步的 ctx，而非重新从 poolCtx 派生：否则 WithCancel 返回的 cancel 会被
		// 覆盖丢弃，其 context 无法主动取消、只能等 poolCtx 结束才回收，每个带超时的任务都泄漏一个 context。
		// 这里组合两个 cancel，确保 WithCancel 与 WithTimeout 两层都能被及时释放。
		var tcancel context.CancelFunc
		ctx, tcancel = context.WithTimeout(ctx, timeout)
		oc := cancel
		cancel = func() {
			tcancel()
			oc()
		}
	}
	if parent != nil {
		// 父 context 结束时自动取消本任务（Go 1.21+）。
		// AfterFunc 返回的 stop 必须保存并在 cancel 时调用：否则每个任务都会在 parent 上
		// 挂一个永不摘除的回调，长生命周期 parent（如全局 ctx）提交大量短任务时内存持续增长。
		stop := context.AfterFunc(parent, func() { cancel() })
		oc := cancel
		cancel = func() {
			stop()
			oc()
		}
	}
	return ctx, cancel
}
