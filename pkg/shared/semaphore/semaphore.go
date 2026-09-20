// Package semaphore 提供带权重的信号量实现。
//
// 信号量用于控制对有限资源的并发访问。与标准库的 channel 或 sync.Mutex 不同，
// 本实现的信号量支持权重（weight），允许 Acquire 以不同权重获取资源，
// 适合限制"总内存用量""总连接数""总并发任务数"等加权场景。
//
// 典型用途：
//   - 限制并发数据库连接数
//   - 限制并发文件操作数
//   - 限制内存密集操作的总权重（如限制总图片处理并发内存）
//   - 任务队列的背压控制
//
// 设计要点：
//   - 持有量由计数器精确记账，而不是靠底层队列长度推断：
//     等待中的 Acquire 不占用任何资源，要么一次拿到全部权重、要么一个都不拿，
//     避免"先占一部分再等剩下的"把并发方互相拖死；
//   - Release 校验持有量：超额释放或未持有即释放返回错误且不扣减，
//     不会把记账扣成负数，也不会让释放方永久阻塞；
//   - TryAcquire 的"检查可用量"与"记账"在同一临界区内完成，不存在 TOCTOU 窗口；
//   - 支持 context 取消/超时，ctx 为 nil 时按永不取消处理；
//   - 纯标准库、零外部依赖。
package semaphore

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrWouldBlock 当 TryAcquire 无法立即获取资源时返回。
	ErrWouldBlock = errors.New("semaphore: would block")

	// ErrExceedsCapacity 当请求权重超过信号量总容量时返回。
	// 这类请求永远不可能被满足，等待只会把调用方挂死，因此立即失败。
	ErrExceedsCapacity = errors.New("semaphore: weight exceeds capacity")

	// ErrNotHeld 当释放的权重超过当前持有量时返回（含未持有即释放）。
	// 此时不扣减持有量，保证后续获取/释放的记账始终自洽。
	ErrNotHeld = errors.New("semaphore: release exceeds held weight")
)

// Semaphore 是一个加权信号量。并发安全。
//
// 零值不可用（容量为 0），请用 NewSemaphore 构造。
type Semaphore struct {
	initOnce sync.Once
	mu       sync.Mutex
	cond     *sync.Cond

	n    int64 // 总容量，构造后不变
	held int64 // 当前已被获取的权重，恒满足 0 <= held <= n
}

// NewSemaphore 创建一个容量为 n 的加权信号量。n <= 0 时按 1 处理。
func NewSemaphore(n int64) *Semaphore {
	if n <= 0 {
		n = 1
	}
	s := &Semaphore{n: n}
	s.init()
	return s
}

// init 惰性创建条件变量，使零值 Semaphore 被误用时返回错误而不是直接 panic。
func (s *Semaphore) init() {
	s.initOnce.Do(func() {
		s.cond = sync.NewCond(&s.mu)
	})
}

// Acquire 以权重 weight 获取信号量，weight <= 0 时按 1 处理。
// ctx 可设置超时或取消：ctx 已取消、或等待期间被取消时返回 ctx.Err()，且不获取任何资源。
// ctx 为 nil 时按 context.Background()（永不取消）处理。
// weight 超过 Capacity() 时立即返回 ErrExceedsCapacity。
func (s *Semaphore) Acquire(ctx context.Context, weight int64) error {
	if weight <= 0 {
		weight = 1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if weight > s.Capacity() {
		return ErrExceedsCapacity
	}
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	// context 取消无法打断 sync.Cond 的等待，用一个短生命周期协程负责唤醒。
	// done 关闭后该协程立即退出，不会随 Acquire 返回而泄漏。
	done := make(chan struct{})
	defer close(done)
	watching := false
	for s.n-s.held < weight {
		if !watching && ctx.Done() != nil {
			watching = true
			go func() {
				select {
				case <-ctx.Done():
					s.mu.Lock()
					s.cond.Broadcast()
					s.mu.Unlock()
				case <-done:
				}
			}()
		}
		// Wait 原子地释放 mu 并挂起；Broadcast 必须先拿到 mu，
		// 因此本协程进入等待与释放 mu 之间不存在唤醒丢失。
		s.cond.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.held += weight
	return nil
}

// Release 归还权重为 weight 的资源，weight <= 0 时按 1 处理。
// 若 weight 超过当前持有量（含未持有即释放），返回 ErrNotHeld 且不改变持有量：
// 超额释放会让记账失去意义，并可能让后续等待者永远等不到资源。
func (s *Semaphore) Release(weight int64) error {
	if weight <= 0 {
		weight = 1
	}
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()
	if weight > s.held {
		return ErrNotHeld
	}
	s.held -= weight
	// 唤醒所有等待者，由它们各自重新判断剩余量是否够用。
	s.cond.Broadcast()
	return nil
}

// TryAcquire 非阻塞尝试获取信号量，weight <= 0 时按 1 处理，成功返回 true。
// 可用量检查与记账在同一临界区内完成，并发下不会出现"看着够却拿不到"的假失败。
func (s *Semaphore) TryAcquire(weight int64) bool {
	if weight <= 0 {
		weight = 1
	}
	if weight > s.Capacity() {
		return false
	}
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tryLocked(weight)
}

// tryLocked 在已持有 s.mu 时尝试记账 weight 个权重，容量不足返回 false。
// 调用方必须保证 weight > 0 且已持有 s.mu。
func (s *Semaphore) tryLocked(weight int64) bool {
	if s.n-s.held < weight {
		return false
	}
	s.held += weight
	return true
}

// Available 返回当前可用的资源数。
func (s *Semaphore) Available() int64 {
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n - s.held
}

// Capacity 返回信号量总容量。
func (s *Semaphore) Capacity() int64 { return s.n }

// AcquireWithTimeout 以权重 weight 和超时时间获取信号量。
// 内部以 timeout 构造带超时的 context 后调用 Acquire。
func (s *Semaphore) AcquireWithTimeout(weight int64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Acquire(ctx, weight)
}
