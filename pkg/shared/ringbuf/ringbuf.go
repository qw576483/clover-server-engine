// Package ringbuf 通用环形缓冲。
//
// 设计要点：一段连续的环形缓冲区，在「一边读一边写」的情况下可不加锁；多个同时读写则需外部加锁。
//   - 用 Go 泛型表达「环形缓冲任意元素类型」；
//   - Ring[T] 是纯 SPSC 无锁环形缓冲（恰好一个 Push 协程 + 恰好一个 Pop 协程），
//     提供 Go 原生 chan 没有的「零锁、零分配、可随时观测 Len/Full」热路径；
//   - MpmcRing[T] 是多生产者多消费者变体（用互斥锁包裹同一套环形逻辑），调用方无需自己加锁；
//   - 两者都是非阻塞 API（Push/Pop/Empty/Full/Len），与 MsgQueue 的非阻塞语义一致；
//     阻塞等待由消费方在自己的 tick/select 里决定（actor 风格）。
//
// 纯标准库、零外部依赖、可纯内存单测。
package ringbuf

import (
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"

	"clover-server-engine/pkg/shared/util"
)

// ErrCapacityOverflow 当请求容量超出可表示的 2 的幂上限时返回。
var ErrCapacityOverflow = errors.New("ringbuf: capacity exceeds maximum power of two")

// maxCapacity 是 int 能表示的最大 2 的幂（64 位平台为 1<<62）。
// 超过它的容量无法向上取整：位移会溢出成负数，只剩"退化成容量 1"和"报错"两种选择。
const maxCapacity = 1 << (bits.UintSize - 2)

// nextPow2 返回 >= n 的最小 2 的幂（n<=1 时返回 1，n 超过上限时夹紧到上限）。
func nextPow2(n int) int {
	if n > maxCapacity {
		return maxCapacity
	}
	return util.NextPow2(n)
}

// checkPow2 返回 n 向上取整到 2 的幂后的容量；n 超过上限时返回 ErrCapacityOverflow。
func checkPow2(n int) (int, error) {
	if n > maxCapacity {
		return 0, ErrCapacityOverflow
	}
	return util.NextPow2(n), nil
}

// Ring 是单生产者单消费者（SPSC）无锁有界环形缓冲。
//
// 恰好一个 goroutine 调用 Push、恰好一个 goroutine 调用 Pop 时是安全的；
// 其他用法需外部同步（多生产者/消费者请用 MpmcRing）。
// 设计源于 MMO 引擎 MsgQueue 的「一边读一边写可不加锁」环形字节区。
type Ring[T any] struct {
	buf  []T
	mask uint64
	w    atomic.Uint64 // 下一个写入位置（单调递增，非回绕）
	r    atomic.Uint64 // 下一个读取位置（单调递增，非回绕）
}

// New 创建容量为 capacity 向上取整到 2 的幂的 SPSC 环形缓冲（最小 1）。
// 超过可表示的 2 的幂上限时 capacity 会被夹紧到该上限，不会因取整溢出而退化成容量 1；
// 需要在启动期暴露非法配置的调用方请用 NewChecked。
func New[T any](capacity int) *Ring[T] {
	cap := nextPow2(capacity)
	if cap < 1 {
		cap = 1
	}
	return &Ring[T]{
		buf:  make([]T, cap),
		mask: uint64(cap - 1), // #nosec G115 -- cap 已由 nextPow2 保证 >=1。
	}
}

// NewChecked 与 New 一致，但 capacity 超出可表示的 2 的幂上限时返回 ErrCapacityOverflow，
// 而不是夹紧。适合校验来自配置的容量。
func NewChecked[T any](capacity int) (*Ring[T], error) {
	cap, err := checkPow2(capacity)
	if err != nil {
		return nil, err
	}
	if cap < 1 {
		cap = 1
	}
	return &Ring[T]{
		buf:  make([]T, cap),
		mask: uint64(cap - 1), // #nosec G115 -- cap 已由 checkPow2 保证 >=1。
	}, nil
}

// Cap 返回实际（2 的幂）容量。
func (r *Ring[T]) Cap() int { return len(r.buf) }

// Push 追加 v；满则返回 false（调用方可丢弃或稍后重试）。
// 使用 uint64 单调计数器 + 位掩码索引，利用无符号回绕语义正确处理计数器溢出。
// SPSC 内存序：先 Load consumer 的 r（对方控制），再 Load producer 的 w，
// 保证空间计算偏向保守，避免双 Load 之间的竞态。
func (r *Ring[T]) Push(v T) bool {
	rval := r.r.Load() // 先读消费者位置（对方控制）
	w := r.w.Load()    // 再读自身位置
	// 无符号减法在 overflow 时回绕，给出正确的环形距离。
	if w-rval >= uint64(len(r.buf)) {
		return false
	}
	// #nosec G115 -- w&mask 被 mask=cap-1 限制，值在 int 范围内。
	r.buf[int(w&r.mask)] = v
	r.w.Store(w + 1)
	return true
}

// Pop 取出最旧元素；空时返回 (零值, false)。
// 使用无符号减法处理计数器溢出（同 Push 语义）。
// SPSC 内存序：先 Load producer 的 w（对方控制），再 Load consumer 的 r，
// 与 Push 对称，保证空间计算偏向保守。
func (r *Ring[T]) Pop() (T, bool) {
	var zero T
	w := r.w.Load()   // 先读生产者位置（对方控制）
	pos := r.r.Load() // 再读自身位置
	if pos == w {
		return zero, false
	}
	// #nosec G115 -- pos&mask 被 mask=cap-1 限制，值在 int 范围内。
	v := r.buf[int(pos&r.mask)]
	r.r.Store(pos + 1)
	return v, true
}

// Len 返回当前缓冲元素个数。
func (r *Ring[T]) Len() int {
	diff := r.w.Load() - r.r.Load()
	// #nosec G115 -- diff 表示当前缓冲元素数，被 len(r.buf) 限制，在 int 范围内。
	n := int(diff)
	switch {
	case n < 0:
		return 0
	case n > len(r.buf):
		return len(r.buf)
	default:
		return n
	}
}

// Empty 报告是否无缓冲元素。使用等于判断，正确处理计数器溢出。
func (r *Ring[T]) Empty() bool { return r.r.Load() == r.w.Load() }

// Full 报告是否已满（需 Pop 后才能继续 Push）。
func (r *Ring[T]) Full() bool { return r.w.Load()-r.r.Load() >= uint64(len(r.buf)) }

// TryPush 是 Push 的别名（非阻塞）。
func (r *Ring[T]) TryPush(v T) bool { return r.Push(v) }

// TryPop 是 Pop 的别名（非阻塞）。
func (r *Ring[T]) TryPop() (T, bool) { return r.Pop() }

// Drain 非阻塞地取出并返回所有缓冲元素（最旧在前）。
func (r *Ring[T]) Drain() []T {
	out := make([]T, 0, r.Len())
	for {
		v, ok := r.Pop()
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

// MpmcRing 是多生产者多消费者（MPMC）有界环形缓冲，并发安全。
//
// 任意数量的 goroutine 都可并发调用 Push/Pop。内部用互斥锁保护
// （参考 MsgQueue「多个同时读写需要加锁」的说明）；若只需单生产者单消费者热路径，
// 请用无锁的 Ring[T]。
type MpmcRing[T any] struct {
	mu   sync.Mutex
	buf  []T
	mask uint64
	w, r uint64 // 单调递增游标（无符号，借回绕语义正确处理计数器溢出，与 SPSC 的 Ring[T] 一致）
}

// NewMpmc 创建容量为 capacity 向上取整到 2 的幂的 MPMC 环形缓冲（最小 1）。
// 超过可表示的 2 的幂上限时 capacity 会被夹紧到该上限，不会因取整溢出而退化成容量 1；
// 需要在启动期暴露非法配置的调用方请用 NewMpmcChecked。
func NewMpmc[T any](capacity int) *MpmcRing[T] {
	cap := nextPow2(capacity)
	if cap < 1 {
		cap = 1
	}
	return &MpmcRing[T]{
		buf:  make([]T, cap),
		mask: uint64(cap - 1), // #nosec G115 -- cap 已由 nextPow2 保证 >=1。
	}
}

// NewMpmcChecked 与 NewMpmc 一致，但 capacity 超出可表示的 2 的幂上限时返回
// ErrCapacityOverflow，而不是夹紧。适合校验来自配置的容量。
func NewMpmcChecked[T any](capacity int) (*MpmcRing[T], error) {
	cap, err := checkPow2(capacity)
	if err != nil {
		return nil, err
	}
	if cap < 1 {
		cap = 1
	}
	return &MpmcRing[T]{
		buf:  make([]T, cap),
		mask: uint64(cap - 1), // #nosec G115 -- cap 已由 checkPow2 保证 >=1。
	}, nil
}

// Cap 返回实际（2 的幂）容量。
func (r *MpmcRing[T]) Cap() int { return len(r.buf) }

// Push 追加 v；满则返回 false。并发安全。
// 使用 uint64 单调计数器 + 位掩码索引，借无符号回绕语义正确处理计数器溢出
// （与 SPSC 的 Ring[T].Push 一致）。
func (r *MpmcRing[T]) Push(v T) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	// 无符号减法在 overflow 时回绕，给出正确的环形距离。
	if r.w-r.r >= uint64(len(r.buf)) {
		return false
	}
	// #nosec G115 -- r.mask=cap-1，cap 是合理的 2 的幂，值在 int 范围内。
	idx := r.w & r.mask
	r.buf[idx] = v
	r.w++
	return true
}

// Pop 取出最旧元素；空时返回 (零值, false)。并发安全。
func (r *MpmcRing[T]) Pop() (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var zero T
	if r.r == r.w {
		return zero, false
	}
	// #nosec G115 -- r.mask=cap-1，cap 是合理的 2 的幂，值在 int 范围内。
	idx := r.r & r.mask
	v := r.buf[idx]
	r.r++
	return v, true
}

// Len 返回当前缓冲元素个数。
func (r *MpmcRing[T]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	diff := r.w - r.r
	// #nosec G115 -- diff 表示当前缓冲元素数，被 len(r.buf) 限制，在 int 范围内。
	n := int(diff)
	switch {
	case n < 0:
		return 0
	case n > len(r.buf):
		return len(r.buf)
	default:
		return n
	}
}

// Empty 报告是否无缓冲元素。使用等于判断，正确处理计数器溢出。
func (r *MpmcRing[T]) Empty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.r == r.w
}

// Full 报告是否已满。
func (r *MpmcRing[T]) Full() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.w-r.r >= uint64(len(r.buf))
}

// TryPush 是 Push 的别名（非阻塞）。
func (r *MpmcRing[T]) TryPush(v T) bool { return r.Push(v) }

// TryPop 是 Pop 的别名（非阻塞）。
func (r *MpmcRing[T]) TryPop() (T, bool) { return r.Pop() }

// Drain 非阻塞地取出并返回所有缓冲元素（最旧在前）。
func (r *MpmcRing[T]) Drain() []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]T, 0, r.w-r.r)
	for r.r < r.w {
		// #nosec G115 -- r.mask=cap-1，cap 是合理的 2 的幂，值在 int 范围内。
		idx := r.r & r.mask
		out = append(out, r.buf[idx])
		r.r++
	}
	return out
}
