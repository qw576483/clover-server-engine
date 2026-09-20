// Package pool 通用对象池原语。
//
// 用 Go 泛型 + sync.Pool + 互斥锁表达，并发安全、零外部依赖、纯标准库可单测。
//
// 提供两种策略：
//   - Pool[T] 无界对象池：基于 sync.Pool，跨 GC 自动回收，适合高频临时对象（消息/封包/临时结构体）。
//   - FreeList[T] 有界空闲链表池：固定容量，Put 满则丢弃，适合需要硬上限的复用场景。
//
// 与 async（后台任务 offload）、net（连接/缓冲区复用）天然互补，是降低 GC 压力、提升吞吐的基础设施。
//
// 业务侧典型用法：
//
//	import "clover-server-engine/pkg/runtime/pool"
//
//	p := pool.New(func() *Packet { return &Packet{} }, pool.WithReset(func(p *Packet){ p.Reset() }))
//	buf := p.Get()
//	defer p.Put(buf)
//
//	fl := pool.NewFreeList(1024, func() []byte { return make([]byte, 0, 1024) }) // 有界缓冲区池
//	if b, ok := fl.Get(); ok { defer fl.Put(b) }
package pool

import "sync"

// config 共享选项。
type config[T any] struct {
	reset func(T)
}

// Option 配置池（Pool 与 FreeList 共用）。
type Option[T any] func(*config[T])

// WithReset 归还对象时调用 reset 清空状态，下次 Get 拿到的是干净对象。
func WithReset[T any](fn func(T)) Option[T] {
	return func(c *config[T]) { c.reset = fn }
}

// Pool 无界对象池（基于 sync.Pool）。
type Pool[T any] struct {
	pool  sync.Pool
	reset func(T)
	newFn func() T // 保留以便 Get 断言失败时兜底新建
}

// New 新建无界对象池。newFn 在池空时创建新对象（不可为 nil）。
func New[T any](newFn func() T, opts ...Option[T]) *Pool[T] {
	if newFn == nil {
		panic("pool: newFn required")
	}
	var c config[T]
	for _, o := range opts {
		o(&c)
	}
	p := &Pool[T]{reset: c.reset, newFn: newFn}
	p.pool.New = func() any { return newFn() }
	return p
}

// Get 取一个对象（池空则经 newFn 新建）。
// 用 comma-ok 断言防御 panic——当 T 为接口类型且 newFn 返回 nil 接口值时，
// sync.Pool.Get() 拿到的 any 装的是 nil，裸断言 x.(T) 会 panic；此处失败即回退 newFn 兜底。
func (p *Pool[T]) Get() T {
	if v, ok := p.pool.Get().(T); ok {
		return v
	}
	return p.newFn()
}

// Put 归还对象（先 reset 再入池；reset 为 nil 则直接入池）。
func (p *Pool[T]) Put(x T) {
	if p.reset != nil {
		p.reset(x)
	}
	p.pool.Put(x)
}

// FreeList 有界空闲链表池：固定容量，Put 满则丢弃（返回 false）。
type FreeList[T any] struct {
	mu    sync.Mutex
	items []T
	cap   int
	newFn func() T
	reset func(T)
}

// NewFreeList 新建容量 capacity 的有界空闲链表池。newFn 在池空且 Get 时创建新对象（可为 nil，
// 此时空池 Get 返回 (零值, false)）。
func NewFreeList[T any](capacity int, newFn func() T, opts ...Option[T]) *FreeList[T] {
	if capacity < 0 {
		capacity = 0
	}
	var c config[T]
	for _, o := range opts {
		o(&c)
	}
	return &FreeList[T]{
		items: make([]T, 0, capacity),
		cap:   capacity,
		newFn: newFn,
		reset: c.reset,
	}
}

// Get 取一个对象：优先复用空闲链表；空链则经 newFn 新建（newFn 为 nil 且空链返回 (零值,false)）。
func (f *FreeList[T]) Get() (T, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := len(f.items); n > 0 {
		x := f.items[n-1]
		f.items = f.items[:n-1]
		return x, true
	}
	if f.newFn == nil {
		var zero T
		return zero, false
	}
	return f.newFn(), true
}

// Put 归还对象：未满则入池（return true），已满则丢弃（return false）。
func (f *FreeList[T]) Put(x T) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.items) >= f.cap {
		return false
	}
	if f.reset != nil {
		f.reset(x)
	}
	f.items = append(f.items, x)
	return true
}

// Len 当前空闲对象数。
func (f *FreeList[T]) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}

// Cap 容量上限。
func (f *FreeList[T]) Cap() int { return f.cap }
