package globalstore

import (
	"context"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	ilog "github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
	"go.uber.org/zap"
)

// memSweepInterval 后台过期键清理周期。
const memSweepInterval = time.Minute

// MemBackend 进程内 KV 后端，用于开发、单测与无 etcd 环境。
// 支持 TTL 过期、CAS、Incr、Watch（按前缀订阅变更）。并发安全。
type MemBackend struct {
	mu       sync.RWMutex
	m        map[string]*memEntry
	watchers []*memWatcher
	closed   bool
	// done 在 Close 时关闭：用来唤醒仍在等 ctx 取消的 watch goroutine
	//（否则它们要等到调用方自己 cancel ctx 才退出，Close 等于没通知到），
	// 同时用于停止后台过期键清理协程。
	done chan struct{}
	// sweepWg 追踪后台清理协程，Close 时等它退出。
	sweepWg sync.WaitGroup
}

type memEntry struct {
	value  string
	expire time.Time // 零值表示不过期
}

type memWatcher struct {
	prefix string
	cb     func(Event)
}

// NewMemBackend 构造空的内存后端（含后台过期键清理协程）。
func NewMemBackend() *MemBackend {
	b := &MemBackend{m: make(map[string]*memEntry), done: make(chan struct{})}
	b.sweepWg.Add(1)
	go b.sweepLoop()
	return b
}

// sweepLoop 周期性清除已过期的键。
// 只靠 Get/Exists/CAS 的惰性清理时，「写入带 TTL 后再也不被读取」的键会永久驻留
// ——bootstrap 以 NewMemStore 装配场景路由即走本后端。
func (b *MemBackend) sweepLoop() {
	defer b.sweepWg.Done()
	t := time.NewTicker(memSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-b.done:
			return
		case <-t.C:
			b.sweepExpired()
		}
	}
}

// sweepExpired 清除当前全部已过期键。
func (b *MemBackend) sweepExpired() {
	b.mu.Lock()
	for k, e := range b.m {
		if e == nil || b.expired(e) {
			delete(b.m, k)
		}
	}
	b.mu.Unlock()
}

// NewMemStore 构造以内存为后端的带前缀 Store（开发/测试最常用）。
func NewMemStore(prefix string) *Store {
	return NewStore(NewMemBackend(), prefix)
}

func (b *MemBackend) expired(e *memEntry) bool {
	return !e.expire.IsZero() && time.Now().After(e.expire)
}

// Get 读取键；过期视为不存在；不存在返回 ErrKeyNotFound。
func (b *MemBackend) Get(ctx context.Context, key string) (string, error) {
	b.mu.RLock()
	e, ok := b.m[key]
	if ok && b.expired(e) {
		ok = false
		e = nil
	}
	if !ok {
		b.mu.RUnlock()
		b.deleteExpired(key)
		return "", ErrKeyNotFound
	}
	// value 必须在持锁内读出：set（CAS/Incr 路径）在写锁内原地改写 e.value，
	// 锁外读同一字段是 data race（string 头可能撕裂）。
	v := e.value
	b.mu.RUnlock()
	return v, nil
}

// deleteExpired 在锁内确认并清理已过期的键，避免过期项在 map 中长期堆积造成内存泄漏。
func (b *MemBackend) deleteExpired(key string) {
	b.mu.Lock()
	if e, ok := b.m[key]; ok && b.expired(e) {
		delete(b.m, key)
	}
	b.mu.Unlock()
}

// set 写入键值并沿用该键原有的过期时刻：CAS / Incr 这类「改值」操作不应延长键的寿命，
// 否则一次自增就把带 TTL 的键变成永不过期。
// 键不存在（或已因过期被清理）时新条目不带过期时间——没有原 TTL 可继承，
// 与 etcd 后端「新键不带租约」的语义保持一致。调用方须持有写锁。
func (b *MemBackend) set(key, value string) {
	if e, ok := b.m[key]; ok && !b.expired(e) {
		e.value = value
		return
	}
	b.m[key] = &memEntry{value: value}
}

// Put 写入键；ttl>0 设置过期时间；触发 Watch 事件。
func (b *MemBackend) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	exp := time.Time{}
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	b.m[key] = &memEntry{value: value, expire: exp}
	b.mu.Unlock()
	b.fire(Event{Type: EventPut, Key: key, Value: value})
	return nil
}

// Delete 删除键；不存在视为成功；触发 Watch 事件（若存在）。
func (b *MemBackend) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if _, ok := b.m[key]; !ok {
		b.mu.Unlock()
		return nil
	}
	delete(b.m, key)
	b.mu.Unlock()
	b.fire(Event{Type: EventDelete, Key: key})
	return nil
}

// Exists 判断键是否存在（过期视为不存在）。
func (b *MemBackend) Exists(ctx context.Context, key string) (bool, error) {
	b.mu.RLock()
	e, ok := b.m[key]
	if ok && b.expired(e) {
		ok = false
	}
	b.mu.RUnlock()
	if !ok {
		b.deleteExpired(key)
	}
	return ok, nil
}

// CAS 比较并交换：old 为期望当前值；old 为空串表示「仅当 key 不存在时设置」。
func (b *MemBackend) CAS(ctx context.Context, key, old, new string) (bool, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return false, ErrClosed
	}
	e, ok := b.m[key]
	if !ok || b.expired(e) {
		if ok && b.expired(e) {
			delete(b.m, key) // 清理过期项，避免泄漏
		}
		if old != "" {
			b.mu.Unlock()
			return false, nil
		}
	} else if e.value != old {
		b.mu.Unlock()
		return false, nil
	}
	b.set(key, new)
	b.mu.Unlock()
	b.fire(Event{Type: EventPut, Key: key, Value: new})
	return true, nil
}

// Incr 原子增减；不存在视为 0；当前值非整数返回 ErrNotInteger。
func (b *MemBackend) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, ErrClosed
	}
	var cur int64
	if e, ok := b.m[key]; ok && !b.expired(e) {
		n, err := strconv.ParseInt(e.value, 10, 64)
		if err != nil {
			b.mu.Unlock()
			return 0, ErrNotInteger
		}
		cur = n
	}
	cur += delta
	next := conv.FormatInt(cur)
	b.set(key, next)
	b.mu.Unlock()
	b.fire(Event{Type: EventPut, Key: key, Value: next})
	return cur, nil
}

// List 列出 prefix 下全部未过期键值；返回键为带此前缀的完整键；无匹配返回空 map。
func (b *MemBackend) List(ctx context.Context, prefix string) (map[string]string, error) {
	b.mu.RLock()
	out := make(map[string]string, len(b.m))
	for k, e := range b.m {
		if e == nil || b.expired(e) {
			continue
		}
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		out[k] = e.value
	}
	b.mu.RUnlock()
	return out, nil
}

// BatchPut 原子批量写入：锁内一次性替换，要么整批生效，要么整批不生效。
// 新键不带过期时间，与 etcd 后端「新键不带租约」的语义一致。
func (b *MemBackend) BatchPut(ctx context.Context, kv map[string]string) error {
	if len(kv) == 0 {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	for k, v := range kv {
		b.m[k] = &memEntry{value: v}
	}
	b.mu.Unlock()
	// 逐个触发事件，让订阅者看到与 etcd 一致的逐键 Put 事件。
	for k, v := range kv {
		b.fire(Event{Type: EventPut, Key: k, Value: v})
	}
	return nil
}

// Watch 订阅前缀下变更；返回 nil 即订阅成功（事件经 cb 异步推送）。
// ctx 取消后自动移除该订阅。
func (b *MemBackend) Watch(ctx context.Context, prefix string, cb func(Event)) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	w := &memWatcher{prefix: prefix, cb: cb}
	b.watchers = append(b.watchers, w)
	b.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-b.done: // Close 也会唤醒，避免订阅 goroutine 一直挂到调用方取消 ctx
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		for i, ww := range b.watchers {
			if ww == w {
				b.watchers = append(b.watchers[:i], b.watchers[i+1:]...)
				break
			}
		}
	}()
	return nil
}

// fire 在锁外把事件推送给匹配前缀的订阅者（避免回调内重入死锁）。
func (b *MemBackend) fire(ev Event) {
	b.mu.RLock()
	matched := make([]func(Event), 0, len(b.watchers))
	for _, w := range b.watchers {
		if w.prefix == "" || strings.HasPrefix(ev.Key, w.prefix) {
			matched = append(matched, w.cb)
		}
	}
	b.mu.RUnlock()
	for _, cb := range matched {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// 走 logger（可落盘/可告警）并带堆栈；fmt.Printf 只到 stdout，
					// 既不进日志系统也定位不到回调里的行号。
					ilog.LogError("globalstore mem: watcher callback panic",
						zap.Any("panic", r), zap.String("stack", string(debug.Stack())))
				}
			}()
			cb(ev)
		}()
	}
}

// Close 标记关闭并清空订阅者（已写入数据保留，调用方不应再读写）。
// 同时唤醒所有 watch goroutine：只把 watchers 置空的话，那些 goroutine 仍挂在
// <-ctx.Done() 上，要等调用方自己取消 ctx 才退出（Close 语义上应当立刻生效）。
func (b *MemBackend) Close() error {
	b.mu.Lock()
	alreadyClosed := b.closed
	b.closed = true
	b.watchers = nil
	b.mu.Unlock()
	if !alreadyClosed {
		close(b.done)
	}
	// 等后台清理协程退出：它读 b.done 后立即 return，无需超时兜底。
	b.sweepWg.Wait()
	return nil
}
