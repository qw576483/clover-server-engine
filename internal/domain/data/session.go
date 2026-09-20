// Package data 数据会话（per-request 数据生命周期）。

// Session 管理单次 handler 请求内的数据生命周期：
//   - Load：store 加载 + identity map 缓存（同 key 只取一次）
//   - Track：修改意图下注册 pending（提交时自动落库 + diff 推送）
//   - Commit：保存全部 pending → 返回 saved 列表供上层推送

// Session 不感知网络 / NATS / 推送——那是 event 层的职责。
// Session 只做纯数据操作（读、写、diff），推送回调解耦给外部。
package data

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// LoadMode 声明加载意图。
type LoadMode int

const (
	// LoadMutable 可修改：handler 成功返回后框架自动 SaveJSON + 推送（默认）。
	LoadMutable LoadMode = iota
	// LoadReadOnly 仅读取，不注册待提交、不自动落库/推送。
	LoadReadOnly
)

// loadOption 加载可选配置。
type loadOption struct {
	mode LoadMode
}

// LoadOption 加载可选参数。
type LoadOption func(*loadOption)

// ReadOnly 声明本次加载为「只读」：不注册待提交，不自动落库/推送。
func ReadOnly() LoadOption {
	return func(o *loadOption) { o.mode = LoadReadOnly }
}

// PendingEdit 一次可修改加载的待提交记录。
type PendingEdit struct {
	Key      Key
	Val      any    // 指向业务结构体（指针）
	Snapshot []byte // Load 时的 JSON 快照，用于提交前比对
}

// CommitResult 提交后产出的单条结果，供上层推送。
//
// JSON 是提交期已序列化好的**全量 JSON**，供落库 / 字段级 diff 推送 / 跨服镜像复用。
// 这三处此前各自 Marshal(Val) 一遍，同一份数据在一次请求里被序列化三次；
// 现由 Commit 统一序列化一次后向下传递（序列化失败时为 nil，调用方需自行兜底）。
// Val 仍保留：推送侧可优先用它的 CommitDiff()（见 transport/event 的 buildSyncBody）。
type CommitResult struct {
	Key      Key
	Val      any
	Snapshot []byte
	JSON     []byte // 提交时序列化好的全量 JSON；nil 表示本次未序列化成功
}

// Session 数据会话——每个 handler 一个，管理请求级数据加载与提交。
// 持有 identity map + pending 列表，不感知 NATS / 推送。
type Session struct {
	store *Store

	mu            sync.Mutex
	loadedRecords map[Key]*Record
	loadedStructs map[Key]any
	pending       []PendingEdit
}

// NewSession 构造数据会话。
func NewSession(store *Store) *Session {
	return &Session{store: store}
}

// LoadStruct 读取一条三元键数据到 v（JSON 反序列化）。
// 默认 LoadMutable：注册 pending，提交时自动落库。
// 传入 ReadOnly() 可仅读取不注册。
func (s *Session) LoadStruct(ctx context.Context, key Key, v any, opts ...LoadOption) error {
	if s.store == nil {
		return fmt.Errorf("data: Session has no store")
	}
	o := &loadOption{}
	for _, fn := range opts {
		fn(o)
	}
	if o.mode == LoadReadOnly {
		return s.store.LoadJSON(ctx, key, v)
	}

	// 可修改加载要求 v 是「非 nil 指针」：Commit 时按指针写回，identity map 命中时
	// 用 Elem().Set 拷贝——nil 指针 / 非指针会在这两处直接 panic。在此显式拒绝，
	// 避免把问题隐藏到后续调用（也避免把 nil 指针缓存进 identity map）。
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("data: LoadStruct requires non-nil pointer for mutable load, got %T", v)
	}

	// 请求级 identity map：同 key 命中直接拷贝缓存值到 v。
	s.mu.Lock()
	if cached, ok := s.loadedStructs[key]; ok {
		cachedRv := reflect.ValueOf(cached)
		vRv := reflect.ValueOf(v)
		if cachedRv.Type() != vRv.Type() {
			s.mu.Unlock()
			return fmt.Errorf("data: LoadStruct key=%v type mismatch: cached=%T, requested=%T", key, cached, v)
		}
		// Elem() 只在双方都是非 nil 指针时才安全（缓存理论上已由入口守卫保证，
		// 这里再防御一次：零 Value 上调用 Set 会直接 panic）。
		if cachedRv.Kind() != reflect.Ptr || vRv.Kind() != reflect.Ptr || cachedRv.IsNil() || vRv.IsNil() {
			s.mu.Unlock()
			logger.Errorf("data: LoadStruct key=%v cannot copy cached value (cached=%T, nil or non-pointer)", key, cached)
			return fmt.Errorf("data: LoadStruct key=%v: cached value is not a non-nil pointer (%T)", key, cached)
		}
		vRv.Elem().Set(cachedRv.Elem())
		// 更新缓存和 pending 里的指针为 v，后续修改才能被 Commit 捕获。
		s.loadedStructs[key] = v
		for i := range s.pending {
			if s.pending[i].Key == key {
				s.pending[i].Val = v
				break
			}
		}
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	// 修改意图：加载（已存在）→ 注册待提交；不存在则保留 v 默认值。
	if err := s.store.LoadJSON(ctx, key, v); err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	snap, mErr := ujson.Marshal(v)
	if mErr != nil {
		// 快照失败只影响「提交前比对」优化（退化为每次必存，不丢数据），但必须留日志。
		logger.Warnf("data: LoadStruct snapshot marshal failed for key=%v: %v (commit will save unconditionally)", key, mErr)
		snap = nil
	}
	s.mu.Lock()
	if s.loadedStructs == nil {
		s.loadedStructs = make(map[Key]any)
	}
	s.loadedStructs[key] = v
	s.pending = append(s.pending, PendingEdit{Key: key, Val: v, Snapshot: snap})
	s.mu.Unlock()
	return nil
}

// LoadRecord 读取一条 Record 数据（表格式，含列定义）。
// 数据不存在时返回空 Record（无行列），handler 中添加行后 Commit 即创建。
func (s *Session) LoadRecord(ctx context.Context, key Key, cols []string, colTypes []object.Type) (*Record, error) {
	if s.store == nil {
		return nil, fmt.Errorf("data: Session has no store")
	}
	// 请求级 identity map：同 key 命中直接返回。
	s.mu.Lock()
	if rec, ok := s.loadedRecords[key]; ok {
		s.mu.Unlock()
		if !recordSchemaMatch(rec, cols, colTypes) {
			return nil, fmt.Errorf("data: LoadRecord key=%v schema mismatch with prior load", key)
		}
		return rec, nil
	}
	s.mu.Unlock()

	rec := NewRecord(cols, colTypes)
	if err := s.store.LoadJSON(ctx, key, rec); err != nil {
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	snap, mErr := ujson.Marshal(rec)
	if mErr != nil {
		logger.Warnf("data: LoadRecord snapshot marshal failed for key=%v: %v (commit will save unconditionally)", key, mErr)
		snap = nil
	}
	s.mu.Lock()
	if s.loadedRecords == nil {
		s.loadedRecords = make(map[Key]*Record)
	}
	s.loadedRecords[key] = rec
	s.pending = append(s.pending, PendingEdit{Key: key, Val: rec, Snapshot: snap})
	s.mu.Unlock()
	return rec, nil
}

// Commit 提交所有 pending：逐条落库（含快照比对 + 一次重试）。
// 返回成功保存的条目列表和失败计数。
// 调用后 loaded 缓存与 pending 清空；Commit 期间并发 Load 注册的新 pending
// 会在循环的下一轮被处理，不再被末尾的清理整体丢弃。
func (s *Session) Commit(ctx context.Context) (saved []CommitResult, failed int) {
	s.mu.Lock()
	if s.store == nil {
		s.mu.Unlock()
		return nil, 0
	}
	s.mu.Unlock()

	for {
		s.mu.Lock()
		if len(s.pending) == 0 {
			s.loadedRecords = nil
			s.loadedStructs = nil
			s.mu.Unlock()
			break
		}
		batch := make([]PendingEdit, len(s.pending))
		copy(batch, s.pending)
		s.pending = s.pending[:0]
		s.mu.Unlock()

		for _, pe := range batch {
			cur, ok := s.saveOne(ctx, pe)
			if !ok {
				failed++
				continue
			}
			saved = append(saved, CommitResult{Key: pe.Key, Val: pe.Val, Snapshot: pe.Snapshot, JSON: cur})
		}
	}
	return saved, failed
}

// saveOne 单条落库（含快照比对与一次重试）。
//
// 返回值 cur 是本次提交序列化好的**全量 JSON**：落库、字段级 diff 推送、跨服镜像
// 三处要用的是同一份字节，所以提交期只序列化这一次（此前落库与推送各序列化一遍）。
// 序列化失败时 cur 为 nil 且 ok=false —— 序列化不出来就没有任何字节可落库，
// 与「保存失败」同类，按失败计数比先记日志再走一次必然失败的保存更诚实。
func (s *Session) saveOne(ctx context.Context, pe PendingEdit) (cur []byte, ok bool) {
	b, err := ujson.Marshal(pe.Val)
	if err != nil {
		logger.Errorf("data: saveOne marshal key=%v failed: %v (nothing to save)", pe.Key, err)
		return nil, false
	}
	// 快照比对：加载后未改动则跳过落库。用 bytes.Equal 而非 string(b)==string(snapshot)：
	// 后者会为两侧各分配一份等长字符串（大对象上是 2×N 字节的纯浪费），结果为 bool 的是它。
	if len(pe.Snapshot) > 0 && bytes.Equal(b, pe.Snapshot) {
		return b, true
	}
	// 直接传已序列化的字节走 Save（不再经 SaveJSON，否则在这里又 Marshal 一遍）。
	if err := s.store.Save(ctx, pe.Key, b); err != nil {
		if retryErr := s.store.Save(ctx, pe.Key, b); retryErr != nil {
			// 两次均失败：必须留日志，否则上层只看到一个 failed 计数，无从定位。
			logger.Errorf("data: saveOne save failed for key=%v after retry: first=%v retry=%v", pe.Key, err, retryErr)
			return nil, false
		}
	}
	return b, true
}

// PendingEdits 返回当前已注册（尚未提交）的可修改加载数量（调试 / 测试用）。
func (s *Session) PendingEdits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// Store 返回注入的数据句柄。
func (s *Session) Store() *Store { return s.store }

// recordSchemaMatch 校验 Record 列定义是否一致。
func recordSchemaMatch(rec *Record, cols []string, colTypes []object.Type) bool {
	if rec.ColCount() != len(cols) || len(cols) != len(colTypes) {
		return false
	}
	for i := range cols {
		if rec.ColName(i) != cols[i] || rec.ColType(i) != colTypes[i] {
			return false
		}
	}
	return true
}
