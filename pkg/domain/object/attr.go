// Package object 的属性元数据框架，与 object.Bag（按名字索引的强类型属性袋 + 增量同步）
// 互补：AttrSet 是「按 ID 索引的轻量数值属性容器 + 变更回调」，供战斗 / Buff / 技能做数值读写与监听。
//
// 设计要点：
//   - 属性以 ID(uint16) 标识，定义（Def）描述其 Name / Kind / Default。
//   - Set 为属性容器，内部用 map[ID]float64 存当前值（统一以 float64 承载，
//     KindInt 的整数语义由 Int/SetInt 提供）。
//   - 通过 OnChange 注册单属性回调、OnAnyChange 注册全局回调；Set/Add
//     仅在「值真正变化」时触发（同值不触发）。
//   - 线程安全：用 sync.RWMutex 保护内部状态。为避免回调内再次调用 Set/Add
//     造成死锁，触发回调前先释放写锁，再在锁外同步调用所有回调。
package object

import "sync"

// AttrID 属性 ID。
type AttrID uint16

// AttrKind 属性值类型。
type AttrKind int

const (
	// AttrKindInt 整型属性（由 Int/SetInt 读写）。
	AttrKindInt AttrKind = iota
	// AttrKindFloat 浮点属性。
	AttrKindFloat
)

// AttrDef 属性定义：名字、值类型、默认值。
type AttrDef struct {
	ID      AttrID
	Name    string
	Kind    AttrKind
	Default float64
}

// AttrChangeFn 属性变更回调。old/new 为变化前后的值；OnAnyChange 的 id 指明变化的属性。
type AttrChangeFn func(id AttrID, old, new float64)

// AttrSet 属性容器：内部包含定义表、当前值表与回调表，并发安全。
type AttrSet struct {
	mu     sync.RWMutex
	defs   map[AttrID]AttrDef
	vals   map[AttrID]float64
	change map[AttrID][]AttrChangeFn
	anyChg []AttrChangeFn
}

// NewAttrSet 用定义表构造属性容器；每个属性按 Def.Default 初始化。
func NewAttrSet(defs ...AttrDef) *AttrSet {
	s := &AttrSet{
		defs:   make(map[AttrID]AttrDef, len(defs)),
		vals:   make(map[AttrID]float64, len(defs)),
		change: make(map[AttrID][]AttrChangeFn),
	}
	for _, d := range defs {
		s.defs[d.ID] = d
		s.vals[d.ID] = d.Default
	}
	return s
}

// Has 判断某属性是否被定义（出现在定义表中）。
func (s *AttrSet) Has(id AttrID) bool {
	s.mu.RLock()
	_, ok := s.defs[id]
	s.mu.RUnlock()
	return ok
}

// Get 取当前值；未定义属性返回 0。
func (s *AttrSet) Get(id AttrID) float64 {
	s.mu.RLock()
	v := s.vals[id]
	s.mu.RUnlock()
	return v
}

// setLocked 在已持有写锁时改值；值变化时收集需触发的回调并返回，调用方负责释放锁后调用。
// 返回 nil 表示值未变化（无回调）。
func (s *AttrSet) setLocked(id AttrID, v float64) (old float64, changed bool, fns []AttrChangeFn) {
	old = s.vals[id]
	if old == v {
		return old, false, nil
	}
	s.vals[id] = v
	fns = append(fns, s.change[id]...)
	fns = append(fns, s.anyChg...)
	return old, true, fns
}

// invoke 在锁外同步调用收集到的回调（避免回调内重入 Set 造成死锁）。
func invoke(fns []AttrChangeFn, id AttrID, old, new float64) {
	for _, fn := range fns {
		if fn != nil {
			fn(id, old, new)
		}
	}
}

// Set 设值；仅当值真正变化时触发回调。
func (s *AttrSet) Set(id AttrID, v float64) {
	s.mu.Lock()
	old, changed, fns := s.setLocked(id, v)
	s.mu.Unlock()
	if changed {
		invoke(fns, id, old, v)
	}
}

// Add 增量修改并返回新值；仅当值真正变化时触发回调。
func (s *AttrSet) Add(id AttrID, delta float64) float64 {
	s.mu.Lock()
	old := s.vals[id]
	newV := old + delta
	_, changed, fns := s.setLocked(id, newV)
	s.mu.Unlock()
	if changed {
		invoke(fns, id, old, newV)
	}
	return newV
}

// OnChange 注册某属性的变更回调（可注册多个；Set/Add 变化后同步调用）。
func (s *AttrSet) OnChange(id AttrID, fn AttrChangeFn) {
	s.mu.Lock()
	s.change[id] = append(s.change[id], fn)
	s.mu.Unlock()
}

// OnAnyChange 注册全局变更回调（任意属性变化都调用，id 参数指明哪个属性）。
func (s *AttrSet) OnAnyChange(fn AttrChangeFn) {
	s.mu.Lock()
	s.anyChg = append(s.anyChg, fn)
	s.mu.Unlock()
}

// Range 遍历所有属性（id, 当前值）；fn 返回 false 提前结束。
func (s *AttrSet) Range(fn func(id AttrID, v float64) bool) {
	s.mu.RLock()
	snap := make(map[AttrID]float64, len(s.vals))
	for k, v := range s.vals {
		snap[k] = v
	}
	s.mu.RUnlock()
	for k, v := range snap {
		if !fn(k, v) {
			return
		}
	}
}

// Snapshot 返回当前值快照（副本，后续修改不影响返回 map）。
func (s *AttrSet) Snapshot() map[AttrID]float64 {
	s.mu.RLock()
	out := make(map[AttrID]float64, len(s.vals))
	for k, v := range s.vals {
		out[k] = v
	}
	s.mu.RUnlock()
	return out
}

// Load 批量载入属性值（不触发回调，用于初始化/存档恢复）。
func (s *AttrSet) Load(vals map[AttrID]float64) {
	s.mu.Lock()
	for k, v := range vals {
		s.vals[k] = v
	}
	s.mu.Unlock()
}

// Int 取整型属性值（KindInt 语义；直接截断为 int64）。
func (s *AttrSet) Int(id AttrID) int64 {
	return int64(s.Get(id))
}

// SetInt 设置整型属性值（KindInt 语义）。
func (s *AttrSet) SetInt(id AttrID, v int64) {
	s.Set(id, float64(v))
}
