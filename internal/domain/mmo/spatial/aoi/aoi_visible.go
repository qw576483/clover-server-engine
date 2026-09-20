package aoi

import (
	"math"
	"sort"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
)

// FilterFn 自定义可见性过滤（分组 / 阵营 / 距离二次校验等）。
// 参数为 viewer、target 的 uint64 线化身份（object.ObjectID.MarshalUint64）；
// 返回 true 表示允许可见。设 nil 关闭过滤（全部放行）。
type FilterFn func(viewer, target uint64) bool

// Quota 单个观察者可保留的最大可见目标数；0 表示不限。
type Quota int

// VisualSystem 在现有九宫格 AOI（Grid）之上提供「双向可见性 + 过滤 + 配额」能力，
// Visual/Observer 双向关系：
// - Visuals(obj)：obj 作为观察者能看到哪些 viewer（正向索引）；
// - Observers(obj)：哪些 viewer 把 obj 视为可见目标（反向索引）；
// - SetFilter：按阵营 / 分组 / 距离二次校验剔除；
// - SetQuota：限制每个观察者的可见目标数（按距离最近优先保留）。
//
// 设计上 VisualSystem 复用 *Grid 做空间索引与位置维护（Enter/Move/Leave 透传），
// 自身只维护可见集合、反向索引、过滤与配额等「视野关系」状态——不改动 Grid 的
// Enter/Leave/Move/Query/Observer 行为。
type VisualSystem struct {
	g *Grid // 底层空间网格（仅借其位置索引与 Around 范围查询）

	mu sync.RWMutex

	defaultR float64  // 未单独设半径时的默认视觉半径
	maxR     float64  // 当前最大视觉半径（用于受影响范围查询）
	filter   FilterFn // 可见性过滤
	quota    Quota    // 每个观察者最大可见数

	radius    map[object.ObjectID]float64                      // 每个观察者的视觉半径覆盖（存在即覆盖 defaultR）
	watchers  map[object.ObjectID]struct{}                     // 登记为观察者的对象集合
	visible   map[object.ObjectID]map[object.ObjectID]struct{} // viewer -> 可见目标集
	observers map[object.ObjectID]map[object.ObjectID]struct{} // target -> 能看到它的 viewer 集（反向索引）

	// observer 视野增量回调：当某 target 因过滤 / 配额裁剪进入或离开 viewer 的
	// 可见集时回调，使配额裁剪掉的目标也能得到 LeaveView，避免下游漏推。锁外调用。
	observer VisualObserver

	stopOnce sync.Once // 保证 Stop 幂等，只释放一次底层网格
}

// VisualObserver 视觉系统的视野增量回调：viewer 对 target 发生了 ev（EnterView/LeaveView）。
type VisualObserver func(viewer, target object.ObjectID, ev Event)

// SetObserver 设置视野增量回调（传 nil 关闭）。回调在释放内部锁后触发，可安全地再调用本系统方法。
func (vs *VisualSystem) SetObserver(o VisualObserver) {
	vs.mu.Lock()
	vs.observer = o
	vs.mu.Unlock()
}

// NewVisualSystem 以格子边长 cellSize 与默认视觉半径 defaultR 构造视觉系统。
// 默认半径用于未单独 SetVisual 的观察者；建议 defaultR 与常见视野半径一致。
// 底层网格与 Grid 共用同一套空间模型（三维球体判定）。
func NewVisualSystem(cellSize, defaultR float64) *VisualSystem {
	return &VisualSystem{
		g:         New(cellSize),
		defaultR:  defaultR,
		radius:    make(map[object.ObjectID]float64),
		watchers:  make(map[object.ObjectID]struct{}),
		visible:   make(map[object.ObjectID]map[object.ObjectID]struct{}),
		observers: make(map[object.ObjectID]map[object.ObjectID]struct{}),
	}
}

// Stop 释放视觉系统持有的后台资源：停掉底层 Grid 的定期清理 ticker 与清理协程。
// 构造出的 VisualSystem 必须配对调用 Stop，否则后台协程会一直存活。
// 可重复调用（幂等），并发安全；停止后系统仍可继续做空间查询，只是不再自动清理空分片。
func (vs *VisualSystem) Stop() {
	vs.stopOnce.Do(func() {
		if vs.g != nil {
			vs.g.Stop()
		}
	})
}

// Close 等价于 Stop，便于按 io.Closer 风格统一管理生命周期。
func (vs *VisualSystem) Close() {
	vs.Stop()
}

// Enter 让对象以坐标 p 进入系统（透传到底层 Grid）。
func (vs *VisualSystem) Enter(id object.ObjectID, p Position) {
	vs.g.Enter(id, p)
	vs.recomputeAffected(id, p)
}

// Move 更新对象坐标（透传到底层 Grid），刷新受影响观察者的可见关系。
// 同时重算新位置与旧位置附近的观察者——否则对象远离后，原视野内的观察者不会及时解除关系。
// 注意：Position 与 Move 之间可能被并发修改，oldPos 仅作为尽力而为的旧位置参考。
func (vs *VisualSystem) Move(id object.ObjectID, p Position) {
	oldPos, ok := vs.g.Position(id)
	vs.g.Move(id, p)
	vs.recomputeAffected(id, p)
	if ok {
		// 旧位置可能已被并发 Move 修改，但尽力刷新仍优于完全不刷新。
		vs.recomputeAffected(id, oldPos)
	}
}

// Leave 让对象离开系统，清理其观察者身份与所有可见 / 被观察关系。
func (vs *VisualSystem) Leave(id object.ObjectID) {
	pos, ok := vs.g.Position(id)
	vs.g.Leave(id)
	vs.mu.Lock()
	// 与 Grid.Leave 同口径：观察者离场要对其旧可见目标补发 LeaveView，
	// 否则这些目标的「离开」增量永久丢失（下游可见集合与网格不一致）。
	var left []object.ObjectID
	if old := vs.visible[id]; old != nil {
		for t := range old {
			left = append(left, t)
		}
	}
	delete(vs.watchers, id)
	delete(vs.radius, id)
	vs.clearWatcherLocked(id)
	vs.recomputeMaxRLocked()
	obs := vs.observer
	vs.mu.Unlock()
	vs.emitDeltas(obs, id, nil, left)
	if ok {
		vs.recomputeAffected(id, pos)
	}
}

// SetVisual 把对象登记为观察者并设定其视觉半径（radius<=0 表示用默认半径）。
// 对象必须先 Enter；随后立即重算其可见集合与受影响观察者的反向索引。
func (vs *VisualSystem) SetVisual(id object.ObjectID, radius float64) {
	vs.mu.Lock()
	vs.watchers[id] = struct{}{}
	if radius > 0 {
		vs.radius[id] = radius
	} else {
		delete(vs.radius, id)
	}
	vs.recomputeMaxRLocked()
	vs.mu.Unlock()
	pos, ok := vs.g.Position(id)
	if !ok {
		// 对象不在网格（未 Enter 或并发 Leave）：没有位置可做「受影响观察者」重算。
		// 之前 mustPos 会静默回退零坐标，导致以世界原点为圆心刷新错区域且无留痕。
		aoiFailf("aoi: SetVisual obj=%d 不在网格，跳过受影响观察者重算", id)
		return
	}
	vs.recomputeAffected(id, pos)
}

// ClearVisual 取消对象的观察者身份（保留其位置，只是不再维护可见关系）。
func (vs *VisualSystem) ClearVisual(id object.ObjectID) {
	vs.mu.Lock()
	// 补发 LeaveView（同 Leave）：取消观察者身份后不再维护可见关系，
	// 旧可见目标的增量必须现在补上，否则永久丢失。
	var left []object.ObjectID
	if old := vs.visible[id]; old != nil {
		for t := range old {
			left = append(left, t)
		}
	}
	delete(vs.watchers, id)
	delete(vs.radius, id)
	vs.clearWatcherLocked(id)
	vs.recomputeMaxRLocked()
	obs := vs.observer
	vs.mu.Unlock()
	vs.emitDeltas(obs, id, nil, left)
	pos, ok := vs.g.Position(id)
	if !ok {
		// 对象不在网格（未 Enter 或并发 Leave）：没有位置可做「受影响观察者」重算。
		// 之前 mustPos 会静默回退零坐标，导致以世界原点为圆心刷新错区域且无留痕。
		aoiFailf("aoi: ClearVisual obj=%d 不在网格，跳过受影响观察者重算", id)
		return
	}
	vs.recomputeAffected(id, pos)
}

// SetFilter 设置可见性过滤（传 nil 关闭）。变更后全量重算所有观察者。
func (vs *VisualSystem) SetFilter(fn FilterFn) {
	vs.mu.Lock()
	vs.filter = fn
	vs.mu.Unlock()
	vs.recomputeAll()
}

// SetQuota 设置每个观察者的最大可见目标数（0=不限）。变更后全量重算所有观察者。
func (vs *VisualSystem) SetQuota(q Quota) {
	vs.mu.Lock()
	vs.quota = q
	vs.mu.Unlock()
	vs.recomputeAll()
}

// Visuals 返回 obj 作为观察者当前能看到的 viewer 列表（稳定排序）；非观察者返回空切片（非 nil）。
func (vs *VisualSystem) Visuals(id object.ObjectID) []object.ObjectID {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return idsOf(vs.visible[id])
}

// Observers 返回哪些 viewer 当前把 obj 视为可见目标（反向索引，稳定排序）；
// 没有 viewer 看到时返回空切片（非 nil）。
func (vs *VisualSystem) Observers(id object.ObjectID) []object.ObjectID {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return idsOf(vs.observers[id])
}

// 内部实现
func (vs *VisualSystem) radiusOfLocked(id object.ObjectID) float64 {
	if r, ok := vs.radius[id]; ok {
		return r
	}
	return vs.defaultR
}

func (vs *VisualSystem) isWatcher(id object.ObjectID) bool {
	vs.mu.RLock()
	_, ok := vs.watchers[id]
	vs.mu.RUnlock()
	return ok
}

func (vs *VisualSystem) getMaxR() float64 {
	vs.mu.RLock()
	m := vs.maxR
	vs.mu.RUnlock()
	return m
}

func (vs *VisualSystem) recomputeMaxRLocked() {
	m := 0.0
	for id := range vs.watchers {
		if r := vs.radiusOfLocked(id); r > m {
			m = r
		}
	}
	if vs.defaultR > m {
		m = vs.defaultR
	}
	vs.maxR = m
}

// recomputeAffected 重算 changed 本身（若是观察者）与位置 p 附近、可能把 changed 纳入视野的观察者。
func (vs *VisualSystem) recomputeAffected(changed object.ObjectID, p Position) {
	affected := make(map[object.ObjectID]struct{})
	if vs.isWatcher(changed) {
		affected[changed] = struct{}{}
	}
	for _, c := range vs.g.Around(p, vs.getMaxR()) {
		if vs.isWatcher(c) {
			affected[c] = struct{}{}
		}
	}
	for w := range affected {
		vs.recomputeWatcher(w)
	}
}

// recomputeAll 全量重算所有观察者（filter/quota 变更时调用）。
func (vs *VisualSystem) recomputeAll() {
	vs.mu.RLock()
	watchers := make([]object.ObjectID, 0, len(vs.watchers))
	for w := range vs.watchers {
		watchers = append(watchers, w)
	}
	vs.mu.RUnlock()
	for _, w := range watchers {
		vs.recomputeWatcher(w)
	}
}

// recomputeWatcher 重算单个观察者 w 的可见集合，并修补反向索引 observers。
//
// 分三阶段，filter 刻意放在**锁外**调用：
//   - 阶段 1（锁内）读取观察者参数快照；
//   - 阶段 2（锁外）候选收集 / filter / 配额 —— filter 是业务回调，若在持 vs.mu
//     时调用，业务在 filter 内再调本系统任何方法都会自死锁（RWMutex 不可重入），
//     与 observer 回调「锁外调用」的口径保持一致；
//   - 阶段 3（锁内）写回可见集合与反向索引（基于当前 visible 做增量）。
//
// g.Around / g.Position 持有的是底层 Grid 的独立锁，不会与 vs.mu 构成死锁。
func (vs *VisualSystem) recomputeWatcher(w object.ObjectID) {
	// ── 阶段 1（锁内）：参数快照 ──
	vs.mu.Lock()
	r := vs.radiusOfLocked(w)
	filter := vs.filter
	quota := vs.quota
	center, ok := vs.g.Position(w)
	if !ok {
		// 观察者已不在网格：其原可见目标全部视为离开，发出 LeaveView 增量。
		var left []object.ObjectID
		if old := vs.visible[w]; old != nil {
			for t := range old {
				left = append(left, t)
			}
		}
		obs := vs.observer
		vs.clearWatcherLocked(w)
		// 幽灵条目清理：不是网格成员却留在 watchers/radius 里，会永久计入 maxR、
		// 每次全量重算（SetFilter/SetQuota/recomputeAll）都白跑一遍它。
		delete(vs.watchers, w)
		delete(vs.radius, w)
		vs.recomputeMaxRLocked()
		vs.mu.Unlock()
		vs.emitDeltas(obs, w, nil, left)
		return
	}
	vs.mu.Unlock()

	// ── 阶段 2（锁外）：候选收集 / filter / 配额 ──
	// 进入半径范围的所有候选（Around 已按精确平方距离裁剪）
	cands := vs.g.Around(center, r)
	var list []object.ObjectID
	for _, t := range cands {
		if t == w {
			continue
		}
		if filter != nil && !filter(w.MarshalUint64(), t.MarshalUint64()) {
			continue
		}
		list = append(list, t)
	}
	// 配额：按到观察者距离最近优先保留前 quota 个
	if quota > 0 && len(list) > int(quota) {
		centerC := center
		// 配额裁剪的「最近优先」必须与视野判定用同一空间维度：三维视野配平面距离会让
		// 保留的目标在垂直方向上并不最近（隔了一整层反而挤掉同层目标）。
		dist2 := func(id object.ObjectID) float64 {
			p, ok := vs.g.Position(id)
			if !ok {
				return math.MaxFloat64
			}
			return vs.g.dist2(p, centerC)
		}
		sort.Slice(list, func(i, j int) bool {
			return dist2(list[i]) < dist2(list[j])
		})
		list = list[:quota]
	}
	newSet := make(map[object.ObjectID]struct{}, len(list))
	for _, t := range list {
		newSet[t] = struct{}{}
	}

	// ── 阶段 3（锁内）：写回 ──
	vs.mu.Lock()
	if _, still := vs.watchers[w]; !still {
		// 计算期间观察者被 Leave / ClearVisual 摘除：本次结果作废，
		// 否则会把已注销的观察者写回（幽灵视野）。
		vs.mu.Unlock()
		return
	}
	var entered, left []object.ObjectID
	// 修补反向索引：移除已不可见的旧目标（含被配额 / 过滤裁剪掉的），加入新可见目标。
	// 同时收集 enter/left 增量——配额裁剪后 newSet 覆盖 visible[w] 会静默丢弃
	// 被裁目标，此处显式对比 old 与 newSet 产出 LeaveView，保证下游不漏推。
	if old, ok := vs.visible[w]; ok {
		for t := range old {
			if _, still := newSet[t]; !still {
				left = append(left, t)
				if m := vs.observers[t]; m != nil {
					delete(m, w)
					if len(m) == 0 {
						delete(vs.observers, t)
					}
				}
			}
		}
		for t := range newSet {
			if _, had := old[t]; !had {
				entered = append(entered, t)
			}
		}
	} else {
		for t := range newSet {
			entered = append(entered, t)
		}
	}
	for t := range newSet {
		if vs.observers[t] == nil {
			vs.observers[t] = make(map[object.ObjectID]struct{})
		}
		vs.observers[t][w] = struct{}{}
	}
	vs.visible[w] = newSet
	obs := vs.observer
	vs.mu.Unlock()
	vs.emitDeltas(obs, w, entered, left)
}

// emitDeltas 在释放内部锁后派发视野增量（稳定排序，先离开后进入）。obs 为 nil 时静默。
func (vs *VisualSystem) emitDeltas(obs VisualObserver, w object.ObjectID, entered, left []object.ObjectID) {
	if obs == nil || (len(entered) == 0 && len(left) == 0) {
		return
	}
	sortIDs(left)
	sortIDs(entered)
	for _, t := range left {
		obs(w, t, LeaveView)
	}
	for _, t := range entered {
		obs(w, t, EnterView)
	}
}

// clearWatcherLocked 从可见集合与反向索引中彻底移除观察者 w（调用方持锁）。
func (vs *VisualSystem) clearWatcherLocked(w object.ObjectID) {
	if old, ok := vs.visible[w]; ok {
		for t := range old {
			if m := vs.observers[t]; m != nil {
				delete(m, w)
				if len(m) == 0 {
					delete(vs.observers, t)
				}
			}
		}
		delete(vs.visible, w)
	}
}

// idsOf 把集合转为稳定排序的切片（按 Type/Seq，与 Grid.sortIDs 一致）。
func idsOf(m map[object.ObjectID]struct{}) []object.ObjectID {
	out := make([]object.ObjectID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sortIDs(out)
	return out
}
