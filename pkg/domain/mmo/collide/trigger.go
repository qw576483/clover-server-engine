// package collide 提供 MMO 服务端的碰撞检测与寻路原语。
//
// 本文件实现多边形触发区（Zone），提供进入/离开事件回调。
// 设计原则：纯几何判定 + 进入/离开事件回调，不绑定任何业务施加逻辑。
// 减速（SlowFactor）、每秒伤害（DamagePerSec）仅作为数据字段，由上层在回调里读取
// 并自行施加；引擎本身不自动扣血/减速，保持中立。
package collide

import "sync"

// Zone 是一个多边形触发区。
type Zone struct {
	name    string
	shape   Shape   // 碰撞外形（多边形）
	slow    float64 // 减速系数（<=1 表示减速，仅数据）
	dps     float64 // 每秒伤害（仅数据）
	onEnter func(zone string, obj uint64)
	onLeave func(zone string, obj uint64)
	mu      sync.Mutex
	inside  map[uint64]bool // 当前在区内的对象
}

// NewZone 以名称与多边形顶点（世界坐标，逆时针）构造触发区。
// slow 默认置 -1 表示「未设置减速」，SlowFactor() 据此返回 1（正常速度），
// 与「显式设为 0（完全停止）」区分开，消除零值歧义。
func NewZone(name string, poly []Vec2) *Zone {
	return &Zone{
		name:   name,
		shape:  PolygonShape(poly),
		slow:   -1, // 未设置：不减速
		inside: make(map[uint64]bool),
	}
}

// Contains 判断点 p（世界坐标）是否在多边形内（射线法）。
// 直接复用 collide.Shape.Contains，保持算法一致性。
func (z *Zone) Contains(p Vec2) bool {
	return z.shape.Contains(p, Vec2{}) // center 为原点，因为 poly 已是世界坐标
}

// OnEnter 注册进入区回调。
func (z *Zone) OnEnter(fn func(zone string, obj uint64)) {
	z.mu.Lock()
	z.onEnter = fn
	z.mu.Unlock()
}

// OnLeave 注册离开区回调。
func (z *Zone) OnLeave(fn func(zone string, obj uint64)) {
	z.mu.Lock()
	z.onLeave = fn
	z.mu.Unlock()
}

// Update 对象移动时调用：更新 inside，并在进入/离开时触发回调。
// 重复 Update 同一状态不会重复触发。
// 回调加锁外调用，但捕获快照避免重入导致的重复触发（回调重入保护）。
func (z *Zone) Update(obj uint64, p Vec2) {
	now := z.Contains(p)
	z.mu.Lock()
	prev := z.inside[obj]
	if now {
		z.inside[obj] = true
	} else {
		// 区外对象直接删除条目而非写 false——否则每个路过的对象都在
		// inside 中留下永久 false 条目，map 无限膨胀。
		delete(z.inside, obj)
	}
	enter := z.onEnter
	leave := z.onLeave
	name := z.name
	z.mu.Unlock()

	// 快照已捕获锁内状态；即使回调内再次调用 Update，也不会改变本次的 now/prev。
	switch {
	case now && !prev:
		if enter != nil {
			enter(name, obj)
		}
	case !now && prev:
		if leave != nil {
			leave(name, obj)
		}
	}
}

// InitScan 对一批「对象→位置」做初始快照扫描。
// 用于新建触发区后、对象已存在于世界中的场景：把当前落在区内的对象登记为 inside，
// 并对每个新进入者触发 onEnter（区外对象不做记录，避免误触发 onLeave）。
// 应在 OnEnter/OnLeave 注册之后、首次逐帧 Update 之前调用一次。
func (z *Zone) InitScan(positions map[uint64]Vec2) {
	type entered struct {
		name string
		obj  uint64
	}
	var fires []entered
	z.mu.Lock()
	enter := z.onEnter
	name := z.name
	for obj, p := range positions {
		// Contains 只读不可变的 shape，不获取 z.mu，可安全在持锁内调用。
		if !z.Contains(p) {
			continue
		}
		if z.inside[obj] {
			continue
		}
		z.inside[obj] = true
		fires = append(fires, entered{name: name, obj: obj})
	}
	z.mu.Unlock()

	if enter != nil {
		for _, f := range fires {
			enter(f.name, f.obj)
		}
	}
}

// Inside 返回对象记录的最后状态；从未 Update 过的对象返回 false。
// 若需要基于实时位置判定，请调用 Contains()。
func (z *Zone) Inside(obj uint64) bool {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.inside[obj]
}

// SlowFactor 返回减速系数（[0,1]：1=正常速度，0=完全停止，<1=减速）。
// slow/dps 由 SetEffect 在锁内写入，读取同样需加锁，否则构成 data race。
func (z *Zone) SlowFactor() float64 {
	z.mu.Lock()
	s := z.slow
	z.mu.Unlock()
	if s < 0 {
		return 1
	}
	if s > 1 {
		return 1
	}
	return s
}

// DamagePerSec 返回每秒伤害。
func (z *Zone) DamagePerSec() float64 {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.dps
}

// SetEffect 设置效果数据：slow 减速系数（[0,1]，1=正常）、dps 每秒伤害。
func (z *Zone) SetEffect(slow, dps float64) {
	z.mu.Lock()
	z.slow = slow
	z.dps = dps
	z.mu.Unlock()
}
