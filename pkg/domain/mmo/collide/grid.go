package collide

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

// Pair 是一对发生重叠的对象 id。
type Pair struct {
	A, B string
}

// CollisionMask 碰撞掩码，用于按位过滤碰撞检查。
// 仅当 (a.Mask & b.Mask) != 0 时才进行碰撞检测。
type CollisionMask uint32

// CollisionGroup 预定义的常用碰撞分组。
// 用户可将自定义分组按位组合：Mask = GroupPlayer | GroupMonster | GroupWall。
const (
	GroupPlayer     CollisionMask = 1 << iota // 玩家
	GroupMonster                              // 怪物/NPC
	GroupWall                                 // 墙体/静态障碍
	GroupProjectile                           // 飞行物/弹道
	GroupTrigger                              // 触发器/区域
	GroupPickup                               // 拾取物
)

// IsValid 校验掩码是否合理（至少设置一个 bit）。
func (m CollisionMask) IsValid() bool { return m != 0 }

// cellKey 是均匀网格的格子坐标。
type cellKey struct {
	x, y int
}

// Grid 是均匀网格空间哈希（广相 / broad-phase），并发安全的泛型容器：
// Insert/Remove 维护对象包围盒，QueryRegion / Nearby 返回候选 id，
// Collisions 在候选上跑 AABB 窄相返回重叠对。id 用 string（通常取 object.ObjectID.String()）。
type Grid struct {
	cell      float64
	mu        sync.RWMutex
	items     map[string]AABB                 // id -> 全局 AABB（快速查找自身包围盒）
	cells     map[cellKey]map[string]struct{} // 每个格子内的对象 id 集合
	itemCells map[string][]cellKey            // 每个对象占据的格子列表（用于 Remove/Update）
	shapes    map[string]Shape                // id -> 碰撞外形（用于深检测）
	masks     map[string]CollisionMask        // id -> 碰撞掩码
}

// NewGrid 以给定格子边长构造空间网格（cell>0）。
func NewGrid(cell float64) *Grid {
	if cell <= 0 {
		cell = 1
	}
	return &Grid{
		cell:      cell,
		items:     make(map[string]AABB),
		cells:     make(map[cellKey]map[string]struct{}),
		itemCells: make(map[string][]cellKey),
		shapes:    make(map[string]Shape),
		masks:     make(map[string]CollisionMask),
	}
}

// SetShape 为对象注册碰撞外形，供 DeepCollisions 使用。
func (g *Grid) SetShape(id string, s Shape) {
	g.mu.Lock()
	g.shapes[id] = s
	g.mu.Unlock()
}

// SetMask 为对象设置碰撞掩码，Insert 时可自动校验。
// 传入无效 mask（0）将返回错误。
func (g *Grid) SetMask(id string, mask CollisionMask) error {
	if !mask.IsValid() {
		return fmt.Errorf("collide: invalid collision mask 0 for id=%s", id)
	}
	g.mu.Lock()
	g.masks[id] = mask
	g.mu.Unlock()
	return nil
}

func (g *Grid) cellRange(b AABB) (x0, y0, x1, y1 int) {
	x0 = int(math.Floor(b.MinX / g.cell))
	x1 = int(math.Floor(b.MaxX / g.cell))
	y0 = int(math.Floor(b.MinY / g.cell))
	y1 = int(math.Floor(b.MaxY / g.cell))
	return
}

// addToCells 把 id 登记到其 AABB 覆盖的所有格子。
// 非法 AABB（NaN/Inf/大跨度）在此被拒绝：容量算式 (x1-x0+1)*(y1-y0+1) 会因
// 超大跨度溢出为负而直接 panic，且循环次数会爆炸（详见 geomguard.go）。
func (g *Grid) addToCells(id string, b AABB) {
	if !validAABB2(b) {
		dropInvalidGeom("Grid.addToCells", b)
		return
	}
	x0, y0, x1, y1 := g.cellRange(b)
	sx, sy := cellSpanCount(x0, x1), cellSpanCount(y0, y1)
	if gridRangeTooLarge(sx, sy) {
		dropInvalidGeom("Grid.addToCells(span)", b)
		return
	}
	cells := make([]cellKey, 0, int(sx*sy))
	for cx := x0; cx <= x1; cx++ {
		for cy := y0; cy <= y1; cy++ {
			ck := cellKey{x: cx, y: cy}
			set := g.cells[ck]
			if set == nil {
				set = make(map[string]struct{})
				g.cells[ck] = set
			}
			set[id] = struct{}{}
			cells = append(cells, ck)
		}
	}
	g.itemCells[id] = cells
}

// removeFromCells 把 id 从它占据的所有格子中移除。
func (g *Grid) removeFromCells(id string) {
	for _, ck := range g.itemCells[id] {
		if set := g.cells[ck]; set != nil {
			delete(set, id)
			if len(set) == 0 {
				delete(g.cells, ck)
			}
		}
	}
	delete(g.itemCells, id)
}

// Insert 登记对象及其包围盒。
// 幂等：若 id 已存在（如 physicsStep 每帧重新登记移动对象），先清理其旧格子引用，
// 否则对象跨格后旧格子会残留幽灵 id，导致 QueryRegion/Collisions 误报且 cells 无限膨胀。
func (g *Grid) Insert(id string, b AABB) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.items[id]; exists {
		g.removeFromCells(id)
	}
	if !validAABB2(b) {
		// 非法包围盒不入册（随后被 Get/查询一律视为不存在），避免把脏数据带进后续计算。
		dropInvalidGeom("Grid.Insert", b)
		delete(g.items, id)
		return
	}
	g.items[id] = b
	g.addToCells(id, b)
}

// Remove 注销对象。
// 同步清理 shapes/masks——否则对象注销后残留（内存泄漏），
// 且新对象若重用同一 id 会继承旧的碰撞外形/掩码。
func (g *Grid) Remove(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.items, id)
	delete(g.shapes, id)
	delete(g.masks, id)
	g.removeFromCells(id)
}

// Update 更新对象包围盒（等价于先 Insert，含非法输入防护）。
func (g *Grid) Update(id string, b AABB) { g.Insert(id, b) }

// QueryRegion 返回与给定区域相交候选格内的所有对象 id（含区域外但同格的，需调用方窄相过滤）。
func (g *Grid) QueryRegion(b AABB) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !validAABB2(b) {
		dropInvalidGeom("Grid.QueryRegion", b)
		return nil
	}
	x0, y0, x1, y1 := g.cellRange(b)
	sx, sy := cellSpanCount(x0, x1), cellSpanCount(y0, y1)
	if gridRangeTooLarge(sx, sy) {
		dropInvalidGeom("Grid.QueryRegion(span)", b)
		return nil
	}
	seen := make(map[string]struct{}, 8)
	var out []string
	for cx := x0; cx <= x1; cx++ {
		for cy := y0; cy <= y1; cy++ {
			set := g.cells[cellKey{x: cx, y: cy}]
			if set == nil {
				continue
			}
			for id := range set {
				if _, ok := seen[id]; ok {
					continue
				}
				// 对象包围盒与查询区域相交（广相宽松判定）
				ib := g.items[id]
				if ib.MinX <= b.MaxX && ib.MaxX >= b.MinX && ib.MinY <= b.MaxY && ib.MaxY >= b.MinY {
					seen[id] = struct{}{}
					out = append(out, id)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// QueryPoint 返回包含该点的所有对象 id。
func (g *Grid) QueryPoint(x, y float64) []string {
	return g.QueryRegion(AABB{x, y, x, y})
}

// Nearby 返回与指定对象同格 / 邻格的候选 id（不含自身），用于后续窄相。
func (g *Grid) Nearby(id string) []string {
	g.mu.RLock()
	b, ok := g.items[id]
	g.mu.RUnlock()
	if !ok {
		return nil
	}
	cands := g.QueryRegion(b)
	// 不要用 cands[:0] 复用底层数组——一旦调用方长期持有返回切片，
	// 后续对同一底层数组的写入会污染其内容。分配独立切片。
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		if c != id {
			out = append(out, c)
		}
	}
	return out
}

// Collisions 返回所有相互重叠（AABB 窄相）的对象对。
func (g *Grid) Collisions() []Pair {
	g.mu.RLock()
	ids := make([]string, 0, len(g.items))
	for id := range g.items {
		ids = append(ids, id)
	}
	// 快照 items 避免后续逐个加锁时对象被 Remove 导致基于旧数据计算。
	itemsSnap := make(map[string]AABB, len(g.items))
	for id, b := range g.items {
		itemsSnap[id] = b
	}
	g.mu.RUnlock()

	sort.Strings(ids)
	var pairs []Pair
	for i := 0; i < len(ids); i++ {
		a := ids[i]
		ba, okA := itemsSnap[a]
		if !okA {
			continue // 快照后被 Remove，跳过
		}
		// 只在 a 自身及其邻格候选里找配对，避免全量 O(n^2)
		for _, c := range g.QueryRegion(ba) {
			if c <= a {
				continue
			}
			bc, okC := itemsSnap[c]
			if !okC {
				continue // 快照后被 Remove，跳过
			}
			if AABBvsAABB(ba, bc) {
				pairs = append(pairs, Pair{A: a, B: c})
			}
		}
	}
	return pairs
}

// DeepCollisions 返回所有深层碰撞对。
// 先在 AABB 广相候选上做 CollisionMask 过滤，再对注册了 Shape 的对象
// 做 OBB/SAT 级深检测（圆-圆、圆-多边形、多边形-多边形）。
// 未注册 Shape 的对象退化为 AABBvsAABB 判定。
func (g *Grid) DeepCollisions() []Pair {
	g.mu.RLock()
	ids := make([]string, 0, len(g.items))
	// 快照 items/masks/shapes，避免逐对象加锁时对象被 Remove 导致基于零值/旧数据计算。
	itemsSnap := make(map[string]AABB, len(g.items))
	for id, b := range g.items {
		ids = append(ids, id)
		itemsSnap[id] = b
	}
	// 快照 masks 和 shapes 避免重复加锁
	maskSnap := make(map[string]CollisionMask, len(g.masks))
	shapeSnap := make(map[string]Shape, len(g.shapes))
	for id, m := range g.masks {
		maskSnap[id] = m
	}
	for id, s := range g.shapes {
		shapeSnap[id] = s
	}
	g.mu.RUnlock()

	sort.Strings(ids)
	var pairs []Pair
	for i := 0; i < len(ids); i++ {
		a := ids[i]
		ba, okA := itemsSnap[a]
		if !okA {
			continue // 快照后被 Remove，跳过
		}
		for _, c := range g.QueryRegion(ba) {
			if c <= a {
				continue
			}
			// CollisionMask 过滤——只有 mask 交集非空的 pair 才检测
			if len(maskSnap) > 0 {
				ma, mokA := maskSnap[a]
				mc, mokC := maskSnap[c]
				if mokA && mokC && (ma&mc) == 0 {
					continue
				}
			}
			bc, okC := itemsSnap[c]
			if !okC {
				continue // 快照后被 Remove，跳过
			}
			if !AABBvsAABB(ba, bc) {
				continue
			}
			// 若有注册 Shape，做深层碰撞检测
			sa, hasA := shapeSnap[a]
			sc, hasC := shapeSnap[c]
			if hasA && hasC {
				// 用对象 AABB 中心作为 Shape 的世界坐标
				centerA := Vec2{(ba.MinX + ba.MaxX) / 2, (ba.MinY + ba.MaxY) / 2}
				centerC := Vec2{(bc.MinX + bc.MaxX) / 2, (bc.MinY + bc.MaxY) / 2}
				if !sa.Intersect(centerA, Vec2{}, sc, centerC) {
					continue // 深检测不通过，过滤掉
				}
			}
			pairs = append(pairs, Pair{A: a, B: c})
		}
	}
	return pairs
}

// SweepCCD 连续碰撞检测：检测对象 id 从 from 移动到 to 的路径上
// 与场景中静态障碍物（Mask 包含 GroupWall）的首个碰撞点，返回碰撞的障碍物 id 与碰撞时间 t∈[0,1]。
// 若全程无碰撞则返回 ("", 1.0)。
func (g *Grid) SweepCCD(id string, from, to Vec2) (hit string, t float64) {
	g.mu.RLock()
	ib, ok := g.items[id]
	g.mu.RUnlock()
	if !ok {
		return "", 1.0
	}
	// 用移动路径构造 sweep AABB 做广相查询
	sweepBox := AABB{
		MinX: math.Min(from.X, to.X) - (ib.MaxX-ib.MinX)/2,
		MaxX: math.Max(from.X, to.X) + (ib.MaxX-ib.MinX)/2,
		MinY: math.Min(from.Y, to.Y) - (ib.MaxY-ib.MinY)/2,
		MaxY: math.Max(from.Y, to.Y) + (ib.MaxY-ib.MinY)/2,
	}
	candidates := g.QueryRegion(sweepBox)

	segment := Segment{from.X, from.Y, to.X, to.Y}
	bestT := 1.0
	bestHit := ""
	for _, c := range candidates {
		if c == id {
			continue
		}
		g.mu.RLock()
		bc := g.items[c]
		cmask := g.masks[c]
		g.mu.RUnlock()
		// 只检测静态障碍物
		if cmask&GroupWall == 0 {
			continue
		}
		t := segmentAABBSweep(segment, bc)
		if t >= 0 && t < bestT {
			bestT = t
			bestHit = c
		}
	}
	return bestHit, bestT
}

// segmentAABBSweep 用 slab 法计算线段到 AABB 的首个碰撞 t∈[0,1]。
// 若无碰撞返回 -1。
func segmentAABBSweep(s Segment, b AABB) float64 {
	dx, dy := s.BX-s.AX, s.BY-s.AY
	tmin, tmax := 0.0, 1.0
	if !slabSweep(s.AX, dx, b.MinX, b.MaxX, &tmin, &tmax) {
		return -1
	}
	if !slabSweep(s.AY, dy, b.MinY, b.MaxY, &tmin, &tmax) {
		return -1
	}
	if tmax < tmin {
		return -1
	}
	return tmin
}

func slabSweep(start, delta, min, max float64, tmin, tmax *float64) bool {
	if delta == 0 {
		return start >= min && start <= max
	}
	t1 := (min - start) / delta
	t2 := (max - start) / delta
	if t1 > t2 {
		t1, t2 = t2, t1
	}
	if t1 > *tmin {
		*tmin = t1
	}
	if t2 < *tmax {
		*tmax = t2
	}
	return *tmax >= *tmin
}

// Len 返回登记对象数（自省）。
func (g *Grid) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.items)
}
