// 本文件提供**三维**几何原语与三维均匀网格宽相（Grid3），与 2D 部分
// （vec.go / shapes.go / grid.go）并存、互不影响：
//
//   - 2D 部分继续服务俯视类玩法（地面平推、平面 AOE、贴地碰撞）；
//   - 3D 部分服务多层地形 / 飞行 / 立体弹道避障。
//
// 为什么必须分成两套：2D 的 AABB{MinX,MinY,MaxX,MaxY} 里 MinY/MaxY 承载的是
// 世界 Z（见 mmo.bodyAABB），高度信息在类型上就没有位置；把它改造成三维会同时
// 改变所有既有调用点的语义。因此这里新增 AABB3/Sphere/Segment3 与 Grid3，
// 而不是给旧类型加一个 Z 字段。
//
// 坐标约定：Y 为高度（与 geom.Vec3、mover、AOI 一致），单位与 2D 部分同为「米」。
package collide

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"clover-server-engine/pkg/shared/geom"
)

// ===== 几何原语 =====

// AABB3 是三维轴对齐包围盒（Min 各分量 <= Max 各分量）。
type AABB3 struct {
	Min, Max geom.Vec3
}

// NewAABB3 以中心与半边长构造三维包围盒（半边长取绝对值的各分量）。
func NewAABB3(center, half geom.Vec3) AABB3 {
	h := geom.Vec3{X: math.Abs(half.X), Y: math.Abs(half.Y), Z: math.Abs(half.Z)}
	return AABB3{Min: center.Sub(h), Max: center.Add(h)}
}

// AABB3FromSphere 返回包住球体的最小轴对齐包围盒。
func AABB3FromSphere(s Sphere) AABB3 {
	r := geom.Vec3{X: s.R, Y: s.R, Z: s.R}
	return AABB3{Min: s.Center.Sub(r), Max: s.Center.Add(r)}
}

// Center 返回盒中心。
func (b AABB3) Center() geom.Vec3 { return b.Min.Add(b.Max).Scale(0.5) }

// Size 返回三轴边长。
func (b AABB3) Size() geom.Vec3 { return b.Max.Sub(b.Min) }

// Contains 判断点是否落在盒内（含边界）。
func (b AABB3) Contains(p geom.Vec3) bool {
	return p.X >= b.Min.X && p.X <= b.Max.X &&
		p.Y >= b.Min.Y && p.Y <= b.Max.Y &&
		p.Z >= b.Min.Z && p.Z <= b.Max.Z
}

// Expand 返回按半径沿三轴外扩后的包围盒。
func (b AABB3) Expand(r float64) AABB3 {
	d := geom.Vec3{X: r, Y: r, Z: r}
	return AABB3{Min: b.Min.Sub(d), Max: b.Max.Add(d)}
}

// Union 返回同时包含 b 与 o 的最小包围盒。
func (b AABB3) Union(o AABB3) AABB3 {
	return AABB3{Min: minVec(b.Min, o.Min), Max: maxVec(b.Max, o.Max)}
}

// Sphere 是三维球体（角色 / 投射物的默认碰撞外形）。
type Sphere struct {
	Center geom.Vec3
	R      float64
}

// Segment3 是三维线段 A→B。
type Segment3 struct {
	A, B geom.Vec3
}

// Dir 返回方向向量 B-A。
func (s Segment3) Dir() geom.Vec3 { return s.B.Sub(s.A) }

// Len 返回线段长度。
func (s Segment3) Len() float64 { return s.Dir().Len() }

// At 返回线段上参数 t（0=A，1=B）处的点。
func (s Segment3) At(t float64) geom.Vec3 { return s.A.Lerp(s.B, t) }

func minVec(a, b geom.Vec3) geom.Vec3 {
	return geom.Vec3{X: math.Min(a.X, b.X), Y: math.Min(a.Y, b.Y), Z: math.Min(a.Z, b.Z)}
}

func maxVec(a, b geom.Vec3) geom.Vec3 {
	return geom.Vec3{X: math.Max(a.X, b.X), Y: math.Max(a.Y, b.Y), Z: math.Max(a.Z, b.Z)}
}

// ===== 相交判定 =====

// AABB3vsAABB3 三维包围盒重叠判定（含边界接触）。
func AABB3vsAABB3(a, b AABB3) bool {
	return a.Min.X <= b.Max.X && a.Max.X >= b.Min.X &&
		a.Min.Y <= b.Max.Y && a.Max.Y >= b.Min.Y &&
		a.Min.Z <= b.Max.Z && a.Max.Z >= b.Min.Z
}

// SphereVsSphere 球体相交判定（含相切）。
func SphereVsSphere(a, b Sphere) bool {
	r := a.R + b.R
	return a.Center.Sub(b.Center).LenSq() <= r*r
}

// SphereVsAABB3 球体与包围盒相交判定：取球心在盒上的最近点比较距离。
func SphereVsAABB3(s Sphere, b AABB3) bool {
	nearest := geom.Vec3{
		X: geom.Clamp(s.Center.X, b.Min.X, b.Max.X),
		Y: geom.Clamp(s.Center.Y, b.Min.Y, b.Max.Y),
		Z: geom.Clamp(s.Center.Z, b.Min.Z, b.Max.Z),
	}
	return nearest.Sub(s.Center).LenSq() <= s.R*s.R
}

// Segment3AABB3Sweep 用 slab 法求线段与三维包围盒的首个相交参数 t∈[0,1]；不相交返回 -1。
//
// 这是三维弹道避障 / 视线的核心：三轴同时做 slab 裁剪，任一轴无交集即判定不相交，
// 因此线段会被楼板、天花板、墙体正确挡住，而不是像 2D 版那样只挡水平面的墙。
func Segment3AABB3Sweep(s Segment3, b AABB3) float64 {
	// 单轴裁剪逻辑与 2D 版完全一致（start/delta/min/max/tmin/tmax），
	// 直接复用 grid.go 的 slabSweep，不另写一份三维版本。
	tmin, tmax := 0.0, 1.0
	if !slabSweep(s.A.X, s.B.X-s.A.X, b.Min.X, b.Max.X, &tmin, &tmax) {
		return -1
	}
	if !slabSweep(s.A.Y, s.B.Y-s.A.Y, b.Min.Y, b.Max.Y, &tmin, &tmax) {
		return -1
	}
	if !slabSweep(s.A.Z, s.B.Z-s.A.Z, b.Min.Z, b.Max.Z, &tmin, &tmax) {
		return -1
	}
	if tmax < tmin {
		return -1
	}
	return tmin
}

// Segment3SphereHit 求线段与球体的首次相交参数 t∈[0,1]；未命中返回 ok=false。
// 线段起点已在球内时返回 t=0 并 ok=true。用于三维弹道命中判定。
func Segment3SphereHit(s Segment3, sp Sphere) (t float64, ok bool) {
	d := s.Dir()
	f := s.A.Sub(sp.Center)
	a := d.Dot(d)
	if a == 0 {
		// 退化成一个点：只需判断该点是否在球内。
		return 0, f.LenSq() <= sp.R*sp.R
	}
	c := f.Dot(f) - sp.R*sp.R
	if c <= 0 {
		// 起点已在球内（含球面）：立即判定命中，否则下面求出的会是**出球点**，
		// 把「贴脸命中」错报成一段距离之后的命中。
		return 0, true
	}
	b := 2 * f.Dot(d)
	disc := b*b - 4*a*c
	if disc < 0 {
		return 0, false
	}
	sq := math.Sqrt(disc)
	tt := (-b - sq) / (2 * a)
	if tt < 0 {
		tt = (-b + sq) / (2 * a)
	}
	if tt < 0 || tt > 1 {
		return 0, false
	}
	return tt, true
}

// ===== 三维宽相 =====

// cellKey3 是三维均匀网格的格子坐标。
type cellKey3 struct {
	x, y, z int
}

// Grid3 是三维均匀网格空间哈希（广相 / broad-phase），并发安全，API 与 2D 版 Grid 同形。
//
// 与 2D Grid 的差别只在「切格轴数」：Grid3 把 Y（高度）当作真正的切格轴，
// 于是多层楼的同一水平坐标会被切进不同格子，
//   - 楼上楼下的 AABB 不再互相误报（2D 版会，因为它们投影到同一 (X,Z)）；
//   - SweepCCD 的三维线段会被楼板/天花板挡住（立体弹道避障）。
type Grid3 struct {
	cell      float64
	mu        sync.RWMutex
	items     map[string]AABB3                 // id -> 三维包围盒
	cells     map[cellKey3]map[string]struct{} // 格子 -> id 集合
	itemCells map[string][]cellKey3            // id -> 占据的格子（Remove/Update 用）
	spheres   map[string]Sphere                // id -> 精确球体（可选，深检测用）
	masks     map[string]CollisionMask         // id -> 碰撞掩码
}

// NewGrid3 以给定格子边长构造三维空间网格（cell<=0 时回落为 1）。
func NewGrid3(cell float64) *Grid3 {
	if cell <= 0 {
		cell = 1
	}
	return &Grid3{
		cell:      cell,
		items:     make(map[string]AABB3),
		cells:     make(map[cellKey3]map[string]struct{}),
		itemCells: make(map[string][]cellKey3),
		spheres:   make(map[string]Sphere),
		masks:     make(map[string]CollisionMask),
	}
}

// SetSphere 为对象登记精确球体外形（深检测优先用它；不登记则退化为 AABB3 判定）。
func (g *Grid3) SetSphere(id string, s Sphere) {
	g.mu.Lock()
	g.spheres[id] = s
	g.mu.Unlock()
}

// SetMask 设置碰撞掩码（0 会被拒绝）。
func (g *Grid3) SetMask(id string, mask CollisionMask) error {
	if !mask.IsValid() {
		return fmt.Errorf("collide: invalid collision mask 0 for id=%s", id)
	}
	g.mu.Lock()
	g.masks[id] = mask
	g.mu.Unlock()
	return nil
}

func (g *Grid3) cellRange(b AABB3) (x0, y0, z0, x1, y1, z1 int) {
	x0 = int(math.Floor(b.Min.X / g.cell))
	x1 = int(math.Floor(b.Max.X / g.cell))
	y0 = int(math.Floor(b.Min.Y / g.cell))
	y1 = int(math.Floor(b.Max.Y / g.cell))
	z0 = int(math.Floor(b.Min.Z / g.cell))
	z1 = int(math.Floor(b.Max.Z / g.cell))
	return
}

// addToCells 把 id 登记到其 AABB3 覆盖的所有格子。
// 非法 AABB3（NaN/Inf/大跨度）在此被拒绝：三轴容量算式会因超大跨度溢出为负
// 而直接 panic / OOM，循环次数亦会爆炸（详见 geomguard.go）。
func (g *Grid3) addToCells(id string, b AABB3) {
	if !validAABB3v(b) {
		dropInvalidGeom("Grid3.addToCells", b)
		return
	}
	x0, y0, z0, x1, y1, z1 := g.cellRange(b)
	sx, sy, sz := cellSpanCount(x0, x1), cellSpanCount(y0, y1), cellSpanCount(z0, z1)
	if gridRangeTooLarge(sx, sy, sz) {
		dropInvalidGeom("Grid3.addToCells(span)", b)
		return
	}
	cells := make([]cellKey3, 0, int(sx*sy*sz))
	for cx := x0; cx <= x1; cx++ {
		for cy := y0; cy <= y1; cy++ {
			for cz := z0; cz <= z1; cz++ {
				ck := cellKey3{x: cx, y: cy, z: cz}
				set := g.cells[ck]
				if set == nil {
					set = make(map[string]struct{})
					g.cells[ck] = set
				}
				set[id] = struct{}{}
				cells = append(cells, ck)
			}
		}
	}
	g.itemCells[id] = cells
}

func (g *Grid3) removeFromCells(id string) {
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

// Insert 登记对象及其三位包围盒。幂等：已存在则先清理旧格子引用，
// 否则对象跨格后旧格子残留幽灵 id，导致 QueryRegion/Collisions 误报并让 cells 无限膨胀。
func (g *Grid3) Insert(id string, b AABB3) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.items[id]; exists {
		g.removeFromCells(id)
	}
	if !validAABB3v(b) {
		// 非法包围盒不入册（查询一律视为不存在），避免脏数据进入后续计算。
		dropInvalidGeom("Grid3.Insert", b)
		delete(g.items, id)
		return
	}
	g.items[id] = b
	g.addToCells(id, b)
}

// Update 更新对象包围盒（等价于先 Insert，语义上更明确）。
func (g *Grid3) Update(id string, b AABB3) { g.Insert(id, b) }

// Remove 注销对象，同时清掉球体与掩码——否则新对象复用同一 id 会继承旧外形。
func (g *Grid3) Remove(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.items, id)
	delete(g.spheres, id)
	delete(g.masks, id)
	g.removeFromCells(id)
}

// Len 返回登记对象数。
func (g *Grid3) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.items)
}

// QueryRegion 返回与给定三维区域相交候选格内的所有对象 id。
// 广相判定用 AABB3 重叠；命中球体等精确外形仍需调用方窄相过滤。
func (g *Grid3) QueryRegion(b AABB3) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !validAABB3v(b) {
		dropInvalidGeom("Grid3.QueryRegion", b)
		return nil
	}
	x0, y0, z0, x1, y1, z1 := g.cellRange(b)
	sx, sy, sz := cellSpanCount(x0, x1), cellSpanCount(y0, y1), cellSpanCount(z0, z1)
	if gridRangeTooLarge(sx, sy, sz) {
		dropInvalidGeom("Grid3.QueryRegion(span)", b)
		return nil
	}
	seen := make(map[string]struct{}, 8)
	var out []string
	for cx := x0; cx <= x1; cx++ {
		for cy := y0; cy <= y1; cy++ {
			for cz := z0; cz <= z1; cz++ {
				set := g.cells[cellKey3{x: cx, y: cy, z: cz}]
				if set == nil {
					continue
				}
				for id := range set {
					if _, ok := seen[id]; ok {
						continue
					}
					if AABB3vsAABB3(g.items[id], b) {
						seen[id] = struct{}{}
						out = append(out, id)
					}
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// QuerySphere 返回与球体相交的对象 id（用已登记的球体外形做精确判定，
// 未登记球体的对象退化为「AABB3 与球体相交」）。
func (g *Grid3) QuerySphere(s Sphere) []string {
	cands := g.QueryRegion(AABB3FromSphere(s))
	g.mu.RLock()
	spheresSnap := make(map[string]Sphere, len(g.spheres))
	for id, sp := range g.spheres {
		spheresSnap[id] = sp
	}
	itemsSnap := make(map[string]AABB3, len(cands))
	for _, id := range cands {
		itemsSnap[id] = g.items[id]
	}
	g.mu.RUnlock()

	out := make([]string, 0, len(cands))
	for _, id := range cands {
		if sp, ok := spheresSnap[id]; ok {
			if SphereVsSphere(s, sp) {
				out = append(out, id)
			}
			continue
		}
		if SphereVsAABB3(s, itemsSnap[id]) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// QueryPoint 返回包含该点的对象 id。
func (g *Grid3) QueryPoint(p geom.Vec3) []string {
	return g.QueryRegion(AABB3{Min: p, Max: p})
}

// Nearby 返回包围盒与 id 自身包围盒重叠的候选 id（不含自身，且已做 AABB3 窄相）。
func (g *Grid3) Nearby(id string) []string {
	g.mu.RLock()
	b, ok := g.items[id]
	g.mu.RUnlock()
	if !ok {
		return nil
	}
	cands := g.QueryRegion(b)
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		if c != id {
			out = append(out, c)
		}
	}
	return out
}

// Collisions 返回所有 AABB3 相互重叠的对象对。
func (g *Grid3) Collisions() []Pair {
	g.mu.RLock()
	ids := make([]string, 0, len(g.items))
	snap := make(map[string]AABB3, len(g.items))
	for id, b := range g.items {
		ids = append(ids, id)
		snap[id] = b
	}
	g.mu.RUnlock()

	sort.Strings(ids)
	var pairs []Pair
	for _, a := range ids {
		ba, okA := snap[a]
		if !okA {
			continue
		}
		for _, c := range g.QueryRegion(ba) {
			if c <= a {
				continue
			}
			bc, okC := snap[c]
			if !okC {
				continue
			}
			if AABB3vsAABB3(ba, bc) {
				pairs = append(pairs, Pair{A: a, B: c})
			}
		}
	}
	return pairs
}

// DeepCollisions 返回所有深层碰撞对：先按 CollisionMask 过滤，
// 双方都登记了球体时用球-球精确判定，否则退化为 AABB3 重叠。
func (g *Grid3) DeepCollisions() []Pair {
	g.mu.RLock()
	ids := make([]string, 0, len(g.items))
	itemsSnap := make(map[string]AABB3, len(g.items))
	for id, b := range g.items {
		ids = append(ids, id)
		itemsSnap[id] = b
	}
	maskSnap := make(map[string]CollisionMask, len(g.masks))
	for id, m := range g.masks {
		maskSnap[id] = m
	}
	sphereSnap := make(map[string]Sphere, len(g.spheres))
	for id, s := range g.spheres {
		sphereSnap[id] = s
	}
	g.mu.RUnlock()

	sort.Strings(ids)
	var pairs []Pair
	for _, a := range ids {
		ba, okA := itemsSnap[a]
		if !okA {
			continue
		}
		for _, c := range g.QueryRegion(ba) {
			if c <= a {
				continue
			}
			// 掩码过滤：只有双方都登记了掩码且无交集时才跳过（与 2D 版语义一致）。
			if len(maskSnap) > 0 {
				ma, mokA := maskSnap[a]
				mc, mokC := maskSnap[c]
				if mokA && mokC && (ma&mc) == 0 {
					continue
				}
			}
			bc, okC := itemsSnap[c]
			if !okC {
				continue
			}
			if !AABB3vsAABB3(ba, bc) {
				continue
			}
			sa, hasA := sphereSnap[a]
			sc, hasC := sphereSnap[c]
			if hasA && hasC {
				if !SphereVsSphere(sa, sc) {
					continue
				}
			}
			pairs = append(pairs, Pair{A: a, B: c})
		}
	}
	return pairs
}

// SweepCCD 三维连续碰撞检测：检测对象 id 从 from 移动到 to 的路径上，
// 与场景中静态障碍（Mask 含 GroupWall）的首个碰撞点，返回障碍 id 与碰撞参数 t∈[0,1]。
// 全程无碰撞返回 ("", 1.0)。
//
// 这是立体弹道避障 / 真实视线判定的入口：与 2D 版的区别是线段在三维空间求解，
// 楼板与天花板也会参与遮挡（2D 版只看水平面的墙）。
func (g *Grid3) SweepCCD(id string, from, to geom.Vec3) (hit string, t float64) {
	g.mu.RLock()
	ib, ok := g.items[id]
	g.mu.RUnlock()
	if !ok {
		return "", 1.0
	}
	half := ib.Size().Scale(0.5)
	// 用移动路径的包围盒做广相查询（沿三轴各外扩半个盒子尺寸）。
	sweepBox := AABB3{
		Min: minVec(from, to).Sub(half),
		Max: maxVec(from, to).Add(half),
	}
	candidates := g.QueryRegion(sweepBox)

	segment := Segment3{A: from, B: to}
	bestT := 1.0
	bestHit := ""
	for _, c := range candidates {
		if c == id {
			continue
		}
		g.mu.RLock()
		bc, hasBox := g.items[c]
		cmask := g.masks[c]
		g.mu.RUnlock()
		if !hasBox || cmask&GroupWall == 0 {
			continue
		}
		ht := Segment3AABB3Sweep(segment, bc)
		if ht >= 0 && ht < bestT {
			bestT = ht
			bestHit = c
		}
	}
	return bestHit, bestT
}

// Raycast 三维射线/线段求交：返回路径上第一个被 GroupWall 挡住的对象 id 与参数 t。
// 与 SweepCCD 的区别是不排除任何对象（没有「自身」概念），适合纯视线判定。
func (g *Grid3) Raycast(from, to geom.Vec3) (hit string, t float64) {
	sweepBox := AABB3{Min: minVec(from, to), Max: maxVec(from, to)}
	candidates := g.QueryRegion(sweepBox)

	segment := Segment3{A: from, B: to}
	bestT := 1.0
	bestHit := ""
	for _, c := range candidates {
		g.mu.RLock()
		bc, hasBox := g.items[c]
		cmask := g.masks[c]
		g.mu.RUnlock()
		if !hasBox || cmask&GroupWall == 0 {
			continue
		}
		ht := Segment3AABB3Sweep(segment, bc)
		if ht >= 0 && ht < bestT {
			bestT = ht
			bestHit = c
		}
	}
	return bestHit, bestT
}
