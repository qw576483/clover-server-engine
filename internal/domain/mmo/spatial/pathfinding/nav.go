// Package pathfinding 提供基于 NavGrid 的寻路能力，输出 WalkTo/RunTo 行为树 Action。
// 针对 MMO 典型需求：A* 寻路 + 动态障碍更新 + 行为树集成。
package pathfinding

import (
	"math"
	"sync/atomic"

	"clover-server-engine/internal/domain/mmo/gameplay/ai/btree"
	pkgbtree "clover-server-engine/pkg/domain/mmo/ai/btree"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/graph"
)

// pathFailf 寻路包的异常降频日志（首次全量 + 之后每 1000 条一条）。
// 不可达可能每帧（按重规划冷却）发生，直打日志会刷屏。
var pathFailCount atomic.Uint64

func pathFailf(format string, args ...any) {
	n := pathFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// maxPathExpand A* 单次搜索的展开上限。0（不限）时大图上一次寻路可无上限扩张、
// 把 tick 卡死（内核注释明确该参数用于防卡死）；与 collide 路点图同口径取 1<<20，
// 超出即按不可达放弃，由下一次重规划再试。
const maxPathExpand = 1 << 20

// NavGridProvider 是寻路所需的最小导航网格接口。
type NavGridProvider interface {
	InBounds(x, y int) bool
	IsWalkable(x, y int) bool
}

// A* 寻路
// Point 二维整数坐标。
type Point struct{ X, Z int32 }

// PathResult 寻路结果。
type PathResult struct {
	Points     []Point // 从起点到终点的路径（含起终点），空切片表示不可达
	Iterations int     // 搜索迭代次数（调试用）
}

// defaultCost 八方向默认边代价（对角 14 / 直线 10，近似 √2 ≈ 1.414）。
func defaultCost(a, b Point) int32 {
	dx := a.X - b.X
	dz := a.Z - b.Z
	if dx < 0 {
		dx = -dx
	}
	if dz < 0 {
		dz = -dz
	}
	if dx > dz {
		return dx*10 + dz*4
	}
	return dz*10 + dx*4
}

// octileHeuristic 与默认边代价同一度量的 octile 启发（10/14）。可采纳且一致，A* 最优。
func octileHeuristic(a, b Point) float64 { return float64(heuristic(a, b)) }

// A* 路径搜索
// FindPath 在 NavGrid 上执行 A* 寻路，返回从 start 到 end 的最短路径。
// 若不可达返回空切片。
func FindPath(nav NavGridProvider, start, end Point) PathResult {
	return findPath(nav, start, end, defaultCost, octileHeuristic)
}

// FindPathWithCost 使用自定义代价函数寻路。costFn 返回从 a 到 b 的移动代价（≥1）。
//
// ⚠️ 自定义代价下没有通用的可采纳启发式：costFn 可以返回小于默认度量（10/14 每格）
// 的代价，此时固定 10/14 的启发式会高估、破坏 admissible，A* 会返回非最优路径。
// 因此这里退化为**零启发式（Dijkstra）**：保证最优，代价是搜索规模更大。
func FindPathWithCost(nav NavGridProvider, start, end Point, costFn func(Point, Point) int32) PathResult {
	return findPath(nav, start, end, costFn, nil)
}

func findPath(nav NavGridProvider, start, end Point, costFn func(Point, Point) int32, h func(a, b Point) float64) PathResult {
	// 起终点相同
	if start.X == end.X && start.Z == end.Z {
		return PathResult{Points: []Point{start}}
	}

	// 终点不可行走
	if !nav.IsWalkable(int(end.X), int(end.Z)) {
		return PathResult{}
	}

	// 搜索骨架走 pkg/shared/graph 的通用 A* 内核（全引擎唯一一份）：
	// 本函数只负责把「NavGridProvider + 代价函数 + 启发式」适配成图。
	res := graph.SearchAStar[Point](navGraph{nav: nav, end: end, cost: costFn, h: h}, start, end, maxPathExpand)
	if res.Path == nil {
		return PathResult{Iterations: res.Iterations} // 不可达（或超出展开上限）
	}
	return PathResult{Points: res.Path, Iterations: res.Iterations}
}

// navGraph 把 NavGridProvider + 自定义代价函数适配成 A* 图。
type navGraph struct {
	nav  NavGridProvider
	end  Point
	cost func(Point, Point) int32
	// h 是到目标的启发式（nil = 零启发式）。只有「边代价度量已知且不低于 h 的估计」
	// 时才可安全传入非零启发（如默认 10/14 配 octile）；自定义 costFn 传 nil。
	h func(a, b Point) float64
}

// Edges 返回八方向邻居（越界 / 不可走 / 斜向切角一律过滤）。
func (g navGraph) Edges(p Point) []graph.Edge[Point] {
	out := make([]graph.Edge[Point], 0, len(neighbourDirs))
	for _, dir := range neighbourDirs {
		nx, nz := p.X+dir.dx, p.Z+dir.dz
		if !g.nav.InBounds(int(nx), int(nz)) {
			continue
		}
		// 八方向斜向移动需检查相邻正交格，防止切角穿过障碍
		if dir.dx != 0 && dir.dz != 0 {
			if !g.nav.IsWalkable(int(p.X+dir.dx), int(p.Z)) ||
				!g.nav.IsWalkable(int(p.X), int(p.Z+dir.dz)) {
				continue
			}
		}
		if !g.nav.IsWalkable(int(nx), int(nz)) {
			continue
		}
		to := Point{X: nx, Z: nz}
		out = append(out, graph.Edge[Point]{To: to, Cost: float64(g.cost(p, to))})
	}
	return out
}

// Heuristic 返回配置的启发式（nil 表示零启发式，保证可采纳）。
// 目标记在结构体里（同一次搜索的启发必须基于同一目标），故忽略参数。
func (g navGraph) Heuristic(p, _ Point) float64 {
	if g.h == nil {
		return 0
	}
	return g.h(p, g.end)
}

// neighbourDir 邻居方向。
type neighbourDir struct{ dx, dz int32 }

var neighbourDirs = []neighbourDir{
	{0, 1}, {1, 1}, {1, 0}, {1, -1},
	{0, -1}, {-1, -1}, {-1, 0}, {-1, 1},
}

// heuristic 启发函数：八方向 Chebyshev + 对角修正（10/14）。
func heuristic(a, b Point) int32 {
	dx := a.X - b.X
	dz := a.Z - b.Z
	if dx < 0 {
		dx = -dx
	}
	if dz < 0 {
		dz = -dz
	}
	if dx > dz {
		return dx*10 + dz*4
	}
	return dz*10 + dx*4
}

// 注：本包不再自带优先队列 —— A* 的 open 表统一由 pkg/shared/graph 的泛型最小堆提供。

// 行为树集成：WalkTo / RunTo
// WalkAction 走到目标地点（Walking 速度）。
type WalkAction struct {
	nav      NavGridProvider
	target   Point   // 目标点
	path     []Point // 当前计算的路径
	pathIdx  int     // 当前路径节点索引
	replanCd int     // 重规划冷却（帧计数）
	reach    float32 // 到达判定距离平方
	// fracX/fracZ 是亚格位移的累积器：Point 是整数格，位移直接 int32(move) 截断
	// 会让 speed<1（格/帧）时每帧位移恒为 0，Action 永远 Running、永远到不了目标
	//（活锁）。小数部分逐帧累积，凑满一格才移动，既不跳格也不丢位移。
	fracX float32
	fracZ float32
}

// NewWalkAction 构造走到目标点的行为树 Action。
// reachDist 为判定到达的距离（>=0），0 使用默认值（1.0）。
func NewWalkAction(nav NavGridProvider, target Point, reachDist float32) *WalkAction {
	if reachDist <= 0 {
		reachDist = 1.0
	}
	return &WalkAction{
		nav:    nav,
		target: target,
		reach:  reachDist * reachDist,
	}
}

// RunAction 跑到目标地点（Running 速度，路径搜索与 WalkAction 相同）。
type RunAction = WalkAction

// NewRunAction 构造跑到目标点的行为树 Action。仅语义不同，内部与 WalkAction 一致。
func NewRunAction(nav NavGridProvider, target Point, reachDist float32) *RunAction {
	return NewWalkAction(nav, target, reachDist)
}

// SetTarget 更新目标点（改变目的地时触发重规划）。
func (a *WalkAction) SetTarget(target Point) {
	if a.target != target {
		a.target = target
		a.path = nil
		a.pathIdx = 0
	}
}

// Tick 实现 btree.Node。Blackboard 中需包含 "pos"（Point）与 "speed"（float32）。
func (a *WalkAction) Tick(bb *btree.Blackboard) pkgbtree.Status {
	posVal := bb.Get("pos")
	if posVal == nil {
		return pkgbtree.StatusFailure
	}
	pos, ok := posVal.(Point)
	if !ok {
		return pkgbtree.StatusFailure
	}

	// 到达目标
	if a.distance2(pos, a.target) <= a.reach {
		a.path = nil
		a.pathIdx = 0
		return pkgbtree.StatusSuccess
	}

	// 重规划冷却
	if a.replanCd > 0 {
		a.replanCd--
	} else if len(a.path) == 0 || a.pathIdx >= len(a.path) {
		res := FindPath(a.nav, pos, a.target)
		if len(res.Points) == 0 {
			// 寻路失败（不可达 / 超出展开上限）必须有留痕：本包此前全程无日志，
			// "怪站着不动"这种问题在线上不可观测。
			pathFailf("pathfinding: 目标不可达 nav=%T start=%v target=%v iter=%d", a.nav, pos, a.target, res.Iterations)
			return pkgbtree.StatusFailure
		}
		a.path = SimplifyPath(res.Points)
		a.pathIdx = 0
		a.replanCd = 30 // 约 0.5s 一次重规划
	}

	if len(a.path) == 0 || a.pathIdx >= len(a.path) {
		return pkgbtree.StatusFailure
	}

	// 沿路径移动
	speed, _ := bb.Get("speed").(float32)
	if speed <= 0 {
		speed = 2.0 // 默认 walking 速度
	}
	next := a.path[a.pathIdx]
	dx := float32(next.X - pos.X)
	dz := float32(next.Z - pos.Z)
	dist2 := dx*dx + dz*dz
	if dist2 <= a.reach {
		// 到达当前路径点，前进到下一个
		a.pathIdx++
		pos = next
	} else {
		// 逐步移动，不跳格穿墙：每帧最多移动 speed 距离（含亚格累积）
		inv := float32(1.0 / math.Sqrt(float64(dist2)))
		moveX := dx * inv * speed
		moveZ := dz * inv * speed
		// 钳制移动量不超过到目标的距离，防止越过路径点
		if moveX*dx+moveZ*dz > dist2 {
			moveX = dx
			moveZ = dz
		}
		// 亚格位移累积：直接 int32(move) 截断时，speed<1（格/帧）每帧位移恒为 0，
		// Action 永远 Running、永远到不了目标（活锁）。小数部分逐帧累积，凑满一格再动。
		a.fracX += moveX
		a.fracZ += moveZ
		if ix := int32(a.fracX); ix != 0 {
			a.fracX -= float32(ix)
			pos.X += ix
		}
		if iz := int32(a.fracZ); iz != 0 {
			a.fracZ -= float32(iz)
			pos.Z += iz
		}
	}
	bb.Set("pos", pos)
	return pkgbtree.StatusRunning
}

func (a *WalkAction) distance2(p1, p2 Point) float32 {
	dx := float32(p1.X - p2.X)
	dz := float32(p1.Z - p2.Z)
	return dx*dx + dz*dz
}

// 简化路径
// SimplifyPath 对路径做斜坡/共线简化：去除中间共线节点，减少步进抖动。
// 返回简化后的新切片（不修改原切片）。
func SimplifyPath(path []Point) []Point {
	if len(path) <= 2 {
		return append([]Point{}, path...)
	}
	result := make([]Point, 0, len(path))
	result = append(result, path[0])
	for i := 1; i < len(path)-1; i++ {
		prev, cur, nxt := path[i-1], path[i], path[i+1]
		dx1, dz1 := cur.X-prev.X, cur.Z-prev.Z
		dx2, dz2 := nxt.X-cur.X, nxt.Z-cur.Z
		// 方向相同才判定共线
		if dx1 == dx2 && dz1 == dz2 {
			continue
		}
		// 检查是否共线（叉积为 0）且方向一致：方向符号必须一起校验 ——
		// 纯水平/纯垂直的 180° 折返（如 dx1=1,dx2=-1,dz 全 0）叉积同样为 0，
		// 若只看「某轴全为 0」会把折返当共线删除，路径被改直穿过原拐点。
		if dx1*dz2 == dz1*dx2 &&
			((dx1 == 0 && dx2 == 0 && dz1*dz2 >= 0) ||
				(dz1 == 0 && dz2 == 0 && dx1*dx2 >= 0) ||
				(dx1*dx2 >= 0 && dz1*dz2 >= 0)) {
			continue
		}
		result = append(result, cur)
	}
	result = append(result, path[len(path)-1])
	return result
}

// 实用工具
// DistanceSq 返回两点间欧几里得距离的平方。
func DistanceSq(a, b Point) float32 {
	dx := float32(a.X - b.X)
	dz := float32(a.Z - b.Z)
	return dx*dx + dz*dz
}

// Distance 返回两点间欧几里得距离。
func Distance(a, b Point) float32 {
	return float32(math.Sqrt(float64(DistanceSq(a, b))))
}

// ManhattanDistance 曼哈顿距离。
func ManhattanDistance(a, b Point) int32 {
	dx := a.X - b.X
	dz := a.Z - b.Z
	if dx < 0 {
		dx = -dx
	}
	if dz < 0 {
		dz = -dz
	}
	return dx + dz
}
