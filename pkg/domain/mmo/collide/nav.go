package collide

import (
	"math"

	"github.com/qw576483/clover-server-engine/pkg/shared/graph"
)

// NavGrid 是基于网格的导航图（寻路 / 视线 / 沿线段滑行），
// 自包含、零依赖的网格实现，供场景移动、怪物 AI、碰撞推送复用。
type NavGrid struct {
	w, h      int
	blocked   []bool
	diagonal  bool
	maxExpand int
	heuristic func(ax, ay, bx, by int) float64
}

// PathOption 配置寻路行为。
type PathOption func(*NavGrid)

// WithDiagonal 允许八方向（含对角）移动；默认四方向。
func WithDiagonal(ok bool) PathOption { return func(n *NavGrid) { n.diagonal = ok } }

// WithMaxExpand 限制 A* 扩展节点数（0=不限），用于防止超大地图卡死。
func WithMaxExpand(n int) PathOption { return func(g *NavGrid) { g.maxExpand = n } }

// WithHeuristic 注入自定义启发函数（默认 octile / manhattan，取决于是否对角）。
func WithHeuristic(h func(ax, ay, bx, by int) float64) PathOption {
	return func(g *NavGrid) { g.heuristic = h }
}

// NewNavGrid 构造 w×h 的导航网格（全可走）。
func NewNavGrid(w, h int) *NavGrid {
	return &NavGrid{
		w: w, h: h,
		blocked:  make([]bool, w*h),
		diagonal: false,
	}
}

func (n *NavGrid) idx(x, y int) int { return y*n.w + x }

// InBounds 坐标是否在网格内。
func (n *NavGrid) InBounds(x, y int) bool {
	return x >= 0 && y >= 0 && x < n.w && y < n.h
}

// SetBlocked 设置某格是否阻挡。
func (n *NavGrid) SetBlocked(x, y int, b bool) {
	if !n.InBounds(x, y) {
		return
	}
	n.blocked[n.idx(x, y)] = b
}

// IsWalkable 该格是否可走（在界内且未阻挡）。
func (n *NavGrid) IsWalkable(x, y int) bool {
	return n.InBounds(x, y) && !n.blocked[n.idx(x, y)]
}

func (n *NavGrid) defaultHeuristic(ax, ay, bx, by int) float64 {
	dx, dy := math.Abs(float64(ax-bx)), math.Abs(float64(ay-by))
	if n.diagonal {
		// octile 距离
		if dx > dy {
			return dx + (math.Sqrt2-1)*dy
		}
		return dy + (math.Sqrt2-1)*dx
	}
	return dx + dy // manhattan
}

// FindPath 用 A* 求从 (sx,sy) 到 (tx,ty) 的路径，返回经过的格子中心坐标（含起终点）。
// 起点/终点不可走、越界或不可达时返回 nil。
func (n *NavGrid) FindPath(sx, sy, tx, ty int, opts ...PathOption) []Vec2 {
	g := &NavGrid{w: n.w, h: n.h, blocked: n.blocked, diagonal: n.diagonal, maxExpand: n.maxExpand, heuristic: n.heuristic}
	for _, o := range opts {
		o(g)
	}
	if !g.IsWalkable(sx, sy) || !g.IsWalkable(tx, ty) {
		return nil
	}

	h := g.defaultHeuristic
	if g.heuristic != nil {
		h = g.heuristic
	}

	// 搜索本身走 pkg/shared/graph 的通用 A* 内核（全引擎唯一一份）：
	// 本函数只负责「把网格适配成图」与「把下标路径还原成坐标」。
	res := graph.SearchAStar[int](navGridGraph{grid: g, h: h}, g.idx(sx, sy), g.idx(tx, ty), g.maxExpand)
	if res.Path == nil {
		return nil
	}
	out := make([]Vec2, len(res.Path))
	for i, id := range res.Path {
		out[i] = Vec2{float64(id%g.w) + 0.5, float64(id/g.w) + 0.5}
	}
	return out
}

// navGridGraph 把 NavGrid 适配成 A* 图：节点是格子线性下标，出边是四/八方向邻居。
type navGridGraph struct {
	grid *NavGrid
	h    func(ax, ay, bx, by int) float64
}

// Edges 返回格子 id 的全部可走邻居（含八方向防切角规则）。
func (n navGridGraph) Edges(id int) []graph.Edge[int] {
	cx, cy := id%n.grid.w, id/n.grid.w
	dirs := neighbors(n.grid.diagonal)
	out := make([]graph.Edge[int], 0, len(dirs))
	for _, d := range dirs {
		nx, ny := cx+d.dx, cy+d.dy
		if !n.grid.IsWalkable(nx, ny) {
			continue
		}
		if n.grid.diagonal && d.diag {
			// 防止切角：两个正交相邻格都必须可走
			if !n.grid.IsWalkable(cx+d.dx, cy) || !n.grid.IsWalkable(cx, cy+d.dy) {
				continue
			}
		}
		cost := 1.0
		if d.diag {
			cost = math.Sqrt2
		}
		out = append(out, graph.Edge[int]{To: n.grid.idx(nx, ny), Cost: cost})
	}
	return out
}

// Heuristic 用网格自身的启发函数（默认 octile / manhattan，可由 WithHeuristic 覆盖）。
func (n navGridGraph) Heuristic(id, goal int) float64 {
	return n.h(id%n.grid.w, id/n.grid.w, goal%n.grid.w, goal/n.grid.w)
}

// Walk 沿 (x0,y0)→(x1,y1) 以半格步进遍历每个经过的格子，对每格调用 fn。
// fn 返回 false 即停止（视为受阻）。返回是否走到终点。
func (n *NavGrid) Walk(x0, y0, x1, y1 float64, fn func(x, y int) bool) bool {
	dx, dy := x1-x0, y1-y0
	dist := math.Hypot(dx, dy)
	steps := int(math.Ceil(dist/0.5)) + 1
	for i := 0; i <= steps; i++ {
		t := 0.0
		if steps > 0 {
			t = float64(i) / float64(steps)
		}
		px, py := x0+dx*t, y0+dy*t
		cx, cy := int(math.Floor(px)), int(math.Floor(py))
		if !fn(cx, cy) {
			return false
		}
	}
	return true
}

// LineOfSight 判断 (x0,y0)→(x1,y1) 之间是否视线无阻挡（所有经过的格子均可走）。
func (n *NavGrid) LineOfSight(x0, y0, x1, y1 float64) bool {
	return n.Walk(x0, y0, x1, y1, func(x, y int) bool {
		return n.IsWalkable(x, y)
	})
}

// TraceWalk 沿线段向目标推进，返回最后一个可走点与该点是否即目标。
// 用于沿障碍滑行到最近可达点。
func (n *NavGrid) TraceWalk(x0, y0, x1, y1 float64) (Vec2, bool) {
	if !n.IsWalkable(int(math.Floor(x0)), int(math.Floor(y0))) {
		return Vec2{x0, y0}, false
	}
	var last Vec2
	reached := n.Walk(x0, y0, x1, y1, func(cx, cy int) bool {
		if !n.IsWalkable(cx, cy) {
			return false
		}
		last = Vec2{float64(cx) + 0.5, float64(cy) + 0.5}
		return true
	})
	return last, reached
}

// 方向表与优先队列
type dir struct {
	dx, dy int
	diag   bool
}

func neighbors(diag bool) []dir {
	orth := []dir{{1, 0, false}, {-1, 0, false}, {0, 1, false}, {0, -1, false}}
	if !diag {
		return orth
	}
	return append(orth, []dir{{1, 1, true}, {1, -1, true}, {-1, 1, true}, {-1, -1, true}}...)
}

// 注：本包不再自带优先队列 —— A* 的 open 表统一由 pkg/shared/graph 的泛型最小堆提供。
