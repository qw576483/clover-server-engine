package collide

import (
	"math"

	"clover-server-engine/pkg/shared/graph"
)

// Waypoint 是路点图中的一个节点。
type Waypoint struct {
	ID  int  // 节点唯一 id
	Pos Vec2 // 世界坐标
}

// WaypointGraph 是无向加权路点图（NavMesh 的简化：把可行走区域抽象为点 + 边）。
// 把实体位置吸附到最近路点，再在图上跑 A* 得到路点序列。
type WaypointGraph struct {
	nodes map[int]Waypoint
	edges map[int]map[int]float64 // 邻接表：from -> to -> cost（无向，双向存储）
}

// NewWaypointGraph 构造空路点图。
func NewWaypointGraph() *WaypointGraph {
	return &WaypointGraph{
		nodes: make(map[int]Waypoint),
		edges: make(map[int]map[int]float64),
	}
}

// AddNode 登记一个路点（重复 id 覆盖）。
func (g *WaypointGraph) AddNode(w Waypoint) {
	g.nodes[w.ID] = w
}

// AddEdge 增加一条无向边（a-b，cost 为通过代价，通常取两端欧氏距离）。
func (g *WaypointGraph) AddEdge(a, b int, cost float64) {
	if _, ok := g.edges[a]; !ok {
		g.edges[a] = make(map[int]float64)
	}
	if _, ok := g.edges[b]; !ok {
		g.edges[b] = make(map[int]float64)
	}
	g.edges[a][b] = cost
	g.edges[b][a] = cost
}

// Nearest 返回离 pos 最近路点的 id（图为空返回 false）。
func (g *WaypointGraph) Nearest(pos Vec2) (int, bool) {
	best := -1
	bestD := math.Inf(1)
	for id, w := range g.nodes {
		if d := w.Pos.Dist(pos); d < bestD {
			bestD = d
			best = id
		}
	}
	if best < 0 {
		return 0, false
	}
	return best, true
}

// FindPath 从 from 吸附到最近路点、to 吸附到最近路点，跑 A*（欧氏距离启发）返回路点坐标序列。
// 起点终点相同返回单点；不可达或图为空返回 (nil, false)。
func (g *WaypointGraph) FindPath(from, to Vec2) ([]Vec2, bool) {
	start, ok1 := g.Nearest(from)
	goal, ok2 := g.Nearest(to)
	if !ok1 || !ok2 {
		return nil, false
	}
	if start == goal {
		return []Vec2{g.nodes[start].Pos}, true
	}

	// 搜索走 pkg/shared/graph 的通用 A* 内核（全引擎唯一一份）。
	res := graph.SearchAStar[int](waypointGraph{g: g, goalPos: g.nodes[goal].Pos}, start, goal, maxWaypointExpand)
	if res.Path == nil {
		return nil, false
	}
	out := make([]Vec2, len(res.Path))
	for i, id := range res.Path {
		out[i] = g.nodes[id].Pos
	}
	return out, true
}

// maxWaypointExpand 路点图搜索的展开上限（防止异常图把 tick 卡死）。
const maxWaypointExpand = 1 << 20

// waypointGraph 把路点图适配成 A* 图：节点是路点 id，出边取邻接表，启发用欧氏距离。
type waypointGraph struct {
	g       *WaypointGraph
	goalPos Vec2 // 同一次搜索的启发必须基于同一目标，故记在结构体里
}

// Edges 返回路点 id 的全部邻边（无向图已双向存储）。
func (w waypointGraph) Edges(id int) []graph.Edge[int] {
	adj := w.g.edges[id]
	out := make([]graph.Edge[int], 0, len(adj))
	for nb, cost := range adj {
		out = append(out, graph.Edge[int]{To: nb, Cost: cost})
	}
	return out
}

// Heuristic 到目标的欧氏距离（边代价通常也取欧氏距离，故不高估）。
func (w waypointGraph) Heuristic(id, _ int) float64 {
	return w.g.nodes[id].Pos.Dist(w.goalPos)
}
