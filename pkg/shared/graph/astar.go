// Package graph 提供图搜索的通用内核。
//
// 全引擎**只有这一份 A\*** 实现。此前 AOI 网格寻路（`collide.NavGrid`）、路点图（`collide.WaypointGraph`）、
// 分层网格（`collide.NavGrid3`）、行为树寻路（`internal/domain/mmo/spatial/pathfinding`）各自抄了一遍
// 「open/closed 表 + gScore + 回溯 + 二叉堆」，四份堆、四条搜索循环、四处回溯上限防御。
// 现在搜索骨架收敛到这里，各调用方只提供两件事：
//
//   - `Edges(n)`：n 的出边（邻居 + 边代价）
//   - `Heuristic(n, goal)`：n 到目标的**估计**代价（admissible，不得高估，否则 A* 不再最优）
//
// 于是优先队列也只有一份泛型堆（取代了此前 2D / 3D 各一份）。
package graph

import "container/heap"

// Edge 一条出边：目标节点与通过代价（应为正数）。
type Edge[N comparable] struct {
	To   N
	Cost float64
}

// Graph A* 所需的最小图抽象。
type Graph[N comparable] interface {
	// Edges 返回 n 的全部出边。
	Edges(n N) []Edge[N]
	// Heuristic 返回 n 到 goal 的估计代价。必须**不高估**（admissible），否则结果可能非最优。
	// 允许「可采纳但不一致」的启发式：已封闭节点被更优路径改进时会重新打开。
	// goal 只是上下文提示：实现可以把它记在自己的状态里而忽略这个参数。
	Heuristic(n, goal N) float64
}

// Result 搜索结果。
type Result[N comparable] struct {
	// Path 含起点与终点的正序路径；不可达（或超出 maxExpand）为 nil。
	Path []N
	// Iterations 搜索规模计数：出队并计数的节点数。口径：
	//   - **不含**终点：命中 goal 时在计数前就返回；
	//   - **含**恰好触发 maxExpand、但尚未展开邻边的那一个节点（上限检查在展开之前）。
	// 因此「已展开邻边的节点数」= Iterations，而 Iterations >= maxExpand 时即按放弃处理。
	// 业务若拿它当「代价预算」，以本口径为准。
	Iterations int
}

// SearchAStar 在任意图上跑 A*。
//
// maxExpand <= 0 表示不限；达到上限即按「放弃」处理（Path 为 nil），
// 避免超大地图上一次寻路把 tick 卡死。
//
// 起终点相同返回单元素路径。
func SearchAStar[N comparable](g Graph[N], start, goal N, maxExpand int) Result[N] {
	open := &minHeap[N]{}
	heap.Init(open)

	first := &item[N]{node: start, f: g.Heuristic(start, goal)}
	best := map[N]*item[N]{start: first}
	closed := make(map[N]struct{})
	heap.Push(open, first)

	iter := 0
	for open.Len() > 0 {
		cur := heap.Pop(open).(*item[N])
		if _, done := closed[cur.node]; done {
			continue // 陈旧条目：该节点已用更优的 g 展开过
		}
		closed[cur.node] = struct{}{}
		if cur.node == goal {
			return Result[N]{Path: trace(cur), Iterations: iter}
		}
		iter++
		if maxExpand > 0 && iter >= maxExpand {
			break
		}
		for _, e := range g.Edges(cur.node) {
			tentative := cur.g + e.Cost
			if old, ok := best[e.To]; ok {
				if tentative >= old.g {
					continue
				}
				old.g = tentative
				old.f = tentative + g.Heuristic(e.To, goal)
				old.parent = cur
				if old.index >= 0 {
					// 仍在 open 中：原地降低 key（比重复入队省一次出队）。
					heap.Fix(open, old.index)
				} else {
					// 已封闭（出队后 index=-1）：更优路径改进了它，必须重新打开。
					// 否则「admissible 但非一致」的启发式下，经已封闭节点绕行的更优路径
					// 会被 closed 检查跳过，返回次优路径（与文档「admissible 即最优」矛盾）。
					delete(closed, e.To)
					heap.Push(open, old)
				}
				continue
			}
			it := &item[N]{
				node:   e.To,
				g:      tentative,
				f:      tentative + g.Heuristic(e.To, goal),
				parent: cur,
			}
			best[e.To] = it
			heap.Push(open, it)
		}
	}
	return Result[N]{Iterations: iter}
}

// trace 从终点沿 parent 链回溯出正序路径。
//
// 与旧实现「用 len(came)+1 当回溯上限、前驱缺失就放弃」的防御相比，这里更简单也更安全：
// parent 只在创建条目时赋值，链上每个节点都必然可达起点，不可能出现环或断链；
// 长度先数一遍再一次性分配，避免 append 反复扩容。
func trace[N comparable](goal *item[N]) []N {
	n := 0
	for it := goal; it != nil; it = it.parent {
		n++
	}
	out := make([]N, n)
	for it, i := goal, n-1; it != nil; it, i = it.parent, i-1 {
		out[i] = it.node
	}
	return out
}

// item open 表元素。index 由堆维护，用于 heap.Fix 降低 key。
type item[N comparable] struct {
	node   N
	g, f   float64
	parent *item[N]
	index  int
}

// minHeap 按 f 排序的最小堆。全引擎唯一的寻路优先队列。
type minHeap[N comparable] []*item[N]

func (h minHeap[N]) Len() int           { return len(h) }
func (h minHeap[N]) Less(i, j int) bool { return h[i].f < h[j].f }
func (h minHeap[N]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *minHeap[N]) Push(x any) {
	it := x.(*item[N])
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *minHeap[N]) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*h = old[:n-1]
	return it
}
