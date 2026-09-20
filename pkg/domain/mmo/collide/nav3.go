// 本文件提供**分层三维寻路**（NavGrid3），与 2D 的 NavGrid 并存、互不影响。
//
// 为什么是「分层」而不是「体素」：
//   - 三维体素 A* 的内存与扩展节点数是 O(w·h·d)，多层地图里绝大多数高度是空气，
//     真正可走的只有薄薄几层楼板，体素化后 90% 以上的格子是永久阻挡 —— 纯浪费；
//   - 分层模型把地图表达成「若干张平面导航图 + 层间连接点」，
//     层内用成熟的八方向格步进（复用 NavGrid），层间用楼梯/坡道/电梯/跳点/传送连接。
//     内存 O(层数·w·h)，与 2D 同量级，且建图管线（§地图数据管线）也更好导出。
//
// 代价与启发函数都用**三维欧氏距离**，因此 A* 是 admissible 的：
// 层内直线步 1、对角步 √2，正好等于其三维长度；跨层连接代价 = 两端三维距离（可再加等待代价）。
package collide

import (
	"math"

	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
	"github.com/qw576483/clover-server-engine/pkg/shared/graph"
)

// NavLayer 分层导航中的一层：高度区间 [BaseY, TopY) 与该层的平面可行走格。
//
// 一个平面格只属于一层，层与层在水平投影上可以完全重叠（二楼盖在一楼上面），
// 这正是 2D 导航图表达不了的。
type NavLayer struct {
	BaseY float64  // 层底高度（该层路径点的 Y）
	TopY  float64  // 层顶高度（用于按高度定位层；可为 0 表示不参与定位）
	Grid  *NavGrid // 该层的平面可走格
}

// NavLink 层间连接：楼梯 / 坡道 / 电梯 / 跳点 / 传送。
// From/To 为两端所在层的平面**格坐标**（不是世界坐标）。
type NavLink struct {
	FromLayer, ToLayer int
	From, To           [2]int  // {x, z}
	Cost               float64 // 附加代价（0 = 仅按三维距离；电梯等待等可加大）
	Bidirectional      bool
}

// Nav3Option 配置分层导航行为。
type Nav3Option func(*NavGrid3)

// WithDiagonal3 层内是否允许八方向移动（默认四方向，与 NavGrid 一致）。
func WithDiagonal3(ok bool) Nav3Option { return func(n *NavGrid3) { n.diagonal = ok } }

// WithMaxExpand3 限制 A* 扩展节点数（0 = 不限），防止超大地图把 tick 卡住。
func WithMaxExpand3(n int) Nav3Option { return func(g *NavGrid3) { g.maxExpand = n } }

// NavGrid3 是分层三维导航网格：层内八方向 A*，层间走 NavLink。
type NavGrid3 struct {
	layers    []NavLayer
	links     []NavLink
	diagonal  bool
	maxExpand int
}

// NewNavGrid3 构造空的分层导航网格（用 AddLayer / AddLink 建图）。
func NewNavGrid3(opts ...Nav3Option) *NavGrid3 {
	g := &NavGrid3{}
	for _, o := range opts {
		if o != nil {
			o(g)
		}
	}
	return g
}

// AddLayer 追加一层并返回其层号。
func (n *NavGrid3) AddLayer(baseY, topY float64, grid *NavGrid) int {
	n.layers = append(n.layers, NavLayer{BaseY: baseY, TopY: topY, Grid: grid})
	return len(n.layers) - 1
}

// AddLink 追加一条层间连接。两端层号越界或指向的格子不可走会被忽略（返回 false）。
func (n *NavGrid3) AddLink(l NavLink) bool {
	if l.FromLayer < 0 || l.FromLayer >= len(n.layers) ||
		l.ToLayer < 0 || l.ToLayer >= len(n.layers) {
		return false
	}
	if !n.layers[l.FromLayer].Grid.IsWalkable(l.From[0], l.From[1]) ||
		!n.layers[l.ToLayer].Grid.IsWalkable(l.To[0], l.To[1]) {
		return false
	}
	n.links = append(n.links, l)
	return true
}

// LayerCount 返回层数。
func (n *NavGrid3) LayerCount() int { return len(n.layers) }

// LayerAt 返回高度 y 所属的层号；没有区间命中时返回距离 y 最近的层。
// 空网格返回 -1。
func (n *NavGrid3) LayerAt(y float64) int {
	if len(n.layers) == 0 {
		return -1
	}
	best, bestDist := 0, math.Inf(1)
	for i, l := range n.layers {
		// 区间命中优先：TopY<=BaseY 视为「无上界」，只按 BaseY 匹配到下一层之前。
		if l.TopY > l.BaseY && y >= l.BaseY && y < l.TopY {
			return i
		}
		d := math.Abs(y - l.BaseY)
		if d < bestDist {
			best, bestDist = i, d
		}
	}
	return best
}

// FindPath3 以世界坐标寻路，返回三维路径点（格中心 + 层 BaseY）。
// 起终点按 Y 自动定位所在层；起点/终点格不可走或层为空时返回 nil。
//
// 返回的路径**含起终点所在格的中心**（首元素 = 起点格中心，末元素 = 终点格中心），
// 但**不含**你传入的原始世界坐标本身；若要求精确到位，业务在首尾自行拼接真实起终点
// （mover.MoveTo 接受任意三维目标）。
func (n *NavGrid3) FindPath3(from, to geom.Vec3) []geom.Vec3 {
	fl := n.LayerAt(from.Y)
	tl := n.LayerAt(to.Y)
	if fl < 0 || tl < 0 {
		return nil
	}
	fx, fz := int(math.Floor(from.X)), int(math.Floor(from.Z))
	tx, tz := int(math.Floor(to.X)), int(math.Floor(to.Z))
	return n.FindPath3Cells(fl, fx, fz, tl, tx, tz)
}

// FindPath3Cells 以「层号 + 平面格坐标」寻路，返回三维路径点（含起终点所在格中心）。
func (n *NavGrid3) FindPath3Cells(fl, fx, fz, tl, tx, tz int) []geom.Vec3 {
	if fl < 0 || fl >= len(n.layers) || tl < 0 || tl >= len(n.layers) {
		return nil
	}
	startLayer, goalLayer := n.layers[fl], n.layers[tl]
	if startLayer.Grid == nil || goalLayer.Grid == nil {
		return nil
	}
	if !startLayer.Grid.IsWalkable(fx, fz) || !goalLayer.Grid.IsWalkable(tx, tz) {
		return nil
	}

	startKey := nav3Key{layer: fl, x: fx, z: fz}
	goalKey := nav3Key{layer: tl, x: tx, z: tz}
	goalPos := n.worldOf(goalLayer, tx, tz)

	// 搜索走 pkg/shared/graph 的通用 A* 内核（全引擎唯一一份）：
	// 本函数只负责「把分层导航适配成图」与「把节点还原成三维坐标」。
	res := graph.SearchAStar[nav3Key](nav3Graph{n: n, goalPos: goalPos}, startKey, goalKey, n.maxExpand)
	if res.Path == nil {
		return nil
	}
	out := make([]geom.Vec3, len(res.Path))
	for i, k := range res.Path {
		out[i] = n.worldOf(n.layers[k.layer], k.x, k.z)
	}
	return out
}

// nav3Key A* 节点：所在层 + 平面格坐标（Y 由层的 BaseY 决定，不参与节点身份）。
type nav3Key struct {
	layer, x, z int
}

// nav3Graph 把分层导航适配成 A* 图：出边 = 层内八方向 + 层间连接。
type nav3Graph struct {
	n       *NavGrid3
	goalPos geom.Vec3
}

// Edges 返回某节点的全部出边（层内邻居 + 楼梯/坡道/电梯/跳点/传送）。
func (g nav3Graph) Edges(k nav3Key) []graph.Edge[nav3Key] {
	layer := g.n.layers[k.layer]
	dirs := neighbors(g.n.diagonal)
	out := make([]graph.Edge[nav3Key], 0, len(dirs)+1)

	// ① 层内八方向（含防切角）
	for _, d := range dirs {
		nx, nz := k.x+d.dx, k.z+d.dy
		if !layer.Grid.IsWalkable(nx, nz) {
			continue
		}
		if d.diag && (!layer.Grid.IsWalkable(k.x+d.dx, k.z) ||
			!layer.Grid.IsWalkable(k.x, k.z+d.dy)) {
			continue
		}
		cost := 1.0
		if d.diag {
			cost = math.Sqrt2
		}
		out = append(out, graph.Edge[nav3Key]{To: nav3Key{layer: k.layer, x: nx, z: nz}, Cost: cost})
	}

	// ② 层间连接：代价 = 两端三维距离 + 附加代价（电梯等待等由 NavLink.Cost 表达）
	for _, lk := range g.n.links {
		target, ok := g.n.linkTarget(lk, k)
		if !ok {
			continue
		}
		// 连接端点可能在建图之后被 SetBlocked 阻塞，这里二次校验，避免走出「穿墙楼梯」。
		if !g.n.layers[target.layer].Grid.IsWalkable(target.x, target.z) {
			continue
		}
		cost := lk.Cost + g.n.worldOf(layer, k.x, k.z).
			Distance(g.n.worldOf(g.n.layers[target.layer], target.x, target.z))
		out = append(out, graph.Edge[nav3Key]{To: target, Cost: cost})
	}
	return out
}

// Heuristic 三维欧氏启发：与边的代价度量一致，因此不会高估（admissible）。
// 目标记在结构体里（同一次搜索的启发必须基于同一目标），故忽略参数。
func (g nav3Graph) Heuristic(k, _ nav3Key) float64 {
	return g.n.heuristic3(g.n.layers[k.layer], k.x, k.z, g.goalPos)
}

// linkTarget 判断当前所在格是否踩在某条连接的端点；是则返回另一端。
func (n *NavGrid3) linkTarget(lk NavLink, cur nav3Key) (nav3Key, bool) {
	if lk.FromLayer == cur.layer && lk.From[0] == cur.x && lk.From[1] == cur.z {
		return nav3Key{layer: lk.ToLayer, x: lk.To[0], z: lk.To[1]}, true
	}
	if lk.Bidirectional && lk.ToLayer == cur.layer && lk.To[0] == cur.x && lk.To[1] == cur.z {
		return nav3Key{layer: lk.FromLayer, x: lk.From[0], z: lk.From[1]}, true
	}
	return nav3Key{}, false
}

// worldOf 返回某层某格中心的世界坐标（Y 取该层 BaseY）。
func (n *NavGrid3) worldOf(l NavLayer, x, z int) geom.Vec3 {
	return geom.Vec3{X: float64(x) + 0.5, Y: l.BaseY, Z: float64(z) + 0.5}
}

// heuristic3 三维欧氏启发：与边的代价度量一致，因此不会高估（admissible）。
func (n *NavGrid3) heuristic3(l NavLayer, x, z int, goal geom.Vec3) float64 {
	return n.worldOf(l, x, z).Distance(goal)
}

// 注：本包不再自带「2D / 3D 各一份」的优先队列与回溯 —— 统一由 pkg/shared/graph 提供。
