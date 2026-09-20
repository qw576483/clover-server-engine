package collide

import "math"

// HeightField 是静态体素高度场：网格化可行走标记 + 每格高度区间。
// 提供 Walkable / Height 查询与 LineWalk 直线可行走检测。
//
// 动态障碍（运行时叠加的方块/圆柱/多边形阻挡）用 Block 叠加到可行走层；
// Unblock 释放。这对「临时封路 / 可破坏物」是引擎级中立能力。
type HeightField struct {
	cols, rows int
	cell       float64
	walk       []bool
	minH, maxH []float64
	blocks     []bool // 动态障碍叠加层（与 walk 同构，true=被挡）
}

// maxHeightFieldCells 单张高度场的格数上限（防御性：约 400 万格）。
// 超过上限或出现负值的尺寸一律夹成空场并留日志——否则 cols*rows 溢出为负时
// make([]bool, n) 直接 panic，大值则按天文数字分配内存。
const maxHeightFieldCells = 1 << 22

// NewHeightField 以列数/行数/格子边长构造高度场（cell>0）。
func NewHeightField(cols, rows int, cell float64) *HeightField {
	if cell <= 0 {
		cell = 1
	}
	if cols < 0 || rows < 0 || (cols > 0 && rows > maxHeightFieldCells/cols) {
		dropInvalidGeom("NewHeightField(size)", [2]int{cols, rows})
		cols, rows = 0, 0
	}
	n := cols * rows
	return &HeightField{
		cols:   cols,
		rows:   rows,
		cell:   cell,
		walk:   make([]bool, n),
		minH:   make([]float64, n),
		maxH:   make([]float64, n),
		blocks: make([]bool, n),
	}
}

func (h *HeightField) idx(cx, cy int) int { return cy*h.cols + cx }

// InBounds 判断格子坐标是否合法。
func (h *HeightField) InBounds(cx, cy int) bool {
	return cx >= 0 && cy >= 0 && cx < h.cols && cy < h.rows
}

// Cell 把世界坐标映射成格子坐标。
func (h *HeightField) Cell(x, z float64) (cx, cy int) {
	return int(math.Floor(x / h.cell)), int(math.Floor(z / h.cell))
}

// SetWalk 设置某格的可行走性与高度区间（minH/maxH 之间可站立）。
func (h *HeightField) SetWalk(cx, cy int, walk bool, minH, maxH float64) {
	if !h.InBounds(cx, cy) {
		return
	}
	i := h.idx(cx, cy)
	h.walk[i] = walk
	h.minH[i] = minH
	h.maxH[i] = maxH
}

// Block 在格子叠加一个动态障碍（阻挡可行走）。
func (h *HeightField) Block(cx, cy int) {
	if h.InBounds(cx, cy) {
		h.blocks[h.idx(cx, cy)] = true
	}
}

// Unblock 释放某格的动态障碍。
func (h *HeightField) Unblock(cx, cy int) {
	if h.InBounds(cx, cy) {
		h.blocks[h.idx(cx, cy)] = false
	}
}

// Walkable 判断世界坐标是否可行走（静态可行走 且 无动态障碍）。
func (h *HeightField) Walkable(x, z float64) bool {
	cx, cy := h.Cell(x, z)
	if !h.InBounds(cx, cy) {
		return false
	}
	i := h.idx(cx, cy)
	return h.walk[i] && !h.blocks[i]
}

// Height 返回世界坐标所在格的高度区间（不可行走格 ok=false）。
func (h *HeightField) Height(x, z float64) (minH, maxH float64, ok bool) {
	cx, cy := h.Cell(x, z)
	if !h.InBounds(cx, cy) {
		return 0, 0, false
	}
	i := h.idx(cx, cy)
	if !h.walk[i] || h.blocks[i] {
		return 0, 0, false
	}
	return h.minH[i], h.maxH[i], true
}

// LineWalk 从 (x0,z0) 朝 (x1,z1) 直线行走，roleRadius 用于避让障碍格
// （被挡格向外扩张 roleRadius 视为不可通行）。返回：
// - ok=true 且 (hx,hz) 为终点：整段可直达；
// - ok=false 且 (hx,hz) 为最远可行走点：途中被阻挡。
//
// 对应 MMO 引擎沿线段滑行到最近可达点的语义。
func (h *HeightField) LineWalk(x0, z0, x1, z1, roleRadius float64) (ok bool, hx, hz float64) {
	dx, dz := x1-x0, z1-z0
	dist := math.Hypot(dx, dz)
	if dist < 1e-9 {
		return h.Walkable(x0, z0), x0, z0
	}
	// 采样步长取格子半边长与半径的较小者，保证不跨过障碍格。
	step := h.cell / 2
	if r := roleRadius; r > 0 && r < step {
		step = r
	}
	// 防止 dist 极大 / step 极小时 Ceil(dist/step)+1 溢出为负或天文数字。
	// step 设下限，采样点数设上限（超过则退化为按上限均分，保证采样密度可接受）。
	const minStep = 1e-3
	if step < minStep {
		step = minStep
	}
	const maxSamples = 1 << 20
	n := int(math.Ceil(dist/step)) + 1
	if n < 1 || n > maxSamples {
		n = maxSamples
	}
	lastX, lastZ := x0, z0
	for i := 1; i <= n; i++ {
		t := float64(i) / float64(n)
		px, pz := x0+dx*t, z0+dz*t
		if !h.walkableWithRadius(px, pz, roleRadius) {
			return false, lastX, lastZ
		}
		lastX, lastZ = px, pz
	}
	return true, x1, z1
}

// walkableWithRadius 判断以 (x,z) 为中心、半径 r 的圆是否完全落在可行走格内。
func (h *HeightField) walkableWithRadius(x, z, r float64) bool {
	if !h.Walkable(x, z) {
		return false
	}
	if r <= 0 {
		return true
	}
	cx, cy := h.Cell(x, z)
	// 半径 r 最多覆盖 ceil(r/cell)+1 格的范围；逐个检查与圆相交的格子是否可走。
	n := int(math.Ceil(r/h.cell)) + 1
	r2 := r * r
	for dx := -n; dx <= n; dx++ {
		for dz := -n; dz <= n; dz++ {
			gx, gz := cx+dx, cy+dz
			if !h.circleIntersectsCell(x, z, r2, gx, gz) {
				continue
			}
			// 越界格视为「世界边界外的半无限可行走区」，否则 roleRadius>0 的角色
			// 永远无法贴到合法边缘格（圆总会与界外邻格相交而被判不可走）。
			if !h.InBounds(gx, gz) {
				continue
			}
			// 按格中心投影判定该格可行走性，而非格左下角——左下角落在格边界上，
			// 浮点 floor 可能归到相邻格，造成边缘误判。
			if !h.Walkable((float64(gx)+0.5)*h.cell, (float64(gz)+0.5)*h.cell) {
				return false
			}
		}
	}
	return true
}

// circleIntersectsCell 判断以 (x,z) 为中心、半径平方 r2 的圆是否与格子 (cx,cz) 相交。
func (h *HeightField) circleIntersectsCell(x, z, r2 float64, cx, cz int) bool {
	minX := float64(cx) * h.cell
	maxX := minX + h.cell
	minZ := float64(cz) * h.cell
	maxZ := minZ + h.cell
	closestX := clampf(x, minX, maxX)
	closestZ := clampf(z, minZ, maxZ)
	dx := x - closestX
	dz := z - closestZ
	return dx*dx+dz*dz <= r2
}
