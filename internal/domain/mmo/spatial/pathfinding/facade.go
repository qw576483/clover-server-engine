// facade.go 是寻路内核的**门面包装真身**：把 internal 的具体实现适配成
// `pkg/domain/mmo/pathfinding` 的公开契约（`pathfinding.WalkAction` / `PathResult` / `Point`）。
//
// 为什么包装必须放在 internal：`pkg/**` 只允许做门面（别名 / 转发 / 极薄适配），
// 见 `结构规则.md` §5.1；这里的包装是行为实现体，所以真身在 internal，
// `pkg/domain/mmo/mmo.go` 只做变量转发。
//
// 适配面只有两类：
//   - `Point`：两侧各自命名（`struct{ X, Z int32 }`），底层类型相同 → 显式转换；
//   - `Blackboard`：门面是接口、真身是 `*ibtree.Blackboard` → 拆箱 + 非法实现拦下留痕。
package pathfinding

import (
	ibtree "clover-server-engine/internal/domain/mmo/gameplay/ai/btree"
	pkgbtree "clover-server-engine/pkg/domain/mmo/ai/btree"
	pkgcollide "clover-server-engine/pkg/domain/mmo/collide"
	pkgpf "clover-server-engine/pkg/domain/mmo/pathfinding"
	"clover-server-engine/pkg/foundation/logger"
)

// walkActionAdapter 把 internal 的 *WalkAction 适配成门面 pathfinding.WalkAction。
type walkActionAdapter struct{ inner *WalkAction }

// SetTarget 设置目标点（门面 Point → internal Point）。
func (w *walkActionAdapter) SetTarget(target pkgpf.Point) {
	w.inner.SetTarget(Point(target))
}

// Tick 驱动一步；黑板非法（nil / 非引擎实现）时拦下并留痕，返回失败让行为树自行处理。
func (w *walkActionAdapter) Tick(bb *pkgbtree.Blackboard) pkgbtree.Status {
	if bb == nil {
		// 解引用 nil 指针会直接 panic；按失败返回让调用方（行为树）自行处理该非法输入。
		logger.Warnf("mmo.WalkAction.Tick: nil blackboard pointer")
		return pkgbtree.StatusFailure
	}
	innerBB, ok := (*bb).(*ibtree.Blackboard)
	if !ok || innerBB == nil {
		// 非引擎黑板实现：内部 Tick 会解引用 nil 黑板 panic，此处显式拦下并留痕。
		logger.Warnf("mmo.WalkAction.Tick: blackboard is not an engine *btree.Blackboard impl (%T)", *bb)
		return pkgbtree.StatusFailure
	}
	return w.inner.Tick(innerBB)
}

// 编译期断言：适配器满足门面接口。
var _ pkgpf.WalkAction = (*walkActionAdapter)(nil)

// FindPathFacade 计算从 start 到 end 的路径（门面点类型，内部转换为引擎点类型）。
func FindPathFacade(nav *pkgcollide.NavGrid, start, end pkgpf.Point) pkgpf.PathResult {
	res := FindPath(nav, Point(start), Point(end))
	return pkgpf.PathResult{Points: toPathPoints(res.Points), Iterations: res.Iterations}
}

// FindPathWithCostFacade 同 FindPathFacade，但允许自定义移动代价函数。
func FindPathWithCostFacade(nav *pkgcollide.NavGrid, start, end pkgpf.Point, costFn func(pkgpf.Point, pkgpf.Point) int32) pkgpf.PathResult {
	wrappedCostFn := func(a, b Point) int32 {
		return costFn(pkgpf.Point(a), pkgpf.Point(b))
	}
	res := FindPathWithCost(nav, Point(start), Point(end), wrappedCostFn)
	return pkgpf.PathResult{Points: toPathPoints(res.Points), Iterations: res.Iterations}
}

// NewWalkActionFacade 构造「走到目标点」的行为树 Action（包装为门面接口）。
func NewWalkActionFacade(nav *pkgcollide.NavGrid, target pkgpf.Point, reachDist float32) pkgpf.WalkAction {
	return &walkActionAdapter{inner: NewWalkAction(nav, Point(target), reachDist)}
}

// NewRunActionFacade 构造「跑到目标点」的行为树 Action（包装为门面接口）。
func NewRunActionFacade(nav *pkgcollide.NavGrid, target pkgpf.Point, reachDist float32) pkgpf.WalkAction {
	return &walkActionAdapter{inner: NewRunAction(nav, Point(target), reachDist)}
}

// SimplifyPathFacade 对路径做拉直简化。
func SimplifyPathFacade(path []pkgpf.Point) []pkgpf.Point {
	internalPath := make([]Point, len(path))
	for i, p := range path {
		internalPath[i] = Point(p)
	}
	res := SimplifyPath(internalPath)
	return toPathPoints(res)
}

// DistanceSqFacade 两点距离的平方。
func DistanceSqFacade(a, b pkgpf.Point) float32 {
	return DistanceSq(Point(a), Point(b))
}

// DistanceFacade 两点欧氏距离。
func DistanceFacade(a, b pkgpf.Point) float32 {
	return Distance(Point(a), Point(b))
}

// ManhattanDistanceFacade 两点曼哈顿距离。
func ManhattanDistanceFacade(a, b pkgpf.Point) int32 {
	return ManhattanDistance(Point(a), Point(b))
}

// toPathPoints 把 internal 的点切片转换为门面点切片。
func toPathPoints(src []Point) []pkgpf.Point {
	dst := make([]pkgpf.Point, len(src))
	for i, p := range src {
		dst[i] = pkgpf.Point(p)
	}
	return dst
}
