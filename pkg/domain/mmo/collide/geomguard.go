package collide

import (
	"math"
	"sync/atomic"

	"clover-server-engine/pkg/foundation/logger"
)

// 本文件提供「非法几何输入」的统一防护：AABB 含 NaN/±Inf、Min>Max、
// 或格子跨度大到会溢出/爆炸时，拒绝登记并降频留痕。
//
// 背景（缺陷）：Grid / Grid3 的 addToCells 用 (x1-x0+1)*(...) 预分配容量，
// 而 cellRange 直接把 float 转 int —— 非法 AABB（NaN/Inf/单位错误/超大跨度）
// 会让容量算式溢出为负（make panic）或循环次数变成天文数字（事实上的死循环）。
// 畸形 mapdata 文件即可把这类坐标喂进来。

// 格子跨度上限（单轴 / 单次操作总量）。远超任何实际体积：
// 单对象在一个均匀网格里覆盖十六万格已属异常输入，按非法处理。
const (
	maxCellSpanPerAxis = 1 << 14 // 16384
	maxCellSpanTotal   = 1 << 18 // 262144
)

// isFinite 返回 f 是否为有限值（非 NaN / ±Inf）。
func isFinite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// validAABB2 报告二维 AABB 是否可安全入网格：分量有限且 Min<=Max。
func validAABB2(b AABB) bool {
	return isFinite(b.MinX) && isFinite(b.MinY) && isFinite(b.MaxX) && isFinite(b.MaxY) &&
		b.MinX <= b.MaxX && b.MinY <= b.MaxY
}

// validAABB3v 报告三维 AABB 是否可安全入网格：分量有限且 Min<=Max。
func validAABB3v(b AABB3) bool {
	return isFinite(b.Min.X) && isFinite(b.Max.X) &&
		isFinite(b.Min.Y) && isFinite(b.Max.Y) &&
		isFinite(b.Min.Z) && isFinite(b.Max.Z) &&
		b.Min.X <= b.Max.X && b.Min.Y <= b.Max.Y && b.Min.Z <= b.Max.Z
}

// cellSpanCount 以 int64 计算 [lo,hi] 覆盖的格数（防 int 溢出）。
// 非法跨度（hi<lo、非正、超单轴上限）返回 -1。
func cellSpanCount(lo, hi int) int64 {
	if hi < lo {
		return -1
	}
	span := int64(hi) - int64(lo) + 1
	if span <= 0 || span > maxCellSpanPerAxis {
		return -1
	}
	return span
}

// gridRangeTooLarge 报告各轴跨度乘积是否超过单次操作总量上限（0 表示任一轴非法）。
func gridRangeTooLarge(spans ...int64) bool {
	total := int64(1)
	for _, sp := range spans {
		if sp <= 0 {
			return true
		}
		if sp > maxCellSpanTotal {
			return true
		}
		if total > maxCellSpanTotal/sp {
			return true
		}
		total *= sp
	}
	return false
}

// droppedGeomCount 非法几何输入累计拒绝次数。
var droppedGeomCount atomic.Uint64

// dropInvalidGeom 记录并降频上报被拒绝的非法几何输入：
// 首次必打（可见），其后每 1024 次一条（防刷屏）。
func dropInvalidGeom(op string, detail any) {
	n := droppedGeomCount.Add(1)
	if n == 1 || n%1024 == 0 {
		logger.Warnf("collide: rejected invalid geometry op=%s total=%d detail=%v", op, n, detail)
	}
}
