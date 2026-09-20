package mapdata

import (
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/geom"
)

// spawnClearance 出生点要求的"空旷半径"（格）：中心格与它的 ±1 圈邻居都要可走。
// 为什么需要：客户端本地碰撞按**角色半径**采样（半径 0.35m，八个方向各取一点），
// 只要脚下有一圈挨着阻挡格，玩家就"站不住"⇒ 被本地碰撞锁死。
const spawnClearance = 1

// spawnSearchRadius 找净空点的最大搜索半径（格）。
const spawnSearchRadius = 24

// sanitizeSpawns 把出生点挪到"有净空"的格中心。
//
// ★ 为什么必须做（真实事故）：导出的出生点落在 **建筑与外墙的夹角** (60,60) 上 ——
// 对位图来说是"可走"，但角色的圆形占位有一半压在建筑格上：
// 客户端 `CanStand(半径 0.35)` = false ⇒ 玩家一出生就**只能原地跑动画**（位置一动不动），
// 朝建筑方向的两条轴位移被完全吃掉（实测 Resolve -X/-Z 位移 = 0.000m）。
// 这类"点可走但站不住"的位置，服务端必须自己兜住，不能指望导出端永远给得准。
func (m *Map) sanitizeSpawns() {
	if m == nil || len(m.Spawns) == 0 {
		return
	}
	for i := range m.Spawns {
		sp := m.Spawns[i]
		if m.hasClearance(sp.X, sp.Z) {
			continue
		}
		fixed, ok := m.findClearance(sp.X, sp.Z)
		if !ok {
			// 非预期分支：整张图找不到净空点（地图数据异常）→ 必须留痕，不能静默
			logger.Warnf("mapdata: 出生点 %d (%.1f,%.1f) 无净空且半径 %d 格内找不到替代点，保持原值",
				i, sp.X, sp.Z, spawnSearchRadius)
			continue
		}
		logger.Warnf("mapdata: 出生点 %d (%.1f,%.1f) 贴墙/贴边（角色半径占不下）→ 已挪到 (%.1f,%.1f)",
			i, sp.X, sp.Z, fixed.X, fixed.Z)
		m.Spawns[i] = fixed
	}
}

// hasClearance 该世界坐标的所在格及其 ±spawnClearance 圈邻居是否全部可走（且不越界）。
func (m *Map) hasClearance(x, z float64) bool {
	ix, iz, ok := m.cellOf(x, z)
	if !ok {
		return false
	}
	for dz := -spawnClearance; dz <= spawnClearance; dz++ {
		for dx := -spawnClearance; dx <= spawnClearance; dx++ {
			nx, nz := ix+dx, iz+dz
			if nx < 0 || nz < 0 || nx >= m.width || nz >= m.depth {
				return false
			}
			if !m.cellWalkable(nx, nz) {
				return false
			}
		}
	}
	return true
}

// cellCenter 格中心的世界坐标（出生点一律落在格心，避免踩在格边界上）。
func (m *Map) cellCenter(ix, iz int) geom.Vec3 {
	return geom.Vec3{
		X: m.Origin.X + (float64(ix)+0.5)*m.CellSize,
		Y: m.Origin.Y,
		Z: m.Origin.Z + (float64(iz)+0.5)*m.CellSize,
	}
}

// findClearance 从 (x,z) 所在格向外按**环形**搜索最近的净空格，返回其格中心。
// 按环（而不是整块）扫是为了"就近"：先看 1 格圈，再看 2 格圈 …… 找到即返回。
func (m *Map) findClearance(x, z float64) (geom.Vec3, bool) {
	ix0, iz0, ok := m.cellOf(x, z)
	if !ok {
		return geom.Vec3{}, false
	}
	for r := 1; r <= spawnSearchRadius; r++ {
		for dz := -r; dz <= r; dz++ {
			for dx := -r; dx <= r; dx++ {
				if absInt(dx) != r && absInt(dz) != r {
					continue // 只扫环边，内圈上一轮已经看过
				}
				ix, iz := ix0+dx, iz0+dz
				if ix < 0 || iz < 0 || ix >= m.width || iz >= m.depth {
					continue
				}
				c := m.cellCenter(ix, iz)
				if m.hasClearance(c.X, c.Z) {
					return c, true
				}
			}
		}
	}
	return geom.Vec3{}, false
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
