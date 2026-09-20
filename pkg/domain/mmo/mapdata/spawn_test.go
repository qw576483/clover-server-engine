package mapdata

import (
	"testing"

	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// TestSpawnClearanceRelocates 单元验证：贴着阻挡格/边界的出生点会被挪到有净空的格心。
//
// 用**手写小地图**（不依赖导出产物）钉住算法本身：
//
//	6x6，左侧一堵墙（x=1, z=0..3 全阻挡），出生点给在 (1.5,0.5) 贴着墙。
func TestSpawnClearanceRelocates(t *testing.T) {
	const w, d = 6, 6
	cells := make([]bool, w*d)
	for i := range cells {
		cells[i] = true
	}
	for z := 0; z < 4; z++ { // 左侧一堵 3 格高的墙（z=0..3, x=1）
		cells[z*w+1] = false
	}

	m := &Map{
		CellSize: 1,
		Origin:   geom.Vec3{},
		Spawns:   []geom.Vec3{{X: 1.5, Y: 0, Z: 0.5}}, // 贴着墙（x=1 是阻挡）
		width:    w,
		depth:    d,
		bits:     packCells(cells),
	}
	for _, ok := range cells {
		if ok {
			m.WalkableCnt++
		} else {
			m.BlockedCnt++
		}
	}

	if m.hasClearance(1.5, 0.5) {
		t.Fatal("测试前提不成立：贴墙点不该有净空")
	}
	m.sanitizeSpawns()

	sp := m.Spawns[0]
	if !m.hasClearance(sp.X, sp.Z) {
		t.Errorf("出生点未被挪到有净空的位置: (%.2f,%.2f)", sp.X, sp.Z)
	}
	if !m.WalkableAt(sp.X, sp.Z) {
		t.Errorf("出生点必须可走: (%.2f,%.2f)", sp.X, sp.Z)
	}
	// 应就近挪（2.5 格内），不该被丢到地图另一头
	if absF(sp.X-1.5) > 2.5 || absF(sp.Z-0.5) > 2.5 {
		t.Errorf("出生点挪得太远: (%.2f,%.2f)，应就近（原 (1.50,0.50)）", sp.X, sp.Z)
	}
	t.Logf("出生点 (1.50,0.50) → (%.2f,%.2f)", sp.X, sp.Z)
}

// TestSpawnKeepWhenAlreadyClear 已经够宽敞的出生点必须**原样保留**（不要无端改动）。
func TestSpawnKeepWhenAlreadyClear(t *testing.T) {
	const w, d = 8, 8
	cells := make([]bool, w*d)
	for i := range cells {
		cells[i] = true
	}
	m := &Map{CellSize: 1, Spawns: []geom.Vec3{{X: 4.5, Y: 0, Z: 4.5}}, width: w, depth: d, bits: packCells(cells)}
	m.sanitizeSpawns()
	sp := m.Spawns[0]
	if sp.X != 4.5 || sp.Z != 4.5 {
		t.Errorf("净空点被无故挪动: (%.2f,%.2f)，期望 (4.50,4.50)", sp.X, sp.Z)
	}
}

func absF(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
