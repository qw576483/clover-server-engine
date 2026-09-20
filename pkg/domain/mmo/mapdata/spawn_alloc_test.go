package mapdata

import (
	"testing"

	"clover-server-engine/pkg/shared/geom"
)

// 造一张 1×5（宽 5、深 1）的可走位图：bit0=(0,0) bit2=(2,0) bit3=(3,0) 可走，1 与 4 格阻挡。
// bits 是行主序、字节内 LSB 优先，0b00001101 = 格 0、2、3 可走。
func newWalkTestMap() *Map {
	return &Map{
		CellSize: 1,
		Origin:   geom.Vec3{},
		width:    5,
		depth:    1,
		bits:     []byte{0b0000_1101},
		Spawns:   []geom.Vec3{{X: 10}, {X: 20}, {X: 30}},
	}
}

// 轮转出生点：同 key 稳定、不同 key 摊开。
// 反面写法 `% (len+1)` 会让 0 号点被分到两次（本用例会抓到）。
func TestSpawnFor_RoundRobin(t *testing.T) {
	m := newWalkTestMap()
	cases := []struct {
		key  uint64
		want float64
	}{
		{0, 10}, {1, 20}, {2, 30},
		{3, 10}, {4, 20}, {5, 30}, // 回到 0 号点，且 0 号点只出现在 key%3==0
		{100, 20},
	}
	for _, c := range cases {
		if got := m.SpawnFor(c.key); got.X != c.want {
			t.Errorf("SpawnFor(%d) = %v, 期望 X=%v", c.key, got.X, c.want)
		}
	}
}

// 没有出生点时回落到原点（与 SpawnAt 同一口径），不返回越界垃圾。
func TestSpawnFor_NoSpawnsFallsBackToOrigin(t *testing.T) {
	m := newWalkTestMap()
	m.Spawns = nil
	m.Origin = geom.Vec3{X: -5, Y: 1, Z: 7}
	if got := m.SpawnFor(9); got != m.Origin {
		t.Fatalf("无出生点应回落到 Origin，实际 %v", got)
	}
	var nilMap *Map
	if got := nilMap.SpawnFor(1); got != (geom.Vec3{}) {
		t.Fatalf("nil Map 应返回零值，实际 %v", got)
	}
}

// 近点刷怪：center 本身可走就用它；不可走则沿 X 两侧试探。
func TestSpawnNear(t *testing.T) {
	m := newWalkTestMap()

	// (0,0) 可走 → 直接用。
	if got, ok := m.SpawnNear(geom.Vec3{X: 0}, 1, 4); !ok || got.X != 0 {
		t.Fatalf("center 可走应直接返回 (0,0)，实际 (%v,%v)", got, ok)
	}

	// (1,0) 阻挡 → 先试 +1 → x=2 可走。
	if got, ok := m.SpawnNear(geom.Vec3{X: 1}, 1, 4); !ok || got.X != 2 {
		t.Fatalf("应偏移到 x=2，实际 (%v,%v)", got, ok)
	}

	// (4,0) 阻挡 → +1 越界、-1 → x=3 可走（越界必须按不可走处理）。
	if got, ok := m.SpawnNear(geom.Vec3{X: 4}, 1, 4); !ok || got.X != 3 {
		t.Fatalf("越界侧应被跳过并回退到 x=3，实际 (%v,%v)", got, ok)
	}

	// 全不可走（步长过小、试探次数用尽，候选点仍落在同一个阻挡格里）
	// → 返回 (center,false)，不瞎猜落点。
	if got, ok := m.SpawnNear(geom.Vec3{X: 1.5}, 0.1, 2); ok || got.X != 1.5 {
		t.Fatalf("试探不出去时应原样返回 center 且 ok=false，实际 (%v,%v)", got, ok)
	}
}
