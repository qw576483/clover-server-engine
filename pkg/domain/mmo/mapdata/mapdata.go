// #nosec G304 -- 地图数据文件路径来自调用方/配置，非网络输入。

package mapdata

import (
	"fmt"
	"math"
	"math/bits"
	"os"

	"github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// navLayerHeight 单层导航网格的厚度（米）：从 origin.Y 起算。
// 当前格式是单层平地（origin.Y 即地面高度），多层/地形要等 FlagHeightField 段落地。
const navLayerHeight = 50.0

// WallTarget 是 ApplyTo 所需的**最小场景能力面**：能拿到三维碰撞宽相即可。
//
// 用最小接口而不是直接依赖 `mmo.Scene`：地图加载器不该绑上整个场景系统，
// 任何提供 `Collider3` 的实现（含测试替身）都能接。`mmo.Scene` 结构上满足本接口，
// 所以业务侧直接传场景对象即可，无需适配：
//
//	m, _ := mapdata.Load("Assets/MapData/map-city.bytes")
//	m.ApplyTo(city)                       // city 是 mmo.Scene
type WallTarget interface {
	Collider3() *collide.Grid3
}

// Map 是加载完成的逻辑地图。
//
// 数据是**只读**的：Load 之后不再变化，可被多场景 / 多 goroutine 并发查询。
type Map struct {
	// Version 数据格式版本。
	Version uint16
	// SceneID 逻辑地图 id（与 `mmo` 场景 id、客户端 CloverScene.SceneID 对齐）。
	SceneID uint64
	// Name 地图名。
	Name string
	// CellSize 格边长（米）。
	CellSize float64
	// Origin 位图原点：格子 (0,0) 的角（世界坐标）。
	Origin geom.Vec3
	// Spawns 出生点（已过净空校验，见 spawn.go）。
	Spawns []geom.Vec3
	// WalkableCnt 可走格数。
	WalkableCnt int
	// BlockedCnt 阻挡格数。
	BlockedCnt int
	// ColliderCnt 碰撞体数量。
	ColliderCnt int

	width     int
	depth     int
	bits      []byte // 可行走位图（行主序，字节内 LSB 优先）；直接引用文件切片，不展开
	colliders []collide.AABB3
	nav       *collide.NavGrid3
	source    string
}

// Load 从文件加载地图数据。
func Load(path string) (*Map, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取地图文件失败 %s: %w", path, err)
	}
	m, err := Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	m.source = path
	return m, nil
}

// Decode 由文件字节构造逻辑地图（含 NavGrid3 构建与出生点净化）。
//
// ★ 零拷贝：返回的 Map 直接引用 data 的位图切片，调用方在此后**不得再修改 data**。
// 需要长期持有又怕被改的场景，请自行拷贝一份再传进来。
func Decode(data []byte) (*Map, error) {
	h, err := DecodeHeader(data)
	if err != nil {
		return nil, err
	}
	s := layout(h)
	if err := s.checkLen(len(data)); err != nil {
		return nil, err
	}

	m := &Map{
		Version:     h.Version,
		SceneID:     h.SceneID,
		Name:        string(data[s.nameOff : s.nameOff+s.nameLen]),
		CellSize:    h.CellSize,
		Origin:      h.Origin,
		width:       h.Width,
		depth:       h.Depth,
		bits:        data[s.bitsOff : s.bitsOff+s.bitsLen],
		ColliderCnt: h.ColliderCount,
	}

	// 位图逐位统计：用 popcount 而不是逐格 if，百万格地图上是数量级差距。
	for _, b := range m.bits {
		m.WalkableCnt += bits.OnesCount8(b)
	}
	// 统计口径必须与位图总位数对齐：最后一字节的补位（超出 width*depth 的位）要扣掉。
	if tail := m.width * m.depth % 8; tail != 0 {
		if last := m.bits[len(m.bits)-1]; last&(0xFF<<uint(tail)) != 0 {
			// 非预期分支：导出端把补位写成了 1。会算出比真实格数多的"可走格"，
			// 让守恒校验（可走+阻挡 == w*d）失败，必须留痕。
			logger.Warnf("mapdata: 位图尾部补位非 0（width*depth=%d 不是 8 的倍数），已按格数截断",
				m.width*m.depth)
		}
		m.WalkableCnt -= bits.OnesCount8(m.bits[len(m.bits)-1] >> uint(tail))
	}
	m.BlockedCnt = m.width*m.depth - m.WalkableCnt

	// 碰撞体：定长 24 字节一个，顺序读取。
	// 逐个校验：分量必须有限（非 NaN/Inf）且 Min<=Max——否则注入 collide.Grid3 后
	// 格子范围计算会被脏值污染（溢出/越界），是典型的静默读出错地图。
	if h.ColliderCount > 0 {
		m.colliders = make([]collide.AABB3, h.ColliderCount)
		for i := 0; i < h.ColliderCount; i++ {
			bb := readAABB(data, s.collOff+i*ColliderStride)
			if !validCollider(bb) {
				return nil, fmt.Errorf("碰撞体[%d]非法（含 NaN/Inf 或 Min>Max）：min=(%v,%v,%v) max=(%v,%v,%v)",
					i, bb.Min.X, bb.Min.Y, bb.Min.Z, bb.Max.X, bb.Max.Y, bb.Max.Z)
			}
			m.colliders[i] = bb
		}
	}

	// 出生点：定长 12 字节一个。
	if h.SpawnCount > 0 {
		m.Spawns = make([]geom.Vec3, h.SpawnCount)
		for i := 0; i < h.SpawnCount; i++ {
			m.Spawns[i] = readVec3(data, s.spawnOff+i*SpawnStride)
		}
	}

	// 单层导航网格：基面 = origin.Y，顶面 = origin.Y + navLayerHeight。
	grid := collide.NewNavGrid(m.width, m.depth)
	for z := 0; z < m.depth; z++ {
		for x := 0; x < m.width; x++ {
			if !m.cellWalkable(x, z) {
				grid.SetBlocked(x, z, true)
			}
		}
	}
	m.nav = collide.NewNavGrid3()
	m.nav.AddLayer(h.Origin.Y, h.Origin.Y+navLayerHeight, grid)

	// 出生点可能落在"位图可走但角色半径占不下"的位置（贴合建筑/边界）——
	// 那种位置会让玩家一出生就被客户端本地碰撞锁死（只能原地跑动画），详见 spawn.go。
	m.sanitizeSpawns()

	logger.Infof("mapdata: 地图已构建 scene=%d name=%s %dx%d cell=%.2f 可走=%d 阻挡=%d 碰撞体=%d 出生点=%d v=%d",
		m.SceneID, m.Name, m.width, m.depth, m.CellSize, m.WalkableCnt, m.BlockedCnt, m.ColliderCnt, len(m.Spawns), m.Version)
	for i, sp := range m.Spawns {
		logger.Infof("mapdata: 出生点[%d] = (%.1f, %.1f, %.1f)", i, sp.X, sp.Y, sp.Z)
	}
	return m, nil
}

// validCollider 校验碰撞体包围盒：六个分量均为有限值（非 NaN/±Inf）且 Min<=Max。
func validCollider(b collide.AABB3) bool {
	finite := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
	return finite(b.Min.X) && finite(b.Min.Y) && finite(b.Min.Z) &&
		finite(b.Max.X) && finite(b.Max.Y) && finite(b.Max.Z) &&
		b.Min.X <= b.Max.X && b.Min.Y <= b.Max.Y && b.Min.Z <= b.Max.Z
}

// ApplyTo 把地图注入场景：碰撞体进 `Collider3`（Mask=`GroupWall`，才参与遮挡与视线判定）。
//
// 导航网格由 Map 自己持有（Scene 不存 nav），业务用 FindPath 取路径。
// 出生点已由 Decode 净化，业务用 SpawnAt 取。
func (m *Map) ApplyTo(target WallTarget) error {
	if target == nil {
		return fmt.Errorf("场景为空")
	}
	col := target.Collider3()
	if col == nil {
		return fmt.Errorf("场景未提供三维碰撞宽相（Collider3）")
	}
	for i := range m.colliders {
		b := m.colliders[i]
		id := fmt.Sprintf("map:%d:wall:%d", m.SceneID, i)
		col.Insert(id, b)
		if err := col.SetMask(id, collide.GroupWall); err != nil {
			// 非预期分支：障碍注册了却没打上 GroupWall 掩码 = 墙不存在（静默失效），必须留日志。
			logger.Errorf("mapdata: 设置碰撞体掩码失败 id=%s err=%v", id, err)
			return fmt.Errorf("设置碰撞体掩码失败 %s: %w", id, err)
		}
	}
	logger.Infof("mapdata: 地图已注入场景 scene=%d 碰撞体=%d 导航层=%d",
		m.SceneID, len(m.colliders), m.nav.LayerCount())
	return nil
}

// WalkableAt 判断世界坐标 (x,z) 是否可走（越界 = 不可走）。
func (m *Map) WalkableAt(x, z float64) bool {
	ix, iz, ok := m.cellOf(x, z)
	if !ok {
		return false
	}
	return m.cellWalkable(ix, iz)
}

// Probe 返回世界坐标处的可走性与命中的碰撞体 id（地图管线验证入口）。
func (m *Map) Probe(target WallTarget, x, y, z float64) (walkable bool, colliderIDs []string) {
	walkable = m.WalkableAt(x, z)
	if target != nil && target.Collider3() != nil {
		colliderIDs = target.Collider3().QueryPoint(geom.Vec3{X: x, Y: y, Z: z})
	}
	return walkable, colliderIDs
}

// FindPath 在导航网格上求路径（三维点，含层间连接）。
func (m *Map) FindPath(from, to geom.Vec3) []geom.Vec3 {
	if m.nav == nil {
		return nil
	}
	return m.nav.FindPath3(from, to)
}

// SpawnAt 返回第 i 个出生点（无出生点返回原点，越界回落到第一个）。
func (m *Map) SpawnAt(i int) geom.Vec3 {
	if len(m.Spawns) == 0 {
		return m.Origin
	}
	if i < 0 || i >= len(m.Spawns) {
		i = 0
	}
	return m.Spawns[i]
}

// Bounds 返回位图尺寸与格边长。
func (m *Map) Bounds() (width, depth int, cell float64) { return m.width, m.depth, m.CellSize }

// Width 返回位图东西向格数。
func (m *Map) Width() int { return m.width }

// Depth 返回位图南北向格数。
func (m *Map) Depth() int { return m.depth }

// Colliders 返回已加载的 AABB 列表（只读，注入场景用的就是它）。
func (m *Map) Colliders() []collide.AABB3 { return m.colliders }

// Nav 返回导航网格（单层 NavGrid3）。需要自己接寻路/AI 时用它，通常直接用 FindPath。
func (m *Map) Nav() *collide.NavGrid3 { return m.nav }

// Source 返回数据文件路径（排障用；Decode 构造的 Map 为空串）。
func (m *Map) Source() string { return m.source }

// cellWalkable 按格坐标查位图（调用方负责保证 ix/iz 在界内）。
func (m *Map) cellWalkable(ix, iz int) bool {
	idx := iz*m.width + ix
	return m.bits[idx>>3]&(1<<uint(idx&7)) != 0
}

// cellOf 世界坐标 → 格坐标；越界返回 ok=false。
//
// ★ 必须用 Floor 而不是 Go 的 int() 截断：两者在负坐标上不同 ——
// `int(-0.5) == 0`（向零截断）会把地图外 (-1,0)×(-1,0) 那一格误判成"第 0 格"，
// 于是"图外可走"。客户端 Map.cs 用 `Mathf.FloorToInt` 保持同一口径，已用回归用例钉住。
func (m *Map) cellOf(x, z float64) (int, int, bool) {
	ix := int(math.Floor((x - m.Origin.X) / m.CellSize))
	iz := int(math.Floor((z - m.Origin.Z) / m.CellSize))
	if ix < 0 || ix >= m.width || iz < 0 || iz >= m.depth {
		return 0, 0, false
	}
	return ix, iz, true
}
