package mapdata

import (
	"strings"
	"testing"

	"clover-server-engine/pkg/domain/mmo"
	"clover-server-engine/pkg/domain/mmo/collide"
	"clover-server-engine/pkg/shared/geom"
)

// TestDecode_WalkableAndNav 验证导出产物的解码、可走性判定与导航网格构建。
func TestDecode_WalkableAndNav(t *testing.T) {
	const (
		w = 4
		d = 4
	)
	// 4x4 全可走，除 (1,1) 与 (2,2)。
	f := newFixture(w, d)
	f.Cells[1*w+1] = false
	f.Cells[2*w+2] = false
	f.Colliders = []collide.AABB3{
		{Min: geom.Vec3{X: 1, Y: 0, Z: 1}, Max: geom.Vec3{X: 2, Y: 3, Z: 2}},
	}
	f.Spawns = []geom.Vec3{{X: 0.5, Y: 0, Z: 0.5}}

	m, err := Decode(f.encode())
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}

	if m.WalkableCnt != w*d-2 {
		t.Errorf("可走格数 = %d, 期望 %d", m.WalkableCnt, w*d-2)
	}
	if m.BlockedCnt != 2 {
		t.Errorf("阻挡格数 = %d, 期望 2", m.BlockedCnt)
	}
	if m.WalkableCnt+m.BlockedCnt != w*d {
		t.Errorf("格数不守恒: %d + %d != %d", m.WalkableCnt, m.BlockedCnt, w*d)
	}
	if m.Name != f.Name || m.SceneID != f.SceneID || m.Version != FormatVersion {
		t.Errorf("头部字段不一致: name=%q scene=%d version=%d", m.Name, m.SceneID, m.Version)
	}
	if m.ColliderCnt != 1 || len(m.Colliders()) != 1 {
		t.Errorf("碰撞体数 = %d/%d, 期望 1", m.ColliderCnt, len(m.Colliders()))
	}
	if len(m.Spawns) != 1 {
		t.Errorf("出生点数 = %d, 期望 1", len(m.Spawns))
	}

	if !m.WalkableAt(0.5, 0.5) {
		t.Error("(0.5,0.5) 应为可走")
	}
	if m.WalkableAt(1.5, 1.5) {
		t.Error("(1.5,1.5) 应为阻挡")
	}
	if m.WalkableAt(-1, 0) {
		t.Error("越界坐标必须判为不可走（否则玩家会走出地图）")
	}
	// ★ 负坐标的取整口径：必须用 Floor。用向零截断会让 (-1,0) 这一格沿用第 0 格 ⇒ 图外可走。
	// （客户端 Map.cs 用 Mathf.FloorToInt，两端必须一致。）
	if m.WalkableAt(-0.5, 0.5) {
		t.Error("(-0.5,0.5) 在图外，必须判为不可走（取整口径错成了向零截断）")
	}
	if m.WalkableAt(0.5, 4.5) {
		t.Error("(0.5,4.5) 超出 depth=4，必须判为不可走")
	}

	if m.nav == nil || m.nav.LayerCount() != 1 {
		t.Errorf("导航网格未构建或层数异常: %+v", m.nav)
	}
	if path := m.FindPath(geom.Vec3{X: 0.5, Z: 0.5}, geom.Vec3{X: 3.5, Z: 3.5}); len(path) == 0 {
		t.Error("FindPath 应能在可走区求出路径")
	}
}

// TestDecode_HeaderFieldsRoundTrip 头部标量字段必须原样读回（含 float32 → float64 提升）。
func TestDecode_HeaderFieldsRoundTrip(t *testing.T) {
	f := newFixture(2, 2)
	f.SceneID = 1<<40 + 7    // 超过 uint32，验证 uint64 段没被截断
	f.Name = "城市主城 map-city" // 多字节 UTF-8：name_len 必须是字节数而不是字符数
	f.CellSize = 0.5
	f.Origin = geom.Vec3{X: 1.25, Y: 3.5, Z: -2.75}

	m, err := Decode(f.encode())
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if m.SceneID != f.SceneID {
		t.Errorf("SceneID = %d, 期望 %d", m.SceneID, f.SceneID)
	}
	if m.Name != f.Name {
		t.Errorf("Name = %q, 期望 %q（多字节 UTF-8 的名字长度口径错了？）", m.Name, f.Name)
	}
	if m.CellSize != 0.5 {
		t.Errorf("CellSize = %v, 期望 0.5", m.CellSize)
	}
	if m.Origin.X != 1.25 || m.Origin.Y != 3.5 || m.Origin.Z != -2.75 {
		t.Errorf("Origin = %+v, 期望 {1.25 3.5 -2.75}", m.Origin)
	}
}

// TestDecode_TrailingBytesAllowed 文件比本版本所需更长时必须能读（V2 追加段后旧加载器仍可用）。
func TestDecode_TrailingBytesAllowed(t *testing.T) {
	f := newFixture(4, 4)
	data := append(f.encode(), make([]byte, 64)...) // 模拟 V2 追加的高程段
	if _, err := Decode(data); err != nil {
		t.Errorf("更长的新版文件应向后兼容可读，实际报错: %v", err)
	}
}

// TestDecode_BitmapTailPadding 位图最后一字节的补位（w*d 不是 8 的倍数时）不该被算成可走格。
func TestDecode_BitmapTailPadding(t *testing.T) {
	const w, d = 3, 3 // 9 格 → 2 字节，第 2 字节只有 1 位有效，其余 7 位是补位
	f := newFixture(w, d)
	data := f.encode()

	bitsOff := HeaderSize + len(f.Name)
	data[bitsOff+1] |= 0xFE // 故意把 7 个补位全写成 1（导出端理论上不该这么写）

	m, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if m.WalkableCnt != w*d {
		t.Errorf("可走格数 = %d, 期望 %d（补位的 1 被算进可走格了）", m.WalkableCnt, w*d)
	}
	if m.BlockedCnt != 0 {
		t.Errorf("阻挡格数 = %d, 期望 0", m.BlockedCnt)
	}
}

// TestDecode_RejectsBadData 非法数据必须报错，不能静默当成"空地图"。
//
// 这张表就是格式契约的可执行版本：每一条对应一个"两端可能漂移"的点。
func TestDecode_RejectsBadData(t *testing.T) {
	badVersion := newFixture(4, 4)
	badVersion.Version = 99

	unknownFlag := newFixture(4, 4)
	unknownFlag.Flags = FlagWalkable | (1 << 15)

	noBitmap := newFixture(4, 4)
	noBitmap.Flags = 0

	zeroDim := newFixture(4, 4)
	zeroDim.Width = 0

	hugeDim := newFixture(4, 4)
	hugeDim.Width = MaxDimension + 1

	badCell := newFixture(4, 4)
	badCell.CellSize = 0

	leJSON := append([]byte(`{"version":1,"scene_id":1,`), make([]byte, 64)...)
	leEmpty := []byte{}
	leShort := make([]byte, HeaderSize-1)

	// 截断：分别砍掉每个段的最后一个字节，验证"声明有、实际没有"能被抓住。
	truncBitmap := func() []byte {
		f := newFixture(4, 4)
		b := f.encode()
		return b[:HeaderSize+len(f.Name)+cellBytes(4, 4)-1]
	}()
	truncCollider := func() []byte {
		f := newFixture(4, 4)
		f.Colliders = []collide.AABB3{{Min: geom.Vec3{}, Max: geom.Vec3{X: 1, Y: 1, Z: 1}}}
		b := f.encode()
		return b[:len(b)-1]
	}()
	truncSpawn := func() []byte {
		f := newFixture(4, 4)
		f.Spawns = []geom.Vec3{{X: 1, Y: 0, Z: 1}}
		b := f.encode()
		return b[:len(b)-1]
	}()

	cases := []struct {
		name string
		data []byte
		hint string // 期望错误信息里出现的关键字（防"报了个不相干的错"）
	}{
		{"空文件", leEmpty, "太小"},
		{"只有半个头", leShort, "太小"},
		{"魔数不符（旧 JSON 产物）", leJSON, "魔数不符"},
		{"版本不符", badVersion.encode(), "版本"},
		{"含未知段 flags", unknownFlag.encode(), "未知段"},
		{"缺可行走位图", noBitmap.encode(), "可行走位图"},
		{"尺寸为 0", zeroDim.encode(), "尺寸非法"},
		{"尺寸超上限", hugeDim.encode(), "超上限"},
		{"格边长为 0", badCell.encode(), "格边长"},
		{"位图被截断", truncBitmap, "截断"},
		{"碰撞体被截断", truncCollider, "截断"},
		{"出生点被截断", truncSpawn, "截断"},
	}
	for _, tc := range cases {
		_, err := Decode(tc.data)
		if err == nil {
			t.Errorf("%s: 期望报错，实际通过", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.hint) {
			t.Errorf("%s: 错误信息应包含 %q，实际: %v", tc.name, tc.hint, err)
		}
	}
}

// 编译期断言：`mmo.Scene` 必须满足 ApplyTo 要求的最小接口。
// 这条断言是刻意的 —— 加载器用最小接口而不是直接依赖 mmo.Scene，这里钉住两者确实兼容，
// 免得接口悄悄变了、业务侧传参编译不过时才发现。
var _ WallTarget = (mmo.Scene)(nil)

// TestApply_InjectsColliders 验证「导出产物的碰撞体 → 场景 Collider3」这一步真的生效：
// 这是地图管线的核心验收点 —— 注入失败等于墙不存在且无人知晓。
func TestApply_InjectsColliders(t *testing.T) {
	f := newFixture(4, 4)
	f.Name = "apply-map"
	f.Colliders = []collide.AABB3{
		{Min: geom.Vec3{X: 1, Y: 0, Z: 1}, Max: geom.Vec3{X: 3, Y: 3, Z: 2}},
	}
	m, err := Decode(f.encode())
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}

	sm := mmo.NewSceneManager(mmo.WithPublisher(fakePublisher{}))
	scene := sm.CreateScene(f.SceneID, f.Name)
	if err := m.ApplyTo(scene); err != nil {
		t.Fatalf("ApplyTo 失败: %v", err)
	}

	col := scene.Collider3()
	if col == nil {
		t.Fatal("场景未提供 Collider3")
	}
	ids := col.QueryPoint(geom.Vec3{X: 2, Y: 1, Z: 1.5})
	if len(ids) == 0 {
		t.Fatalf("碰撞体内查询应有命中（id 前缀 map:%d:wall:），实际 0 个 —— 说明地图没被注入场景", f.SceneID)
	}

	// 探针路径与直接查询必须一致。
	walk, probeIDs := m.Probe(scene, 2, 1, 1.5)
	if len(probeIDs) == 0 {
		t.Error("Probe 未返回碰撞体 id")
	}
	if !walk {
		t.Error("(2,1.5) 位图上是可走格，Probe 应回 walkable=true（碰撞体与位图是两套语义，二者可同时成立）")
	}
}

// fakePublisher 满足 mmo.Publisher（测试不需要真的发消息）。
type fakePublisher struct{}

func (fakePublisher) Publish(string, []byte) error { return nil }
