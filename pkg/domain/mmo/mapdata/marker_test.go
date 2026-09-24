package mapdata

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// 命名标记点段（FlagMarkers，bit2）的契约测试。
//
// 三条契约缺一条就是"文件能读、服务能起，只是按名取点全落空 / 两端判定不一致"的静默失败：
//	① 不含该段的旧产物**行为逐字节不变**（段不会凭空出现，既有段偏移不动）；
//	② 含该段时**完整解析**（名字含中文、同名多点按文件顺序）并回填 Map.Markers；
//	③ 段被截断 / 写坏必须**当场报错** —— 不能出现"客户端与导出端都判非法、服务端却照收"。
//
// 跨端对照：本文件用 fixture（Go 侧的独立实现）造字节；与客户端引擎
// `Runtime/Presentation/{MapWriter.cs,MapFormat.cs}` 的段布局必须逐字节一致
// （客户端侧同批用例见 `Tests/Editor/MapFormatTests.cs`）。

// markerFixture 造一份「既有段齐全 + 命名标记点段」的数据（含中文名与同名多点）。
func markerFixture() fixture {
	f := newFixture(4, 4)
	f.Name = "城市地图"
	f.Colliders = []collide.AABB3{
		{Min: geom.Vec3{X: 1, Y: 0, Z: 1}, Max: geom.Vec3{X: 2, Y: 3, Z: 2}},
	}
	f.Spawns = []geom.Vec3{{X: 0.5, Y: 0, Z: 0.5}}
	f.Flags = FlagWalkable | FlagMarkers
	f.Markers = []Marker{
		{Name: "Spawn_T", Pos: geom.Vec3{X: -11.5, Y: 3.556, Z: -46.5}},
		{Name: "Spawn_T", Pos: geom.Vec3{X: -11.5, Y: 3.556, Z: -48.5}}, // 同名多点：一组出生点
		{Name: "包点A", Pos: geom.Vec3{X: 1.25, Y: 0.5, Z: -2.75}},        // 中文名：UTF-8
	}
	return f
}

// markerOffsetOf 返回标记段起点，并顺带钉住 layout 的 markerOff == 既有段末尾。
func markerOffsetOf(t *testing.T, data []byte) int {
	t.Helper()
	h, err := DecodeHeader(data)
	if err != nil {
		t.Fatalf("DecodeHeader 失败: %v", err)
	}
	s := layout(h)
	if s.markerOff != s.total {
		t.Fatalf("markerOff=%d 应等于既有段末尾 total=%d（标记段必须追加在末尾）", s.markerOff, s.total)
	}
	return s.markerOff
}

func almostEq(a, b float64) bool { return math.Abs(a-b) < 1e-4 }

// ② 含标记段：名字（含中文）/ 坐标逐条读回，同名多点按文件顺序，既有字段不受影响。
func TestDecode_Markers(t *testing.T) {
	f := markerFixture()
	m, err := Decode(f.encode())
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}

	if len(m.Markers) != len(f.Markers) {
		t.Fatalf("标记点数 = %d, 期望 %d", len(m.Markers), len(f.Markers))
	}
	for i, want := range f.Markers {
		got := m.Markers[i]
		if got.Name != want.Name {
			t.Errorf("标记点[%d].Name = %q, 期望 %q（UTF-8 名字长度口径错了？）", i, got.Name, want.Name)
		}
		if !almostEq(got.Pos.X, want.Pos.X) || !almostEq(got.Pos.Y, want.Pos.Y) || !almostEq(got.Pos.Z, want.Pos.Z) {
			t.Errorf("标记点[%d](%q) 坐标 = (%v,%v,%v), 期望 (%v,%v,%v)",
				i, got.Name, got.Pos.X, got.Pos.Y, got.Pos.Z, want.Pos.X, want.Pos.Y, want.Pos.Z)
		}
	}
	// 同名多点：两条 Spawn_T 都在，且顺序 = 文件顺序（不是被去重/合并）。
	if m.Markers[0].Name != "Spawn_T" || m.Markers[1].Name != "Spawn_T" {
		t.Errorf("同名多点丢失：%q / %q", m.Markers[0].Name, m.Markers[1].Name)
	}
	if !almostEq(m.Markers[0].Pos.Z, -46.5) || !almostEq(m.Markers[1].Pos.Z, -48.5) {
		t.Errorf("同名多点顺序被打乱：%v / %v", m.Markers[0].Pos.Z, m.Markers[1].Pos.Z)
	}
	// Y 必须是**真实高度**（同一名字的点可以在不同楼层），不是地面高度。
	if !almostEq(m.Markers[0].Pos.Y, 3.556) {
		t.Errorf("标记点 Y = %v, 期望 3.556（真实高度被当成地面高度抹平了？）", m.Markers[0].Pos.Y)
	}

	// 既有段不受标记段影响。
	if m.WalkableCnt != 16 || m.BlockedCnt != 0 {
		t.Errorf("位图统计被污染: 可走=%d 阻挡=%d", m.WalkableCnt, m.BlockedCnt)
	}
	if len(m.Colliders()) != 1 || len(m.Spawns) != 1 {
		t.Errorf("碰撞体/出生点被污染: 碰撞体=%d 出生点=%d", len(m.Colliders()), len(m.Spawns))
	}
	if m.Name != f.Name || m.SceneID != f.SceneID {
		t.Errorf("头部字段被污染: name=%q scene=%d", m.Name, m.SceneID)
	}
}

// ① 不含标记段的旧产物：行为与改动前完全一致（段不会凭空出现）。
func TestDecode_NoMarkers_LegacyUnchanged(t *testing.T) {
	f := newFixture(4, 4) // Flags 默认只有 FlagWalkable
	f.Colliders = []collide.AABB3{{Min: geom.Vec3{}, Max: geom.Vec3{X: 1, Y: 1, Z: 1}}}
	f.Spawns = []geom.Vec3{{X: 0.5, Y: 0, Z: 0.5}}
	data := f.encode()

	// 旧产物的字节长度 = 既有段公式（README 校验规则 8），一个字节都不多。
	want := HeaderSize + len(f.Name) + cellBytes(4, 4) + ColliderStride + SpawnStride
	if len(data) != want {
		t.Fatalf("旧产物长度 = %d, 期望 %d（无标记段时不该多写字节）", len(data), want)
	}
	if got := binary.LittleEndian.Uint16(data[6:8]); got != FlagWalkable {
		t.Fatalf("旧产物 flags = 0x%04x, 期望 0x%04x（不该凭空置 FlagMarkers）", got, FlagWalkable)
	}
	if FlagMarkers&FlagWalkable != 0 {
		t.Fatal("FlagMarkers 与 FlagWalkable 位重叠了")
	}

	m, err := Decode(data)
	if err != nil {
		t.Fatalf("旧产物 Decode 失败（向后兼容破了）: %v", err)
	}
	if len(m.Markers) != 0 {
		t.Errorf("旧产物不该解出标记点，实际 %d 个", len(m.Markers))
	}
	if m.WalkableCnt != 16 || len(m.Colliders()) != 1 || len(m.Spawns) != 1 {
		t.Errorf("旧产物既有字段被改坏: 可走=%d 碰撞体=%d 出生点=%d",
			m.WalkableCnt, len(m.Colliders()), len(m.Spawns))
	}
	// 尾随多余字节仍是允许的（规则 9 未因新段而收紧）。
	if _, err := Decode(append(append([]byte{}, data...), make([]byte, 32)...)); err != nil {
		t.Errorf("更长的新版文件应向后兼容可读，实际报错: %v", err)
	}
}

// ③ 截断：砍在标记段内部必须报错（既有段的 checkLen 看不见标记段，只有解析才能发现）。
func TestDecode_MarkersTruncated(t *testing.T) {
	full := markerFixture().encode()
	markerOff := markerOffsetOf(t, full)
	markerBytes := len(full) - markerOff
	if markerBytes <= 4 {
		t.Fatalf("用例前提不成立：标记段只有 %d 字节", markerBytes)
	}

	for cut := 1; cut <= markerBytes; cut++ {
		cutOff := full[:len(full)-cut]
		_, err := Decode(cutOff)
		if err == nil {
			t.Errorf("砍掉尾部 %d 字节后仍被当成合法文件（标记段截断必须报错）", cut)
			continue
		}
		if !strings.Contains(err.Error(), "标记点") {
			t.Errorf("砍掉尾部 %d 字节的报错没点名标记段: %v", cut, err)
		}
	}
	// 继续砍进既有段：也必须报错（此时是既有段的截断检测接手）。
	for cut := markerBytes + 1; cut <= markerBytes+3; cut++ {
		if _, err := Decode(full[:len(full)-cut]); err == nil {
			t.Errorf("砍掉尾部 %d 字节（进既有段）后仍被当成合法文件", cut)
		}
	}
}

// ③ 段内坏数据：负数数量 / 名字长度越界 / 空名字 / NaN 坐标都必须报错并点名。
func TestDecode_MarkersBadData(t *testing.T) {
	full := markerFixture().encode()
	off := markerOffsetOf(t, full)
	// 第 0 条名字 "Spawn_T" = 7 字节 ⇒ 坐标起点 = 段头(4) + 长度前缀(4) + 名字(7)
	firstCoord := off + 4 + 4 + 7

	cases := []struct {
		name  string
		at    int
		patch []byte
		hint  string
	}{
		// 0xFFFFFFFF：64 位平台上 int(4294967295) 仍是正数，由「按数量整体拦截断」抓住（need ≫ 实际长度）；
		// 32 位平台上它会先变成负数，由 readMarkers 的数量检查抓住 —— 两条路都必须报错，只是文案不同。
		{"数量为 0xFFFFFFFF（远超文件长度）", off, []byte{0xFF, 0xFF, 0xFF, 0xFF}, "截断"},
		{"第 0 条名字长度为 0（空名字）", off + 4, []byte{0, 0, 0, 0}, "名字为空"},
		{"第 0 条名字长度超出文件剩余", off + 4, []byte{0xFF, 0xFF, 0x00, 0x00}, "名字长度非法"},
		{"第 0 条 X 坐标 NaN", firstCoord, float32Bytes(float32(math.NaN())), "坐标非法"},
		{"第 0 条 Y 坐标 +Inf", firstCoord + 4, float32Bytes(float32(math.Inf(1))), "坐标非法"},
	}
	for _, tc := range cases {
		data := append([]byte{}, full...)
		copy(data[tc.at:], tc.patch)
		_, err := Decode(data)
		if err == nil {
			t.Errorf("%s: 期望报错，实际通过", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.hint) {
			t.Errorf("%s: 错误信息应包含 %q，实际: %v", tc.name, tc.hint, err)
		}
	}
}

// 置位但 count=0（空段）：合法可读、标记点为空 —— 读端不该把"空段"当成坏数据。
func TestDecode_MarkersFlagZeroCount(t *testing.T) {
	f := newFixture(2, 2)
	f.Flags = FlagWalkable | FlagMarkers
	f.Markers = nil

	m, err := Decode(f.encode())
	if err != nil {
		t.Fatalf("空标记段 Decode 失败: %v", err)
	}
	if len(m.Markers) != 0 {
		t.Errorf("空标记段应解出 0 个点，实际 %d", len(m.Markers))
	}
}

// flags 位是"段是否存在"的**唯一**判据：位不置时，尾部多出来的字节按规则 9 当尾随数据忽略。
func TestDecode_MarkersWithoutFlag_TreatedAsTrailing(t *testing.T) {
	data := markerFixture().encode()
	binary.LittleEndian.PutUint16(data[6:8], FlagWalkable) // 只清位，段字节原样留在文件里

	m, err := Decode(data)
	if err != nil {
		t.Fatalf("位未置时应按「文件更长是允许的」兼容读入，实际报错: %v", err)
	}
	if len(m.Markers) != 0 {
		t.Errorf("位未置却解出了 %d 个标记点（应当只认位）", len(m.Markers))
	}
}

// 两条相反的决定必须同时被钉住：FlagMarkers **在** knownFlags（完整解析），
// FlagHeightField **不在**（V1 不解析 ⇒ 置位即报未知段）。
func TestKnownFlags_MarkersInHeightFieldOut(t *testing.T) {
	if knownFlags&FlagMarkers == 0 {
		t.Error("knownFlags 必须收录 FlagMarkers，否则含标记段的新文件会被判「未知段」")
	}
	if knownFlags&FlagHeightField != 0 {
		t.Error("knownFlags 不许收录 FlagHeightField：V1 不解析高度场，收录就等于放行一份会整体错位的数据")
	}
}

// 高度场位至今仍被明确拒绝（多层地图走"逐层各烘一份"，真高度场列 V2）。
func TestDecode_HeightFieldStillRejected(t *testing.T) {
	f := newFixture(4, 4)
	f.Flags = FlagWalkable | FlagHeightField

	_, err := Decode(f.encode())
	if err == nil {
		t.Fatal("含高度场段的文件不该被接受（V1 不解析该段，放行会整体错位）")
	}
	if !strings.Contains(err.Error(), "未知段") {
		t.Errorf("错误信息应含「未知段」，实际: %v", err)
	}
}

// float32Bytes 小端编码一个 float32（构造坏坐标用）。
func float32Bytes(v float32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(v))
	return b
}
