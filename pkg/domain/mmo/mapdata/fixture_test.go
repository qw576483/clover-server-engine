package mapdata

import (
	"encoding/binary"
	"math"

	"github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// fixture 是测试用的 CloverMap 数据构造器：按 format.go 的布局逐字节产出。
//
// ★ 它与 Unity 端编码器（客户端引擎 `Runtime/Presentation/MapWriter.cs` 的 `CloverMapWriter`）是**两份独立实现**
// （一份 Go 一份 C#），互为对照：同一组输入两边产出的字节必须一致，否则说明契约被改歪了。
// 真·跨端那一环（C# 真编码器产出的字节喂 Go 解码器）目前**没有用例** —— 原计划的
// `crosslang_test.go` 未落地（见本包 `README.md` 的测试策略表）。
//
// 约定：encode 不做任何默认值填充，要默认值用 newFixture；这样测试才能显式构造"flags=0"
// 这类非法头（若 encode 擅自兜底，非法数据就永远造不出来）。
type fixture struct {
	Version   uint16
	Flags     uint16
	Magic     string
	SceneID   uint64
	Name      string
	CellSize  float64
	Origin    geom.Vec3
	Width     int
	Depth     int
	Cells     []bool // 行主序，长度应为 Width*Depth
	Colliders []collide.AABB3
	Spawns    []geom.Vec3
	// Markers 命名标记点。⚠️ 与真实导出端**刻意相反**：真实导出端是"有数据才置 FlagMarkers"，
	// 这里以 Flags 位为准（置位就写段，Markers 为空则写 count=0）——测试要能构造
	// "置了位但段缺/坏/空"这些导出端不该产生、读端必须正确处理的用例。
	Markers []Marker
}

// newFixture 返回一份填好合法默认值的 fixture（全可走、无碰撞体、无出生点）。
func newFixture(width, depth int) fixture {
	return fixture{
		Version:  FormatVersion,
		Flags:    FlagWalkable,
		Magic:    FileMagic,
		SceneID:  1,
		Name:     "unit-map",
		CellSize: 1,
		Width:    width,
		Depth:    depth,
		Cells:    allWalkable(width, depth),
	}
}

// encode 按格式布局产出字节。
func (f fixture) encode() []byte {
	name := []byte(f.Name)
	bits := packCells(f.Cells)

	// 标记段只在置了 FlagMarkers 时出现（段字节数 = 段头 4 + Σ(MarkerStride + 名字字节)）。
	markersLen := 0
	if f.Flags&FlagMarkers != 0 {
		markersLen = 4
		for _, mk := range f.Markers {
			markersLen += MarkerStride + len(mk.Name)
		}
	}

	out := make([]byte, HeaderSize+len(name)+len(bits)+
		len(f.Colliders)*ColliderStride+len(f.Spawns)*SpawnStride+markersLen)

	le := binary.LittleEndian
	copy(out[0:4], f.Magic)
	le.PutUint16(out[4:6], f.Version)
	le.PutUint16(out[6:8], f.Flags)
	le.PutUint64(out[8:16], f.SceneID)
	le.PutUint32(out[16:20], math.Float32bits(float32(f.CellSize)))
	putVec3(out[20:32], f.Origin)
	le.PutUint32(out[32:36], uint32(f.Width))
	le.PutUint32(out[36:40], uint32(f.Depth))
	le.PutUint32(out[40:44], uint32(len(f.Colliders)))
	le.PutUint32(out[44:48], uint32(len(f.Spawns)))
	le.PutUint32(out[48:52], uint32(len(name)))
	// 52..64 为保留段，保持全 0

	off := HeaderSize
	off += copy(out[off:], name)
	off += copy(out[off:], bits)
	for _, c := range f.Colliders {
		putVec3(out[off:off+12], c.Min)
		putVec3(out[off+12:off+24], c.Max)
		off += ColliderStride
	}
	for _, s := range f.Spawns {
		putVec3(out[off:off+12], s)
		off += SpawnStride
	}
	// 命名标记点段：**追加在末尾**（与 format.go 的段顺序一致）。
	if f.Flags&FlagMarkers != 0 {
		le.PutUint32(out[off:off+4], uint32(len(f.Markers)))
		off += 4
		for _, mk := range f.Markers {
			nb := []byte(mk.Name)
			le.PutUint32(out[off:off+4], uint32(len(nb)))
			off += 4
			off += copy(out[off:], nb)
			putVec3(out[off:off+12], mk.Pos)
			off += 12
		}
	}
	return out
}

// putVec3 写一个 xyz 三元组（3 个 float32）。
func putVec3(b []byte, v geom.Vec3) {
	le := binary.LittleEndian
	le.PutUint32(b[0:4], math.Float32bits(float32(v.X)))
	le.PutUint32(b[4:8], math.Float32bits(float32(v.Y)))
	le.PutUint32(b[8:12], math.Float32bits(float32(v.Z)))
}

// packCells 把 []bool 按「行主序、字节内 LSB 优先」打包（与导出端、客户端同一约定）。
func packCells(cells []bool) []byte {
	out := make([]byte, (len(cells)+7)/8)
	for i, ok := range cells {
		if ok {
			out[i>>3] |= 1 << uint(i&7)
		}
	}
	return out
}

// allWalkable 生成全可走的位图。
func allWalkable(w, d int) []bool {
	out := make([]bool, w*d)
	for i := range out {
		out[i] = true
	}
	return out
}
