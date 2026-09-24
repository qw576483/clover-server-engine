// Package mapdata 实现「Unity 关卡导出产物 → 服务端逻辑地图」的加载：
// 字节解码 → 导航网格构建 → 碰撞体注入 → 出生点净化。
//
// # 为什么它在引擎里
//
// 地图数据的**消费者是引擎自己的容器**：可行走位图喂 `collide.NavGrid3`、障碍 AABB 喂
// `collide.Grid3`（Mask=`GroupWall`）、出生点喂 `mmo.Scene.Enter`。引擎已经有这些容器的
// 运行时形态，却缺少「怎么从美术场景得到它们」的那条路径 —— 那是能力面缺了半截，
// 也正是每个用 Unity 做 3D 的项目都要重写一遍的东西。
// 况且导出端与加载端遵守的是**同一份字节契约**，分处两个仓库维护必然漂移。
//
// # 边界（谁不在这里）
//
//   - **地图内容**（布局 / 障碍 / 出生点坐标）是业务数据，不是引擎代码；
//   - **烘焙参数**（分辨率 / 原点 / 什么算障碍 / 阈值）由业务在导出面板上给；
//   - **客户端本地预测解算**（输入 → 位移 → 滑墙）不在引擎：引擎只回答「这一格能不能走」，
//     拿它做什么属于业务策略（依据客户端引擎 `结构规则.md` §3.1「引擎不做本地预测」）。
//
// # 同一份字节的三处实现
//
// 逐字节规范见本包 README.md：
//
//	clover-client-unity-engine/Editor/MapBake/      读场景烘焙 + 编码（Unity Editor）
//	pkg/domain/mmo/mapdata/     本包：服务端加载（碰撞 / 寻路 / 出生点）
//	clover-client-unity-engine/Runtime/Presentation/Map.cs  客户端本地预测（读同一份字节）
package mapdata

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// CloverMap 二进制格式 v1（小端）：
//
//	偏移   长度                内容
//	0      4                  magic "CLVM"
//	4      2                  version        uint16
//	6      2                  flags          uint16
//	8      8                  scene_id       uint64（逻辑地图 id，与服务端 mmo 场景 id 对齐）
//	16     4                  cell_size      float32
//	20     12                 origin         x/y/z 各 float32
//	32     4                  width          uint32（格，东西向）
//	36     4                  depth          uint32（格，南北向）
//	40     4                  collider_count uint32
//	44     4                  spawn_count    uint32
//	48     4                  name_len       uint32（UTF-8 字节数，不含 NUL）
//	52     12                 保留段（必须全 0；V2 用来放新字段）
//	---    64 = HeaderSize
//	64     name_len            name          UTF-8
//	       ceil(w*d/8)         walkable 位图  行主序 idx = z*width+x，字节内 LSB 优先，1=可走
//	       collider_count*24   colliders     每个 min(x,y,z) + max(x,y,z) 共 6 个 float32
//	       spawn_count*12      spawns        每个 (x,y,z) 共 3 个 float32
//	       ---                 命名标记点段    **仅当 flags 含 FlagMarkers 时存在**，且**追加在文件末尾**：
//	                           u32 marker_count，然后每条 u32 name_len + name(UTF-8 name_len 字节)
//	                           + 3 个 float32 世界坐标（x/y/z，y 是真实高度）
//
// 为什么是二进制而不是 JSON：位图是空间数据、按格增长，真实地图动辄百万格。
// JSON 版本要么把每格写成布尔（体积爆炸），要么 base64（膨胀 33% 且要额外解码一趟）。
// 二进制版位图就是原始字节，服务端**零拷贝**直接引用（见 Map.bits），大图加载时间与内存都降一个量级。
const (
	// FileMagic 文件头 4 字节魔数（ASCII "CLVM"）。
	FileMagic = "CLVM"
	// FormatVersion 本加载器支持的数据版本；导出端必须写同一个值。
	FormatVersion = 1
	// HeaderSize 定长头字节数（与版本无关，改格式只加段不改头长）。
	HeaderSize = 64
	// ColliderStride 单个 AABB 碰撞体占用字节数（6 个 float32）。
	ColliderStride = 24
	// SpawnStride 单个出生点占用字节数（3 个 float32）。
	SpawnStride = 12
	// MarkerStride 单个标记点的**定长部分**占用字节数（u32 name_len + 3 个 float32 坐标）：
	// 名字长度为 0 时占用的字节数就是它。按数量整体拦截断时用它做下界。
	MarkerStride = 16

	// FlagWalkable flags bit0：带可行走位图（V1 必有，缺失即视为非法数据）。
	FlagWalkable uint16 = 1 << 0
	// FlagHeightField flags bit1：带逐格地面高度场。
	//
	// ★ V1 未实现（当前地图是单层平地，`origin.y` 即地面高度），此处只占位：
	// 一旦实现多层/地形，导出端置位并追加 `width*depth` 个 float32 高度段（在 name 之后），
	// 同时把 Decode 里的单层导航层改为按真实高度建层 + 层间 NavLink。
	// 在此之前**该位必须被拦死**（见 knownFlags）：置位的文件会在 name 之后多出一段高度数据，
	// 而本加载器按「无高度段」计算偏移，位图/碰撞体/出生点会整体错位 ——
	// 读出来的地图看起来正常、实际上每一段都错了，是典型的静默失败。
	FlagHeightField uint16 = 1 << 1

	// FlagMarkers flags bit2：带**命名标记点**段（名字 + 世界坐标，追加在文件末尾）。
	//
	// 用途：出生点 / 包点 / 买枪区 / AI 路线锚点这些"按名字取点"的东西 —— 位图里只有
	// "这一格能不能走"，**没有名字**。段布局见文件头的布局注释与 readMarkers。
	//
	// ★ 前向兼容（与 README「版本演进」一致）：该段**追加在末尾**且**只有真的有标记点时导出端才置位**，
	// 因此不含该段的旧产物字节逐字节不变、旧读端（本加载器改动前）会因为置位而走"未知段"报错，
	// 而不是按"没有该段"静默读错。
	FlagMarkers uint16 = 1 << 2

	// knownFlags 本加载器**能正确解析**的 flags 位集合；出现集合外的位 = 数据比加载器新。
	//
	// 注意「认识这个常量」不等于「能解析这个段」：FlagHeightField 的段格式已定义但 V1 不解析，
	// 因此它**不在此集合内**，置位即走未知位分支明确报错（与 README 校验规则 4 一致）。
	//
	// FlagMarkers **在集合内**：本加载器会**完整解析并校验**该段（见 readMarkers）——
	// 不能只"认识位、跳过去"：该段在文件末尾、长度不定，若不解析就无法判断它是否被截断，
	// 结果是"客户端拒收、服务端照收"的两端不一致（正是客户端侧刚踩过的坑）。
	knownFlags = FlagWalkable | FlagMarkers

	// MaxDimension 单边格数上限。真实地图 4096 格（4km）已很大，32768 是安全天花板：
	// 既挡住畸形文件（width=0xFFFFFFFF）导致的内存爆炸，也保证 w*d 不撑爆 32 位 int。
	// 导出端（客户端引擎 MapBake）用同一个上限做写入前校验 —— 两边不同就是"能导出、读不了"。
	MaxDimension = 32768
)

// Header 是文件头的解码视图（导出端各字段的只读投影）。
type Header struct {
	Version       uint16
	Flags         uint16
	SceneID       uint64
	CellSize      float64
	Origin        geom.Vec3
	Width         int
	Depth         int
	ColliderCount int
	SpawnCount    int
	NameLen       int
}

// sections 是各段在文件内的偏移与长度。由 layout 计算，Decode 与测试共用，
// 保证「校验用的长度」与「真正读取用的长度」不可能算出两个答案。
type sections struct {
	nameOff, nameLen   int
	bitsOff, bitsLen   int
	collOff, collLen   int
	spawnOff, spawnLen int
	// markerOff 命名标记点段的起点（= 既有段末尾）。该段**长度不定**（每条带变长名字），
	// 因此不进 total、也不算在 checkLen 里，由 readMarkers 逐条解析并检验完整性。
	markerOff int
	// total 本版本所需的总字节数。实际文件可以更长（V2 追加段后旧加载器仍可读），
	// 短于 total 一律判为截断。
	total int
}

// DecodeHeader 解析定长头，并校验魔数 / 版本 / flags / 尺寸 / 格边长。
func DecodeHeader(data []byte) (Header, error) {
	var h Header
	if len(data) < HeaderSize {
		return h, fmt.Errorf("文件太小：%d 字节 < 定长头 %d 字节", len(data), HeaderSize)
	}
	if got := string(data[0:4]); got != FileMagic {
		return h, fmt.Errorf("魔数不符：期望 %q，实际 %q（拿到了非 CloverMap 产物或旧格式文件，请用引擎的地图导出器重新导出）",
			FileMagic, got)
	}

	le := binary.LittleEndian
	h.Version = le.Uint16(data[4:6])
	h.Flags = le.Uint16(data[6:8])
	h.SceneID = le.Uint64(data[8:16])
	h.CellSize = float64(math.Float32frombits(le.Uint32(data[16:20])))
	h.Origin = geom.Vec3{
		X: float64(math.Float32frombits(le.Uint32(data[20:24]))),
		Y: float64(math.Float32frombits(le.Uint32(data[24:28]))),
		Z: float64(math.Float32frombits(le.Uint32(data[28:32]))),
	}
	h.Width = int(le.Uint32(data[32:36]))
	h.Depth = int(le.Uint32(data[36:40]))
	h.ColliderCount = int(le.Uint32(data[40:44]))
	h.SpawnCount = int(le.Uint32(data[44:48]))
	h.NameLen = int(le.Uint32(data[48:52]))

	if h.Version != FormatVersion {
		return h, fmt.Errorf("不支持的数据版本 %d（本加载器支持 %d），请重新导出地图", h.Version, FormatVersion)
	}
	if unknown := h.Flags &^ knownFlags; unknown != 0 {
		// 前向兼容的护栏：数据里有本加载器不认识的段时，读出来的地图会**少一块**。
		// 这种失败是静默的（地图看起来正常、只是某些地方不对），所以必须在此拦死。
		return h, fmt.Errorf("数据 flags=0x%04x 含未知段 0x%04x（加载器只认识 0x%04x），请升级引擎或重新导出",
			h.Flags, unknown, knownFlags)
	}
	if h.Flags&FlagWalkable == 0 {
		return h, fmt.Errorf("数据没有可行走位图（flags=0x%04x），无法构建导航网格", h.Flags)
	}
	if h.Width <= 0 || h.Depth <= 0 {
		return h, fmt.Errorf("位图尺寸非法：%dx%d", h.Width, h.Depth)
	}
	if h.Width > MaxDimension || h.Depth > MaxDimension {
		return h, fmt.Errorf("位图尺寸超上限 %d：%dx%d", MaxDimension, h.Width, h.Depth)
	}
	if int64(h.Width)*int64(h.Depth) > math.MaxInt32 {
		return h, fmt.Errorf("位图格数过多：%d 格超过 %d", int64(h.Width)*int64(h.Depth), math.MaxInt32)
	}
	if h.CellSize <= 0 || math.IsNaN(h.CellSize) || math.IsInf(h.CellSize, 0) {
		return h, fmt.Errorf("格边长非法：%v", h.CellSize)
	}
	if h.NameLen < 0 {
		return h, fmt.Errorf("地图名长度非法：%d", h.NameLen)
	}
	if h.ColliderCount < 0 || h.SpawnCount < 0 {
		return h, fmt.Errorf("段计数非法：colliders=%d spawns=%d", h.ColliderCount, h.SpawnCount)
	}
	return h, nil
}

// layout 计算各段偏移（纯算术，不校验长度）。
func layout(h Header) sections {
	var s sections
	s.nameOff, s.nameLen = HeaderSize, h.NameLen
	s.bitsOff, s.bitsLen = s.nameOff+s.nameLen, cellBytes(h.Width, h.Depth)
	s.collOff, s.collLen = s.bitsOff+s.bitsLen, h.ColliderCount*ColliderStride
	s.spawnOff, s.spawnLen = s.collOff+s.collLen, h.SpawnCount*SpawnStride
	s.markerOff = s.spawnOff + s.spawnLen // 标记段紧跟在既有段之后（追加在末尾）
	s.total = s.spawnOff + s.spawnLen
	return s
}

// checkLen 校验实际文件长度。短于所需 = 截断（报错）；长于所需 = 允许（V2 追加段，本版忽略）。
func (s sections) checkLen(n int) error {
	if n < s.total {
		return fmt.Errorf("文件被截断：需要 %d 字节（头 %d + 名 %d + 位图 %d + 碰撞 %d + 出生点 %d），实际 %d",
			s.total, HeaderSize, s.nameLen, s.bitsLen, s.collLen, s.spawnLen, n)
	}
	return nil
}

// readAABB 读取第 i 个 AABB 碰撞体（min.xyz + max.xyz）。
func readAABB(b []byte, off int) collide.AABB3 {
	return collide.AABB3{
		Min: readVec3(b, off),
		Max: readVec3(b, off+12),
	}
}

// readVec3 读取一个 xyz 三元组（3 个 float32）。
func readVec3(b []byte, off int) geom.Vec3 {
	le := binary.LittleEndian
	return geom.Vec3{
		X: float64(math.Float32frombits(le.Uint32(b[off : off+4]))),
		Y: float64(math.Float32frombits(le.Uint32(b[off+4 : off+8]))),
		Z: float64(math.Float32frombits(le.Uint32(b[off+8 : off+12]))),
	}
}

// cellBytes 位图字节数：每格 1 bit，向上取整到字节。
func cellBytes(w, d int) int { return (w*d + 7) / 8 }

// Marker 一个**命名标记点**（名字 + 世界坐标）。
//
// 名字由导出端（现场对象名 / 项目自己的命名约定）决定，服务端**不解释**它的含义；
// 名字允许重复：同一名字下的多个点（一组出生点、一条路线的路点）按文件顺序排列。
// Pos.Y 是对象的**真实高度**（同一名字的点可以在不同楼层），不是地面高度。
type Marker struct {
	Name string
	Pos  geom.Vec3
}

// readMarkers 解析命名标记点段（起点 off = sections.markerOff；调用方须先确认 flags 含 FlagMarkers）。
//
//	u32 marker_count
//	repeat marker_count:
//	    u32   name_len        名字的 UTF-8 **字节**数（不含终止符；可为中文等任意 UTF-8）
//	    byte[name_len] name
//	    f32   x, y, z         世界坐标（米）
//
// 为什么**必须完整解析**而不是"认识 flags 位就跳过去"：该段在文件末尾、长度不定，
// 只跳过就无法发现它被截断 / 被写坏 —— 于是"导出端与客户端都判非法、服务端却照收"，
// 两端对同一份字节给出不同结论（客户端侧已经踩过一次同类坑）。校验口径与既有段一致：
// 先按数量整体拦截断，再逐条校验长度前缀、空名字与坐标有限性。
func readMarkers(data []byte, off int) ([]Marker, error) {
	le := binary.LittleEndian
	if len(data)-off < 4 {
		return nil, fmt.Errorf("文件被截断：标记点段缺少 4 字节段头（数量），剩余 %d 字节", len(data)-off)
	}
	// uint32 → int：32 位平台上超大值会变成负数，不拦的话下面 need 变负、段长度校验恒通过；
	// 64 位平台上它仍是正数，由紧跟着的「按数量整体拦截断」抓住（need 远超实际长度）。
	// 两条路都必须报错，只是文案不同 —— 不依赖平台位宽是这一段的要点。
	count := int(le.Uint32(data[off : off+4]))
	off += 4
	if count < 0 {
		return nil, fmt.Errorf("标记点数量非法：%d", count)
	}

	// 按数量整体拦一刀（每条至少 MarkerStride 字节）：count 来自外部数据，乘法用 int64 防溢出。
	need := int64(count) * int64(MarkerStride)
	if need > int64(len(data)-off) {
		return nil, fmt.Errorf("文件被截断：标记点段需要 ≥%d 字节（%d 个 × 至少 %d 字节），实际 %d",
			need, count, MarkerStride, len(data)-off)
	}

	out := make([]Marker, 0, count)
	for i := 0; i < count; i++ {
		if len(data)-off < MarkerStride {
			return nil, fmt.Errorf("文件被截断：第 %d 个标记点的定长部分（%d 字节）不完整", i, MarkerStride)
		}
		nameLen := int(le.Uint32(data[off : off+4]))
		off += 4
		if nameLen < 0 || nameLen > len(data)-off {
			return nil, fmt.Errorf("第 %d 个标记点名字长度非法：%d（剩余 %d 字节）", i, nameLen, len(data)-off)
		}
		if nameLen == 0 {
			// 无名标记点永远取不到 ⇒ 写端 bug，必须当场暴露（客户端同判据）。
			return nil, fmt.Errorf("第 %d 个标记点名字为空（按名取点取不到，属写端 bug）", i)
		}
		name := string(data[off : off+nameLen])
		off += nameLen
		if len(data)-off < 12 {
			return nil, fmt.Errorf("文件被截断：第 %d 个标记点（%q）坐标不完整，剩余 %d 字节", i, name, len(data)-off)
		}
		pos := readVec3(data, off)
		off += 12
		if !validMarkerPos(pos) {
			return nil, fmt.Errorf("第 %d 个标记点（%q）坐标非法（NaN/Inf）：(%v,%v,%v)", i, name, pos.X, pos.Y, pos.Z)
		}
		out = append(out, Marker{Name: name, Pos: pos})
	}
	return out, nil
}

// validMarkerPos 标记点坐标必须是有限值：NaN/Inf 会让消费方（距离比较 / 寻路起点）静默错乱。
func validMarkerPos(p geom.Vec3) bool {
	return !math.IsNaN(p.X) && !math.IsInf(p.X, 0) &&
		!math.IsNaN(p.Y) && !math.IsInf(p.Y, 0) &&
		!math.IsNaN(p.Z) && !math.IsInf(p.Z, 0)
}
