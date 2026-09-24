# pkg/domain/mmo/mapdata — 地图数据加载

把 Unity 导出的地图产物（`CloverMap` 二进制 v1）变成服务端可用的逻辑地图：
**可行走位图 → `NavGrid3`**、**障碍 AABB → `Grid3`（Mask=`GroupWall`）**、**出生点 → 净空净化后的落点**。

## 为什么它在引擎里（而不是各业务各写一份）

地图数据的**消费者是引擎自己的容器**：`collide.NavGrid3` / `collide.Grid3` / `mmo.Scene.Enter`。
引擎已经有这些容器的运行时形态，却缺少「怎么从美术场景得到它们」的那条路径 —— 那是能力面缺了半截。
况且导出端与加载端遵守同一份字节契约，分处两处维护必然漂移（而且失败是**静默**的：
文件能读、服务能起、只是地图不对）。

## 边界

| 属于引擎 | 属于业务 |
|---|---|
| 字节格式契约、解码、校验 | 地图**内容**（布局 / 障碍 / 出生点坐标） |
| 位图 → 导航网格、AABB → 碰撞宽相、出生点净化 | 烘焙**参数**（分辨率 / 原点 / 什么算障碍 / 阈值） |
| 客户端解码与"这格能不能走"查询 | 客户端**本地预测解算**（输入 → 位移 → 滑墙） |

> 最后一行是刻意的：客户端引擎 `结构规则.md` §3.1 明确「引擎不做本地预测」。
> 引擎给你空间事实（`WalkableAt` / 碰撞查询），**怎么用它做预测属于业务策略**。

## 快速上手

```go
import (
    "github.com/qw576483/clover-server-engine/pkg/domain/data"   // data.OwnerPlayer 等常量
    "github.com/qw576483/clover-server-engine/pkg/domain/mmo"
    "github.com/qw576483/clover-server-engine/pkg/domain/mmo/mapdata"
    "github.com/qw576483/clover-server-engine/pkg/shared/geom"   // geom.Vec3
)

m, err := mapdata.Load("Assets/MapData/map-city.bytes")
if err != nil { return err }

city := sm.CreateScene(m.SceneID, m.Name)
city.EnablePhysics()
if err := m.ApplyTo(city); err != nil { return err }   // 障碍注入 Collider3 + GroupWall

sp := m.SpawnAt(0)                                     // 已净空的出生点
city.EnterOwnerTypeInstance(playerID, data.OwnerPlayer, sp, 0)

path := m.FindPath(sp, geom.Vec3{X: 30, Y: 0, Z: 40})   // 三层导航（当前是单层）
```

`ApplyTo` 接受的是**最小接口** `WallTarget{ Collider3() *collide.Grid3 }` ——
`mmo.Scene` 结构上满足它，用测试替身也能接，加载器不必绑上整个场景系统。

## 字节布局（小端）

```text
偏移   长度                内容
0      4                  magic "CLVM"
4      2                  version        uint16   = 1
6      2                  flags          uint16
8      8                  scene_id       uint64   逻辑地图 id（与 mmo 场景 id、客户端 CloverScene.SceneID 对齐）
16     4                  cell_size      float32  格边长（米）
20     12                 origin         float32×3 位图原点：格子 (0,0) 的角（世界坐标 x/y/z）
32     4                  width          uint32   格数（东西向）
36     4                  depth          uint32   格数（南北向）
40     4                  collider_count uint32
44     4                  spawn_count    uint32
48     4                  name_len       uint32   地图名 UTF-8 **字节数**（不是字符数）
52     12                 保留段                   必须全 0；V2 加字段用
---    64 = HeaderSize
64     name_len            name                     UTF-8
       ceil(w*d/8)         walkable 位图             见下
       collider_count×24   colliders                见下
       spawn_count×12      spawns                   见下
       ---                 命名标记点段               **仅当 flags 含 FlagMarkers**，且**追加在文件末尾**（见下）
```

> 「追加在末尾」不是风格偏好而是契约：新段一律追加、既有段位置不动（见「版本演进」）。

### flags

| 位 | 名 | 含义 |
|---|---|---|
| bit0 | `FlagWalkable` | 带可行走位图。**V1 必需**，缺失即判非法 |
| bit1 | `FlagHeightField` | 带逐格地面高度场。**V1 未实现**（当前是单层平地，`origin.y` 即地面高度） |
| bit2 | `FlagMarkers` | 带**命名标记点**段（名字 + 世界坐标），追加在文件末尾。**只有真的有标记点时导出端才置位** |
| 其它 | — | 保留。**出现未知位一律报错** |

> ⚠️ `FlagHeightField`（bit1）**不在**读端的「能解析」集合内（置位即报未知段），
> `FlagMarkers`（bit2）**在**集合内且必须**完整解析**（不能只"认识位就跳过去"——
> 该段长度不定，跳过就无法判断它是否被截断，结果会是"客户端拒收、服务端照收"的两端不一致）。

### 可行走位图

- 每格 **1 bit**，向上取整到字节：`ceil(width*depth/8)`
- 行主序：`idx = z * width + x`（"行"是 z，"列"是 x）
- 字节内 **LSB 优先**：格 `idx` 的位 = `bytes[idx>>3] & (1 << (idx & 7))`
- `1` = 可走，`0` = 阻挡
- **补位**：`width*depth` 不是 8 的倍数时，最后一字节超出格数的位必须为 0。
  读端**不信任**这一点，会按格数截断统计（并把非 0 补位记成告警）

世界坐标 → 格坐标（两端必须同口径）：

```text
ix = floor((x - origin.x) / cell_size)
iz = floor((z - origin.z) / cell_size)
越界（ix<0 || iz<0 || ix>=width || iz>=depth）⇒ 不可走
```

> ⚠️ 必须是 **floor**，不能用向零截断（C# 的 `(int)`、Go 的 `int()` 都是向零截断）。
> 向零截断会让图外 `(-cell_size, 0)` 那一格沿用到第 0 格 ⇒ **图外可走**。
> 服务端 `mapdata.go: cellOf` 用 `math.Floor`，客户端 `Map.cs` 用 `Mathf.FloorToInt` —— 已用回归用例钉住。

### 碰撞体（colliders）

每个 24 字节 = 6 个 float32：`min.x, min.y, min.z, max.x, max.y, max.z`（世界坐标 AABB）。

去重与合并**不做**：导出端收集的是场景里每个 `Collider.bounds`（已排除地面与贴地薄板）。
加载后逐个插进 `Collider3` 并打 `GroupWall` 掩码（才参与遮挡与视线判定）。

### 命名标记点（markers）

**仅当 `flags` 含 `FlagMarkers`（bit2）时存在**，且**追加在全部既有段之后**：

```text
u32   marker_count                    标记点总数
repeat marker_count:
    u32   name_len                    名字的 UTF-8 **字节**数（不含终止符；可为中文等任意 UTF-8）
    byte[name_len] name
    f32   x, y, z                     世界坐标（米）；y 是对象的**真实高度**，不是地面高度
```

- 单条的**定长部分**是 16 字节（`u32 name_len` + 3 个 float32），`MarkerStride` 就是它；
  名字为 0 字节时正好占用 16 字节。
- 名字**允许重复**：同一名字下的多个点（一组出生点、一条路线的路点）**按文件顺序**排列，
  消费方按名过滤即可。名字的语义（`Spawn_T` / `Bombsite_A` / `Route_*`）属于**业务**，引擎不解释。
- **导出端只在真的有标记点时才置位**：没有标记点时既不置位也不写段 ⇒ 产物与旧版**逐字节一致**。
- **读端必须完整解析**（不能只认位就跳过）：段长不定，跳过就发现不了截断/坏数据，
  那会让"客户端拒收"的文件在服务端"照收"，两端对同一份字节给出不同结论。
- 服务端目前**不消费**这些点（出生点走 `spawns`，加载后做净空净化）：解析它们是为了
  ①校验段完整性、②后续（AI 路线锚点 / 包点 / 巡逻点）扩展时不必再改一次格式契约。
  客户端用 `Game.Map.GetPoints(名字)` / `TryGetPoint` 按名取点。

### 出生点（spawns）

每个 12 字节 = 3 个 float32：`x, y, z`。

导出端给的是约定位置；**加载端会做净空校验并可能挪动**（`spawn.go`）：
"位图可走"不等于"角色半径站得下"，贴墙出生会让玩家一进图就被本地碰撞锁死（只能原地跑动作）。

## 校验规则（读端必须全做，错一条即报错）

按顺序：

1. 长度 ≥ 64
2. magic == `"CLVM"`
3. `version == 1`
4. `flags & ~(FlagWalkable|FlagHeightField|FlagMarkers) == 0` —— **含未知段必须报错**，不能忽略
   （忽略了就是"地图少一块"的静默失败）
5. `flags & FlagWalkable != 0`
6. `0 < width, depth <= MaxDimension(32768)`，且 `width*depth <= 2^31`
7. `cell_size > 0` 且有限
8. 文件长度 ≥ `64 + name_len + ceil(w*d/8) + collider_count*24 + spawn_count*12`（**截断=报错**）
9. 文件**更长是允许的**（V2 追加段后旧读端仍可用）
10. 含 `FlagMarkers` 时，末尾的标记点段必须**完整可解析**：`marker_count` 非负、
    每条的名字长度前缀不越界、名字非空、坐标有限（非 NaN/Inf）—— 任一条不满足即报错
    （**截断=报错**；只"认识这个 flags 位"是不够的，必须真的走一遍解析）

## 版本演进

- **只加不改**：新字段一律追加在末尾 + 置一个新的 `flags` 位。旧读端因为第 4 条会**明确报错**
  而不是读出错地图。
- 已有字段语义变更 ⇒ `version` 必须 +1，且三处实现同步改。
- `FlagHeightField`（bit1）是给**多层 / 地形高度**预留的：实现时在 `name` 之后、
  位图之前追加 `width*depth` 个 float32（每格地面高度），并把这里的单层导航层
  （`mapdata.go: navLayerHeight`）改为按真实高度建层 + 层间 `NavLink`。
- `FlagMarkers`（bit2）**已落地**：段追加在文件末尾、**只有真的有标记点时导出端才置位**
  （因此不含该段的旧产物字节逐字节不变、旧读端行为不变），读端完整解析并校验。
  多层地图在真高度场落地前，用**导出端的层过滤逐层各烘一份**（一份数据一个楼层），
  而不是把多层塞进单层位图 —— 那会把上层楼板整片算成阻挡、连通性当场断掉。

## 产物落地

| 路径 | 谁读 |
|---|---|
| `<client 工程>/Assets/MapData/<name>.bytes` | 服务端（业务按自己的路径探测） |
| `<client 工程>/Assets/Resources/MapData/<name>.bytes` | 客户端（`Game.Res` 异步读，再交 `Game.Map.Load`） |

导出端**一次写两份**同一份字节。扩展名用 `.bytes` 是 Unity 的约定：它被导入为 `TextAsset`，
`TextAsset.bytes` 能拿到原始字节。

## 验证

| 层 | 用例 | 抓什么 |
|---|---|---|
| Go 解析层 | `format_test.go` | 魔数 / 版本 / flags / 尺寸 / 截断各条校验 |
| Go 标记段 | `marker_test.go` | 含标记段可加载（中文名/同名多点/真实高度）、不含该段的旧产物逐字节不变、段截断与坏数据（负数数量 / 名字长度越界 / 空名字 / NaN 坐标）逐条报错、高度场位仍拒绝 |
| Go 出生点 | `spawn_test.go` | 贴墙出生点是否被挪到净空格心 |
| 跨端（自造字节） | `fixture_test.go`（原计划的 `crosslang_test.go` 未落地） | 按布局逐字节自造数据、与 C# 编码器互为独立实现对照（不依赖 Unity） |
| 客户端字节层 | [`clover-client-unity-engine/Tests/Editor`](https://github.com/qw576483/clover-client-unity-engine/tree/main/Tests/Editor) | 偏移、字节序、位图方向、补位、截断、负坐标 floor 口径 |
| 端到端（真产物） | 业务工程自己的 golden 用例 | Unity 真导出的文件 → 服务端加载 → 碰撞/寻路生效 |
