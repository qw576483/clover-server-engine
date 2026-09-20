# pkg/domain/mmo/collide — 碰撞与空间查询

提供基础几何类型、碰撞检测、分组掩码、以及基于格子的空间查询。用于为实体分配物理身份、做粗粒度相交检测。

## 二维 / 三维两套并存

包内同时提供 2D 与 3D 两套原语，**互不影响**：

| | 2D（俯视玩法 / 贴地碰撞） | 3D（多层地形 / 飞行 / 立体弹道） |
|---|---|---|
| 几何 | `Vec2` / `Circle` / `AABB` / `Segment` / `Shape` | `geom.Vec3` / `AABB3` / `Sphere` / `Segment3` |
| 宽相 | `Grid` | `Grid3` |
| 寻路 | `NavGrid`（单层八方向 A*） | `NavGrid3`（层内 `NavGrid` + 层间 `NavLink`） |
| 线段扫描 | `Grid.SweepCCD`（只挡水平面的墙） | `Grid3.SweepCCD` / `Grid3.Raycast`（**楼板/天花板同样遮挡**） |

为什么是两套而不是一套：2D 的 `AABB{MinX,MinY,MaxX,MaxY}` 里 `MinY/MaxY` 承载的是**世界 Z**
（见 `mmo.bodyAABB`），类型层面就没有高度；3D 要能表达楼板与天花板，因此用自己的 `AABB3` / `Sphere` / `Segment3`。
两套类型各服务一类玩法，互不影响。

> 坐标约定：Y 为高度，与 `geom.Vec3` / `mover` / AOI 一致，单位同为「米」。
>
> **寻路算法不重复实现**：本包的 3 处 A\*（`NavGrid` / `NavGrid3` / `WaypointGraph`）与
> 行为树寻路（`internal/domain/mmo/spatial/pathfinding`）共用 `pkg/shared/graph` 的**同一个 A\* 内核与同一份优先队列**，
> 本包只提供「把网格/路点图适配成图」的 `Edges` / `Heuristic`。见该包 README。

## 核心组件

### 基础几何

- `Vec2` — 二维向量，支持 Add/Sub/Scale/Len/Dist/Dot/Normalize/Mid
- `Circle` / `AABB` / `Segment` — 基础几何体
- `Shape` — 统一形状接口（Circle / Polygon），支持 Contains 和 Intersect

### 碰撞检测

提供 CircleVsCircle / CircleVsAABB / AABBvsAABB 等宽相检测函数。

### 碰撞掩码

`CollisionMask` 用于分组过滤，内置分组：`GroupPlayer` / `GroupMonster` / `GroupWall` / `GroupProjectile` / `GroupTrigger` / `GroupPickup`。

### 空间结构

| 组件 | 说明 |
|------|------|
| `Grid` | 2D 碰撞格子，快速空间查询 |
| `NavGrid` | 2D 寻路格子，管理可行走区域 |
| `Zone` | 2D 触发区，检测进出事件 |
| `HeightField` | 高度场，管理地形高低（2.5D 地形） |
| `WaypointGraph` | 路点图，用于寻路 |
| `Grid3` | **3D** 碰撞格子：切格含 Y，楼上楼下不再互相误报 |
| `NavGrid3` | **3D** 分层寻路：层内 `NavGrid` + 层间连接 |

## 三维（立体空间）

### 原语

```go
import (
    "github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
    "github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

b := collide.AABB3{Min: geom.Vec3{X: -1, Y: 0, Z: -1}, Max: geom.Vec3{X: 1, Y: 3, Z: 1}}
s := collide.Sphere{Center: geom.Vec3{X: 0, Y: 1, Z: 0}, R: 2}
seg := collide.Segment3{A: geom.Vec3{X: 0, Y: 1, Z: 0}, B: geom.Vec3{X: 0, Y: 5, Z: 0}}

collide.AABB3vsAABB3(b, b)        // true
collide.SphereVsAABB3(s, b)       // true
collide.Segment3AABB3Sweep(seg, b) // 首个相交参数 t∈[0,1]，不相交为 -1
collide.Segment3SphereHit(seg, s)  // 线段命中球体的 t 与是否命中（起点在球内返回 t=0）
```

### Grid3（三维宽相）

| 方法 | 说明 |
|------|------|
| `NewGrid3(cell)` | 构造三维空间网格 |
| `Insert(id, AABB3)` / `Update` / `Remove` | 登记 / 更新 / 注销（幂等，跨格不会残留幽灵 id） |
| `SetSphere(id, Sphere)` | 登记精确球体外形（深检测优先用它） |
| `SetMask(id, CollisionMask)` | 设置碰撞掩码（`GroupWall` 才参与 `SweepCCD`/`Raycast` 遮挡） |
| `QueryRegion(AABB3)` / `QuerySphere(Sphere)` / `QueryPoint(p)` | 三维区域 / 球体 / 点查询 |
| `Nearby(id)` | 包围盒重叠的候选（不含自身） |
| `Collisions()` / `DeepCollisions()` | 重叠对：前者用 `AABB3`，后者优先用已登记球体 |
| `SweepCCD(id, from, to)` | 立体弹道避障：返回首个阻挡对象与 `t∈[0,1]`（排除自身，只挡 `GroupWall`）。窄相用**中心线段**扫掠，自身包围盒只参与广相 —— 即把移动体当**质点**（与 2D 版同语义）；要按半径判定请自行外扩障碍或用 `SetSphere` + `QuerySphere` |
| `Raycast(from, to)` | 视线判定：不排除任何对象，取路径上第一个 `GroupWall` |

> **cell 取值（三维是三次方）**：`Grid3` 展开的格子数 = `⌈X/cell⌉·⌈Y/cell⌉·⌈Z/cell⌉`。
> 2D 插一块大平板只按面积膨胀，3D 会再乘一层高度：**cell 取「常见视野半径」量级**即可（`scene.Collider3()` 用的是场景 `cellSize`）；
> 若把 cell 设成 1 米再插一面 1000×1000 的墙，会瞬时展开 10⁶ 个格子。

### NavGrid3（分层寻路）

```go
n := collide.NewNavGrid3(collide.WithDiagonal3(true))
f1 := n.AddLayer(0, 3, collide.NewNavGrid(64, 64)) // 一楼：高度区间 [0,3)
f2 := n.AddLayer(3, 6, collide.NewNavGrid(64, 64)) // 二楼：高度区间 [3,6)
n.AddLink(collide.NavLink{
    FromLayer: f1, ToLayer: f2,
    From: [2]int{10, 20}, To: [2]int{10, 0}, // 楼梯：一楼 (10,20) ↔ 二楼 (10,0)
    Bidirectional: true,
})

path := n.FindPath3(from, to) // []geom.Vec3，含分层高度；跨层必须走 NavLink
```

| 方法 | 说明 |
|------|------|
| `NewNavGrid3(opts...)` | `WithDiagonal3(bool)` / `WithMaxExpand3(n)` |
| `AddLayer(baseY, topY, grid)` | 追加一层（`topY>baseY` 时参与 `LayerAt` 的区间匹配） |
| `AddLink(NavLink)` | 追加层间连接；两端不可走时拒绝（防止建出穿墙楼梯） |
| `LayerAt(y)` | 按高度定位层号；无区间命中返回最近层 |
| `FindPath3(from, to)` / `FindPath3Cells(...)` | 三维寻路：层内八方向 A* + 层间 `NavLink`，代价与启发同为三维欧氏距离 |

> 为什么是「分层」而不是「体素」：三维体素 A* 的内存与扩展节点数是 O(w·h·d)，
> 多层地图里绝大多数高度是空气，体素化后 90% 以上格子是永久阻挡 —— 纯浪费。
> 分层模型的内存是 O(层数·w·h)，与 2D 同量级，且地图导出管线更好写。

## 快速开始

```go
import "github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"

c1 := collide.Circle{Center: collide.Vec2{X: 0, Y: 0}, Radius: 5}
c2 := collide.Circle{Center: collide.Vec2{X: 3, Y: 4}, Radius: 5}
collide.CircleVsCircle(c1, c2) // true

grid := collide.NewGrid(10) // 10 米一格
grid.Insert("npc:1", collide.AABB{MinX: -1, MinY: -1, MaxX: 1, MaxY: 1}) // 宽相盒
grid.SetShape("npc:1", collide.CircleShape(1))                           // 精确外形（深检测用）
near := grid.QueryRegion(collide.AABB{MinX: -5, MinY: -5, MaxX: 5, MaxY: 5}) // 区域查询
hit, t := grid.SweepCCD("npc:1", from, to) // 线段扫描：只挡掩码含 GroupWall 的对象
```

## API 参考

### Vec2

| 方法 | 说明 |
|------|------|
| `Add(v)` | 加法 |
| `Sub(v)` | 减法 |
| `Scale(s)` | 缩放 |
| `Len()` | 向量长度 |
| `Dist(v)` | 两点距离 |
| `Dot(v)` | 点积 |
| `Normalize()` | 归一化 |
| `Mid(v)` | 中点 |

### Grid（碰撞格子）

| 方法 | 说明 |
|------|------|
| `NewGrid(cellSize)` | 创建格子 |
| `Insert(id, AABB)` / `Update(id, AABB)` | 登记 / 更新宽相包围盒 |
| `SetShape(id, Shape)` | 登记精确外形（圆 / 多边形，深检测用） |
| `SetMask(id, CollisionMask)` | 设置分组掩码（`GroupWall` 才参与 `SweepCCD` 遮挡） |
| `Remove(id)` | 移除实体 |
| `QueryRegion(AABB)` / `QueryPoint(x, y)` / `Nearby(id)` | 区域 / 点 / 邻居查询 |
| `Collisions()` / `DeepCollisions()` | 重叠对（后者用精确外形） |
| `SweepCCD(id, from, to)` | 线段扫描，返回首个阻挡对象与 `t∈[0,1]` |
| `Len()` | 登记对象数 |

### NavGrid（寻路格子）

| 方法 | 说明 |
|------|------|
| `NewNavGrid(...)` | 创建寻路格子 |
| `SetBlocked(x, z, blocked)` | 设置格子阻塞状态 |
| `IsWalkable(x, z)` | 判断是否可行走 |
| `InBounds(x, z)` | 判断是否在边界内 |

### Zone（触发区）

| 方法 | 说明 |
|------|------|
| `NewZone(...)` | 创建触发区 |
| `Contains(pos)` | 判断点是否在区域内 |
| `OnEnter(fn)` / `OnLeave(fn)` | 注册进出回调 |
| `Update()` | 更新触发区状态 |

### HeightField（高度场）

| 方法 | 说明 |
|------|------|
| `NewHeightField(cols, rows, cell)` | 创建高度场 |
| `InBounds(cx, cy)` / `Cell(x, z)` | 格子边界判断 / 世界坐标→格 |
| `SetWalk(cx, cy, walk, minH, maxH)` / `Block(cx, cy)` / `Unblock(cx, cy)` | 设置可行走 / 阻塞 / 解除 |
| `Walkable(x, z)` / `Height(x, z)` | 某点是否可行走 / 取高度区间 |
| `LineWalk(x0, z0, x1, z1, roleRadius)` | 沿线通行判定（含角色半径） |

### WaypointGraph（路点图）

| 方法 | 说明 |
|------|------|
| `NewWaypointGraph()` | 创建路点图 |
| `AddNode(pos)` | 添加路点 |
| `AddEdge(from, to, cost)` | 添加边 |
| `Nearest(pos)` | 查找最近路点 |
| `FindPath(from, to)` | A* 寻路 |
