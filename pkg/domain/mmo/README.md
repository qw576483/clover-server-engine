# pkg/domain/mmo — MMO 场景系统

## 模块职责

MMO 场景的统一组合层。把 AOI 视野、场景管理、物理碰撞、分帧定时器、跨节点迁移等底层能力组装成三级结构，向业务提供「进场 / 移动 / 离场 / 查询 / 广播」这一层接口。

结构方向（见 `结构规则.md` §五）：本包是**门面包** —— 只做**类型别名 / 变量转发 / 极薄参数适配**，
真身（`SceneManagerFacade` / `SceneFacade` / `InstanceFacade` + 各自包装实现、工厂函数、错误与常量）全部在
`internal/domain/mmo`（主门面 `facade.go`；`aoi` / `pathfinding` / `buff` / `skill` / `mob` 的对口包装分别在
`internal/domain/mmo/{spatial/aoi,spatial/pathfinding,gameplay/buff,gameplay/skill,gameplay/mob}/facade.go`）。

子包 `ai/btree`、`aoi`、`buff`、`collide`、`combat`、`mapdata`、`mob`、`mover`、`pathfinding`、`skill`、`sync`
**自身不 import internal**，属 §5.2 的**自包含包**（类型真身 + 自包含实现留在 pkg）。

```go
import "github.com/qw576483/clover-server-engine/pkg/domain/mmo"
```

## 三级结构

| 层级 | 类型 | 职责 |
|------|------|------|
| **SceneManager** | 全局容器 | 管理所有 Scene 的生命周期与跨节点传输 |
| **Scene** | 一张地图 | 持有共享的物理碰撞网格与多个 Instance |
| **Instance** | 隔离最小单位 | 同一 Instance 内实体互相可见，跨 Instance 互不可见 |

## 快速上手

```go
sm := mmo.NewSceneManager(mmo.WithStore(store), mmo.WithPublisher(pub))
city := sm.CreateScene(1, "city")
city.EnablePhysics()

// Vec3 是三维坐标：X=东西、Y=高度、Z=南北。Y 会被透传给 AOI、物理与跨节点同步，
// 不能省（省了就是"站在地面上"，跳跃/飞行/多层地形全部失效）。
city.Enter(playerID, mmo.Vec3{X: 0, Y: 0, Z: 0})
city.Move(playerID, mmo.Vec3{X: 10, Y: 0, Z: 10})
neighbors := city.Neighbors(playerID, 100)
_ = neighbors
city.Leave(playerID)
```

## 地图数据加载（mapdata 子包）

`mapdata` 把 Unity 烘焙出的 CloverMap 二进制变成上面这些容器：可行走位图 → `NavGrid3`、
障碍 AABB → `Collider3`（Mask=`GroupWall`）、出生点 → 净空净化后的落点。
格式契约（逐字节）与边界说明见 [`mapdata/README.md`](mapdata/README.md)。

```go
import (
    "github.com/qw576483/clover-server-engine/pkg/domain/mmo/mapdata"
    "github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

m, err := mapdata.Load("Assets/MapData/map-city.bytes")   // 也可用 mapdata.Decode(raw) 从字节构造
if err != nil { return err }
if err := m.ApplyTo(city); err != nil { return err }      // 注入场景（city 是 mmo.Scene）
sp := m.SpawnAt(0)                                        // 已做过净空校验的出生点
path := m.FindPath(sp, geom.Vec3{X: 30, Z: 40})           // 导航寻路
```

> 归属：地图数据的**消费者是引擎自己的容器**（`NavGrid3` / `Grid3` / `Scene.Enter`），
> 所以「怎么从美术场景得到它们」这条链路属引擎（否则每个 3D 项目都要重写一遍，且两端契约必然漂移）。
> 地图**内容**与**烘焙参数**仍是业务数据。

## 立体空间（3D）

引擎默认按**三维**处理空间，2D 俯视玩法把 Y 固定为 0 即可，无需任何配置：

| 能力 | 入口 | 说明 |
|------|------|------|
| 三维视野 | `Enter/Move/Neighbors/Around/SetViewRadius` | 视野是**球体**（X/Y/Z 全参与距离）：楼上楼下按真实空间距离算，不会因水平投影重合而互相看见 |
| 三维碰撞宽相 | `scene.Collider3()` → `*collide.Grid3` | `QueryRegion` / `QuerySphere` / `Nearby` / `Collisions` / `DeepCollisions` / `SweepCCD` / `Raycast`；楼板与三层墙体一并参与遮挡 |
| 立体弹道 / 视线 | `Grid3.SweepCCD(id, from, to)` / `Grid3.Raycast(from, to)` | 线段在三维空间求首个碰撞点（slab 法），返回 `(被挡对象, t∈[0,1])` |
| 三维碰撞原语 | `collide.AABB3` / `Sphere` / `Segment3` + `Segment3AABB3Sweep` / `Segment3SphereHit` | 与 2D 的 `AABB/Circle/Segment` 并存 |
| 多层寻路 | `collide.NewNavGrid3()` + `AddLayer` / `AddLink` | 层内复用 `NavGrid` 八方向 A*，层间走 `NavLink`（楼梯/坡道/电梯/跳点/传送），`FindPath3` 返回三维路径点 |
| 三维物理 | `AddBody` / `ApplyForce` / `SetVelocity` | `Body` 的位置/速度/力均为 `Vec3`，施力带 Y 分量即可做垂直运动（无内置重力，需要重力用 `mover`） |

**2D 俯视玩法**：把 Y 固定为 0 即可，空间管线本身就是三维的，无需任何开关。

> 「隔层完全不可见」这类**遮挡**语义不属于 AOI（AOI 只负责距离范围）：
> 用 `SetFilter` / `SetPermChecker` 按层过滤，或用 `Grid3.Raycast` 做视线判定。

## API 速查表

### 场景管理

| 函数 | 说明 |
|------|------|
| `NewSceneManager(opts ...Option)` | 创建全局场景管理器 |
| `sm.CreateScene(id, name)` / `sm.GetScene(id)` | 创建 / 获取场景 |
| `sm.DestroyScene(id)` | 销毁场景并踢出全部成员 |
| `sm.TransferRemote(dstID, objID, pos)` | 跨机器迁移对象到目标场景坐标（`pos` 为三维落点，含高度）。**需先注入** `WithClusterRoute` + `WithRemoteTransferSubscriber`，否则返回 `ErrNoRoute` |
| `WithClusterRoute(route, nodeID)` | 开启跨机迁移：注入 scene→node 路由表与本节点数字 ID（配 `node_id` 才有） |
| `WithRemoteTransferSubscriber(sub)` | 注入订阅能力，接收跨机迁移指令；与 `WithClusterRoute` 成对使用 |
| `sm.Run(ctx)` | 阻塞启动，运行心跳直到 ctx 取消 |

### 场景操作

| 函数 | 说明 |
|------|------|
| `Enter(objID, pos)` | 以坐标进入场景 |
| `EnterOwnerType(objID, ownerType, pos)` | 以指定实体类型进入默认 instance |
| `EnterOwnerTypeInstance(objID, ownerType, pos, instanceID)` | 以指定实体类型进入指定 instance |
| `Leave(objID)` | 离开场景 |
| `Move(objID, pos)` | 移动到指定坐标（触发 AOI 同步） |
| `MoveBatch(moves)` | 批量移动，整帧只刷新一次 AOI |
| `BeginBatch()` / `EndBatch()` | 手动批量模式，累积后统一刷新 |
| `SetViewRadius(objID, radius)` | 设置视野半径；r≤0 取消观察 |

### 查询 / 广播

| 函数 | 说明 |
|------|------|
| `Members()` | 场景内全部成员 id |
| `Neighbors(objID, radius)` | 半径内其他对象 id |
| `Around(objID, radius)` | 半径内全部对象（含自身） |
| `Position(objID)` | 对象坐标（三维，含高度 Y） |
| `Broadcast(msgID, body)` | 全员广播（可靠传输） |
| `SendTo(objID, msgID, body)` | 单点发送 |

### 物理

| 函数 | 说明 |
|------|------|
| `EnablePhysics()` / `DisablePhysics()` | 开启 / 关闭物理步进 |
| `AddBody(objID, body)` / `RemoveBody(objID)` | 注册 / 移除物理体（`Body` 的位置/速度/力均为 `Vec3`，Y 为高度） |
| `ApplyForce(objID, force)` / `SetVelocity(objID, vel)` | 施力 / 设速（三分量，带 Y 分量即可做垂直运动） |
| `Collider()` / `Collider3()` | 取 2D / 3D 碰撞宽相：业务注册墙体（`SetMask(GroupWall)`）后即可做区域查询、立体弹道与视线判定；2D 的 `AABB{MinY,MaxY}` 承载的是**世界 Z**，按真实高度请用 3D 那套 |

### Instance / 同步

| 函数 | 说明 |
|------|------|
| `CreateInstance(id)` / `RemoveInstance(id)` | 创建 / 删除隔离实例 |
| `WireEntitySync(...)` | 接入实体同步管线 |
| `NewSyncManager(mode)` | 创建同步管理器 |
| `NewInterpolation(...)` / `NewExtrapolation(...)` | 插值 / 外推策略 |
| `NewPrediction(...)` | 客户端预测策略 |

### 构造选项

| 选项 | 说明 |
|------|------|
| `WithStore(store)` / `WithPublisher(pub)` | 注入数据存储 / 消息发布器 |
| `WithTickRate(d)` | 心跳频率（如 50ms = 20fps） |
| `WithCellSize(cell)` | AOI 格子边长 |
| `WithViewSubject(subject)` | 视野同步的 NATS subject |
