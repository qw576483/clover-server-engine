# pkg/domain/mmo/pathfinding — 寻路

提供寻路相关类型定义与接口。具体寻路算法实现在引擎内部，业务通过 WalkAction 接口集成到行为树。

> 搜索骨架（open/closed、松弛、回溯、优先队列）来自 `pkg/shared/graph` 的**全引擎唯一一份 A\***
> 内核，本包只负责把 `NavGridProvider` + 代价函数适配成图。

## 核心概念

- **Point** — 二维整数坐标，用于格子化寻路
- **PathResult** — 寻路结果，包含路径点序列和迭代次数
- **WalkAction** — 移动行为接口，可挂载到行为树驱动实体移动

## 快速开始

```go
import "clover-server-engine/pkg/domain/mmo"

result := mmo.FindPath(navGrid, mmo.Point{X: 0, Z: 0}, mmo.Point{X: 10, Z: 10})
for _, p := range result.Points {
    fmt.Printf("(%d, %d)\n", p.X, p.Z)
}

// WalkAction 是行为树 Action：构造时给出目标点与到达距离，运行时由行为树每帧调用 Tick(bb)
walk := mmo.NewWalkAction(navGrid, mmo.Point{X: 10, Z: 10}, 0.5)
walk.SetTarget(mmo.Point{X: 20, Z: 20}) // 需要改目标时调用
status := walk.Tick(bb)                 // bb 为 *btree.Blackboard；Success 表示已到达
```

## 三维 / 多层地形

本包的 `Point` / `NavGridProvider` / `FindPath` 都是**单层二维**格子寻路（对应 `collide.NavGrid`）。

多层地形（上下楼、多层副本）、飞行、立体弹道需要的是 `collide.NavGrid3` —— **分层导航**：
每层一张 `NavGrid`（复用本包同款八方向 A*），层间用 `NavLink`（楼梯/坡道/电梯/跳点/传送）连通，
`FindPath3(from, to)` 返回三维路径点 `[]geom.Vec3`，可直接喂给 `mover.MoveTo`。
用法见 `pkg/domain/mmo/collide/README.md` 的「三维（立体空间）」一节。

## API 参考

### Point

```go
type Point struct {
    X int32
    Z int32
}
```

二维整数坐标，对应 NavGrid 中的格子索引。

### PathResult

```go
type PathResult struct {
    Points     []Point
    Iterations int
}
```

`Points` 为有序路径点序列，`Iterations` 为算法迭代次数（可用于性能监控）。

### WalkAction 接口

| 方法 | 说明 |
|------|------|
| `SetTarget(target Point)` | 设置移动目标点 |
| `Tick(bb *btree.Blackboard) btree.Status` | 每帧更新（行为树节点接口） |

- 由行为树（或调用方）每帧驱动一次 `Tick`，节点状态经黑板传递
- 返回 `Success` 表示已到达目标

### 工厂函数（位于 `pkg/domain/mmo`）

| 函数 | 说明 |
|------|------|
| `FindPath(grid, from, to)` | 一次性计算完整路径 |
| `FindPathWithCost(grid, from, to, costFn)` | 计算路径并返回总代价（`costFn` 自定义相邻格代价） |
| `NewWalkAction(grid, target, reachDist)` | 创建行走动作（目标点 + 到达距离阈值） |
| `NewRunAction(grid, target, reachDist)` | 创建奔跑动作（同上，跑步速度） |
| `SimplifyPath(points)` | 路径简化，去除冗余拐点 |
