# pkg/domain/mmo/aoi — 兴趣区域 (AOI)

格子化 AOI 系统 + 视觉系统。维护实体在**三维**空间中的位置，自动计算视野进出事件。

## 空间模型

- **视野是三维球体**：距离判定 X/Y/Z 全参与 —— 楼上楼下按真实空间距离计算，
  不会因为「水平投影重合」而互相看见。
- **索引仍按 (X,Z) 分格与分片**：垂直方向不分桶，多层楼的同一水平坐标落在一「列」。
  这样**分片数不会随楼层数膨胀**，而一列内的对象通常是个位数，扫描成本可忽略。
- **2D 俯视玩法**：把 Y 固定为 0 即可 —— 空间管线本身是三维的，没有平面/三维开关。
- **遮挡不属于 AOI**：AOI 只负责距离范围。「隔层完全不可见 / 被墙挡住」用
  `SetFilter` / `SetPermChecker`（按层过滤），或用 `collide.Grid3.Raycast`（视线判定）。

## 核心组件

### Grid

稀疏格子空间分区：把 (X,Z) 水平面按 `cellSize` 切成稀疏格子，每格挂一「列」对象。
对象的进入 / 移动 / 离开只更新所在格子，范围查询只扫描半径覆盖到的少量格子 ——
把 O(N) 的「谁在我周围」降到接近 O(1)。
按世界区域分片（super-shard）+ 分片锁，互不重叠的区域可完全并行。

### VisualSystem

在 Grid 之上提供「双向可见性 + 过滤 + 配额」：正向 `Visuals`、反向 `Observers`、`SetFilter`、`SetQuota`。
配额裁剪的「最近优先」与视野判定使用**同一空间维度**（三维模式下按三维距离），
避免出现「球体视野 + 平面配额」这种保留的对象在垂直方向上并不最近的错配。

## 快速开始

```go
import (
    "github.com/qw576483/clover-server-engine/pkg/domain/mmo"
    "github.com/qw576483/clover-server-engine/pkg/domain/mmo/aoi"
    "github.com/qw576483/clover-server-engine/pkg/domain/object"
)

// 注意：构造入口在 mmo 包（本包只提供接口与类型，不含实现）。
grid := mmo.NewGrid(64) // 64 米一格
grid.SetObserver(func(watcher, target object.ObjectID, ev aoi.Event) {
    switch ev {
    case aoi.EnterView:
        // 把「目标进入视野」推送给 watcher 对应的客户端
    case aoi.LeaveView:
        // 把「目标离开视野」推送给 watcher
    }
})

player := object.NewObjectID(object.TypePlayer, 1001)
grid.Enter(player, aoi.Position{X: 100, Y: 0, Z: 200}) // Y 是高度
grid.Watch(player, 96)                                 // 玩家视野半径 96 米
grid.Move(player, aoi.Position{X: 120, Y: 0, Z: 200})  // 自动算出视野增量并回调 Observer
grid.Leave(player)
```

## API 参考

### Grid

| 方法 | 说明 |
|------|------|
| `SetObserver(fn Observer)` | 设置视野事件回调（`EnterView` / `LeaveView` / `LeaveAll`） |
| `SetPermChecker(fn)` | 设置权限校验器：返回 false 的目标不进入视野 |
| ~~`SetRefreshRate(d)`~~ | **不提供**：实现侧无接线点（`shouldRefresh` 无调用），设了也不生效、刷新频率实际不受限。暴露静默失效的限流开关比没有更糟；将来做节流需带 enter/leave 补偿语义 |
| `Enter(id, pos)` / `Move(id, pos)` / `Leave(id)` | 实体进入 / 移动 / 离开（自动刷新视野增量） |
| `Watch(id, radius)` | 登记为观察者并设定视野半径，返回初始可见集合；`radius<=0` 等价 `Unwatch` |
| `Unwatch(id)` | 取消观察者身份（保留位置） |
| `BeginBatch()` / `EndBatch()` | 批量模式：一帧内多次移动合并为一次视野重算 |
| `Visible(id)` | 观察者当前可见实体列表 |
| `Around(center, radius)` | 指定坐标半径内的全部实体 |
| `Neighbors(id, radius)` | 半径内其他实体（不含自身） |
| `Position(id)` | 实体三维坐标 |
| `Count()` / `CellSize()` | 实体总数 / 格子边长 |
| `RemoveAll()` / `Stop()` | 清空所有实体 / 停止后台清理 |

### VisualSystem

| 方法 | 说明 |
|------|------|
| `SetObserver(fn VisualObserver)` | 设置视野增量回调（含被配额 / 过滤裁剪掉的） |
| `Enter(id, pos)` / `Move(id, pos)` / `Leave(id)` | 实体进入 / 移动 / 离开 |
| `SetVisual(id, radius)` | 登记为观察者并设定视觉半径；`radius<=0` 用默认半径 |
| `ClearVisual(id)` | 取消观察者身份（保留位置） |
| `SetFilter(fn FilterFn)` | 设置可见性过滤（阵营 / 分组 / 距离二次校验），变更后全量重算 |
| `SetQuota(q Quota)` | 每个观察者最大可见数（按距离最近优先保留），0 = 不限 |
| `Visuals(id)` | 该观察者能看到谁 |
| `Observers(id)` | 谁能看到该实体（反向索引） |
| `Stop()` / `Close()` | 释放底层网格（构造后必须配对调用） |

### 类型定义

| 类型 | 说明 |
|------|------|
| `Position{X, Y, Z}` | 三维坐标（Y = 高度） |
| `Observer` | `func(watcher, target object.ObjectID, ev Event)` |
| `VisualObserver` | `func(viewer, target object.ObjectID, ev Event)` |
| `FilterFn` | `func(viewer, target uint64) bool` |
| `Quota` | 单个观察者的可见目标上限 |
| `Event` | `EnterView` / `LeaveView` / `LeaveAll` |
