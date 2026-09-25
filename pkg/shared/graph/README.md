# pkg/shared/graph — 图搜索内核

全引擎**唯一一份 A\*** 实现。纯标准库（仅 `container/heap`）、无域依赖。

## 为什么存在

四类调用方共用同一份搜索骨架与优先队列，各自只提供两件事：**出边**与**启发值**：

| 位置 | 用途 |
|------|------|
| `pkg/domain/mmo/collide/nav.go` | 2D 网格寻路（`NavGrid`） |
| `pkg/domain/mmo/collide/waypoint.go` | 路点图寻路（`WaypointGraph`） |
| `pkg/domain/mmo/collide/nav3.go` | 分层三维寻路（`NavGrid3`） |
| `internal/domain/mmo/spatial/pathfinding/nav.go` | 行为树寻路（`WalkTo` / `RunTo`） |

## API

```go
import "github.com/qw576483/clover-server-engine/pkg/shared/graph"

// 1) 描述你的图
type myGraph struct{ /* ... */ }

// Edges 返回节点 n 的全部出边（邻居 + 边代价）。
func (g myGraph) Edges(n int) []graph.Edge[int] {
    return []graph.Edge[int]{{To: n + 1, Cost: 1}, {To: n + 10, Cost: 1.5}}
}

// Heuristic 返回 n 到 goal 的估计代价。**不得高估**（admissible），否则结果可能非最优。
func (g myGraph) Heuristic(n, goal int) float64 { return 0 } // 0 也能用，退化为 Dijkstra

// 2) 搜索
res := graph.SearchAStar[int](myGraph{}, 0, 42, 0) // maxExpand<=0 表示不限
// res.Path       含起点与终点的正序路径；不可达为 nil
// res.Iterations 展开过的节点数（性能监控 / 限流）
```

| 类型 | 说明 |
|------|------|
| `Edge[N]` | 出边：`To`（目标节点）+ `Cost`（通过代价，应为正数） |
| `Graph[N]` | 图抽象：`Edges(n) []Edge[N]` + `Heuristic(n, goal) float64` |
| `Result[N]` | `Path []N` + `Iterations int` |
| `SearchAStar[N](g, start, goal, maxExpand)` | 跑 A\*；起终点相同返回单元素路径 |

## 设计要点

- **节点类型是类型参数** `N comparable`：网格用线性下标 `int`、分层导航用 `nav3Key`、行为树寻路用 `Point{X,Z int32}`，都是 map 键，无需装箱。
- **降低 key 而非重复入队**：已有条目能用更小的 g 到达时走 `heap.Fix`（`item.index` 由堆维护），省掉一次出队。
- **回溯不需要防御性上限**：`parent` 只在创建条目时赋值，链上每个节点必然可达起点，不可能断链或成环。
- **`Heuristic` 的 goal 参数是提示**：实现可以忽略它（把目标记在自己的结构体里），只要是同一次搜索的同一目标即可。
