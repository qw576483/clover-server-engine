# pkg/domain/mmo/ai/btree — 行为树

轻量行为树框架。提供 Status / Blackboard / Node 等基础类型，以及 Sequence / Selector / Parallel 等组合节点。工厂函数统一在 `mmo.NewXxx` 中提供。

## 核心概念

- **Status** — 节点执行状态：Success / Failure / Running
- **Blackboard** — 共享黑板，节点间传递数据
- **Node** — 行为树节点接口，所有节点实现 `Tick` 方法

## 快速开始

```go
import (
    "time"

    "github.com/qw576483/clover-server-engine/pkg/domain/mmo"
)

tree := mmo.NewTree(
    mmo.NewSequence(
        mmo.NewCondition(func(bb mmo.Blackboard) bool {
            return bb.GetBool("enemy_in_range")
        }),
        mmo.NewAction(func(bb mmo.Blackboard) mmo.Status {
            // 执行攻击逻辑
            return mmo.StatusSuccess
        }),
    ),
)

bb := mmo.NewBlackboard()
bb.Set("enemy_in_range", true)
status := tree.Tick(bb, time.Second) // Tick(黑板, 帧间隔)
```

## 节点类型

### 控制节点（组合节点）

| 节点 | 说明 |
|------|------|
| `NewSequence(children...)` | 顺序执行，全部成功才成功 |
| `NewSelector(children...)` | 选择执行，任一成功即成功 |
| `NewParallel(policy, children...)` | 并行执行，按 `ParallelPolicy` 策略判定成功 |
| `NewInverter(child)` | 反转子节点结果 |
| `NewRepeater(max, child)` | 重复执行指定次数 |
| `NewUntilFailure(child)` | 持续执行直到失败 |
| `NewLimiter(limit, window, child)` | 限制窗口内执行次数（`window` 为 `time.Duration`） |
| `NewCooldown(d, child)` | 冷却时间，间隔执行（`d` 为 `time.Duration`） |
| `NewTimeout(d, child)` | 超时中断（`d` 为 `time.Duration`） |

> **时间源（全树一套）**：`Tree.Tick(bb, dt)` 每帧写入 `"dt"`（本帧步长，Timeout 读它）
> 并保证 `"now"`（当前逻辑时刻，Limiter / Cooldown 读它）每帧存在且推进，归属按**首帧**判定：
> 首次 Tick **之前** `bb.Set("now", mmo.LogicalTime(逻辑秒))` ⇒ 归驱动方（Tree 只读）；
> 否则归 Tree，由它按 `dt` 自累加推进。
> 只有**直接 tick 节点、不经 `Tree.Tick`** 时才回落墙钟（并留一条降频 Warn）。

### 叶子节点

| 节点 | 说明 |
|------|------|
| `NewCondition(fn)` | 条件判断（`fn(bb) bool`） |
| `NewAction(fn)` | 执行动作（`fn(bb) Status`） |
| `NewActionFn(fn)` | 便捷版动作（`fn(bb)`，执行即成功） |

### Blackboard 接口

| 方法 | 说明 |
|------|------|
| `Set(key, value)` | 设置值 |
| `Get(key)` | 获取值 |
| `Del(key)` | 删除值 |
| `GetFloat64(key)` | 获取 float64 |
| `GetInt64(key)` | 获取 int64 |
| `GetString(key)` | 获取 string |
| `GetBool(key)` | 获取 bool |

## 工厂函数

所有工厂函数位于 `pkg/domain/mmo` 包：

```go
mmo.NewTree(root)                    // 构造根节点
mmo.NewSequence(children...)         // 顺序组合
mmo.NewSelector(children...)         // 选择组合
mmo.NewParallel(policy, children...) // 并行组合（policy 为 ParallelPolicy）
mmo.NewCondition(fn)                 // 条件节点（fn(bb) bool）
mmo.NewAction(fn)                    // 动作节点（fn(bb) Status）
mmo.NewActionFn(fn)                  // 便捷动作节点（fn(bb)）
```
