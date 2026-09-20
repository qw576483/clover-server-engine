# pkg/ 暴露面审计

本文记录引擎对外公开接口及其分类。

## 结构方向（现行，见 `结构规则.md` §五）

**门面在 `pkg`，真身在 `internal`。** `pkg` 只做门面 —— 类型别名（`type X = internal.X`）、声明转发
（`var F = internal.F` / `const K = internal.K`）、极薄参数适配；**实现体一律留 `internal`**。
`pkg` **允许** import `internal`（那是门面的定义）。

另有一类**自包含包**：自身**不** import internal（`pkg/shared/**`、`pkg/foundation/**`、`pkg/runtime/**`、
`pkg/domain/mmo/<子包>`、`pkg/domain/data/**`、`pkg/domain/room/**`、`pkg/transport/event`…），
按 §5.2 允许把类型真身与整段实现留在 `pkg`。

**判据只有一句**：`grep -n "github.com/qw576483/clover-server-engine/internal" <包>/*.go` —— 命中 = 门面包（只许别名 / 转发 / 极薄适配）；
不命中 = 自包含包。

## 公开接口（stable，外部业务可依赖）

| 包 | 导出 | 用途 |
|----|------|------|
| `pkg/domain/data/{account,player,order}` | EAccount, EPlayer, EOrder | **类型真身**（`account/account.go` 等）；`internal/domain/data/*` 反向引用它们 |
| `pkg/shared/proto` | EMsgXxx, EPushXxx | **消息号真身**（`pkg/shared/proto/msg.go` / `push.go`）；回包体类型（`E*Reply`）与内部信封在 `internal/shared/proto` |

## 实验性接口（use at own risk）

| 包 | 导出 | 风险 |
|----|------|------|
| `pkg/foundation/metrics` | Counter, Gauge, Histogram, ModuleMetrics | 指标接口透传（零第三方依赖，纯标准库实现） |
| `pkg/foundation/logger` | WithTrace, Field, CtxXxx, Debug/Info/Warn/Error/Panic/Fatal(+f), Sync, **Config / DefaultConfig / Init / Get / LevelHandler** | 结构化日志门面。`Config`/`DefaultConfig`/`Init` 仍是 `pkg/app/app.go` 的 `Run` 的装配入口（`pkg/foundation/logger/facade.go`），`Get`/`LevelHandler` 供 admin 控制面与 `AdminServer` 使用——**不属于业务日常 API**，业务侧只用 `WithTrace/Field/Ctx*/Debug..Fatal/Sync` |
| `pkg/foundation/logbuf` | Option, WithSubType/WithReason/WithSubReason/WithLevel/WithTraceID | 业务日志选填项，供 `app.Game.AddLog` 使用 |
| `pkg/runtime` | - | Go runtime 辅助 |

## 分类原则

- **stable**: 业务代码可直接引用，当前 API 形态已确定
- **实验性**: 接口尚未冻结，后续可能调整
- **内部**: 不在 `pkg/` 导出（`internal/` 可见性即够用）

## 现状

- `pkg/` 暴露面保持精简，公开包均有明确的业务入口
- `pkg/domain/mmo/*` 子包在 pkg 侧按能力**平铺**（`ai/btree`、`aoi`、`collide`、`pathfinding` 等）；三子域拆分（spatial/gameplay/sync）只体现在 `internal/domain/mmo` 侧

## 引擎当前无引用点的通用工具包（保留，供业务按需使用）

下列包在引擎、demo、tools 里**都没有 import 点**——深度扫描会把它们标成"疑似死代码"。
它们是通用工具库、各自带 README，**当前按「保留」处理**（不是遗留物，也未被删）：

| 包 | 用途 |
|----|------|
| `pkg/shared/bitset` | 位集合（大量布尔标记的紧凑表示） |
| `pkg/shared/bloom` | 布隆过滤器（存在性预判，省一次后端查询） |
| `pkg/shared/cache` | 分片 LRU + per-item TTL + 防击穿 `GetOrLoad` |
| `pkg/shared/compress` | 压缩/解压封装（按算法选择） |
| `pkg/shared/hyperloglog` | 基数估算（UV/DAU 类去重计数） |
| `pkg/shared/semaphore` | 带权信号量（并发/配额限流） |
| `pkg/shared/timewindow` | 滑动时间窗口计数（限流、统计） |
| `pkg/runtime/async` | 异步任务与背压 |
| `pkg/runtime/pool` | 对象池 |

> 判定口径：这些包**不构成"第二套实现"**——引擎内没有与它们同能力的另一份。
> 但反向约束成立：若某项能力已有唯一真相包（如退避策略 → `internal/shared/retry`、
> 日志 → `pkg/foundation/logger`），新代码必须复用真相包，**不得**改用上述备选库另起一套。
> 若确认长期无用，请在此表按行删除并同时删包，避免"文档保留、代码已删"的漂移。
