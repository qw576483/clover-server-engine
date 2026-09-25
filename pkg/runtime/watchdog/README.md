# watchdog

> 进程级「周期巡检 + 告警」。把「引擎/业务自己发现异常并叫人」收敛成一个可复用的运行时原语。

## 它是什么 / 不是什么

| 做 | 不做 |
|---|---|
| 按规则周期调用只读检查函数 | **不采集数据**（进程 CPU 由 `pkg/foundation/metrics` 接） |
| 命中后按连续门限 + 冷却窗口抑制，按级别记日志与指标 | **不定义业务阈值**（唯一内置规则是「进程 CPU 饱和」，见 `internal/app/watchdog.go`） |
| 把告警交给可替换的出口 `Sink` | **不落库、不持久化历史**（历史由 TSDB 负责，见下） |
| 提供 `Snapshot()` / admin `/watchdog` 给运维看瞬时状态 | 不做可视化面板 |

## 谁在跑

引擎**按进程统一拉起**：`internal/app/app.go` 的 `runApp` 里 `installWatchdog`，
不论 `server_type` 是 game / gateway / master / log / auth 都生效（与 admin 控制面同思路）：

- 出口默认**不接任何外部通道**：告警一律落引擎日志（`logAlert`，零依赖零网络）；
- 运维端点：`GET /watchdog`（admin 控制面，默认 `127.0.0.1:8041`）返回每条规则的
  `running / firing / consecutive / last_level / last_message / last_alert_at` 与 `dropped` 计数；
- 指标埋点见下表；`/metrics` 由 Prometheus 抓取。

## 业务怎么加规则

```go
// 在 RegisterMount / bootstrap 里注册（此时引擎已 Install 完默认实例）。
watchdog.Default().RegisterFunc("my_backlog", func() watchdog.Result {
    if n := myQueue.Len(); n > 1000 {
        return watchdog.Critical("待处理积压 %d 条", n)
    }
    return watchdog.OK
})
```

需要去抖动 / 单独周期时用完整 `Rule`：

```go
watchdog.Default().Register(watchdog.Rule{
    Name:           "my_latency_high",
    Interval:       30 * time.Second, // 0 = 全局默认 10s
    Cooldown:       5 * time.Minute,  // 0 = 全局默认 5m
    MinConsecutive: 3,                // 连续 3 次命中才告警（滤瞬时毛刺）
    Check:          myCheck,
})
```

语义要点：

- **连续门限**与**冷却**是两道独立的闸：前者滤毛刺，后者防轰炸；命中期间状态照常更新（`Snapshot` 里 `firing=true`）。
- **恢复也会通知**：从告警态回到正常时投一条 `level=recovered` 的告警（正常轮次保持安静）。
- **巡检函数 panic / 返回非法级别**：按 `critical` 报出来并计 `panic` —— 静默失灵的规则比没有规则更危险。

## 怎么接真实告警通道（webhook / IM / 邮件）

```go
func init() {
    watchdog.Install(watchdog.New(watchdog.Options{
        Sink: watchdog.SinkFunc(func(a watchdog.Alert) { myWebhook(a) }),
    }))
}
```

约束：

1. **必须先 `Install` 再注册规则**。引擎在 `runApp` 里先 Install 再进入各角色装配，
   所以业务在 mount / bootstrap 里注册的规则一定落在同一个实例上；
   反过来（先注册后 Install）会丢规则，`Install` 会打印 Error 日志指出这一点。
2. **`Notify` 必须快速返回**：投递走「单 worker + 有界队列（默认 64）+ 满即丢」，
   外部通道要自带超时 / 重试；丢弃计数在 `clover_watchdog_alerts_dropped_total`
   与 `/watchdog` 的 `dropped` 字段里 —— **非零就是「有告警没送出去」**，要治的是出口。
3. `Notify` 里 panic 会被捕获（只记 Error 日志），不会打死看门狗。

## 指标埋点

命名遵循 `clover_<模块>_<指标>_<单位>`，模块名固定 `watchdog`。

| 指标 | 类型 | 含义 |
|---|---|---|
| `clover_watchdog_rules` | Gauge | 已注册规则数（为 0 = 没人在看，比没指标更危险） |
| `clover_watchdog_checks_total{rule,status}` | Counter | 巡检执行数，`status=ok/firing/skipped/panic` |
| `clover_watchdog_check_duration_seconds{rule}` | Histogram | 单次巡检耗时（判断规则本身是否成了负担） |
| `clover_watchdog_alerts_total{rule,level}` | Counter | 触发告警数（`level` 含 `recovered`） |
| `clover_watchdog_alerts_dropped_total` | Counter | 出口队列满被丢弃数 |

`skipped` 持续增长 = 某条规则卡住了（上一轮没跑完）；`alerts_total` 与 `alerts_dropped_total`
对照即可区分「要不要告警」与「有没有送出去」。

label 基数：`rule` 是注册时的固定规则名（规则数就是基数），**禁止**把 player_id / conn_id / 时间戳
拼进规则名。

## 硬约束（红线）

1. **Check 必须只读、快速、不阻塞**。它在独立 goroutine 里跑，卡住只影响自己那条规则
   （下轮 due 发现仍在跑 → 跳过并计 `skipped`，不会堆成 goroutine 洪水），但长久卡住等于规则失效。
2. **禁止持锁遍历 / 全表分配快照**：drain 持锁调 `ListConnIDs` 全表分配，
   万级连接时会阻塞派发。Check 里只取只读快照，计算放在锁外。
3. **Sink 不得阻塞**（见上）。
4. **规则名是有限集合**（见上）。

## 为什么引擎不落库

指标经 `/metrics` 暴露给 Prometheus，**历史留存由 TSDB 负责**；告警出口由 Sink 接。
把指标写进 MySQL / 文件会让进程变成监控系统的一部分，且必然与游戏数据抢 IO。
需要离线留存（没有 Prometheus 的部署）时，由部署侧拉取 / 转发 —— 都不该做进本包。
