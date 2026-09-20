# pkg/domain/master — 主节点协调

## 模块职责

引擎的顶层协调服公开 API。本包暴露排行榜模块（MasterRank）、玩家定位查询（PlayerLookup），以及 master TCP 客户端（MasterClient）。

```go
import "github.com/qw576483/clover-server-engine/pkg/domain/master"
```

## 快速上手

```go
// 创建排行榜（业务层显式创建并持有句柄）
rank := master.NewMasterRank(g)

// 写入排行榜：board 为榜名，score 为排行分数，extra 为可选的附加 JSON 数据
if err := rank.Add(ctx, "leaderboard", "player-1001", 12500, nil); err != nil {
    return err
}

// 读取 Top N
top, err := rank.Top(ctx, "leaderboard", 100)

// 获取玩家排名（1-based，found 表示该成员是否在榜）
n, found, err := rank.GetRank(ctx, "leaderboard", "player-1001")

// 段位门槛
err = rank.SetRankThresholds(ctx, "leaderboard", master.Thresholds{
    {MinRank: 1, MaxRank: 10, MinScore: 10000},
    {MinRank: 11, MaxRank: 100, MinScore: 5000},
})
```

```go
// 玩家定位查询（跨服好友在线状态 / 邀请 / 观战寻址）
lk := master.NewPlayerLookup(g)

// 查询某玩家是否在线、在哪个节点
nodeID, online, err := lk.Locate(ctx, "player-1001")
if err != nil {
    return err // 查询通道不可用（master 未就绪等），不能当作「离线」
}
if !online {
    // 玩家不在线（nodeID 为空串）
}
```

## API 速查表

### MasterRank 接口

| 方法 | 说明 |
|------|------|
| `Add(ctx, board, member, score, extra)` | 添加或更新排行项 |
| `Top(ctx, board, n)` | 获取前 N 条 |
| `GetMember(ctx, board, member)` | 获取成员详情（含排名与分数） |
| `GetRank(ctx, board, member)` | 获取成员排名（1-based） |
| `GetByRankRange(ctx, board, start, stop)` | 按排名区间查询 |
| `Len(ctx, board)` | 排行榜总人数 |
| `Remove(ctx, board, member)` | 移除成员 |
| `Incr(ctx, board, member, delta)` | 累加分数并返回新值 |
| `Clear(ctx, board, deleteBackup)` | 清空排行榜 |
| `BackupAll(ctx)` / `Backup(ctx, board)` | 备份到 Redis |
| `RestoreAll(ctx)` / `Restore(ctx, board)` | 从 Redis 恢复 |
| `SetRankThresholds(ctx, board, ts)` | 设置段位门槛 |

### RankMember 结构体

| 字段 | 类型 | 说明 |
|------|------|------|
| `Member` | `string` | 成员 ID |
| `Score` | `float64` | 分数 |
| `Rank` | `int` | 排名（1-based） |
| `Extra` | `json.RawMessage` | 绑定的额外数据 |

### Threshold / Thresholds

| 类型 | 说明 |
|------|------|
| `Threshold` | 单条段位门槛：`MinRank` / `MaxRank` / `MinScore` |
| `Thresholds` | 门槛列表，支持 `Validate()` 校验与 `AdjustMembers()` 排名调整 |

### MasterClient 接口

| 方法 | 说明 |
|------|------|
| `Addr()` | 返回当前连接的 master 地址 |
| `Close()` | 关闭连接（幂等） |
| `Call(ctx, msgID, req, resp)` | 统一 RPC 调用 |

### PlayerLookup 接口

| 方法 | 说明 |
|------|------|
| `Locate(ctx, uid)` | 查询玩家所在节点，返回 `(nodeID, online, err)` |

> `online=false` 且 `err=nil` = 玩家不在线；`err!=nil` = 查询通道不可用
> （哨兵 `master.ErrPlayerLookupUnavailable`），**不要**把 error 当作「离线」。
> 定位表由引擎在玩家上/下线时自动维护，业务只查询、不写入。

### 构造函数

| 函数 | 说明 |
|------|------|
| `NewMasterRank(g)` | 创建排行榜模块句柄 |
| `RegisterMasterRankFactory(fn)` | 注册底层实现（引擎装配用） |
| `NewPlayerLookup(g)` | 创建玩家定位查询句柄 |
| `RegisterPlayerLookupFactory(fn)` | 注册底层实现（引擎装配用） |
| `app.NewMasterClient(addr)` | 创建 master TCP 客户端（不带共享密钥，仅适用于 master 绑回环的部署） |
| `app.NewMasterClientWithToken(addr, token)` | 创建 master TCP 客户端并携带 `master_token`（master 绑非回环时必须用） |
