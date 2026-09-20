# pkg/domain/room — 房间子系统

## 模块职责

为业务提供「房间」概念的统一门面。房间子系统以 `Module` 形式挂载到 `app.Game`。

**核心设计：外壳与内核分离。**

- **外壳（`Module`）**：房间归谁管（owner 路由）、节点挂了怎么搬（跨服接管）、房间从生到死（生命周期）。
  这部分与「用什么同步」无关，两种内核共用。
- **内核（`Kernel`）**：房间内部到底怎么同步。可插拔：
  - **引擎内置帧同步内核**：`Config` 传 `FrameCfg` / `FrameSvc` / `FrameSvcOpts`
  - **业务自写内核**：实现 `Kernel` 接口后经 `Config.Kernel` 传入（例如状态同步房间）

本包是 `internal/domain/room` 的接口化封装：所有具体实现被遮住，对外只暴露业务可达的抽象。

## 快速上手

### 挂引擎内置帧同步内核

```go
func bootstrap(g *app.Game) error {
    mod := room.NewModule(room.Config{
        MasterCaller: g,
        // g.PushToPlayer 带变参（opts ...proto.DeliveryMode），不能直接赋给 Pusher，需包一层
        Pusher: func(playerID string, msgID uint32, v any) error {
            return g.PushToPlayer(playerID, msgID, v)
        },
        NodeAddr: g.Addr(),
        FrameCfg: frame.DefaultConfig(), // 房间默认配置（TargetFPS / 快照频率等）
    })
    mod.EnsureRoom("room-001")
    mod.JoinRoom(connID, "room-001", playerID)
    return nil
}
```

### 挂业务自写内核（例如状态同步）

```go
// myKernel 实现 room.Kernel 的 8 个方法：EnsureRoom / Join / Leave / Destroy /
// ExportState / ImportState / Players / Close。
mod := room.NewModule(room.Config{
    MasterCaller: g,
    Pusher:       g.PushToPlayer,
    NodeAddr:     g.Addr(),
    Kernel:       myKernel, // ← 挂自己的内核，FrameCfg 被忽略
})
mod.EnsureRoom("room-001") // 外壳 API 与帧同步房间完全一致
```

## API 速查表

### Kernel（内核接口）

| 方法 | 说明 |
|------|------|
| `EnsureRoom(roomID)` | 确保房间存在（幂等） |
| `Join(roomID, playerID)` | 玩家进房 |
| `Leave(roomID, playerID)` | 玩家离房 |
| `Destroy(roomID)` | 销毁房间 |
| `ExportState(roomID) (ExportPack, error)` | 导出可迁移态（接管用；外壳不解析内容） |
| `ImportState(roomID, state)` | 导入可迁移态（接管时由外壳调用） |
| `Players(roomID) []string` | 房间当前玩家（外壳据此下发接管恢复包） |
| `Close()` | 关闭内核并释放资源 |

`ExportPack` 字段：`State`（给新 owner 恢复运行态）、`Recovery`（可选，推给客户端的恢复包）、
`Frame` / `Hash`（可选，恢复基准帧与哈希）。

### Module（房间外壳）

| API | 说明 |
|-----|------|
| `Config` | 构造参数：`MasterCaller`、`Pusher`、`NodeAddr`、`Kernel`、`FrameCfg`、`FrameSvc`、`FrameSvcOpts` |
| `NewModule(cfg)` | 构造房间外壳，按 `Config` 装配内核 |
| `EnsureRoom(roomID)` | 确保房间存在（按挂载的内核创建）+ owner 注册 + 接管激活 |
| `JoinRoom(connID, roomID, playerID)` | 跨节点所有权校验 + 本进程进房，返回 `(owner, switched, err)` |
| `LeaveRoom(roomID, playerID)` | 玩家离房 |
| `DestroyRoom(roomID)` | 销毁房间并注销 owner |
| `Frame()` | `frame.Service` 接口；**仅在挂载帧同步内核时非 nil**（内核专属参数走它，如每房间 `WithFPS`） |
| `Close()` | 关闭房间子系统 |

> `FrameCfg` 的语义是**逐项覆盖** `frame.DefaultConfig()`：零值字段不覆盖。
> 只想改帧率时只写 `TargetFPS` 即可，`AutoStart` 等仍是默认值。
> 要**显式关闭**某个开关，请在该房间的创建 Option 里用 `frame.WithAutoStart(false)`。

> `EnsureRoom` / `JoinRoom` 在未挂载内核时**返回错误并打日志**（不再静默返回 nil）。

### MasterCaller 接口

| 方法 | 说明 |
|------|------|
| `CallMaster(msgID, req, resp)` | 跨节点调用 master |
| `SwitchUpstream(connID, targetAddr)` | 切换连接上游节点 |

> `app.Game` 自动满足此接口。

### MasterRegistry 接口

| 方法 | 说明 |
|------|------|
| `Register(roomID, nodeAddr)` | 注册房间 owner 节点地址 |
| `Unregister(roomID, nodeAddr)` | 注销房间 owner |
| `Find(roomID)` | 查询房间 owner 节点地址 |
| `Reassign(roomID, old, new)` | 重新分配房间 owner（CAS 语义） |
| `SaveTakeoverState(roomID, state)` | 保存接管状态（`state` 为 `any`，与内核无关） |
| `PopTakeoverState(roomID)` | 获取并清除接管状态 |
| `ClaimWithState(roomID, old, new)` | 原子注册 owner 并获取接管状态 |

### MasterHandlers

| API | 说明 |
|-----|------|
| `NewMasterHandlers(mg)` | 创建 master 侧房间协议 handler（`mg` 满足 `MasterHandlerGame`）；构造时**已注册一次** |
| `Register()` | 向 master 注册内核内置的 room owner 消息处理器（重复调用会重复登记，一般无需显式再调） |

> 这 4 条协议号（`EMasterRoomRegister` / `Unregister` / `Find` / `TakeoverClaim` = 6001..6004）
> 是**引擎内建**的保留号（≤ `InternalMsgMax`），注册走的是**内部号路径**（宿主
> `MasterGame.InternalOnMsg`），不是业务的 `OnMsg` —— 后者有「业务消息号必须 > 10000」的守卫，
> 那是给业务用户的保护，不因内建号而放宽。
>
> 要让房间的 owner 注册 / 寻主 / 接管生效，业务侧在 master 角色挂一次
> `room.NewMasterHandlers(mg)`（`mg` = `app.Mount(app.RoleMaster, func(m *app.MasterGame){...})` 的宿主），
> 并在 `room.Config` 里传 `MasterCaller: g`（不传 = 整条跨节点接管链路被跳过）。

### 辅助函数

| API | 说明 |
|-----|------|
| `NewMasterRegistry()` | 创建主人注册表，返回门面接口 |
| `WrapMasterRegistry(inner any)` | 把内部实现包装成门面接口（内部实现经 `init()` 注册的 wrapper 完成，签名不出现 internal 类型） |

## 架构说明

```
app.Game
  └─ room.Module（外壳：owner 路由 + 跨服接管 + 生命周期）
       ├─ Kernel           ← 可插拔
       │    ├─ 帧同步内核（引擎内置，包装 frame.Service）
       │    └─ 业务自写内核（实现 Kernel 接口，如状态同步）
       └─ MasterRegistry   (owner 路由 & 接管)
```

`Module` 是业务层与房间子系统交互的唯一入口。所有底层实现通过 `pkg` 层接口化封装，业务代码无需直接依赖 `internal/domain/room`。

> 状态字段（`ExportPack.State` / `Recovery`）以 `json.RawMessage` 承载，外壳不解析其内容——
> 这是「换内核不用改外壳」的前提。
