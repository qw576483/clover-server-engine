# pkg/domain/room/frame — 锁步帧同步

## 模块职责

引擎的锁步帧同步内核。业务通过 `Service` 接口管理多个房间，通过 `Room` 接口操作单个房间。所有房间共享一个固定帧率的推进循环。

本包是 `internal/domain/room/frame` 的接口化封装：所有具体实现被遮住，对外只暴露业务可达的抽象。

## 快速上手

```go
// 本包不提供构造入口：Service 由「房间外壳」装配（见 pkg/domain/room 的 room.NewModule +
// Config.FrameCfg / FrameSvcOpts），业务从 room.Module.Frame() 取得（未挂帧同步内核时为 nil）。
svc := mod.Frame() // frame.Service

// ServiceOption（如 WithBroadcaster）在外壳装配内核时注入，回调签名为 Broadcaster：
//   func(playerID string, msgID uint32, v any) error
room, err := svc.NewRoom("room-001", frame.WithFPS(15))
_ = svc.Join("room-001", "player-001")
_ = svc.Input("room-001", "player-001", frame.Input{Frame: 1, Payload: []byte(`{"x":1}`)})
_ = svc.Leave("room-001", "player-001")
_ = svc.Destroy("room-001")
```

## API 速查表

### Service 接口

| 方法 | 说明 |
|------|------|
| `NewRoom(roomID, opts...)` | 创建房间，已存在返回 `ErrRoomExists` |
| `EnsureRoom(roomID, opts...)` | 确保存在；不存在则创建 |
| `Get(roomID)` / `MustGet(roomID)` | 获取房间句柄 |
| `Join(roomID, playerID)` | 玩家加入房间 |
| `Leave(roomID, playerID)` | 玩家离开房间 |
| `Input(roomID, playerID, input)` | 投递一帧输入 |
| `Destroy(roomID)` | 销毁房间 |
| `Snapshot(roomID)` | 返回最近一份快照 |
| `ExportState(roomID)` / `ImportState(state)` | 导出/导入完整运行态 |
| `Recovery(roomID, playerID)` / `Reconnect(roomID, playerID)` | 重连恢复 |
| `Disconnect(roomID, playerID)` | 标记玩家断线（不离房） |
| `Info(roomID)` | 返回房间摘要 |
| `ListRooms()` | 返回全部房间 ID |
| `Close()` | 停止并清空所有房间 |
| `Metrics()` | 返回运行时指标快照 |
| `SetInputApplier(fn)` | 设置默认输入应用器 |

### Room 接口

| 方法 | 说明 |
|------|------|
| `ID()` | 房间 ID |
| `Config()` | 配置副本 |
| `Frame()` | 当前已完成帧号 |
| `Join(playerID)` / `Leave(playerID)` | 加入/移除玩家 |
| `Input(playerID, input)` | 投递一帧输入 |
| `MarkDisconnected(playerID)` | 标记断线但暂不移出 |
| `Reconnect(playerID)` | 重新标记在线并返回恢复包 |
| `Recovery(playerID)` | 返回完整恢复包 |
| `Snapshot()` | 最近快照 |
| `ExportState()` / `ImportState(state)` | 导出/导入运行态 |
| `Info()` | 房间摘要 |

### Config 结构体

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `TargetFPS` | `int` | — | 目标帧率（Option 工厂仍叫 `WithFPS`） |
| `SnapshotEvery` | `int` | — | 每 N 帧保存一份快照 |
| `SnapshotLimit` | `int` | — | 内存中保留的快照数量上限 |
| `HistoryLimit` | `int` | — | 历史增量保留帧数 |
| `RecoveryMaxFrames` | `int` | — | 重连追帧最大帧数 |
| `InputTimeoutTicks` | `int` | — | 输入超时 tick 数 |
| `IdleTimeoutTicks` | `int` | — | 空闲超时 tick 数（无任何输入超过即关闭房间） |
| `DisconnectRetentionFrames` | `int64` | — | 断线玩家保留帧数（超时后从房间移除） |
| `MaxInputLead` | `int64` | — | 客户端可预发送输入的最大领先帧数 |
| `PushMessageID` | `uint32` | — | 每帧广播消息 ID |
| `CloseMessageID` | `uint32` | — | 房间关闭消息 ID |
| `AutoStart` | `bool` | — | 首名玩家加入后自动启动主循环 |
| `AutoDestroyEmpty` | `bool` | — | 空房间自动销毁 |
| `MaxPlayers` | `int` | `0` | 房间人数上限；`<=0` 表示不限（满员拦新玩家，房内玩家重连放行） |

### Option 工厂（单房间配置）

`WithFPS` · `WithSnapshotEvery` · `WithSnapshotLimit` · `WithHistoryLimit` · `WithRecoveryMaxFrames` · `WithInputTimeoutTicks` · `WithIdleTimeoutTicks` · `WithDisconnectRetentionFrames` · `WithMaxInputLead` · `WithPushMessageID` · `WithCloseMessageID` · `WithAutoStart` · `WithAutoDestroyEmpty` · `WithMaxPlayers`

### ServiceOption 工厂（全局服务配置）

| 工厂 | 说明 |
|------|------|
| `WithBroadcaster(fn)` | 注册广播回调 |
| `WithBroadcasterWithMode(fn)` | 注册带传输模式的广播回调 |
| `WithTimeNow(fn)` | 自定义时间源 |
| `WithOnRoomCreated(fn)` | 房间创建钩子 |
| `WithOnRoomDestroy(fn)` | 房间销毁钩子 |
| `WithTakeoverHook(fn)` | 接管钩子 |
| `WithInputApplier(fn)` | 注册输入应用器 |

### 错误

| 错误 | 说明 |
|------|------|
| `ErrRoomExists` | 房间已存在 |
| `ErrRoomNotFound` | 房间不存在 |
| `ErrPlayerNotIn` | 玩家不在房间中 |
| `ErrPlayerExists` | 玩家已加入 |
| `ErrRoomDestroyed` | 房间已销毁 |
| `ErrFrameTooOld` | 帧号过旧（超出历史范围） |
| `ErrFrameTooFar` | 帧号过远（超出未来缓冲） |
| `ErrInputDuplicated` | 输入重复投递 |

## 核心数据类型

| 类型 | 说明 |
|------|------|
| `Input` | 单帧输入（`Frame` + `Payload`） |
| `PlayerState` | 玩家状态（位置 / HP / 最后帧号） |
| `Snapshot` | 某一帧的完整状态快照 |
| `FrameDelta` | 一帧增量（输入 + 状态 + hash） |
| `RecoveryPack` | 快照 + 历史增量的重连恢复包 |
| `RoomState` | 房间可导出/导入的完整运行态 |
| `RoomInfo` | 房间对外查看的摘要信息 |
| `Metrics` | 运行时指标（房间数 / 玩家数 / 总帧数 / 丢帧数） |
