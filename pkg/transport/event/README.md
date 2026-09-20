# pkg/transport/event — 事件与消息调度

引擎的事件驱动内核。业务通过 `g.OnMsg` / `g.OnEvent` 注册 handler，框架负责解信封、派发、回包。
`Ctx` 是 handler 的请求上下文，`Envelope` 是统一的事件信封格式，`Bus` 是进程内发布订阅原语。

## 快速上手

```go
// 注册客户端消息 handler（消息号 >= 10001）
g.OnMsg(10001, func(c event.Ctx) error {
    var req MyRequest
    if err := c.BindMsg(&req); err != nil {
        return err
    }
    g.Reply(c, &MyResponse{OK: true})
    return nil
})

// 注册领域事件 handler
g.OnEvent("player.levelup", func(c event.Ctx) error {
    var evt LevelUpEvent
    if err := c.BindEvent(&evt); err != nil {
        return err
    }
    return nil
})
```

## API 速查

**Handler** — `func(c Ctx) error`。返回非 nil 自动回错误码；返回 nil 且未 `MarkReplied` 则不回包。

**Ctx 接口** — `Context` / `TraceID` / `Account` / `PlayerID` / `MsgID` / `Body` / `BindMsg` / `Payload` / `BindEvent` / `MarkReplied` / `SetNoAutoReply` / `SetNoPush` / `ConnID` / `RequestID` / `Line` / `SetConnValue` / `ConnValue`

**Envelope 信封** — `ID` / `Type` / `MsgID` / `Source` / `UID` / `ConnID` / `TraceID` / `Timestamp` / `Payload` / `Ctx`

**事件类型常量** — `EventClientRequest` (`client.request`) · `EventServerNotify` (`server.notify`) · `EventServerInternal` (`server.internal`)

**投递目标常量** — `TargetPlayer` · `TargetGroup` · `TargetGate`

**事件构造器**
- `NewClientRequestEvent(uid, connID, msgID, body, opts...)` — 客户端上行
- `NewServerNotifyEvent(msgID, target, targetID, body, opts...)` — 服务器下行
- `NewInternalServerEvent(msgID, playerUID, object, body, opts...)` — 跨服内部
- `NewEvent(typ, payload, opts...)` — 自定义领域事件

**Bus 接口** — `Publish(e)` / `Subscribe(typ, h)` / `SubscribePattern(pattern, h)` / `Close()`

```go
bus := event.NewBus()                    // 默认同步派发
bus := event.NewBus(event.WithAsync())   // 异步派发
```

派发顺序：精确匹配 → 模式匹配。handler error 仅记日志，不影响其他订阅者。

**模式匹配 `MatchPattern`** — `*` 单级通配，`**` 多级通配（零层或多层）

```
MatchPattern("player.*", "player.login")     → true
MatchPattern("player.*", "player")            → false
MatchPattern("player.**", "player.a.b.c")    → true
```
