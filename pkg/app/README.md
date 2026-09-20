# pkg/app — 应用启动层

clover 引擎的应用启动门面。业务项目只需 `app.Run(configPath)` 即可启动网关 + 逻辑服并阻塞到退出信号。

> **结构方向（见 `结构规则.md` §五）**：本包是**门面包** —— 只有类型别名（`type Game = internal.app.GameFacade` 等）、
> 常量转发（`const RoleGame = …`）与声明转发（`var Run = internal.app.RunFacade` 等），**不含实现体**。
> 真身（`GameFacade` / `MasterGameFacade` / `LogGameFacade` / `AuthGameFacade` 包装结构、
> `Mount` / `RunFacade` / `RunWithConfigFacade` / `bridgeHandler` / `unwrapEventCtx`、admin 与 timer 包装）
> 全部在 `internal/app/facade.go`（`channel.go` 的渠道校验器真身在 `internal/domain/auth`）。
> 这也是它必须留在 `pkg` 的原因：业务模块是独立 Go module，受 Go `internal` 规则限制进不去 `internal/`，
> 而扩展点（`Mount` / `RegisterChannelVerifier` / `RegisterLogBackend`）必须业务可见。

## 快速上手

```go
package main

import (
    "clover-server-engine/pkg/app"
    "clover-server-engine/pkg/transport/event"
)

func main() {
    if err := app.Run("config/", bootstrap); err != nil {
        panic(err)
    }
}

func bootstrap(g *app.Game) error {
    g.OnMsg(10001, func(c event.Ctx) error {
        var req CreatePlayerReq
        if err := c.BindMsg(&req); err != nil {
            return err
        }
        g.Reply(c, &CreatePlayerResp{OK: true})
        return nil
    })
    return nil
}
```

## API 速查表

### 进程角色常量

| 常量 | 说明 |
|------|------|
| `ServerTypeGame` | 游戏服（玩法 + 玩家状态） |
| `ServerTypeGateway` | 网关服 |
| `ServerTypeAll` | 网关 + 游戏服同进程启动 |
| `EngineVersion` | 引擎版本号（只读） |

### 启动函数

| 函数 | 说明 |
|------|------|
| `Run(configPath, ...bootstrap)` | 从配置路径加载并启动，阻塞到退出信号 |
| `RunWithConfig(cfg, ...bootstrap)` | 使用已构造好的 `*Config` 启动（跳过加载） |
| `RunGame(ctx, cfg)` | 仅启动游戏服，返回 `*Game`（可用于单元测试 / 嵌入式启动） |
| `LoadFromFile(path)` | 从单个 YAML/JSON/TOML 文件加载配置 |
| `LoadFromDir(dir)` | 从配置目录加载（按文件名排序合并） |
| `LoadConfig(configPath)` | 加载配置（自动识别目录 / 单文件） |
| `DefaultConfig()` | 返回开发基线配置 |

### Game 方法

| 方法 | 说明 |
|------|------|
| `OnMsg(msgID, handler, ...priority)` | 注册消息 handler |
| `OnEvent(typ, handler)` | 注册领域事件 handler |
| `OnBeforeDispatch(fn)` | 注册消息级横切钩子（引擎内置横切逻辑之后、业务 handler 之前；返回 `error` 即拒绝派发，带 `*proto.BizError` 时回包含错误码） |
| `Reply(ctx, v)` | 以结构体 JSON 编码回包 |
| `ReplyRaw(ctx, body)` | 以原始字节回包 |
| `Alert(ctx, alert)` | 自适应弹窗（PlayerID 优先，回退 Account） |
| `SendEventToPlayer(ctx, playerID, typ, payload)` | 向指定玩家投递领域事件 |
| `SendQueueEventToPlayer(ctx, playerID, typ, payload)` | 向指定玩家投递串行 FIFO 事件 |
| `LoadStruct(ctx, schema, id, v)` | 可修改加载 / 自动落库 |
| `LoadRecord(ctx, schema, id)` | 记录加载 |
| `PlayerStore()` | 角色存储 |
| `AccountStore()` | 账号存储 |
| `ChannelStore()` | 账号渠道绑定存储 |
| `OrderStore()` | 订单存储 |
| `MasterClient()` | 跨服 master TCP 客户端 |
| `Call(role, msgID, req, resp)` | **统一转发**到其他角色（`RoleMaster`/`RoleLog`/`RoleAuth`）；取代 `CallMaster`/`CallLog`/`CallAuth` 三个专用方法 |
| `AddLog(ownerType, ownerID, typ, info, ...opts)` | 写入业务日志，攒积后批量上报 |
| `Timer` | 共享定时器调度器（`*TimeEvent`） |

### MasterGame 方法

| 方法 | 说明 |
|------|------|
| `OnMsg(msgID, handler)` | 注册 Master TCP 消息 handler |
| `OnEvent(typ, handler)` | 注册 Master 侧领域事件 handler |
| `Reply(ctx, v)` | 以结构体 JSON 编码回包 |
| `ReplyRaw(ctx, body)` | 以原始字节回包 |
| `LoadStruct(ctx, schema, id, v)` | 可修改加载 / 自动落库 |
| `LoadRecord(ctx, schema, id)` | 记录加载 |
| `MasterRegistry()` | 房间 owner 注册表 |

### 注册函数

| 函数 | 说明 |
|------|------|
| `Mount(role, fn)` | **业务挂载唯一入口**（在 `init` 中调用）。`role` 取 `RoleGame`/`RoleMaster`/`RoleLog`/`RoleAuth`，`fn` 必须是 `func(*Game)` / `func(*MasterGame)` / `func(*LogGame)` / `func(*AuthGame)` 且与 role 匹配；不匹配在启动期 panic |
| `RoleGame` / `RoleMaster` / `RoleLog` / `RoleAuth` | 四个进程角色（客户端只直连 `game` 与账号服 HTTP；master/log 的业务消息由 game 用 `g.Call(role, ...)` 转发） |
| `RegisterChannelVerifier(v)` | 注册渠道票据校验器（微信 / QQ / Steam 登录；须在 `Run` 之前调用） |
| `RegisterLogBackend(name, factory)` | 注册自定义日志落盘后端（`log_backend` 按名切换；内置 "mysql" 不走注册表，它读 `data.mysql`） |
| `PublishTableLoaded(g, names)` | 广播 table.Loaded 事件 |

### 辅助函数

| 函数 | 说明 |
|------|------|
| `NewMasterClient(addr)` | 创建到 master 的客户端连接（不带共享密钥，仅适用于 master 绑回环的部署） |
| `NewMasterClientWithToken(addr, token)` | 同上，携带 `master_token`（master 绑非回环地址时必须用它：服务端要求每条连接首帧完成 `MsgAuth` 握手） |
| `FromAdminServer(s)` | internal AdminServer → 门面 AdminServer 接口 |
| `InternalAdminServer(s)` | 门面 AdminServer → internal `*AdminServer` |
| `FromTimeEvent(t)` | internal TimeEvent → 门面 TimeEvent 接口 |
| `InternalTimeEvent(t)` | 门面 TimeEvent → internal `*TimeEvent` |
