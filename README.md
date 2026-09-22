# Clover Server Engine

Clover 的 **Go 游戏服务端引擎**：进程编排、网关与鉴权、集群服务发现、数据持久化、事件总线、跨节点通信、MMO 世界、房间与对象系统。

## 从没用过 Clover？

照着 **[新手指南：用 AI 从零做一个 Clover 游戏](https://github.com/qw576483/clover-doc/blob/main/ai/ai-quick-start.md)** 走一遍即可 —— 从装 Unity 6 到让 AI 开出第一个工程，全程不用自己写代码。

用这套流程做出来的成品见 **[游戏 Demo 清单](https://github.com/qw576483/clover-doc/blob/main/ai/game-demo.md)**。

## 交流群

QQ 群：**clover-engine交流1群** `1101150552`

## 安装

```bash
go get github.com/qw576483/clover-server-engine
```

要求 **Go 1.25+**。

## 快速开始

### 1. 入口（业务工程 `main.go`）

```go
package main

import (
    "github.com/qw576483/clover-server-engine/pkg/app"
    "github.com/qw576483/clover-server-engine/pkg/foundation/logger"

    _ "your-server/logic"          // 业务逻辑包：在 init 中完成挂载
)

func main() {
    if err := app.Run("configs/all"); err != nil {
        logger.Fatal("启动失败", logger.Field("err", err))
    }
}
```

### 2. 挂载业务（`logic/logic.go`）

```go
package logic

import (
    "github.com/qw576483/clover-server-engine/pkg/app"
    "github.com/qw576483/clover-server-engine/pkg/transport/event"
)

func init() {
    app.Mount(app.RoleGame, func(g *app.Game) {
        g.OnMsg(1000101, onGetPlayerList)      // 业务消息号必须 >= 10001
    })
}

// handler 签名固定为 func(event.Ctx) error；返回即自动提交（读 → 改 → 返回）
func onGetPlayerList(c event.Ctx) error { return nil }
```

`app.Mount` 是**四个角色共用的唯一入口**（通常在业务包 `init` 里调用）。fn 签名必须与 role 匹配，不匹配会在进程启动时立刻 panic，不会出现"挂错角色、消息永远到不了"的静默失败：

| role | fn 签名 |
|---|---|
| `app.RoleGame` | `func(*app.Game)` |
| `app.RoleMaster` | `func(*app.MasterGame)` |
| `app.RoleLog` | `func(*app.LogGame)` |
| `app.RoleAuth` | `func(*app.AuthGame)` |

### 3. 启动

```bash
go build -o server.exe .
./server.exe -config configs/all
```

`configs/all` 表示**单进程承载全部角色**（网关 + 逻辑 + 协调 + 日志 + 账号），本地开发不需要 etcd / NATS 等外部依赖。配置字段见文档 [配置](https://github.com/qw576483/clover-doc/blob/main/server/development/configuration.md)。

## 角色与拓扑

| 角色 | 职责 |
|---|---|
| Gateway | 客户端接入（WebSocket / TCP / QUIC），会话与鉴权 |
| Game | 唯一接收客户端消息的角色；业务逻辑与实体 |
| Master | 协调服：玩家定位、房间寻主、排行榜、session token（可多分片） |
| Log | 日志落盘 |
| Auth | 账号服（HTTP） |

客户端只直连网关与账号服；master / log 的业务消息一律由 game 用 `Game.Call(role, msgID, req, resp)` 转发。集群通过 etcd 做服务发现、NATS 做事件总线，两者都不配时自动降级为单机模式。

## 消息号约定

- 引擎占 `[1, 10000]`（`proto.InternalMsgMax`）；**业务消息号必须 `>= 10001`**，由 `OnMsg` 在启动期校验（误用直接 panic）。
- 引擎常量用 `EMsg*` / `EPush*`；普通回包 `msgID` 恒为 0，按 `requestID` 配对。
- 引擎内建协议号（如 master↔game 房间协议的 6001–6004）走内部注册路径，业务侧 `OnMsg` 的硬约束不变。

## 工程结构

**门面在 `pkg`，真身在 `internal`**：`pkg` 只做类型别名 / 变量转发 / 极薄参数适配，实现体一律留在 `internal`。

```text
internal/    app / foundation / runtime / transport / domain / shared   （私有实现）
pkg/         同名六类                                                   （业务 import 入口）
```

判据与现场自查见 [`结构规则.md`](结构规则.md) §五。

## 相关仓库

| 仓库 | 说明 |
|---|---|
| [clover-client-unity-engine](https://github.com/qw576483/clover-client-unity-engine) | Unity 客户端引擎 UPM 包（协议与 API 语义两端对齐） |
| [clover-server-tools](https://github.com/qw576483/clover-server-tools) | 本地一键依赖环境、调试客户端、压测机器人、集群编排、运营后台 |
| [clover-tools](https://github.com/qw576483/clover-tools) | 打表工具（Excel → Go / C# 强类型代码） |
| [clover-ai-skill](https://github.com/qw576483/clover-ai-skill) | AI 交付 skill（规则 / 范式 / 脚手架） |
| [clover-doc](https://github.com/qw576483/clover-doc) | 完整文档 |

## 开发

```bash
go build ./... && go vet ./... && go test ./...
```

包约定：**对外包应配套单测与 `README.md`**（现状未全覆盖：部分自包含小包仍缺 README，属待补项）。

## 许可证

[MIT](LICENSE)
