# clover-server-engine

Clover 的 **Go 游戏服务端引擎**：进程编排、网关与鉴权、集群服务发现、数据与持久化、事件总线、跨节点通信、MMO 世界、房间与对象系统 —— 开箱即用的服务端底座。

## 环境要求

| 项 | 要求 |
|---|---|
| Go | **1.25+** |
| 中间件 | etcd（服务发现）、NATS（事件总线）、MySQL（数据持久化）、Redis（可选：session token / 排行榜后端） |

本地一键起这些中间件（Windows）→ [clover-server-tools](https://github.com/qw576483/clover-server-tools) 的 `windows-env`。

## 目录结构（六类）

```text
clover-server-engine/
├── internal/       # 引擎实现（不对外）
│   ├── app/        # 进程编排、挂载、路由与生命周期
│   ├── foundation/ # 基础底座（config / ophttp）
│   ├── runtime/    # 运行时原语（globalstore）
│   ├── transport/  # 网关、鉴权、协议编解码、网络与跨节点通信
│   ├── domain/     # data、master、log、auth、mmo、object、room 等领域
│   └── shared/     # 协议、校验、重试与通用工具
└── pkg/            # 门面层（别名 / 转发 / 极薄适配）+ 自包含工具库
    ├── app/ domain/ foundation/ runtime/ transport/ shared/
```

**门面在 `pkg`，真身在 `internal`**：`pkg` 只做类型别名 / 变量转发 / 极薄参数适配，允许 import `internal`；实现体一律留在 `internal`。判据与现场自查见 [`结构规则.md`](结构规则.md) §五。

## 业务侧怎么用

```go
package main

import (
    "clover-server-engine/pkg/app"
    "clover-server-engine/pkg/foundation/logger"
    _ "your-game/server/logic"        // 业务逻辑包，注册期完成 OnMsg 接线
)

func main() {
    if err := app.Run("configs/all"); err != nil {
        logger.Fatal("启动失败", logger.Field("err", err))
    }
}
```

- 角色挂载：`app.Mount(app.RoleGame, func(g *app.Game) { ... })`，另有 `RoleMaster` / `RoleGateway` 等。
- 消息路由：`g.OnMsg(msgID, handler)`；跨节点调用 `g.CallMaster(...)`。
- 推送：`g.PushToPlayer(playerID, msgID, v)` / `g.PushToScene(...)`。
- 数据读取：`g.LoadStruct(...)`，handler 返回即自动提交。

## 消息号约定

- 引擎占 `[1, 10000]`（`proto.InternalMsgMax`）；**业务消息号必须 `>= 10001`**，由 `OnMsg` 在启动期统一校验（误用直接 panic）。
- 引擎常量用 `EMsg*` / `EPush*`（回包为 `E*Reply` 结构体）；普通回包 `msgID` 恒为 0，按 `requestID` 配对。
- 引擎内建协议号（如 master↔game 房间协议的 6001–6004）走内部注册路径 `InternalOnMsg`，**业务侧 `OnMsg` 的硬约束不变**。

## 开发

```bash
go build ./...
go vet ./...
go test ./...
```

## 文档

| 文件 / 路径 | 内容 |
|---|---|
| [`结构规则.md`](结构规则.md) | **结构铁律**：分层、目录归属、依赖方向、评审口令 ——**最该先读** |
| [`clover-server-engine-index.md`](clover-server-engine-index.md) | 索引：有什么、在哪、怎么读（六类结构、模块总览） |
| [`修复记录.md`](修复记录.md) | **S 编号**体系的缺陷修复记录（与客户端的 E 编号是两套独立体系） |
| `pkg/<模块>/README.md` | 具体模块的用法（`internal/**` 不放 README，实现注释即文档） |

## 相关仓库

| 仓库 | 说明 |
|---|---|
| [clover-client-unity-engine](https://github.com/qw576483/clover-client-unity-engine) | Unity 客户端引擎 UPM 包（协议与 API 语义两端对齐） |
| [clover-server-tools](https://github.com/qw576483/clover-server-tools) | 本地依赖环境、调试客户端、压测机器人、集群编排、运营后台 |
| [clover-tools](https://github.com/qw576483/clover-tools) | 打表工具、AI 交付 skill |
| [clover-doc](https://github.com/qw576483/clover-doc) | 框架文档（`server/` 即本引擎的完整文档） |
