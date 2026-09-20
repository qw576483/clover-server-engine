# Clover Engine 索引

> 一个开箱即用的 Go 游戏服务器引擎。
> 结构铁律见 [`结构规则.md`](结构规则.md)。

---

## 0. 怎么读这套文档

| 你想了解 | 去看 |
|---|---|
| 分层、目录归属、依赖方向、评审口令（**最该先读**） | [`结构规则.md`](结构规则.md) |
| `pkg` ↔ `internal` 的**门面方向与现场自查**（不查名单，逐包判） | [`结构规则.md`](结构规则.md) §五 |
| 某个具体模块 | 各 `pkg/<模块>/README.md`（`internal/**` 不放 README，实现注释即文档） |
| 消息号、线格式、协议边界 | `pkg/shared/proto/`（真身）与 `internal/shared/proto/`（引擎侧复用/内部信封） |
| 业务侧如何用引擎（pkg 入口） | `pkg/app/README.md` + 各 `pkg/domain/*/README.md` |
| 当前已知问题与待办 | 工作区根 `服务器待做.md`、`客户端待做.md`（不在本引擎目录内） |

> 约定：引擎常量用 `EMsg*` / `EPush*`（回包为 `E*Reply` 结构体，无 `EReply*` 常量前缀）；业务别名用 `Msg*`。
> 结构方向（现行）：**门面在 `pkg`，真身在 `internal`**。`pkg` 只做门面（类型别名 / 变量转发 / 极薄参数适配）
> 并**允许** import `internal`；实现体一律留 `internal`（规则与现场自查见 `结构规则.md` §五）。
> 另有一类**自包含包**（自身不 import internal，如 `pkg/shared/**`、`pkg/foundation/**`、`pkg/runtime/**`、
> `pkg/domain/mmo/<子包>`）按 §5.2 允许把类型真身与整段实现留在 `pkg` —— 判据是「它自己 import internal 吗」。

---

## 1. 目录总览

```

├── internal/       # 引擎实现（不对外）
│   ├── app/        # 进程编排、挂载、路由与生命周期
│   ├── foundation/ # 真基础底座（现存 config/ophttp；logger/metrics/trace 在 pkg/foundation）
│   ├── runtime/    # 运行时原语（现存 globalstore；timer/fsm/pool/ratelimit/async 在 pkg/runtime）
│   ├── transport/  # 网关、鉴权、协议编解码、网络与跨节点通信
│   ├── domain/     # data、master、log、auth、mmo、object、room 等业务领域
│   └── shared/     # 协议、校验、重试及通用算法/工具
└── pkg/            # 门面层（别名 / 转发 / 极薄适配）+ 自包含工具库（允许 import internal，见 `结构规则.md` §五）
    ├── app/        # 应用启动层公开门面（配置类型、Game 等契约）
    ├── domain/     # 领域门面与自包含类型真身（data、master、mmo、object、room）
    ├── foundation/ # 基础设施（logger / logbuf / logstore / metrics / trace）
    ├── runtime/    # 运行时原语（timer 等，自包含）
    ├── transport/  # 传输层契约（event 等，自包含）
    └── shared/     # 共享工具与协议真身（proto / json / id / cache…，自包含）
```

