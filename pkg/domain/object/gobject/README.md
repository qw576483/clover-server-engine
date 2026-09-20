# pkg/domain/object/gobject — 统一游戏对象

在 ObjectID 之上叠加持久化、组件挂载、定时器、序列化等能力，形成完整的「统一游戏对象」。业务通过 `GameObject` 接口操作，不感知具体实现。

> **结构方向（见 `结构规则.md` §五）**：本包是**门面包** —— 只有类型别名（`type GameObject = internal.…GameObjectFacade`）
> 与变量转发（`New` / `FromGameObject` / `InternalGameObject` / `ErrVersionConflict`），**不含实现体**。
> 真身（`GameObjectFacade` 接口 + `gameObjectFacade` 包装 + 工厂）在 `internal/domain/object/gobject/facade.go`；
> 同目录 `objstore` 同理（真身 `internal/domain/object/objstore/facade.go`）。

## 快速上手

```go
obj := gobject.New(store, object.NewObjectID(object.TypePlayer, 1001))
_ = obj.Load(ctx)
obj.Attach(&MyBuff{ID: "speed_up"})
_ = obj.Save(ctx)
```

1. `New` — 传入存储层与 ObjectID，构造空对象。
2. `Load` — 从持久化层载入属性与记录。
3. `Attach` — 挂载运行时组件（Buff/状态机/AI 行为树等）。
4. `Save` — 落库并递增乐观并发版本号。

## API 速查

### GameObject 接口

| 方法 | 说明 |
|------|------|
| `ObjectID()` | 返回对象号，满足 `object.Object` |
| `Load(ctx)` | 从 Store 载入 props 与 records |
| `Save(ctx)` | 落库并 bump 版本号 |
| `SaveIfVersion(ctx, expected)` | 仅在版本匹配时保存，否则返回 `ErrVersionConflict` |
| `Delete(ctx)` | 从 Store 彻底删除 |
| `Version()` | 当前乐观并发版本号 |
| `Attach(c)` | 挂载运行时组件 |
| `Detach(name)` | 按名称卸载组件 |
| `Component(name)` | 获取已挂载组件，未挂返回 nil |
| `ComponentNames()` | 全部已挂载组件名 |
| `DumpComponents()` | 导出全部组件二进制快照（迁移用） |
| `ImportComponents(list)` | 从快照批量恢复组件 |
| `StopComponents()` | 停止全部组件 |
| `AttachTimer(grp)` / `Timer()` | 绑定/获取定期器组 |
| `MarshalJSON()` / `UnmarshalJSON(b)` | 完整线化/反线化（含 props、records、子对象归属） |

### Component

GameObject 可挂载的运行时组件**接口**（不是函数类型）：

| 方法 | 说明 |
|------|------|
| `Name() string` | 组件唯一标识，同一对象上不可重复 |
| `OnAttach(obj)` | 组件挂载到对象时调用 |
| `OnDetach()` | 组件从对象卸载时调用 |
| `Dump() []byte` | 导出运行状态（跨节点迁移用；nil/空表示无状态） |
| `Import(data []byte)` | 从 Dump 数据恢复运行状态 |

- 组件以 `Name()` 为键：`Attach(c)` 挂载时若同名会先 `OnDetach` 旧组件并替换；快照恢复（`ImportComponents`）按名称匹配复原。

### 工厂与转换

| 函数 | 说明 |
|------|------|
| `New(store, id)` | 构造空 GameObject（仅身份 + 空属性袋） |
| `FromGameObject(g)` | 将 internal `*GameObject` 包装为接口 |
| `InternalGameObject(g)` | 将接口还原为 internal `*GameObject`（非底层则 ok=false） |

### 错误

```go
var ErrVersionConflict = gobject.ErrVersionConflict
```

`SaveIfVersion` 检测到对象自加载后被他人改动时返回，业务据此提示「数据已被修改，请重新拉取」。
