# pkg/domain/data — 数据存储层

## 模块职责

通用数据存储抽象。业务只依赖 `Store` 接口，不感知底层是 Redis 还是 MySQL。
任何业务实体只是 `OwnerType` 的一个取值，无需为每类实体单独建表。

## 快速上手

**业务侧**（推荐路径）：只声明 schema，读写经 `app.Game` 完成（自动落库 + 广播）。

```go
// init() 中登记表结构，字段类型见 pkg/domain/object 的 Type 常量
data.RegisterTypeBySchema(data.StructSchema{
    Type: "bag", OwnerType: data.OwnerPlayer, Visibility: data.ClientSelfOnly,
})

// handler 内读写（c 为 event.Ctx）
var bag Bag
if err := g.LoadStruct(c, bagSchema, "bag", &bag); err != nil { /* ... */ }
```

**引擎装配侧 / 测试**：直接构造 Store。注意 `data.Config` 是**不透明句柄**（`type Config any`），
字段真身在引擎内部，**不能就地改**；要改连接参数请改配置文件的 `data:` 段。

```go
cfg := data.DefaultConfig()          // 或 MemoryConfig() / RedisConfig() / MMOConfig()
store, err := data.NewStore(cfg)
if err != nil { /* ... */ }
key := data.Key{Owner: data.OwnerPlayer, ID: "1001", Type: "bag"}
_ = store.SaveJSON(ctx, key, &Bag{Items: []int{1, 2, 3}})
var bag Bag
_ = store.LoadJSON(ctx, key, &bag)
```

## API 速查表

### 配置工厂

| 函数 | 说明 |
|------|------|
| `DefaultConfig()` | Redis + MySQL（默认） |
| `MemoryConfig()` | 纯内存，适合本地开发 / 单测 |
| `MMOConfig()` | MMO 快照模式 |
| `RedisConfig()` | 纯 Redis |
| `NewStore(cfg)` | 根据配置创建 Store 实例 |

> 以上工厂返回的 `Config` 是**不透明句柄**（`type Config any`），实现由 `internal/domain/data`
> 的 `init()` 注册（见下方「工厂注册」）。默认值来自引擎配置文件的 `data:` 段，代码里不就地改字段。

### Store 接口

| 方法 | 说明 |
|------|------|
| `Save(ctx, key, data)` | 写入原始字节 |
| `Load(ctx, key)` | 读取原始字节，不存在返回 `ErrNotFound` |
| `SaveJSON(ctx, key, v)` | 将 `v` 序列化为 JSON 后写入 |
| `LoadJSON(ctx, key, v)` | 读取并反序列化 JSON 到 `v` |
| `Delete(ctx, key)` | 删除一条数据 |

### Key 组合主键

```go
type Key struct {
    Owner        OwnerType // 归属实体类型
    ID           string    // 归属实体 ID
    Type         string    // 业务数据类型
    NoLocalCache bool      // 跳过本地缓存，直读持久层（跨区镜像等场景）
}
```

### OwnerType 预置常量

| 常量 | 值 | 用途 |
|------|----|------|
| `OwnerAccount` | `"account"` | 账号 |
| `OwnerPlayer` | `"player"` | 玩家 |
| `OwnerServer` | `"server"` | 服务器 |
| `OwnerObject` | `"object"` | 游戏对象 |
| `OwnerMeta` | `"meta"` | 内部簿记 |

### Schema 注册

| 函数 | 说明 |
|------|------|
| `RegisterType(ownerType, typ)` | 按 OwnerType + 类型名注册 |
| `RegisterTypeBySchema(s)` | 传入 `StructSchema` / `RecordSchema` 自动注册 |
| `SchemaTypes(ownerType)` | 查询已注册的类型列表 |

### Record 接口（行式表格数据）

`Record` 为强类型行列表接口，支持增删行列、单元格读写、脏行增量同步等。
配合 `RecordSchema` 声明列名与列类型，通过 `app.Game` 上下文读写。

### 工厂注册（引擎装配用）

| 函数 | 说明 |
|------|------|
| `RegisterStoreFactory(fn)` | 注册底层 `NewStore` 实现 |
| `RegisterMemoryConfig(fn)` | 注册 `MemoryConfig` 实现 |
| `RegisterDefaultConfig(fn)` | 注册 `DefaultConfig` 实现 |
| `RegisterMMOConfig(fn)` | 注册 `MMOConfig` 实现 |
| `RegisterRedisConfig(fn)` | 注册 `RedisConfig` 实现 |
