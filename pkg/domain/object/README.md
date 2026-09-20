# pkg/domain/object — 游戏对象内核

定义游戏对象的统一身份标识（ObjectID）、值类型系统（Value）、属性元数据框架（AttrSet），以及全局对象管理器（Manager）。所有可寻址实体（玩家、怪物、场景对象等）都通过 ObjectID 唯一标识。

## ObjectID

类型 `struct { Type uint16; Seq uint64 }`，高 16 位为对象类别，低 48 位为实例序号，可直接作为 map 键。

| API | 说明 |
|-----|------|
| `NewObjectID(typ, seq)` | 构造对象号，seq 溢出 48 位时截断 |
| `ParseObjectID("type:seq")` | 从字符串解析 |
| `FromUint64(v)` | 从 `MarshalUint64` 结果还原 |
| `id.IsZero()` | 是否未赋值 |
| `id.String()` | `"type:seq"` 格式，便于日志 |
| `id.MarshalUint64()` | 编码为单个 uint64，用于网络/存储 |

### 预置常量

```go
TypePlayer uint16 = 1  // 玩家
TypeScene  uint16 = 2  // 场景/地图区块
```

## 接口

| 接口 | 方法 | 用途 |
|------|------|------|
| `Message` | `MsgType() uint32` | Manager 绑定与派发的消息体 |
| `Object` | `ObjectID() ObjectID` | 可被 Manager 管理的对象 |
| `MessageReceiver` | `Object` + `OnMessage(ctx, msg)` | 对象自管消息（Manager 找不到类型处理器时的回退） |

## Manager

全局对象注册表 + 消息/事件派发内核，并发安全。

| 方法 | 说明 |
|------|------|
| `Handle(objType, msgType, handler)` | 绑定 `(对象类型, 消息类型) → 处理器` |
| `Register(obj)` / `Unregister(id)` | 注册/注销对象 |
| `Get(id)` | 按 ObjectID 查找对象 |
| `Send(ctx, id, msg)` | 向指定对象发消息，优先走 Handle 绑定，回退 OnMessage |
| `SendAll(ctx, typ, msg)` | 向某类型全部对象广播 |
| `OnEvent(objType, eventType, handler)` | 绑定 `(对象类型, 事件名) → 事件处理器` |
| `SendEvent(ctx, id, eventType, payload)` | 向指定对象发事件 |
| `ForEach(typ, fn)` | 遍历某类型全部对象（typ=0 全部） |
| `Count(typ)` | 返回注册对象数 |

## BasicObject

可内嵌的极简对象实现，仅持有 ObjectID。业务结构体内嵌即可满足 `Object` 接口。

```go
type MyEntity struct {
    object.BasicObject
}
```

## Value

类型安全的值容器，用于属性系统、持久化 codec、跨服同步。

| 构造函数 | 对应类型常量 |
|----------|-------------|
| `NewInt(n)` | `TypeInt = 1` |
| `NewFloat(f)` | `TypeFloat = 2` |
| `NewString(s)` | `TypeString = 3` |
| `NewBool(b)` | `TypeBool = 5` |
| `NewBytes(b)` | `TypeBytes = 4` |
| `NewObject(id)` | `TypeObject = 6` |
| — | `TypeNil = 0`（零值） |

支持 JSON `{"t":类型,"v":值}` 与二进制紧凑序列化，双向 `MarshalJSON`/`UnmarshalJSON`、`MarshalBinary`/`UnmarshalBinary`。

## AttrSet

按 ID 索引的轻量数值属性容器 + 变更回调，供战斗/Buff/技能做数值读写与监听。内部以 `float64` 承载，整型语义通过 `Int`/`SetInt` 提供。

| 方法 | 说明 |
|------|------|
| `NewAttrSet(defs...)` | 用定义表构造，按 `Default` 初始化 |
| `Get(id)` / `Set(id, v)` | 读/写当前值，同值不触发回调 |
| `Add(id, delta)` | 增量修改，返回新值 |
| `Int(id)` / `SetInt(id, v)` | 整型快捷方法 |
| `OnChange(id, fn)` | 注册单属性变更回调 |
| `Snapshot()` | 返回当前值快照副本 |
| `Load(vals)` | 批量载入（不触发回调，用于存档恢复） |

### AttrDef

```go
type AttrDef struct {
    ID      AttrID    // 属性 ID
    Name    string    // 属性名
    Kind    AttrKind  // AttrKindInt 或 AttrKindFloat
    Default float64   // 默认值
}
```
