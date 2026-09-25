# id 模块

## 模块职责

`id` 提供**全局唯一短 ID 生成**。包含三类 ID：请求 ID（`GenRequestID`，用于 NATS 请求应答）、链路追踪 ID（`GenTraceID`，用于日志与消息透传）、以及引擎数据层唯一标识 `GenUID`（固定 16 位大写字母数字串，作为 `player.id` / `player_id` 等表字段的主键）。设计取向是**无中心依赖**（不需要发号器服务、不需要 Redis）、**单进程绝对唯一**、**跨进程极低碰撞概率**，同时保证 ID 字典序与时间序大致一致（便于按 ID 排序即按时间排序）。实现依赖标准库与 `pkg/shared` 的两个轻量工具包（`timeutil` / `traceid`）。

## 规则与约束

1. **UID 长度恒为 16 位，不得变更**：多张数据库表以 `VARCHAR(16)` 存放（`player.id`、`player_id` 等），变更格式或长度前必须先完成 DDL 迁移。
2. **必须处理 `GenUID` 的错误**：时钟大幅回拨（超过 `uidClockWaitLimit`）时返回 `ErrClockBackward`，不得写成 `uid, _ := id.GenUID()`。
3. **跨进程唯一性必须由数据库唯一索引与冲突重试兜底**：末尾 2 位强随机的碰撞概率约为 `7.7×10⁻⁴`，多实例高并发造号场景不得依赖本包保证绝对唯一。
4. **UID 前 10 位可直接反解为创建时间**：ID 暴露给客户端前必须评估创建时间与创建速率的泄漏风险，必要时另行映射。
5. **UID 的字典序仅近似时间序**：定长补零只保证时间戳段可比较，同毫秒内的次序由取模后的计数器决定，跨进程完全无序，不得把 UID 排序当作严格创建顺序。
6. **不得假设 `GenRequestID` / `GenTraceID` 的输出长度恒定**：`GenRequestID` 在随机源失败时降级为 `req_fallback` + 14 位日期（26 字符、秒级精度），下游解析必须兼容两种格式；`GenTraceID` 的降级在 `pkg/foundation/trace`（crypto/rand 播种失败时退化为纳秒时钟 + 地址熵），仍是 32 位十六进制。
7. **`GenRequestID` 与 `GenTraceID` 只是前缀相同的两类 ID，熵源不同**：`GenRequestID` 为 12 字节 `crypto/rand` 强随机；`GenTraceID` 的随机段来自 `pkg/shared/traceid.NewTraceID`（16 字节，转发 `pkg/foundation/trace` 的 xorshift64*）。长度不同且不可互相推导。
8. **`uidCounter` 为进程级全局计数器**：极高并发下存在 `atomic.AddUint64` 的缓存行争用，超高频造号场景须自行分片或批量预取。
9. **`GenUID` 每次调用至少读取一次 `crypto/rand`**：每秒百万级造号时该调用会成为瓶颈，此类场景必须自建批量随机缓冲。
10. **UID 字符集同时包含 `0`/`O` 与 `1`/`I` 等易混字符**：需人工抄录或语音传达的 ID 必须另行编码，本包不剔除易混字符。
11. **UID 与请求/追踪 ID 的大小写风格不同**：`GenUID` 输出全大写 base36，`GenRequestID` / `GenTraceID` 输出小写十六进制，字符串比较与大小写敏感查询时必须先统一。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `id.go` | 157 | 全部内容：字符表 `uidChars`、计数器上限 `uidCntBand`、全局计数器 `uidCounter`、导出函数 `GenRequestID`/`GenTraceID`/`GenUID`/`RandomHex` 与错误值 `ErrClockBackward`，内部辅助 `genID`/`encodeBase36`/`randomBase36`/`fillFromClock` |

## 核心类型与接口

本包**不定义任何类型**：导出四个函数（`GenUID` / `GenRequestID` / `GenTraceID` / `RandomHex`）与一个错误值（`ErrClockBackward`）。

### 包级状态

| 标识符 | 类型 | 含义 |
| --- | --- | --- |
| `uidChars` | `string` 常量 | 36 进制字符表 `"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"` |
| `uidCntBand` | 常量 `= 36⁴ = 1679616` | 4 位 36 进制计数器的取值上限 |
| `uidCounter` | `uint64` 包级变量 | 全局单调自增计数器 |

**并发安全性**：

- `GenUID`：**并发安全**。`uidCounter` 通过 `atomic.AddUint64` 原子自增，其余部分（时间读取、随机数、编码）都无共享状态。
- `GenRequestID`：**并发安全**。依赖 `crypto/rand`（内部自带并发保护），无包级状态。
- `GenTraceID`：**并发安全**。转发到 `pkg/shared/traceid` → `pkg/foundation/trace`，随机源是那里的原子 xorshift64*（无锁），无本包状态。

## 算法与实现原理

### GenUID：时间戳 + 计数器 + 随机后缀

**格式（固定 16 位）**：

```
┌──────────────┬─────────┬────────┐
│ 10 位 base36 │ 4 位    │ 2 位   │
│ 毫秒时间戳    │ 计数器   │ 强随机  │
└──────────────┴─────────┴────────┘
```

| 段 | 宽度 | 来源 | 说明 |
| --- | --- | --- | --- |
| 时间戳 | 10 位 base36 | `time.Now().UnixMilli()` | 值域 36¹⁰ ≈ 3.66×10¹⁵ 毫秒，**理论可用到约公元 118000 年** |
| 计数器 | 4 位 base36 | `atomic.AddUint64(&uidCounter,1) % 36⁴` | 同一毫秒内可分配 **1,679,616** 个不冲突序号 |
| 随机 | 2 位 base36 | `crypto/rand` 拒绝采样 | 用于跨进程去重，碰撞概率 36⁻² ≈ **7.7×10⁻⁴** |

**唯一性论证**：

- **单进程内绝对唯一**：`uidCounter` 是 `uint64` 单调自增，在同一毫秒内不同调用必然拿到不同的 `cnt`（因为一毫秒内不可能产生超过 167 万次调用），配合毫秒时间戳即可保证唯一。
- **跨进程**：靠末尾 2 位强随机，同一毫秒同一序号的两个进程碰撞概率约 1/1296。

**为什么用 uint64 计数器**：注释明确指出，`uint32` 计数器在高频调用下约 **49.7 天**后会回绕，回绕时若恰好撞上相同毫秒会产生 ID 冲突。`uint64` 在任何现实时间尺度内都不会回绕。

**为什么要 `% uidCntBand`**：`encodeBase36(cnt, 4)` 只取低 4 位 36 进制，超出部分会被**静默截断**。先对 36⁴ 取模，可保证 `cnt` 恒定落在可表达区间内，逻辑上更清晰、无截断歧义。

**长度是硬性契约**：注释明确警告——16 位长度是与数据库的契约（多张表以 `VARCHAR(16)` 存放，如 `player` 表的 `id` / `player_id`），**切勿改动**。

### encodeBase36：定长高位补零编码

```go
for i := length - 1; i >= 0; i-- {
    chars[i] = uidChars[v%36]
    v /= 36
}
```

从**低位向高位**填充，不足部分自然是 `'0'`（因为 `v` 归零后 `v%36 == 0`）。这保证输出**恒为 length 位**，从而 UID 总长恒定 16 位，且**字典序 = 数值序**（定长 + 高位补零是字典序可比的前提）。

超长会被截断（高位丢弃），调用方需保证 `v < 36^length`。

### randomBase36：拒绝采样消除取模偏置

直接用 `randByte % 36` 会有**取模偏置**：256 不是 36 的整数倍，`256 = 7×36 + 4`，导致字符 `0`~`3` 出现概率是其余字符的 8/7 倍。

解决方案是**拒绝采样（rejection sampling）**：

```go
const bound = 36 * 7 // 252，<256 的最大 36 倍数
if one[0] >= bound {
    continue  // 丢弃 252~255，重新取
}
out[i] = uidChars[one[0]%36]
```

只接受 `[0, 252)` 的字节，落在 `[252, 256)` 的（概率 4/256 ≈ 1.6%）直接丢弃重取。这样 36 个字符**严格等概率**。期望迭代次数约 1.016 次，开销可忽略。

### fillFromClock：随机源失败的降级兜底

`crypto/rand.Read` 在主流平台不会失败，但代码仍做了兜底：用纳秒时间戳逐位取模填充。

关键细节是**余数耗尽保护**：

```go
if now == 0 {
    now = uint64(time.Now().UnixNano()) | 1  // 或 1 保证非零
}
```

若不做这个检查，当 `now` 被反复 `/= 36` 除到 0 后，剩余位会全部坍缩成 `'0'`（因为 `0 % 36 == 0`）。重新读时钟并 `| 1` 保证非零，避免尾部退化。这是**低熵降级**路径，仅在极端情况生效。

### genID：请求 ID

```
格式：<prefix>_<24 位小写十六进制>
```

- 从 `crypto/rand` 读取 **12 字节（96 位）**强随机（经 `RandomHex(12)`）。
- 用 `encoding/hex` 的 `hex.EncodeToString` 编码为 24 个小写十六进制字符。
- **总长度**：`"req_" + 24` = **28 字符**。
- **碰撞概率**：96 位随机，实际可视为不会碰撞。
- **失败兜底**：`rand.Read` 失败时返回 `prefix + "_fallback" + time.Now().Format("20060102150405")`，例如 `req_fallback20260803120000`（**长度不同，且秒级精度会重复**）。
- `GenTraceID` 走 `pkg/shared/traceid` 的 `NewTraceID()`，输出为 `"trc_" + 32 位小写十六进制`（16 字节，与 W3C trace-id 位宽一致），其随机源与兜底策略见 `pkg/foundation/trace`。

### 十六进制编码

十六进制编码统一用标准库 `encoding/hex` 的 `hex.EncodeToString`（小写输出，`RandomHex` 内）。

## 对外 API

### `GenUID`

```go
func GenUID() (string, error)
```

用途：生成引擎数据层唯一标识，固定 16 位全大写英文 + 数字。

```go
uid, err := id.GenUID()
if err != nil {
    return err // ErrClockBackward：时钟大幅回拨，造号前必须先修时间
}
fmt.Println(uid, len(uid)) // 如 "0000LP3K8XZ0001A7" 形态，长度恒为 16

// 典型用途：造号
player := &EPlayer{
    ID:       uid,
    PlayerID: uid,
}
```

### `GenRequestID`

```go
func GenRequestID() string
```

用途：NATS 请求应答的关联 ID、消息追踪。

```go
reqID := id.GenRequestID()
// "req_3f2a1c8b9d0e4f5a6b7c8d9e"
msg.Header.Set("X-Request-Id", reqID)
```

### `GenTraceID`

```go
func GenTraceID() string
```

用途：链路追踪 ID，用于日志与跨节点消息透传。

```go
traceID := id.GenTraceID()
// "trc_a1b2c3d4e5f60718293a4b5c6d7e8f90"（32 位十六进制）
logger.Info("请求开始", logger.WithTrace(traceID))
```

> 相关：`pkg/shared/traceid` 包负责把 trace ID 在 `context.Context` 中传递，本包只负责生成。

## 依赖关系

- **标准库依赖**：`crypto/rand`（强随机源）、`encoding/hex`（十六进制编码）、`sync/atomic`（无锁计数器）、`time`（毫秒/纳秒时间戳）。
- **引擎内部依赖**：`pkg/shared/timeutil`（`NowMS` / `NowTime`）、`pkg/shared/traceid`（`NewTraceID`）。
- 零第三方依赖。
- **相关包**：`pkg/shared/traceid`（trace ID 的上下文传递）。
