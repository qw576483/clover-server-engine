# conv 模块

## 模块职责

`conv` 提供**通用类型与数据转换**工具：字符串到整型的容错转换、任意值到字符串的统一表达、同类型结构体间的反射浅拷贝、以及 `map[string]any` 的浅合并。设计取向是**「不 panic、给出可用结果」**——转换失败返回零值而非抛错，适合配置解析、日志打印、调试输出等宽容场景。对于结构体拷贝这类可能静默丢数据的操作，则额外提供 `Strict` 变体让调用方感知漏拷。所有函数仅依赖标准库，可独立测试。

## 规则与约束

1. **区分「合法的 0」与「解析失败」**：`ToInt` / `ToInt64` 等容错转换在解析失败时返回零值，与输入为 `"0"` 的结果无法区分；需要区分这两种情形时必须使用 `strconv.Atoi` / `strconv.ParseInt` 并自行检查 error。
2. **整型解析只接受十进制**：`ToInt64` 的进制硬编码为 `10`，`"0x1F"`、`"0b101"`、`"017"` 一律解析失败并返回 `0`；需要前缀进制自动识别时必须直接使用 `strconv.ParseInt` 并传入 `base = 0`。
3. **`ToInt` 的位宽跟随平台**：`ToInt` 基于 `strconv.Atoi`，结果位宽等同于 `int`，在 32 位构建下超出 int32 范围的输入会解析失败并返回 `0`；跨平台或大数值场景必须使用 `ToInt64`。
4. **二进制内容必须先编码**：`ToString` 对 `[]byte` 直接做字节到字符串的转换，不做转义或 base64 编码；含不可打印字节的二进制数据必须先自行 `hex.EncodeToString` 或 `base64` 编码后再传入。
5. **`ToString` 的输出格式不保证稳定**：对 `json.Marshal` 失败的类型（如含 chan / func / complex 字段的值）会回落到 `fmt.Sprintf("%v", v)`，输出格式与 JSON 完全不同；日志格式的稳定性不得依赖该函数。
6. **`Stringer` 优先于 JSON**：`ToString` 的类型分派中 `fmt.Stringer` 位于 `json.Marshal` 之前，同时实现 `Stringer` 与 `json.Marshaler` 的类型必须确认 `String()` 的输出即为期望结果。
7. **优先使用 `StructCopyStrict`**：含非导出字段的结构体（如内嵌 `sync.Mutex`、私有缓存字段）必须使用 `StructCopyStrict` 并显式处理 error，`StructCopy` 会丢弃全部 error 并静默漏拷这些字段。
8. **漏拷 error 不代表回滚**：`StructCopyStrict` 返回的漏拷 error 在导出字段全部拷贝完成之后才产生，`dst` 已被修改；该 error 不得当作「操作已回滚」来处理。
9. **浅拷贝语义**：`StructCopy` / `StructCopyStrict` 对切片 / 映射 / 指针 / channel 字段只复制引用，`dst` 与 `src` 此后共享同一底层数据；需要深拷贝必须自行实现或走 JSON 往返。
10. **类型必须完全相同**：两个结构体指针的类型必须严格相等，字段布局相同但类型名不同（含「同名不同包」「有无 type alias」）一律拒绝；需要跨类型拷贝必须自行实现转换。
11. **反射开销**：结构体拷贝每次调用都走反射逐字段遍历，比手写赋值慢一到两个数量级，热路径必须手写字段赋值。
12. **`MapMerge` 就地修改 `dst`**：`dst != nil` 时合并结果直接写入传入的 map，返回值与入参是同一对象；需要保留原 map 必须以 `nil` 作为 dst 并先合并原 map，即 `conv.MapMerge(conv.MapMerge(nil, base), over)`。
13. **`MapMerge` 只做一层合并**：嵌套的 `map[string]any` 会被整体替换而非递归合并，多层配置覆盖场景必须自行实现深合并。
14. **`MapMerge` 不忽略 `src` 的 nil 值**：`src[k] == nil` 会把 `dst[k]` 覆盖为 `nil`，既不保留原值也不删除该 key；需要跳过空值必须自行过滤 `src`。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `conv.go` | 188 | 全部内容：字符串转数值（`ToInt` / `ToInt64` / `ToFloat64` / `ToFloat32` / `ToBool`）、数值转字符串（`FormatInt` / `FormatIntBase` / `FormatUint` / `FormatBool` / `FormatFloat`）、`ToString` 任意值转字符串、`StructCopy` / `StructCopyStrict` 结构体反射浅拷贝、`MapMerge` 映射浅合并 |

## 核心类型与接口

本包**不定义任何类型**，只导出十四个函数。

**并发安全性**：

- `ToInt` / `ToInt64` / `ToFloat64` / `ToFloat32` / `ToBool` / `ToString` / `FormatInt` / `FormatIntBase` / `FormatUint` / `FormatBool` / `FormatFloat`：**纯函数，完全并发安全**（`ToString` 只读取入参）。
- `StructCopy` / `StructCopyStrict` / `MapMerge`：函数本身无共享状态，但**会写入调用方传入的 `dst`**。若 `dst` 被多 goroutine 共享，需调用方自行加锁。

## 算法与实现原理

### `ToString` 的类型分派顺序

按下列**固定顺序**做类型断言，命中即返回：

1. `v == nil` → 返回 `""`（**显式短路**，目的是避免 `json.Marshal(nil)` 产生字面量 `"null"`）。
2. `string` → 原样返回。
3. `[]byte` → `string(val)` 直接转换（不做 base64 编码）。
4. `fmt.Stringer` → 调用 `val.String()`。
5. 其余 → `json.Marshal`；失败时兜底 `fmt.Sprintf("%v", v)`。

注意 **`[]byte` 分支在 `Stringer` 之前**，且 `string`/`[]byte` 都在 JSON 之前——这保证了常见类型走零分配或最快路径，也决定了 `[]byte` 的输出是原始字节串而非 JSON 的 base64 表示。

### 结构体浅拷贝（反射逐字段 Set）

`StructCopyStrict` 的完整流程：

1. **参数校验**（任一不满足即返回描述性 error）：
   - `dst` 与 `src` 都必须是 `reflect.Ptr`；
   - 都不能是 nil 指针；
   - `dv.Elem().Type() == sv.Elem().Type()`（**类型必须完全相同**，不做隐式转换）；
   - 解引用后必须是 `reflect.Struct`。
2. **逐字段拷贝**：遍历 `NumField()`，对每个字段：
   - `f.CanSet()` 为真（即**导出字段**）→ `f.Set(se.Field(i))`；
   - 不可设置（非导出字段）→ 检查源端 `IsZero()`：**只有源值非零**才记入 `skipped` 列表（源值本就是零值时拷不拷都一样，不算漏拷）。
3. **结果**：`skipped` 非空则返回列出字段名的 error（**但导出字段已经拷贝完成**，不会回滚）。

`StructCopy` 就是 `_ = StructCopyStrict(dst, src)`，即**丢弃所有 error 的宽松版本**。

**浅拷贝语义**：`f.Set` 做的是值赋值。对于切片 / 映射 / 指针 / channel 字段，复制的是**引用（header/指针）**，`dst` 与 `src` 之后共享同一底层数据；对于嵌套结构体字段，则是整块值复制（其内部的引用字段仍是共享的）。

### `MapMerge` 浅合并

`dst == nil` 时先 `make`，然后 `for k, v := range src { dst[k] = v }`——**src 覆盖 dst 的同名 key**，并返回 `dst` 以支持链式调用。只做一层覆盖，**不递归合并嵌套 map**。

## 对外 API

### `ToInt` / `ToInt64`

```go
func ToInt(s string) int
func ToInt64(s string) int64
```

用途：容错的字符串转整型，失败返回 `0` 而不是 error / panic。`ToInt` 内部用 `strconv.Atoi`，`ToInt64` 用 `strconv.ParseInt(s, 10, 64)`（十进制、64 位）。

```go
conv.ToInt("42")           // 42
conv.ToInt("-7")           // -7
conv.ToInt("abc")          // 0（失败）
conv.ToInt("")             // 0
conv.ToInt64("9007199254740993") // 9007199254740993
conv.ToInt64("0x1F")       // 0（不支持十六进制）
```

### `ToFloat64` / `ToFloat32` / `ToBool`

```go
func ToFloat64(s string) float64
func ToFloat32(s string) float32
func ToBool(s string) bool
```

用途：容错的字符串转浮点数与布尔值，失败返回零值。`ToFloat32` 用 `strconv.ParseFloat(s, 32)`（单精度解析，不做 float64 再窄化）；`ToBool` 走 `strconv.ParseBool`，接受 `1/t/T/TRUE/true/True` 与 `0/f/F/FALSE/false/False`，其余返回 `false`。

```go
conv.ToFloat64("3.14")   // 3.14
conv.ToFloat64("abc")    // 0（失败）
conv.ToFloat32("1.5")    // 1.5
conv.ToBool("true")      // true
conv.ToBool("1")         // true
conv.ToBool("yes")       // false（不接受）
```

### `FormatInt` / `FormatIntBase` / `FormatUint` / `FormatBool` / `FormatFloat`

```go
func FormatInt(n int64) string             // 十进制
func FormatIntBase(n int64, base int) (string, error) // 指定进制，base 取值 2~36；越界返回 ErrInvalidBase
func FormatUint(n uint64) string           // 无符号十进制
func FormatBool(b bool) string             // "true" / "false"
func FormatFloat(f float64) string         // 最短表示，不截断
```

用途：数值转字符串的便捷封装，覆盖 `ToString` 之外的确定性格式化需求。

```go
conv.FormatInt(-7)           // "-7"
conv.FormatUint(42)          // "42"
conv.FormatBool(true)        // "true"
conv.FormatFloat(1.5)        // "1.5"

s, err := conv.FormatIntBase(255, 16) // s == "ff"；base 越界时返回 ("", ErrInvalidBase)
```

### `ToString`

```go
func ToString(v any) string
```

用途：把任意值转成人类可读的字符串，用于日志、调试、错误信息拼接。

```go
conv.ToString(nil)                 // ""
conv.ToString("hi")                // "hi"
conv.ToString([]byte("hi"))        // "hi"
conv.ToString(time.Second)         // "1s"（time.Duration 实现了 Stringer）
conv.ToString(42)                  // "42"（走 JSON）
conv.ToString(map[string]int{"a":1}) // {"a":1}
conv.ToString(struct{ A int }{1})  // {"A":1}
```

### `StructCopy` / `StructCopyStrict`

```go
func StructCopy(dst, src any)              // 宽松版，忽略所有错误
func StructCopyStrict(dst, src any) error  // 严格版，感知漏拷与参数错误
```

用途：同类型结构体指针之间的字段浅拷贝。

```go
type Player struct {
    ID   int64
    Name string
    Tags []string
}

var dst Player
src := &Player{ID: 1001, Name: "clover", Tags: []string{"vip"}}

conv.StructCopy(&dst, src)
fmt.Println(dst.ID, dst.Name) // 1001 clover

// 严格版：暴露参数错误与漏拷
if err := conv.StructCopyStrict(&dst, src); err != nil {
    log.Printf("拷贝不完整: %v", err)
}

// 含非导出字段时
type WithPriv struct {
    Pub  int
    priv int // 非导出
}
a := &WithPriv{Pub: 1}
var b WithPriv
err := conv.StructCopyStrict(&b, a) // priv 是零值 → err == nil
```

`StructCopyStrict` 返回 error 的三种情形：

| 情形 | error 示例 |
| --- | --- |
| 非指针 | `conv: StructCopy 要求 dst/src 均为指针，实际 dst=struct src=ptr` |
| nil 指针 | `conv: StructCopy 的 dst/src 指针不可为 nil` |
| 类型不符 | `conv: StructCopy 类型不符 dst=A src=B` |
| 非结构体 | `conv: StructCopy 仅支持结构体，实际为 int` |
| 漏拷非导出字段 | `conv: StructCopy 跳过了非导出且源值非零的字段 [priv]（这些字段未被拷贝）` |

### `MapMerge`

```go
func MapMerge(dst, src map[string]any) map[string]any
```

用途：把 `src` 合并进 `dst`，同名 key 由 `src` 覆盖；`dst` 为 nil 时新建。

```go
base := map[string]any{"a": 1, "b": 2}
over := map[string]any{"b": 20, "c": 3}
out := conv.MapMerge(base, over)
// out == map[string]any{"a":1, "b":20, "c":3}
// 注意 base 本身也被就地修改了！

// dst 为 nil 时新建
m := conv.MapMerge(nil, over) // 等价于 over 的浅拷贝

// 链式
cfg := conv.MapMerge(conv.MapMerge(nil, defaults), userOverrides)
```

## 依赖关系

- **仅依赖 Go 标准库**：`encoding/json`（ToString 兜底序列化）、`fmt`（Stringer 接口与错误构造）、`reflect`（结构体拷贝）、`strconv`（字符串转整型）。
- 零第三方依赖、**零引擎内部依赖**，是依赖树的叶子节点。
