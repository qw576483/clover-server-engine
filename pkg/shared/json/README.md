# json 模块

## 模块职责

`json` 提供**统一的 JSON 序列化 / 反序列化封装**。它是对标准库 `encoding/json` 的一层极薄包装，目的有二：一是**收敛引用点**——全引擎统一走这个包，将来若要整体切换到更快的 JSON 实现（如 `sonic`、`go-json`），只需改这一个文件而不必全仓库替换 import；二是**统一错误包装**——所有错误都加上 `json.Marshal:` / `json.Unmarshal:` 前缀，日志中一眼可辨错误来源，同时用 `%w` 保留原始错误链以便 `errors.Is` / `errors.As` 判定。

## 规则与约束

1. **必须为本包或标准库 `encoding/json` 起别名**：包名 `json` 与标准库同名，同时使用两者的文件必须采用引擎约定的 `ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"`。
2. **本包仅统一 `Marshal` / `Unmarshal` 两个入口**：`MarshalIndent`、`Encoder` / `Decoder`、`Valid`、`Compact`、`RawMessage` 等仍需直接引用 `encoding/json`。
3. **高频大对象序列化的优化由调用方负责**：本包不做缓冲池复用与预分配，每次 `Marshal` 都会新分配字节切片。
4. **必须遵守 `encoding/json` 的行为约定**：map 键按字典序排序输出；`[]byte` 序列化为 base64 字符串；nil slice 输出 `null` 而空 slice 输出 `[]`；JSON 中缺失的字段保留目标变量原值；字段名匹配大小写不敏感；目标为 `any` 时数字解析为 `float64`；未导出字段不参与编解码；HTML 特殊字符默认转义。
5. **`Unmarshal` 的目标必须是非 nil 指针**：传值或 nil 指针会返回 `InvalidUnmarshalError`。
6. **错误日志按 `json.Marshal` / `json.Unmarshal` 前缀检索**：前缀与包名一致（`json.go:13` / `json.go:21`）。
7. **不得序列化含 `chan` / `func` / `complex` 字段的类型**：此类字段直接返回 `UnsupportedTypeError`，循环引用返回 `UnsupportedValueError`。
8. **`Marshal` 失败时返回 nil 切片**：成功与否必须由 error 判定，不得用 `len(b) == 0` 判断。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `json.go` | 24 | 全部内容：`Marshal`（序列化 + 错误包装）、`Unmarshal`（反序列化 + 错误包装） |

## 核心类型与接口

本包**不定义任何类型**，只导出两个函数，签名与标准库完全一致。

**并发安全性**：两者都直接转发到 `encoding/json`，**无任何包级状态**，与标准库同样**并发安全**（前提是调用方传入的 `v` 本身没有并发读写冲突）。

## 算法与实现原理

本包**没有自己的算法**，完全委托给标准库 `encoding/json`：

- **序列化**：`encoding/json` 通过反射遍历值的类型结构，按 `json` tag 决定字段名与选项（`omitempty`、`-`、`string` 等），生成 UTF-8 编码的 JSON 字节流。对实现了 `json.Marshaler` 的类型优先调用其 `MarshalJSON`。
- **反序列化**：`encoding/json` 用状态机扫描 JSON 词法，通过反射把值写入目标结构体字段（字段名匹配**大小写不敏感**）。对实现了 `json.Unmarshaler` 的类型优先调用其 `UnmarshalJSON`。

**唯一的增量逻辑是错误包装**：

```go
// Marshal
if err != nil {
    return nil, fmt.Errorf("json.Marshal: %w", err)
}

// Unmarshal
if err := stdjson.Unmarshal(data, v); err != nil {
    return fmt.Errorf("json.Unmarshal: %w", err)
}
```

用 `%w` 而非 `%v` 是关键——它保留了错误链，使调用方仍可用：

```go
var se *json.SyntaxError          // 标准库类型
if errors.As(err, &se) { ... }

var ute *stdjson.UnmarshalTypeError
if errors.As(err, &ute) { ... }
```

**别名导入**：文件内用 `stdjson "encoding/json"` 给标准库起别名，因为本包自身就叫 `json`，不别名会导致包名冲突。

## 对外 API

### `Marshal`

```go
func Marshal(v any) ([]byte, error)
```

用途：把任意值序列化为 JSON 字节。成功返回字节切片，失败返回带 `json.Marshal:` 前缀的包装错误。

```go
type Settings struct {
    Volume int    `json:"volume"`
    Lang   string `json:"lang"`
}

b, err := ujson.Marshal(&Settings{Volume: 80, Lang: "zh"})
if err != nil {
    return err
}
// b == []byte(`{"volume":80,"lang":"zh"}`)
```

### `Unmarshal`

```go
func Unmarshal(data []byte, v any) error
```

用途：把 JSON 字节反序列化到 `v`（必须是**非 nil 指针**）。失败返回带 `json.Unmarshal:` 前缀的包装错误。

```go
var s Settings
if err := ujson.Unmarshal(b, &s); err != nil {
    return err
}
fmt.Println(s.Volume, s.Lang) // 80 zh
```

### 引擎中的典型用法

在 auth handler 中构造回包（无 `Game` 引用时）：

```go
b, _ := ujson.Marshal(v)
c.MarkReplied(b)
```

配合 `errors.As` 精细化处理错误：

```go
var s Settings
if err := ujson.Unmarshal(raw, &s); err != nil {
    var se *stdjson.SyntaxError
    if errors.As(err, &se) {
        log.Printf("JSON 语法错误，偏移 %d: %v", se.Offset, err)
    }
    return err
}
```

## 依赖关系

- **仅依赖 Go 标准库**：`encoding/json`（以 `stdjson` 别名导入）、`fmt`（错误包装）。
- 零第三方依赖、**零引擎内部依赖**，是依赖树的叶子节点，可被任意模块引用。
- 引擎内约定的 import 别名通常为 `ujson`（避免与标准库 `json` 混淆）。
