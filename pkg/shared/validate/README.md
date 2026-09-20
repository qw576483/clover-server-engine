# shared/validate — 数据校验框架

基于 struct tag 的声明式校验框架，通过反射实现零配置校验。

## 内置校验器

| Tag | 适用类型 | 说明 | 示例 |
|-----|----------|------|------|
| `required` | 所有 | 非零值/非空列表 | `validate:"required"` |
| `min=N` | 仅数值 | 数值最小值，用于列表/字符串会直接报错 | `validate:"min=1"` |
| `max=N` | 仅数值 | 数值最大值，用于列表/字符串会直接报错 | `validate:"max=100"` |
| `len=N` | 字符串/slice/map | 长度等于 N | `validate:"len=8"` |
| `email` | 字符串 | 邮箱格式 | `validate:"email"` |
| `regex=pat` | 字符串 | 正则匹配 | `validate:"regex=^[a-z]+$"` |

> 列表/字符串的长度范围请用 `required` + `len` 表达，不要用 `min`/`max`（`min`/`max` 误用于非数值字段时会在校验时直接返回明确错误，而不是静默通过）。

## 示例

```go
type Player struct {
    Name  string `validate:"required,min=1,max=32"`
    Level int    `validate:"required,min=1"`
    Email string `validate:"email"`
}
err := validate.Struct(Player{Name: "", Level: -1})
// validate: 3 errors: field "Name" is required; field "Level" must be >= 1, got -1; ...
```
