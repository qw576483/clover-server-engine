// Package conv 提供通用类型与数据转换。
//
// 所有函数仅依赖标准库，可独立测试。
package conv

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// ToInt 字符串转 int，转换失败返回 0，不 panic。
func ToInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// ToInt64 字符串转 int64，转换失败返回 0，不 panic。
func ToInt64(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// ToFloat64 字符串转 float64，转换失败返回 0，不 panic。
func ToFloat64(s string) float64 {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return n
}

// ToFloat32 字符串转 float32，转换失败返回 0，不 panic。
// 直接用 ParseFloat(s, 32)：先按 float64 解析再窄化时，"1e40" 会得 ±Inf、
// "1e-50" 得 0 且不报错，与「转换失败返回 0」的契约不符。
func ToFloat32(s string) float32 {
	f, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0
	}
	return float32(f)
}

// ToBool 字符串转 bool，语义等同 strconv.ParseBool：
// 接受 "1"/"t"/"T"/"TRUE"/"true"/"True" 为 true，"0"/"f"/"F"/"FALSE"/"false"/"False" 为 false；
// 其余（含无法解析的输入）返回 false。
func ToBool(s string) bool {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}
	return b
}

// FormatInt int64 转字符串（十进制）。
func FormatInt(n int64) string { return strconv.FormatInt(n, 10) }

// ErrInvalidBase 表示 FormatIntBase 的进制参数越界（合法范围为 [2, 36]）。
var ErrInvalidBase = errors.New("conv: base 超出合法范围 [2, 36]")

// FormatIntBase int64 按指定进制转字符串（base 2~36）。
// base 越界时 strconv 会直接 panic，故这里先校验并返回 ErrInvalidBase（结果与错误值均为零值）。
func FormatIntBase(n int64, base int) (string, error) {
	if base < 2 || base > 36 {
		return "", ErrInvalidBase
	}
	return strconv.FormatInt(n, base), nil
}

// FormatUint uint64 转字符串。
func FormatUint(n uint64) string { return strconv.FormatUint(n, 10) }

// FormatBool bool 转字符串。
func FormatBool(b bool) string { return strconv.FormatBool(b) }

// FormatFloat float64 转字符串（最短表示，不截断）。
func FormatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

// ToString 任意类型转字符串。
// 字符串原样返回；实现 fmt.Stringer 的调用其 String()；
// 其余类型走 JSON 序列化表达。nil 显式返回 ""，
// 避免 json.Marshal(nil) 产生 "null" 字符串。
func ToString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case fmt.Stringer:
		// typed-nil（如 (*T)(nil) 且 *T 实现 Stringer）装箱后接口不为 nil，
		// 直接调 String() 即对 nil 接收者解引用 panic，先挡掉。
		if isNilValue(v) {
			return ""
		}
		return val.String()
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// isNilValue 报告接口值内部是否为 nil 指针 / 映射 / 切片等（typed-nil 检测）。
func isNilValue(v any) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// StructCopy 结构体浅拷贝，将 src 字段值复制到 dst（同类型指针）。
// 字段为切片/映射时仅复制引用（浅拷贝）。
// 存在非导出字段且源值非零时，对应字段静默跳过。
// 调用方须确保目标结构体仅包含导出字段，或使用 StructCopyStrict 感知漏拷。
func StructCopy(dst, src any) {
	_ = StructCopyStrict(dst, src)
}

// StructCopyStrict 与 StructCopy 行为一致（同类型指针间浅拷贝导出字段），
// 但在遇到「因非导出而无法设置、且源端该字段值非零」的情况时返回 error，
// 从而暴露可能被静默丢弃的关键数据。
//
// 返回值语义：
// - 参数非法（非指针 / nil / 类型不符）返回描述性 error；
// - 存在非导出且源值非零的字段被跳过时，返回列出这些字段名的 error（导出字段仍已拷贝）；
// - 全部字段成功拷贝、或被跳过的非导出字段源值均为零值时，返回 nil。
func StructCopyStrict(dst, src any) error {
	dv := reflect.ValueOf(dst)
	sv := reflect.ValueOf(src)
	if dv.Kind() != reflect.Ptr || sv.Kind() != reflect.Ptr {
		return fmt.Errorf("conv: StructCopy 要求 dst/src 均为指针，实际 dst=%s src=%s", dv.Kind(), sv.Kind())
	}
	if dv.IsNil() || sv.IsNil() {
		return fmt.Errorf("conv: StructCopy 的 dst/src 指针不可为 nil")
	}
	de := dv.Elem()
	se := sv.Elem()
	if de.Type() != se.Type() {
		return fmt.Errorf("conv: StructCopy 类型不符 dst=%s src=%s", de.Type(), se.Type())
	}
	if de.Kind() != reflect.Struct {
		return fmt.Errorf("conv: StructCopy 仅支持结构体，实际为 %s", de.Kind())
	}
	var skipped []string
	t := de.Type()
	for i := 0; i < de.NumField(); i++ {
		f := de.Field(i)
		if f.CanSet() {
			f.Set(se.Field(i))
			continue
		}
		// 非导出字段无法设置：仅当源端该字段值非零（即确有数据会丢失）时才计为漏拷。
		if !se.Field(i).IsZero() {
			skipped = append(skipped, t.Field(i).Name)
		}
	}
	if len(skipped) > 0 {
		return fmt.Errorf("conv: StructCopy 跳过了非导出且源值非零的字段 %v（这些字段未被拷贝）", skipped)
	}
	return nil
}

// MapMerge 合并 src 到 dst，同名 key 以 src 覆盖 dst（浅合并）。
// dst 为 nil 时新建并返回；返回合并后的 map 以便链式使用。
func MapMerge(dst, src map[string]any) map[string]any {
	if dst == nil {
		dst = make(map[string]any)
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
