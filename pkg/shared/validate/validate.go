// Package validate 提供基于 struct tag 的数据校验框架。
//
// 通过反射读取 struct 字段上 validate tag 实现声明式校验。
// 内置校验器：required, min, max, len, email, regex。
//
// 使用示例：
//
//	type Player struct {
//	    Name  string `validate:"required,min=1,max=32"`
//	    Level int    `validate:"required,min=1,max=100"`
//	    Email string `validate:"email"`
//	}
//	err := validate.Struct(Player{Name: "", Level: -1}) // 返回聚合错误
package validate

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// ValidationError 表示单个字段的校验失败。
type ValidationError struct {
	Field   string
	Tag     string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validate: field %q %s", e.Field, e.Message)
}

// Errors 聚合多个校验错误。
type Errors []error

func (e Errors) Error() string {
	if len(e) == 1 {
		return e[0].Error()
	}
	msgs := make([]string, len(e))
	for i, err := range e {
		msgs[i] = err.Error()
	}
	return fmt.Sprintf("validate: %d errors: %s", len(e), strings.Join(msgs, "; "))
}

// Struct 对传入的 struct 实例执行 validate tag 校验。
// 支持嵌套 struct 和指针字段。
func Struct(v interface{}) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("validate: expected struct, got %s", val.Kind())
	}

	var errs Errors
	validateStruct(val, &errs, 0)
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// maxStructDepth 嵌套 struct 递归的最大深度：自引用类型（如 type Node struct{ Next *Node }）
// 会在指针链上无限下钻，必须设上限。
const maxStructDepth = 16

func validateStruct(val reflect.Value, errs *Errors, depth int) {
	if depth > maxStructDepth {
		return
	}
	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := val.Field(i)

		// 处理嵌入 struct
		if field.Anonymous && fieldVal.Kind() == reflect.Struct {
			validateStruct(fieldVal, errs, depth+1)
			continue
		}

		tag := field.Tag.Get("validate")
		if tag == "" {
			// 具名嵌套 struct / 结构体指针：自身没有 validate tag 时也要下钻。
			// 旧实现只递归匿名字段，`Player PlayerInfo`（内部字段带 tag、自身不带）
			// 的内部规则会被完全跳过，与包注释「支持嵌套 struct 和指针字段」矛盾。
			fv := fieldVal
			if fv.Kind() == reflect.Ptr {
				if fv.IsNil() {
					continue
				}
				fv = fv.Elem()
			}
			if fv.Kind() == reflect.Struct {
				validateStruct(fv, errs, depth+1)
			}
			continue
		}

		rules := splitRules(tag)
		for _, rule := range rules {
			rule = strings.TrimSpace(rule)
			if rule == "" {
				continue
			}
			if err := applyRule(field.Name, rule, fieldVal); err != nil {
				*errs = append(*errs, err)
			}
		}
	}
}

func applyRule(fieldName string, rule string, val reflect.Value) error {
	// 处理指针
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			if strings.HasPrefix(rule, "required") {
				return &ValidationError{Field: fieldName, Tag: "required", Message: "is required but nil"}
			}
			return nil
		}
		val = val.Elem()
	}

	switch {
	case rule == "required":
		if isZero(val) {
			return &ValidationError{Field: fieldName, Tag: "required", Message: "is required"}
		}
	case strings.HasPrefix(rule, "min="):
		if !isNumeric(val) {
			return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("rule %q only applies to numeric fields, got %s", rule, val.Kind())}
		}
		minStr := rule[4:]
		// 优先用整数精确路径（避免 float64 对 >2^53 的 int64/uint64 精度丢失：
		// 两侧都舍入成同一个 float 时 `v < min` 为 false，越界值被判通过）。
		if raw, ok := rawInt64Val(val); ok {
			threshold, err := strconv.ParseInt(minStr, 10, 64)
			if err != nil {
				// 解析失败时回退到 float64 路径（min=0.5 等非整数阈值）。
				min, err2 := strconv.ParseFloat(minStr, 64)
				if err2 != nil {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid min value: %s", minStr)}
				}
				if float64(raw) < min {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be >= %v, got %v", min, raw)}
				}
			} else {
				if raw < threshold {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be >= %d, got %d", threshold, raw)}
				}
			}
		} else if raw, ok := rawUint64Val(val); ok {
			threshold, err := strconv.ParseUint(minStr, 10, 64)
			if err != nil {
				// 解析失败时回退到 float64 路径（min=0.5 等非整数阈值）。
				min, err2 := strconv.ParseFloat(minStr, 64)
				if err2 != nil {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid min value: %s", minStr)}
				}
				if float64(raw) < min {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be >= %v, got %v", min, raw)}
				}
			} else {
				if raw < threshold {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be >= %d, got %d", threshold, raw)}
				}
			}
		} else {
			min, err := strconv.ParseFloat(minStr, 64)
			if err != nil {
				return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid min value: %s", minStr)}
			}
			v := numericVal(val)
			if v < min {
				return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be >= %v, got %v", min, v)}
			}
		}
	case strings.HasPrefix(rule, "max="):
		if !isNumeric(val) {
			return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("rule %q only applies to numeric fields, got %s", rule, val.Kind())}
		}
		maxStr := rule[4:]
		// 同 min 逻辑：优先走整数精确路径（见上方注释）。
		if raw, ok := rawInt64Val(val); ok {
			threshold, err := strconv.ParseInt(maxStr, 10, 64)
			if err != nil {
				max, err2 := strconv.ParseFloat(maxStr, 64)
				if err2 != nil {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid max value: %s", maxStr)}
				}
				if float64(raw) > max {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be <= %v, got %v", max, raw)}
				}
			} else {
				if raw > threshold {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be <= %d, got %d", threshold, raw)}
				}
			}
		} else if raw, ok := rawUint64Val(val); ok {
			threshold, err := strconv.ParseUint(maxStr, 10, 64)
			if err != nil {
				max, err2 := strconv.ParseFloat(maxStr, 64)
				if err2 != nil {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid max value: %s", maxStr)}
				}
				if float64(raw) > max {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be <= %v, got %v", max, raw)}
				}
			} else {
				if raw > threshold {
					return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be <= %d, got %d", threshold, raw)}
				}
			}
		} else {
			max, err := strconv.ParseFloat(maxStr, 64)
			if err != nil {
				return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid max value: %s", maxStr)}
			}
			v := numericVal(val)
			if v > max {
				return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("must be <= %v, got %v", max, v)}
			}
		}
	case strings.HasPrefix(rule, "len="):
		lenStr := rule[4:]
		target, err := strconv.Atoi(lenStr)
		if err != nil {
			return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid len value: %s", lenStr)}
		}
		n := lenVal(val)
		if n != target {
			return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("length must be %d, got %d", target, n)}
		}
	case rule == "email":
		s := stringVal(val)
		if s == "" {
			return nil // email 允许空值，required 应单独声明
		}
		if !emailRegex.MatchString(s) {
			return &ValidationError{Field: fieldName, Tag: "email", Message: "invalid email format"}
		}
	case strings.HasPrefix(rule, "regex="):
		pattern := rule[6:]
		re, err := getOrCompileRegex(pattern)
		if err != nil {
			return &ValidationError{Field: fieldName, Tag: rule, Message: fmt.Sprintf("invalid regex: %s", pattern)}
		}
		s := stringVal(val)
		if !re.MatchString(s) {
			return &ValidationError{Field: fieldName, Tag: "regex", Message: fmt.Sprintf("does not match pattern %q", pattern)}
		}
	}
	return nil
}

var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)

// maxCachedRegex 已编译正则的缓存上限。
//
// 规则来自 struct tag 时集合天然有界；但本包也允许运行时传入 pattern（Var/自定义规则），
// 一旦 pattern 由外部输入拼出，无上限的缓存就是一条无界增长路径。满了整体清空：
// 简单、严格有界，代价只是重新编译一次。
const maxCachedRegex = 1024

var (
	regexCacheMu sync.Mutex
	regexCache   = make(map[string]*regexp.Regexp, 16)
)

func getOrCompileRegex(pattern string) (*regexp.Regexp, error) {
	regexCacheMu.Lock()
	if re, ok := regexCache[pattern]; ok {
		regexCacheMu.Unlock()
		return re, nil
	}
	regexCacheMu.Unlock()

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	regexCacheMu.Lock()
	if len(regexCache) >= maxCachedRegex {
		for k := range regexCache {
			delete(regexCache, k)
		}
	}
	regexCache[pattern] = re
	regexCacheMu.Unlock()
	return re, nil
}

// splitRules 按顶层逗号切分 validate tag。
//
// 不能直接用 strings.Split：`regex=` 的值里可以含逗号（`{1,3}` 量词），
// 无脑切会把 `regex=^[a-z]{1,3}$` 切成两段，前段编译失败 → 合法输入被判「invalid regex」。
// 这里把 `{...}` 内的逗号视作正则语法的一部分。
func splitRules(tag string) []string {
	var out []string
	depth := 0
	start := 0
	for i := 0; i < len(tag); i++ {
		switch tag[i] {
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, tag[start:i])
				start = i + 1
			}
		}
	}
	return append(out, tag[start:])
}

// isZero 判断 reflect.Value 是否为零值。
func isZero(v reflect.Value) bool {
	if !v.IsValid() {
		// 零值 reflect.Value（如 Var(nil, ...) 的入参）应当被视为零：
		// 旧实现走 default 返回 false，使 `required` 对 nil 误判为通过。
		return true
	}
	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Slice, reflect.Map, reflect.Array:
		return v.Len() == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// isNumeric 判断 reflect.Value 是否为数值类型（int/uint/float）。
func isNumeric(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// numericVal 将 reflect.Value 转为 float64。
func numericVal(v reflect.Value) float64 {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// 注意：uint64 超过 2^53 时 float64 会丢失精度，min/max 比较时应优先用 rawUint64Val。
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	default:
		return 0
	}
}

// rawUint64Val 当 reflect.Value 为无符号整型时返回精确的 uint64 值，
// 供 min/max 校验避免 float64 精度丢失（>2^53 的 uint64）。
func rawUint64Val(v reflect.Value) (uint64, bool) {
	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint(), true
	default:
		return 0, false
	}
}

// rawInt64Val 当 reflect.Value 为有符号整型时返回精确的 int64 值。
// 与 rawUint64Val 同理：避免 >2^53 的整值经 float64 比较时精度丢失导致误判通过。
func rawInt64Val(v reflect.Value) (int64, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	default:
		return 0, false
	}
}

// stringVal 将 reflect.Value 转为 string。
func stringVal(v reflect.Value) string {
	if !v.IsValid() {
		return ""
	}
	switch v.Kind() {
	case reflect.String:
		return v.String()
	default:
		// 非导出字段（或未取地址的接口值）调 Interface() 会 panic，
		// 这里退化成类型占位串，绝不让一次校验配置错误击穿调用方。
		if !v.CanInterface() {
			return fmt.Sprintf("<%s>", v.Kind())
		}
		return fmt.Sprintf("%v", v.Interface())
	}
}

// lenVal 返回 slice/string/map 的长度，否则返回 -1。
func lenVal(v reflect.Value) int {
	switch v.Kind() {
	case reflect.String, reflect.Slice, reflect.Array, reflect.Map:
		return v.Len()
	default:
		return -1
	}
}

// Var 对单个变量执行校验规则（用于非 struct 场景）。
func Var(v interface{}, rule string) error {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		// reflect.ValueOf(nil) 得到零值 Value：直接交给 applyRule 会在 Kind()==Invalid
		// 的路径上误判（isZero 走 default 返回 false）。nil 只对 required 有明确答案。
		if strings.HasPrefix(rule, "required") {
			return &ValidationError{Field: "value", Tag: "required", Message: "is required"}
		}
		return nil
	}
	return applyRule("value", rule, val)
}
