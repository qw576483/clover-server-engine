package config

import (
	stdjson "encoding/json"
	"reflect"
	"regexp"
	"strings"
	"sync"

	ujson "clover-server-engine/pkg/shared/json"
	"clover-server-engine/pkg/shared/util"
)

// 配置敏感信息脱敏

// 场景：配置结构体常含 DB 密码 / etcd 账号 / 第三方 token，
// 一旦被日志、/debug/config 端点、错误信息原样打印即构成凭据泄漏。
// 本文件提供 Mask / MaskString，在「打印前」把敏感字段替换为 MaskPlaceholder。

// 判定敏感的两条路径（任一命中即脱敏）：
//  1. 显式 tag：`mask:"true"` 或 `sensitive:"true"`；
//  2. 字段名启发式：字段名（小写、去下划线前）包含默认敏感词表中任一词。

// 另对 DSN / URL 形态的字符串做「保留结构、只抹密码」处理，
// 便于排障时仍能看出连的是哪个库，却看不到口令。

// MaskPlaceholder 脱敏后的占位符。
const MaskPlaceholder = "***"

// defaultSensitiveKeys 默认敏感字段名词表（小写、子串匹配）。
//
// 必须含裸 "pass"：引擎内 NatsConfig.Pass / redis.Config.Pass / mysql 的 Pass
// 归一化后正是 "pass"，而 SentinelPass → "sentinelpass" 也含该子串。
// 缺它等于把 Redis / MySQL / NATS 口令明文写进启动日志（数据泄露）。
var defaultSensitiveKeys = []string{
	"password", "passwd", "pass", "pwd",
	"token", "secret",
	"apikey", "api_key",
	"accesskey", "access_key",
	"privatekey", "private_key",
	"credential", "auth", "dsn",
}

var (
	// sensitiveMu 保护 sensitiveKeys 的并发读写（业务可能在启动期动态扩充词表）。
	sensitiveMu   sync.RWMutex
	sensitiveKeys = append([]string(nil), defaultSensitiveKeys...)
)

// AddSensitiveKeys 追加自定义敏感词（大小写不敏感，子串匹配），不影响默认词表。
func AddSensitiveKeys(keys ...string) {
	sensitiveMu.Lock()
	defer sensitiveMu.Unlock()
	for _, k := range keys {
		k = strings.ToLower(strings.TrimSpace(k))
		if k == "" {
			continue
		}
		if !util.Contains(sensitiveKeys, k) {
			sensitiveKeys = append(sensitiveKeys, k)
		}
	}
}

// SetSensitiveKeys 用给定词表整体替换敏感词表（传空则退回默认词表）。
func SetSensitiveKeys(keys ...string) {
	sensitiveMu.Lock()
	defer sensitiveMu.Unlock()
	if len(keys) == 0 {
		sensitiveKeys = append([]string(nil), defaultSensitiveKeys...)
		return
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		k = strings.ToLower(strings.TrimSpace(k))
		if k != "" && !util.Contains(out, k) {
			out = append(out, k)
		}
	}
	sensitiveKeys = out
}

// SensitiveKeys 返回当前敏感词表副本（只读用途，修改副本不影响内部状态）。
func SensitiveKeys() []string {
	sensitiveMu.RLock()
	defer sensitiveMu.RUnlock()
	return append([]string(nil), sensitiveKeys...)
}

// ResetSensitiveKeys 恢复默认敏感词表。
func ResetSensitiveKeys() { SetSensitiveKeys() }

// IsSensitiveName 按启发式判断字段名/映射键是否敏感。
// 归一化规则：转小写并去掉 '_' '-' ' '，使 access_key / Access-Key / AccessKey 等价。
func IsSensitiveName(name string) bool {
	n := normalizeName(name)
	if n == "" {
		return false
	}
	sensitiveMu.RLock()
	defer sensitiveMu.RUnlock()
	for _, k := range sensitiveKeys {
		if strings.Contains(n, normalizeName(k)) {
			return true
		}
	}
	return false
}

// normalizeName 归一化名称：小写 + 去分隔符。
func normalizeName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		if r == '_' || r == '-' || r == ' ' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// isSensitiveTag 判断结构体字段是否被显式标记为敏感。
// 支持 `mask:"true"` 与 `sensitive:"true"`（值不区分大小写，"1"/"yes" 亦可）。
func isSensitiveTag(f reflect.StructField) bool {
	for _, key := range [...]string{"mask", "sensitive"} {
		v := strings.ToLower(strings.TrimSpace(f.Tag.Get(key)))
		switch v {
		case "true", "1", "yes", "on":
			return true
		}
	}
	return false
}

// DSN / URL 脱敏
// dsnRe 匹配 scheme://user:pass@ 形态（mysql / redis / amqp / mongodb / postgres 等通用 URL）。
// 捕获组 1 = "scheme://user:"，捕获组 2 = 密码，后接 '@'。
// 用户名部分用 `*`（可空）：`redis://:pwd@host` 这类「空用户名 + 密码」形态
// 同样必须脱敏，要求非空会直接漏掉它（密码明文入日志）。
var dsnRe = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.\-]*://[^:/?#@\s]*:)([^@/\s]*)@`)

// goDSNRe 匹配 Go 风格 mysql DSN：user:pass@tcp(host:port)/db（无 scheme:// 前缀）。
// 捕获组 1 = "user:"，捕获组 2 = 密码，后接 "@" + 协议名 + "("。
var goDSNRe = regexp.MustCompile(`^([^:/?#@\s]+:)([^@\s]*)@([a-zA-Z0-9]+\()`)

// MaskDSN 对 DSN / URL 字符串做「保留结构、只抹密码」处理：

//	mysql://root:s3cret@127.0.0.1:3306/db  →  mysql://root:***@127.0.0.1:3306/db
//	root:s3cret@tcp(127.0.0.1:3306)/db     →  root:***@tcp(127.0.0.1:3306)/db

// 不含凭据的字符串原样返回。
func MaskDSN(s string) string {
	if s == "" {
		return s
	}
	if goDSNRe.MatchString(s) {
		return goDSNRe.ReplaceAllString(s, "${1}"+MaskPlaceholder+"@${3}")
	}
	if dsnRe.MatchString(s) {
		return dsnRe.ReplaceAllString(s, "${1}"+MaskPlaceholder+"@")
	}
	return s
}

// looksLikeDSN 粗判字符串是否为带凭据的 DSN/URL。
func looksLikeDSN(s string) bool {
	return strings.Contains(s, "@") && (dsnRe.MatchString(s) || goDSNRe.MatchString(s))
}

// Mask 深拷贝 v 并将其中的敏感字段替换为占位符，返回脱敏后的副本。

// 行为说明：
//   - 原值不被修改（深拷贝语义），可安全用于日志打印；
//   - 递归处理嵌套 struct / map / slice / array / 指针 / interface；
//   - string 敏感字段置 MaskPlaceholder；数值/布尔等非字符串敏感字段置零值
//     （既不泄漏原值，又保持类型不变，仍可被 json.Marshal）；
//   - 循环引用通过 visited 集合（指针地址+类型）检测，二次遇到时返回占位符，
//     避免无限递归导致栈溢出；
//   - 未导出字段无法读取/设置，一律跳过（保持零值）；
//   - 非 DSN 的普通字符串原样保留，DSN/URL 形态自动抹掉密码段。
func Mask(v any) any {
	if v == nil {
		return nil
	}
	visited := make(map[visitKey]bool)
	out := maskValue(reflect.ValueOf(v), false, visited)
	if !out.IsValid() {
		return nil
	}
	return out.Interface()
}

// MaskString 返回脱敏后的 JSON 字符串，供日志/HTTP 输出直接使用。
// 序列化失败时退回占位符，绝不返回原始（含敏感信息的）内容。
func MaskString(v any) string {
	b, err := ujson.Marshal(Mask(v))
	if err != nil {
		return MaskPlaceholder
	}
	return string(b)
}

// MaskStringIndent 同 MaskString，但输出带缩进的可读 JSON。
func MaskStringIndent(v any) string {
	b, err := stdjson.MarshalIndent(Mask(v), "", "  ")
	if err != nil {
		return MaskPlaceholder
	}
	return string(b)
}

// visitKey 循环引用检测键：指针地址 + 类型（不同类型可能共享同一地址，如结构体首字段）。
type visitKey struct {
	ptr uintptr
	typ reflect.Type
}

// maskValue 递归脱敏核心。
// sensitive 表示「本值整体被判定为敏感」（由上层字段名/tag 传下来）。
func maskValue(v reflect.Value, sensitive bool, visited map[visitKey]bool) reflect.Value {
	if !v.IsValid() {
		return v
	}

	switch v.Kind() {
	case reflect.String:
		if sensitive {
			return reflect.ValueOf(MaskPlaceholder).Convert(v.Type())
		}
		// 非敏感字符串仍可能是 DSN（如字段名叫 Addr 但值是完整连接串），做一次结构化脱敏。
		if s := v.String(); looksLikeDSN(s) {
			return reflect.ValueOf(MaskDSN(s)).Convert(v.Type())
		}
		return v

	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		if sensitive {
			// 非字符串敏感值置零，保持类型不变。
			return reflect.Zero(v.Type())
		}
		return v

	case reflect.Ptr:
		if v.IsNil() {
			return v
		}
		key := visitKey{ptr: v.Pointer(), typ: v.Type()}
		if visited[key] {
			// 循环引用：返回 nil 指针切断递归（JSON 序列化为 null）。
			return reflect.Zero(v.Type())
		}
		visited[key] = true
		defer delete(visited, key) // 出栈后放行，保证 DAG（非环）的重复引用仍能正常展开
		elem := maskValue(v.Elem(), sensitive, visited)
		if !elem.IsValid() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(elem)
		return out

	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		inner := maskValue(v.Elem(), sensitive, visited)
		if !inner.IsValid() {
			return reflect.Zero(v.Type())
		}
		// 内层类型可能与接口静态类型不同，需可赋值才回填，否则退回占位符。
		if inner.Type().AssignableTo(v.Type()) {
			out := reflect.New(v.Type()).Elem()
			out.Set(inner)
			return out
		}
		return reflect.ValueOf(MaskPlaceholder)

	case reflect.Struct:
		return maskStruct(v, sensitive, visited)

	case reflect.Map:
		return maskMap(v, sensitive, visited)

	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		// 切片共享底层数组，需按元素地址做环检测。
		key := visitKey{ptr: v.Pointer(), typ: v.Type()}
		if visited[key] {
			return reflect.Zero(v.Type())
		}
		visited[key] = true
		defer delete(visited, key)
		return maskList(v, sensitive, visited, reflect.MakeSlice(v.Type(), v.Len(), v.Len()))

	case reflect.Array:
		return maskList(v, sensitive, visited, reflect.New(v.Type()).Elem())

	default:
		// Chan / Func / UnsafePointer 等无法有意义拷贝，返回零值。
		return reflect.Zero(v.Type())
	}
}

// maskStruct 逐字段脱敏结构体。
func maskStruct(v reflect.Value, sensitive bool, visited map[visitKey]bool) reflect.Value {
	t := v.Type()
	out := reflect.New(t).Elem()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		// 未导出字段既读不出也写不进（reflect 限制），跳过留零值。
		if f.PkgPath != "" {
			continue
		}
		// 字段敏感性：继承上层 + tag 显式标记 + 字段名启发式 + json tag 名启发式。
		fs := sensitive || isSensitiveTag(f) || IsSensitiveName(f.Name) || IsSensitiveName(jsonTagName(f))
		mv := maskValue(v.Field(i), fs, visited)
		if mv.IsValid() && mv.Type().AssignableTo(f.Type) {
			out.Field(i).Set(mv)
		}
	}
	return out
}

// jsonTagName 取字段 json tag 的名字部分（如 `json:"api_key,omitempty"` → "api_key"）。
// 便于字段名本身不敏感、但序列化键名敏感的场景（如 Field string `json:"password"`）。
func jsonTagName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" || tag == "-" {
		return ""
	}
	if i := strings.IndexByte(tag, ','); i >= 0 {
		tag = tag[:i]
	}
	return tag
}

// maskMap 逐键脱敏映射：键名命中敏感词时整个值被脱敏。
func maskMap(v reflect.Value, sensitive bool, visited map[visitKey]bool) reflect.Value {
	if v.IsNil() {
		return v
	}
	key := visitKey{ptr: v.Pointer(), typ: v.Type()}
	if visited[key] {
		return reflect.Zero(v.Type())
	}
	visited[key] = true
	defer delete(visited, key)

	out := reflect.MakeMapWithSize(v.Type(), v.Len())
	iter := v.MapRange()
	for iter.Next() {
		k := iter.Key()
		// 仅字符串（含自定义 string 类型）键参与启发式判定。
		ks := sensitive
		if !ks && k.Kind() == reflect.String {
			ks = IsSensitiveName(k.String())
		}
		mv := maskValue(iter.Value(), ks, visited)
		if !mv.IsValid() {
			continue
		}
		if !mv.Type().AssignableTo(v.Type().Elem()) {
			// 类型不匹配（如 interface 内层被替换成 string），尽量转换，不行就跳过。
			if !mv.Type().ConvertibleTo(v.Type().Elem()) {
				continue
			}
			mv = mv.Convert(v.Type().Elem())
		}
		out.SetMapIndex(k, mv)
	}
	return out
}

// 说明：原本这里还有 Loader.SafeDump / SafeSettings / SafeString / SafeEtcdInfo 一组
// 「吐出整份脱敏配置」的出口。它们全仓零调用点（启动日志走 app/config.go 自己的脱敏打印
// 路径，/debug/config 端点从未实现），已按「死代码删净」删除；`Loader.v.AllSettings()` +
// 本文件的 `Mask` / `MaskString*` 仍可随时重建该能力，不需要预留空壳。

// maskList 逐元素脱敏切片/数组，out 由调用方按类型预建。
func maskList(v reflect.Value, sensitive bool, visited map[visitKey]bool, out reflect.Value) reflect.Value {
	for i := 0; i < v.Len(); i++ {
		mv := maskValue(v.Index(i), sensitive, visited)
		if mv.IsValid() && mv.Type().AssignableTo(out.Type().Elem()) {
			out.Index(i).Set(mv)
		}
	}
	return out
}
