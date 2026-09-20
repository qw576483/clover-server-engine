package object

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"clover-server-engine/pkg/foundation/logger"
)

// int64 可精确表示的浮点边界（float64 下界 = -2^63，上界开区间 = 2^63）。
// float64(math.MaxInt64) 其实等于 2^63（>MaxInt64），所以上界必须用严格小于。
const (
	minInt64AsFloat     = -9223372036854775808.0 // -2^63
	maxInt64AsFloatExcl = 9223372036854775808.0  //  2^63（不含）
)

// 线上/存盘补丁均为「序号(字符串) -> 紧凑值」的 JSON 对象。
// 因客户端/服务器共用同一份 schema，仅凭序号即可按定义的类型解码，无字段名冗余、体积小。
//
// 例：schema 定义 gold(index=2,int)、name(index=1,string)，
//
//	{"1":"Bob","2":12345}  ——双端都能据 schema 还原成 name/gold 字段。
//
// compactOf 把一个 Value 转成可直接 json.Marshal 的裸值（不含类型包装）。
func compactOf(v Value) any {
	switch v.Type() {
	case TypeInt:
		return v.Int()
	case TypeFloat:
		return v.Float()
	case TypeString:
		return v.String()
	case TypeBool:
		return v.Bool()
	case TypeBytes:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case TypeObject:
		return v.Object().String()
	default:
		return nil
	}
}

// decodeTyped 依据字段类型，把一段裸 JSON 解码为对应类型的 Value。
func decodeTyped(typ Type, raw json.RawMessage) (Value, error) {
	switch typ {
	case TypeInt:
		var n int64
		if err := json.Unmarshal(raw, &n); err == nil {
			return NewInt(n), nil
		}
		// 容忍浮点写法（如 12.0），但必须是「整数值且在 int64 范围内」：
		// 越界/非整浮点做 int64(f) 是实现相关行为（1e300 → MinInt64、12.9 → 12 静默截断），
		// 会静默写坏金币/数量类字段，必须报错而不是照单全收。
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return Value{}, err
		}
		if f != math.Trunc(f) || f < minInt64AsFloat || f >= maxInt64AsFloatExcl {
			return Value{}, fmt.Errorf("object: 整数类型字段收到非法数值 %v（越界或非整数）", f)
		}
		return NewInt(int64(f)), nil
	case TypeFloat:
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return Value{}, err
		}
		return NewFloat(f), nil
	case TypeString:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return Value{}, err
		}
		return NewString(s), nil
	case TypeBool:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return Value{}, err
		}
		return NewBool(b), nil
	case TypeBytes:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return Value{}, err
		}
		by, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return Value{}, err
		}
		return NewBytes(by), nil
	case TypeObject:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return Value{}, err
		}
		id, err := ParseObjectID(s)
		if err != nil {
			return Value{}, err
		}
		return NewObject(id), nil
	default:
		// 未知类型（schema 定义了本包不认识的新类型）：返回零值同时留日志，
		// 否则反序列化异常（字段被静默写成 TypeNil）完全无法排查。
		logger.Warnf("object: decodeTyped unknown field type %d, value dropped (raw=%s)", int(typ), truncateRaw(raw))
		return Value{}, nil
	}
}

// truncateRaw 截断超长原始 JSON，避免日志被大字段刷屏。
func truncateRaw(raw json.RawMessage) string {
	const maxLen = 64
	s := string(raw)
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

// encodeFields 把选定字段（从 bag 取值）编码为「序号 -> 紧凑值」JSON。
// 返回是否有任何字段被写入（用于增量补丁判空）。
func encodeFields(b *Bag, fields []Field) ([]byte, bool, error) {
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		if !b.Has(f.Name) {
			continue
		}
		out[strconv.Itoa(f.Index)] = compactOf(b.Get(f.Name))
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// MarshalIndexed 把 bag 中所有 schema 已定义的字段，编码为按序号索引的紧凑 JSON。
// 用于整对象快照的双端互通传输。
func MarshalIndexed(s *Schema, b *Bag) ([]byte, error) {
	data, _, err := encodeFields(b, s.fields)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return []byte("{}"), nil
	}
	return data, nil
}

// ApplyIndexed 用按序号索引的紧凑 JSON 回写 bag（序号→字段名→按定义类型写入）。
// 未在 schema 中定义的序号被忽略。
func ApplyIndexed(s *Schema, b *Bag, data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for k, rm := range raw {
		idx, err := strconv.Atoi(k)
		if err != nil {
			// 非数字序号：静默 continue 会让整段数据无声丢失，必须留痕。
			logger.Warnf("object: ApplyIndexed skip non-numeric index %q (value=%s)", k, truncateRaw(rm))
			continue
		}
		f, ok := s.ByIndex(idx)
		if !ok {
			// schema 未定义的序号：可能双端 schema 版本不一致，留痕便于定位。
			logger.Warnf("object: ApplyIndexed skip undefined index %d (value=%s)", idx, truncateRaw(rm))
			continue
		}
		v, err := decodeTyped(f.Type, rm)
		if err != nil {
			return err
		}
		b.Set(f.Name, v)
	}
	return nil
}

// SyncPatch 仅取 bag 中「已变动(dirty) 且 schema 标记 FlagSync」的字段，
// 编码为按序号索引的增量补丁——自动推送同步时只推真正需要下发的字段。
// public=true 时进一步剔除 FlagPrivate 字段（用于向他人广播）。
// 第二个返回值表示是否有内容（false 时无需推送）。
// ★ 调用方推送后应使用 MarkCleanNames(synced) 仅清除实际同步的字段，
//
//	而非调用 MarkClean() 清除全部 dirty，否则 FlagSync=false 的脏字段会丢变更。
func SyncPatch(s *Schema, b *Bag, public bool) ([]byte, []string, bool, error) {
	dirty := b.Dirty()
	if len(dirty) == 0 {
		return nil, nil, false, nil
	}
	dirtySet := make(map[string]bool, len(dirty))
	for _, n := range dirty {
		dirtySet[n] = true
	}
	var fields []Field
	for _, f := range s.fields {
		if !f.CanSync() || !dirtySet[f.Name] {
			continue
		}
		if public && f.IsPrivate() {
			continue
		}
		fields = append(fields, f)
	}
	if len(fields) == 0 {
		return nil, nil, false, nil
	}
	data, _, err := encodeFields(b, fields)
	if err != nil {
		return nil, nil, false, err
	}
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return data, names, true, nil
}

// SaveSnapshot 仅取 schema 标记 FlagSave 的字段，编码为按序号索引的持久化快照——
// 落库时只存需要持久化的字段（临时/派生字段不落库）。
func SaveSnapshot(s *Schema, b *Bag) ([]byte, error) {
	data, ok, err := encodeFields(b, s.SaveFields())
	if err != nil {
		return nil, err
	}
	if !ok {
		return []byte("{}"), nil
	}
	return data, nil
}
