// #nosec G115 -- ObjectValue 二进制编解码层：全部整数转换都在「已按本线格式完成长度/边界校验」之后，byte(v>>56)…byte(v) 的语义就是取低 8 位，不存在可被外部输入放大的溢出（报告 §四「已排除」）。逐行加注会淹没真正的边界校验，故文件级标注。

package object

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// Type 值类型标签（uint8）：标识属性列 / Bag 字段 / Record 的数据类型。
// 与 ObjectID.Type（uint16，标识对象类别）语义不同，不要混淆。
type Type = uint8

// 值类型标签常量——属性列 / Bag / RecordSchema 定义时使用。
const (
	TypeNil    Type = iota // 0 — 零值 / 未赋值
	TypeInt                // 1 — 整型
	TypeFloat              // 2 — 浮点型
	TypeString             // 3 — 字符串
	TypeBytes              // 4 — 二进制
	TypeBool               // 5 — 布尔
	TypeObject             // 6 — 对象号（ObjectID 的 String 表示）
)

// Value 值的类型安全容器（用于属性系统 Bag / 持久化 codec / 跨服同步）。
// 导出字段 I/F/S/B/Y/O 供外部直接读写；typ 为内部类型标签。
type Value struct {
	I   int64    // TypeInt 时存 int 值
	F   float64  // TypeFloat 时存 float 值
	S   string   // TypeString 时存 string；TypeObject 时存 ObjectID 的 String() 表示
	B   bool     // TypeBool 时存 bool
	Y   []byte   // TypeBytes 时存 bytes
	O   ObjectID // TypeObject 时存对象号
	typ Type     // 内部类型标签
}

// Type 返回值的类型标签。
func (v Value) Type() Type { return v.typ }

// NewInt 构造整数值。
func NewInt(n int64) Value { return Value{typ: TypeInt, I: n} }

// NewFloat 构造浮点值。
func NewFloat(f float64) Value { return Value{typ: TypeFloat, F: f} }

// NewString 构造字符串值。
func NewString(s string) Value { return Value{typ: TypeString, S: s} }

// NewBytes 构造字节值；外部切片的底层数组会被零拷贝引用，调用方后续修改可能影响 Value。
func NewBytes(b []byte) Value { return Value{typ: TypeBytes, Y: b} }

// NewBool 构造布尔值。
func NewBool(b bool) Value { return Value{typ: TypeBool, B: b} }

// NewObject 构造对象号值（ObjectID → Value，用于 Bag / 持久化）。
func NewObject(id ObjectID) Value { return Value{typ: TypeObject, O: id, S: id.String()} }

// NewZeroValue 按类型构造零值 Value。
func NewZeroValue(typ Type) Value { return Value{typ: typ} }

// NewValueFromRaw 从类型 + 原始 JSON 构造 Value（紧凑反序列化入口）。
// 解析失败时降级为该类型零值，并经结构化日志告警。
func NewValueFromRaw(typ Type, raw json.RawMessage) Value {
	v := Value{typ: typ}
	switch typ {
	case TypeInt:
		if err := json.Unmarshal(raw, &v.I); err != nil {
			logger.Warnf("object: unmarshal int value failed: %v", err)
		}
	case TypeFloat:
		if err := json.Unmarshal(raw, &v.F); err != nil {
			logger.Warnf("object: unmarshal float value failed: %v", err)
		}
	case TypeString:
		if err := json.Unmarshal(raw, &v.S); err != nil {
			logger.Warnf("object: unmarshal string value failed: %v", err)
		}
	case TypeBool:
		if err := json.Unmarshal(raw, &v.B); err != nil {
			logger.Warnf("object: unmarshal bool value failed: %v", err)
		}
	case TypeBytes:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			logger.Warnf("object: unmarshal bytes value failed: %v", err)
			break
		}
		dec, derr := base64.StdEncoding.DecodeString(s)
		if derr != nil {
			// 与同一 switch 的 int/float/string/bool 分支一致，都要 Warnf。
			logger.Warnf("object: decode base64 bytes value failed: %v", derr)
			break
		}
		v.Y = dec
	case TypeObject:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			logger.Warnf("object: unmarshal object value failed: %v", err)
			break
		}
		id, perr := ParseObjectID(s)
		if perr != nil {
			logger.Warnf("object: parse object id value failed: %v", perr)
			break
		}
		v.O = id
		v.S = s
	}
	return v
}

// IsNil 是否为 Nil 类型（零值 Value 亦为 Nil）。
func (v Value) IsNil() bool { return v.typ == TypeNil }

// Int 取整数值。
func (v Value) Int() int64 { return v.I }

// Float 取浮点值。
func (v Value) Float() float64 { return v.F }

// String 取字符串值。
func (v Value) String() string { return v.S }

// Bool 取布尔值。
func (v Value) Bool() bool { return v.B }

// Bytes 取字节值的**副本**。
//
// 返回内部切片本身（配合 NewBytes 的零拷贝）会让调用方拿到 Value 的底层数组：
// 调用方改 `Bytes()[i]` 就绕过了所有 setter 直接污染 Value，且没有任何痕迹。
// 这里与 GetCell/Changes 对 TypeBytes 的处理口径保持一致——读出去的就是副本。
func (v Value) Bytes() []byte {
	if v.Y == nil {
		return nil
	}
	cp := make([]byte, len(v.Y))
	copy(cp, v.Y)
	return cp
}

// Object 取对象号。
func (v Value) Object() ObjectID { return v.O }

// equal 值等值比较：逐类型比较，浮点做 ε 级容差（1e-9）。
func (v Value) equal(o Value) bool {
	if v.typ != o.typ {
		return false
	}
	switch v.typ {
	case TypeNil:
		return true
	case TypeInt:
		return v.I == o.I
	case TypeFloat:
		return math.Abs(v.F-o.F) < 1e-9
	case TypeString, TypeObject:
		return v.S == o.S
	case TypeBytes:
		return bytes.Equal(v.Y, o.Y)
	case TypeBool:
		return v.B == o.B
	default:
		return false
	}
}

// ValueEqual 值等值比较（导出版本）。
func ValueEqual(a, b Value) bool { return a.equal(b) }

// JSON 线化
// 标准线化格式：每个值编码为 {"t":<类型>, "v":<按类型的值>}。
//   - int   → JSON number
//   - float → JSON number
//   - string→ JSON string
//   - bool  → JSON bool
//   - bytes → JSON string（base64，JSON 原生编码 []byte）
//   - object→ JSON string（"type:seq"，人类可读且无损）

// 这套格式客户端 / 服务器 / GM 共用，保证「定义数据格式是标准化的」「代码互通」。

type valueWire struct {
	T Type            `json:"t"`
	V json.RawMessage `json:"v"`
}

// MarshalJSON 把单个值编码为标准线化格式。
func (v Value) MarshalJSON() ([]byte, error) {
	var payload any
	switch v.typ {
	case TypeInt:
		payload = v.I
	case TypeFloat:
		payload = v.F
	case TypeString:
		payload = v.S
	case TypeBool:
		payload = v.B
	case TypeBytes:
		payload = v.Y // json 自动 base64
	case TypeObject:
		payload = v.O.String()
	default:
		payload = nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(valueWire{T: v.typ, V: raw})
}

// MarshalValueOnly 只序列化值本体（不含 {"t","v"} 类型包装），
// 供 Record 紧凑格式使用——colTypes 已声明类型，无需每格重复。
func (v Value) MarshalValueOnly() ([]byte, error) {
	var payload any
	switch v.typ {
	case TypeInt:
		payload = v.I
	case TypeFloat:
		payload = v.F
	case TypeString:
		payload = v.S
	case TypeBool:
		payload = v.B
	case TypeBytes:
		payload = v.Y // json 自动 base64
	case TypeObject:
		payload = v.O.String()
	default:
		payload = nil
	}
	return json.Marshal(payload)
}

// UnmarshalJSON 从标准线化格式还原单个值。类型不匹配 / 解析失败时降级为对应零值。
func (v *Value) UnmarshalJSON(data []byte) error {
	var w valueWire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	// 解到全新的 out 再整体赋值：直接往 *v 上按类型写单个字段的话，
	// 复用同一个 Value 解码时会残留上一次的其它类型字段（如旧的 v.Y），
	// 导致 equal / MarshalBinary 结果不一致且白白持有旧内存。
	out := Value{typ: w.T}
	switch w.T {
	case TypeNil:
		// 与 MarshalJSON 对称：零值 Value 编码为 {"t":0,"v":null}。
	// 标量分支与 NewValueFromRaw 同口径：失败时降级为零值，但必须留日志。
	case TypeInt:
		if err := json.Unmarshal(w.V, &out.I); err != nil {
			logger.Warnf("object: value_json unmarshal int failed (type=%d): %v", w.T, err)
		}
	case TypeFloat:
		if err := json.Unmarshal(w.V, &out.F); err != nil {
			logger.Warnf("object: value_json unmarshal float failed (type=%d): %v", w.T, err)
		}
	case TypeString:
		if err := json.Unmarshal(w.V, &out.S); err != nil {
			logger.Warnf("object: value_json unmarshal string failed (type=%d): %v", w.T, err)
		}
	case TypeBool:
		if err := json.Unmarshal(w.V, &out.B); err != nil {
			logger.Warnf("object: value_json unmarshal bool failed (type=%d): %v", w.T, err)
		}
	case TypeBytes:
		var s string
		if err := json.Unmarshal(w.V, &s); err != nil {
			return fmt.Errorf("value_json: failed to unmarshal bytes: %w", err)
		}
		dec, derr := base64.StdEncoding.DecodeString(s)
		if derr != nil {
			return fmt.Errorf("value_json: failed to decode base64 bytes: %w", derr)
		}
		out.Y = dec
	case TypeObject:
		var s string
		if err := json.Unmarshal(w.V, &s); err != nil {
			return fmt.Errorf("value_json: failed to unmarshal object: %w", err)
		}
		id, perr := ParseObjectID(s)
		if perr != nil {
			return fmt.Errorf("value_json: failed to parse object id: %w", perr)
		}
		out.O = id
	default:
		return fmt.Errorf("value_json: unsupported type %d", w.T)
	}
	*v = out
	return nil
}

// 二进制线化
// Binary format for Value:

//	[1 byte type][type-specific bytes]

// Type-specific:

//	Nil:    (无负载，仅 1 字节类型标签)
//	Int:    [8 bytes int64]
//	Float:  [8 bytes float64]
//	String: [4 bytes len][len bytes]
//	Bool:   [1 byte]
//	Bytes:  [4 bytes len][len bytes]
//	Object: [8 bytes MarshalUint64]

var errValueBinaryTruncated = fmt.Errorf("object: binary value truncated")

// MarshalBinary 把 Value 编码为紧凑二进制（零反射）。
func (v Value) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, 32)
	buf = append(buf, byte(v.typ))
	switch v.typ {
	case TypeNil:
		// 与 JSON 路径对称：零值/未赋值 Value 过去在二进制路径直接报 unknown value type，
		// 而 JSON 路径「成功」，两条线化路径行为不一致。
	case TypeInt:
		// #nosec G115 -- int64 按补码编码为 uint64，与 UnmarshalBinary 对称。
		buf = appendUint64(buf, uint64(v.I))
	case TypeFloat:
		buf = appendUint64(buf, math.Float64bits(v.F))
	case TypeString:
		if len(v.S) > 0xFFFFFFFF {
			return nil, fmt.Errorf("object: string value too long")
		}
		buf = appendUint32(buf, uint32(len(v.S)))
		buf = append(buf, v.S...)
	case TypeBool:
		if v.B {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case TypeBytes:
		if len(v.Y) > 0xFFFFFFFF {
			return nil, fmt.Errorf("object: bytes value too long")
		}
		buf = appendUint32(buf, uint32(len(v.Y)))
		buf = append(buf, v.Y...)
	case TypeObject:
		buf = appendUint64(buf, v.O.MarshalUint64())
	default:
		return nil, fmt.Errorf("object: unknown value type")
	}
	return buf, nil
}

// UnmarshalBinary 从紧凑二进制还原 Value。
func (v *Value) UnmarshalBinary(data []byte) error {
	_, err := v.unmarshalBinaryN(data)
	return err
}

// UnmarshalBinaryN 从紧凑二进制还原 Value，并返回本次实际消耗的字节数。
func (v *Value) UnmarshalBinaryN(data []byte) (int, error) {
	return v.unmarshalBinaryN(data)
}

// unmarshalBinaryN 从紧凑二进制还原 Value，并返回本次实际消耗的字节数。
func (v *Value) unmarshalBinaryN(data []byte) (int, error) {
	if len(data) < 1 {
		return 0, errValueBinaryTruncated
	}
	typ := Type(data[0])
	off := 1
	out := Value{typ: typ}
	switch typ {
	case TypeNil:
		// 无负载，仅消费类型字节（与 MarshalBinary 的 TypeNil 分支对称）。
	case TypeInt:
		if len(data) < off+8 {
			return 0, errValueBinaryTruncated
		}
		out.I = int64(readUint64(data[off : off+8]))
		off += 8
	case TypeFloat:
		if len(data) < off+8 {
			return 0, errValueBinaryTruncated
		}
		out.F = math.Float64frombits(readUint64(data[off : off+8]))
		off += 8
	case TypeString:
		if len(data) < off+4 {
			return 0, errValueBinaryTruncated
		}
		l := uint64(readUint32(data[off : off+4]))
		off += 4
		if uint64(len(data)-off) < l {
			return 0, errValueBinaryTruncated
		}
		out.S = string(data[off : off+int(l)])
		off += int(l)
	case TypeBool:
		if len(data) < off+1 {
			return 0, errValueBinaryTruncated
		}
		out.B = data[off] != 0
		off++
	case TypeBytes:
		if len(data) < off+4 {
			return 0, errValueBinaryTruncated
		}
		l := uint64(readUint32(data[off : off+4]))
		off += 4
		if uint64(len(data)-off) < l {
			return 0, errValueBinaryTruncated
		}
		out.Y = make([]byte, int(l))
		copy(out.Y, data[off:off+int(l)])
		off += int(l)
	case TypeObject:
		if len(data) < off+8 {
			return 0, errValueBinaryTruncated
		}
		out.O = FromUint64(readUint64(data[off : off+8]))
		off += 8
	default:
		return 0, fmt.Errorf("object: unknown value type")
	}
	*v = out
	return off, nil
}

// big-endian helpers (avoid importing encoding/binary in this file)
func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func appendUint64(buf []byte, v uint64) []byte {
	return append(buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func readUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
func readUint64(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}
