// Package schema 标准化数据格式定义（PropSchema）。
//
// 属性以「名字↔序号↔类型↔标志」的声明式表来定义——线上按序号紧凑同步，name 供按名读改。
// Bag 本身是无 schema 的 name→value 动态袋，本包为其补上「一份统一的字段定义」这一环：
//
//   - 数据格式标准化：字段名、序号、类型集中声明一处，双端一致（客户端与服务器引用同一序号即互通）；
//   - 线上紧凑：同步/存盘按「序号」编码，无字段名冗余（MarshalIndexed / ApplyIndexed）；
//   - 按需同步/存盘：每字段带 Flag（Save 持久化 / Sync 推客户端 / Private 仅属主），
//     自动推送同步时只推标 Sync 的脏字段、落库时只存标 Save 的字段；
//   - name↔index 双向查询保留，仍可按字段名读改。
//
// 本包纯标准库 + 复用 internal/prop、internal/domain/object。
package object

import (
	"fmt"
	"sort"
)

// Flag 字段行为标志位。
type Flag uint8

const (
	// FlagSave 该字段需持久化到存储（落库快照只含标此位的字段）。
	FlagSave Flag = 1 << iota
	// FlagSync 该字段变动需同步推送到客户端（自动同步补丁只含标此位的脏字段）。
	FlagSync
	// FlagPrivate 私有字段：仅属主可见，向他人/公开广播时应剔除。
	FlagPrivate
)

// Has 判断是否包含某标志位。
func (f Flag) Has(x Flag) bool { return f&x != 0 }

// SaveSync 组合 Save 与 Sync：既存盘又同步（最常见的玩家可见持久字段）。
const SaveSync = FlagSave | FlagSync

// Field 一个字段的标准化定义。
type Field struct {
	Name  string // 字段名（按名读写）
	Index int    // 序号（线上紧凑同步/存盘的键，双端共用）
	Type  Type   // 值类型
	Flags Flag   // 行为标志
}

// CanSave 是否需持久化。
func (f Field) CanSave() bool { return f.Flags.Has(FlagSave) }

// CanSync 是否需同步到客户端。
func (f Field) CanSync() bool { return f.Flags.Has(FlagSync) }

// IsPrivate 是否私有。
func (f Field) IsPrivate() bool { return f.Flags.Has(FlagPrivate) }

// Schema 一张标准化数据格式定义表：不可变，构造后可被并发只读访问。
type Schema struct {
	name    string
	fields  []Field        // 按 Index 升序
	byName  map[string]int // name  -> fields 下标
	byIndex map[int]int    // index -> fields 下标
}

// Name 返回该 schema 名字。
func (s *Schema) Name() string { return s.name }

// Len 返回字段数。
func (s *Schema) Len() int { return len(s.fields) }

// Fields 返回按序号升序的字段副本。
func (s *Schema) Fields() []Field {
	out := make([]Field, len(s.fields))
	copy(out, s.fields)
	return out
}

// ByName 按字段名查询定义。
func (s *Schema) ByName(name string) (Field, bool) {
	if i, ok := s.byName[name]; ok {
		return s.fields[i], true
	}
	return Field{}, false
}

// ByIndex 按序号查询定义。
func (s *Schema) ByIndex(idx int) (Field, bool) {
	if i, ok := s.byIndex[idx]; ok {
		return s.fields[i], true
	}
	return Field{}, false
}

// filter 返回满足谓词的字段（保持序号升序）。
func (s *Schema) filter(pred func(Field) bool) []Field {
	var out []Field
	for _, f := range s.fields {
		if pred(f) {
			out = append(out, f)
		}
	}
	return out
}

// SaveFields 返回所有需持久化的字段。
func (s *Schema) SaveFields() []Field { return s.filter(Field.CanSave) }

// SyncFields 返回所有需同步到客户端的字段。
func (s *Schema) SyncFields() []Field { return s.filter(Field.CanSync) }

// PublicFields 返回可公开广播的字段（需同步且非私有）。
func (s *Schema) PublicFields() []Field {
	return s.filter(func(f Field) bool { return f.CanSync() && !f.IsPrivate() })
}

// Validate 校验 bag 中出现的、schema 已定义的字段类型是否与定义一致。
// bag 中 schema 未定义的字段被忽略（宽松模式，便于渐进式接入）。
func (s *Schema) Validate(b *Bag) error {
	for _, name := range b.Names() {
		f, ok := s.ByName(name)
		if !ok {
			continue
		}
		if got := b.Type(name); got != f.Type {
			return fmt.Errorf("schema %q: field %q type mismatch: want %v, got %v",
				s.name, name, f.Type, got)
		}
	}
	return nil
}

// Builder 声明式构造 Schema，重复的字段名/序号会在 Build 时报错。
type Builder struct {
	name   string
	fields []Field
	names  map[string]bool
	index  map[int]bool
	err    error
}

// NewBuilder 新建一个命名 schema 构造器。
func NewBuilder(name string) *Builder {
	return &Builder{
		name:  name,
		names: make(map[string]bool),
		index: make(map[int]bool),
	}
}

// Field 追加一个字段定义（链式）。
func (b *Builder) Field(name string, index int, typ Type, flags Flag) *Builder {
	if b.err != nil {
		return b
	}
	if name == "" {
		b.err = fmt.Errorf("schema %q: empty field name", b.name)
		return b
	}
	if index < 0 {
		b.err = fmt.Errorf("schema %q: field %q negative index %d", b.name, name, index)
		return b
	}
	if b.names[name] {
		b.err = fmt.Errorf("schema %q: duplicate field name %q", b.name, name)
		return b
	}
	if b.index[index] {
		b.err = fmt.Errorf("schema %q: duplicate field index %d (name %q)", b.name, index, name)
		return b
	}
	b.names[name] = true
	b.index[index] = true
	b.fields = append(b.fields, Field{Name: name, Index: index, Type: typ, Flags: flags})
	return b
}

// Int/Float/String/Bool/Bytes/Object 声明各类型字段。
func (b *Builder) Int(name string, index int, flags Flag) *Builder {
	return b.Field(name, index, TypeInt, flags)
}
func (b *Builder) Float(name string, index int, flags Flag) *Builder {
	return b.Field(name, index, TypeFloat, flags)
}
func (b *Builder) String(name string, index int, flags Flag) *Builder {
	return b.Field(name, index, TypeString, flags)
}
func (b *Builder) Bool(name string, index int, flags Flag) *Builder {
	return b.Field(name, index, TypeBool, flags)
}
func (b *Builder) Bytes(name string, index int, flags Flag) *Builder {
	return b.Field(name, index, TypeBytes, flags)
}
func (b *Builder) Object(name string, index int, flags Flag) *Builder {
	return b.Field(name, index, TypeObject, flags)
}

// Build 完成构造。字段按序号升序排列。
func (b *Builder) Build() (*Schema, error) {
	if b.err != nil {
		return nil, b.err
	}
	fields := make([]Field, len(b.fields))
	copy(fields, b.fields)
	sort.Slice(fields, func(i, j int) bool { return fields[i].Index < fields[j].Index })

	s := &Schema{
		name:    b.name,
		fields:  fields,
		byName:  make(map[string]int, len(fields)),
		byIndex: make(map[int]int, len(fields)),
	}
	for i, f := range fields {
		s.byName[f.Name] = i
		s.byIndex[f.Index] = i
	}
	return s, nil
}

// MustBuild 同 Build，出错则 panic（用于包级 var 初始化的静态 schema）。
func (b *Builder) MustBuild() *Schema {
	s, err := b.Build()
	if err != nil {
		panic(err)
	}
	return s
}
