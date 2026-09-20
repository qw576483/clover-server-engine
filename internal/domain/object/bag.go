package object

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"sync"
)

// Bag 强类型、按名字索引的属性袋（MMO 的 props 精华）。

// 业务对象（玩家档案 / 背包 / 军团 / 服务器配置……）内嵌或持有一个 Bag，即可：
//   - 按字段名读写任意类型（SetInt/GetInt/SetString/...，类型不符返回零值，不 panic）；
//   - 标准化 JSON 线化（客户端 / 服务器共用同一套编码）；
//   - 内建变更追踪（MarkClean / Dirty / Changes）——落库或广播时只取变动字段，增量同步零成本。

// Bag 并发安全，所有读写走内部 RWMutex。
type Bag struct {
	mu       sync.RWMutex
	fields   map[string]Value
	dirty    map[string]struct{}
	onChange func(field string, v Value) // 可选：字段变动即时回调（自动同步底座）
}

// NewBag 构造空属性袋。
func NewBag() *Bag {
	return &Bag{
		fields: make(map[string]Value),
		dirty:  make(map[string]struct{}),
	}
}

// Set 写入一个带类型的值；仅当字段不存在或值真正变化时才标记脏（无变化不触发同步）。
// 若已注册 onChanged 回调（SetNotifier），值真正变化时在释放内部锁后回调，参数是 (字段名, 新值)，
// 业务可据此立即推送增量（呼应 MMO 的 Modified() 单一分发钩子，写即自动同步，业务零侵入）。
func (b *Bag) Set(name string, v Value) {
	b.mu.Lock()
	if old, ok := b.fields[name]; !ok || !ValueEqual(old, v) {
		b.fields[name] = v
		b.dirty[name] = struct{}{}
		fn := b.onChange
		b.mu.Unlock()
		if fn != nil {
			fn(name, v)
		}
		return
	}
	b.mu.Unlock()
}

// SetNotifier 注册「字段变动即时回调」（MMO 写即自动同步的底座）。
// 任意字段真实变动（Set 且值确实改变）时会回调，参数为 (字段名, 新值)；
// 业务可据此立即推送增量，无需显式 Save。传 nil 注销回调。
// 回调在释放内部锁后调用，可安全回读 Bag（不会死锁）。
func (b *Bag) SetNotifier(fn func(field string, v Value)) {
	b.mu.Lock()
	b.onChange = fn
	b.mu.Unlock()
}

// Get 取字段值（不存在返回零值 Value）。
// TypeBytes 字段返回深拷贝，防止调用方通过 Get(name).Y 改写内部切片绕过 Set 脏标记。
func (b *Bag) Get(name string) Value {
	b.mu.RLock()
	defer b.mu.RUnlock()
	v := b.fields[name]
	if v.Type() == TypeBytes && len(v.Y) > 0 {
		cp := make([]byte, len(v.Y))
		copy(cp, v.Y)
		v.Y = cp
	}
	return v
}

// GetRef 取字段值的只读引用（零拷贝，TypeBytes 不做深拷贝）。
// 调用方切勿修改返回的 Value 中的 []byte 字段，否则会绕过 Set 脏标记导致数据不一致。
func (b *Bag) GetRef(name string) Value {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.fields[name]
}

// Has 字段是否存在。
func (b *Bag) Has(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.fields[name]
	return ok
}

// Type 取字段类型（不存在返回 TypeInt 零值；用 Has 区分）。
func (b *Bag) Type(name string) Type {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.fields[name].Type()
}

// Delete 删除字段并标记脏（同步层据此可推一次删除事件）。
// 若已注册 onChanged 回调，删除发生后在释放内部锁后回调，参数为 (字段名, 零值)，
// 业务可据此推送一次删除（与 MMO 字段删除事件同源语义）。
func (b *Bag) Delete(name string) {
	b.mu.Lock()
	if _, ok := b.fields[name]; ok {
		delete(b.fields, name)
		b.dirty[name] = struct{}{}
		fn := b.onChange
		b.mu.Unlock()
		if fn != nil {
			fn(name, Value{})
		}
		return
	}
	b.mu.Unlock()
}

// Names 返回全部字段名（升序，便于稳定输出 / 测试）。
func (b *Bag) Names() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ns := make([]string, 0, len(b.fields))
	for n := range b.fields {
		ns = append(ns, n)
	}
	sort.Strings(ns)
	return ns
}

// Range 遍历全部字段（RLock 下快照键列表，释放锁后遍历回调，避免回调中 Set/Delete 死锁）。
// 回调返回 false 可提前终止遍历。
func (b *Bag) Range(fn func(name string, v Value) bool) {
	b.mu.RLock()
	snapshot := make(map[string]Value, len(b.fields))
	for k, v := range b.fields {
		snapshot[k] = v
	}
	b.mu.RUnlock()
	for k, v := range snapshot {
		if !fn(k, v) {
			return
		}
	}
}

// Len 字段数量。
func (b *Bag) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.fields)
}

// SetInt 写入 int64 字段。
func (b *Bag) SetInt(name string, v int64) { b.Set(name, NewInt(v)) }

// GetInt 读 int64 字段（类型不符返回 0）。
func (b *Bag) GetInt(name string) int64 { return b.Get(name).Int() }

// SetFloat 写入 float64 字段。
func (b *Bag) SetFloat(name string, v float64) { b.Set(name, NewFloat(v)) }

// GetFloat 读 float64 字段（类型不符：Int 提升为 float，否则 0）。
func (b *Bag) GetFloat(name string) float64 { return b.Get(name).Float() }

// SetString 写入字符串字段。
func (b *Bag) SetString(name string, v string) { b.Set(name, NewString(v)) }

// GetString 读字符串字段（类型不符返回 ""）。
func (b *Bag) GetString(name string) string { return b.Get(name).String() }

// SetBool 写入布尔字段。
func (b *Bag) SetBool(name string, v bool) { b.Set(name, NewBool(v)) }

// GetBool 读布尔字段（类型不符返回 false）。
func (b *Bag) GetBool(name string) bool { return b.Get(name).Bool() }

// SetBytes 写入二进制字段。
func (b *Bag) SetBytes(name string, v []byte) { b.Set(name, NewBytes(v)) }

// GetBytes 读二进制字段（类型不符返回 nil）。
func (b *Bag) GetBytes(name string) []byte { return b.Get(name).Bytes() }

// SetObject 写入对象引用字段（跨对象引用，复用对象内核 ObjectID）。
func (b *Bag) SetObject(name string, v ObjectID) { b.Set(name, NewObject(v)) }

// GetObject 读对象引用字段（类型不符返回零值 ObjectID）。
func (b *Bag) GetObject(name string) ObjectID { return b.Get(name).Object() }

// MarkClean 清除脏标记（落库 / 广播完成后调用）。
func (b *Bag) MarkClean() {
	b.mu.Lock()
	b.dirty = make(map[string]struct{})
	b.mu.Unlock()
}

// MarkCleanNames 只清除指定字段名的 dirty 标记（保留其余未同步脏字段），
// 防止 SyncPatch 中 FlagSync=false 的字段 dirty 被错误批量清除。
func (b *Bag) MarkCleanNames(names []string) {
	b.mu.Lock()
	for _, n := range names {
		delete(b.dirty, n)
	}
	b.mu.Unlock()
}

// Changed 某字段自上次 MarkClean 以来是否变动。
func (b *Bag) Changed(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.dirty[name]
	return ok
}

// Dirty 返回所有变动字段名（升序）。
func (b *Bag) Dirty() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ns := make([]string, 0, len(b.dirty))
	for n := range b.dirty {
		ns = append(ns, n)
	}
	sort.Strings(ns)
	return ns
}

// Changes 返回变动字段的快照（name→Value 深拷贝）。
func (b *Bag) Changes() map[string]Value {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]Value, len(b.dirty))
	for n := range b.dirty {
		v := b.fields[n]
		if v.Type() == TypeBytes && len(v.Y) > 0 {
			// 深拷贝 []byte，防止调用方修改污染内部
			cp := make([]byte, len(v.Y))
			copy(cp, v.Y)
			v.Y = cp
		}
		out[n] = v
	}
	return out
}

// MarshalJSON 标准线化：{字段名: {"t":类型,"v":值}, ...}。
// 在 RLock 下直接编码（编码是只读操作，不修改 map），避免大 Bag 场景下的全量 map 拷贝开销。
func (b *Bag) MarshalJSON() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return json.Marshal(b.fields)
}

// UnmarshalJSON 从标准线化格式整体载入（完全替换当前字段集，而非合并补齐）。
// 同时清除脏标记——Load 后不应把库旧值当作"变动"推送给客户端。
// 反序列化得到 nil map 时补建，避免对 nil 赋值 panic。
func (b *Bag) UnmarshalJSON(data []byte) error {
	var m map[string]Value
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	b.mu.Lock()
	if m == nil {
		m = make(map[string]Value)
	}
	b.fields = m
	b.dirty = make(map[string]struct{})
	b.mu.Unlock()
	return nil
}

// MergeJSON 用标准线化 JSON 合并写入（不删除已有字段），变动字段标记脏。
// 语义同 maps.Copy：同名覆盖、名不存在时补齐，JSON 未出现的旧字段保留不动。

// 与 Set 同律：仅当字段不存在或值真正变化时才落脏并触发 onChange；无变化的字段
// 不会被重复推送，也不会让 dirty 集合无谓膨胀。
func (b *Bag) MergeJSON(data []byte) error {
	var m map[string]Value
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	type changed struct {
		name string
		v    Value
	}
	b.mu.Lock()
	if b.fields == nil {
		b.fields = make(map[string]Value, len(m))
	}
	if b.dirty == nil {
		b.dirty = make(map[string]struct{})
	}
	var dirty []changed
	var fn func(string, Value)
	for name, v := range m {
		if old, ok := b.fields[name]; ok && ValueEqual(old, v) {
			continue
		}
		b.fields[name] = v
		b.dirty[name] = struct{}{}
		dirty = append(dirty, changed{name: name, v: v})
		fn = b.onChange
	}
	b.mu.Unlock()
	// 与 Set 一致：回调在释放内部锁后调用，可安全回读 Bag。
	if fn == nil {
		return nil
	}
	for _, c := range dirty {
		fn(c.name, c.v)
	}
	return nil
}

// MarshalCompact 紧凑线化：{字段名: 裸值, ...}（无类型标签，人类可读）。
// 数字统一为 int64 或 float64，object 为 "type:seq"，bytes 为 base64 字符串。
func (b *Bag) MarshalCompact() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m := make(map[string]any, len(b.fields))
	for k, v := range b.fields {
		m[k] = compactValue(v)
	}
	return json.Marshal(m)
}

// ApplyJSON 用标准线化 JSON（{字段名: {"t","v"}}）合并写入，变动字段标记脏。
// 「按字段名改」的标准入口（精确类型，不丢精度）。
func (b *Bag) ApplyJSON(data []byte) error {
	var m map[string]Value
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	for k, v := range m {
		b.Set(k, v)
	}
	return nil
}

// ApplyCompact 用紧凑 JSON（{字段名: 裸值}）合并写入，变动字段标记脏。
// 命令行友好：{"gold":999,"name":"bob"} 即可按名改任意字段；
// 数字按「整数→Int，小数→Float」尽力推断，字符串 / 布尔 / 对象("type:seq")按型解析。
func (b *Bag) ApplyCompact(data []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	for k, raw := range m {
		v, ok := inferValue(raw)
		if !ok {
			continue // 跳过无法推断的复杂值（数组 / 对象）
		}
		b.Set(k, v)
	}
	return nil
}

// compactValue 把 Value 转为无类型标签的紧凑表示。
// compactBytesPrefix 紧凑编码中 bytes 类型的前缀标记，防止经 inferValue 被降级为普通 string。
const compactBytesPrefix = "$b64:"

func compactValue(v Value) any {
	switch v.Type() {
	case TypeInt:
		return v.I
	case TypeFloat:
		return v.F
	case TypeString:
		return v.S
	case TypeBool:
		return v.B
	case TypeBytes:
		// 用 "$b64:..." 前缀标记 bytes，避免往返经 inferValue 时被误判为普通 string 导致类型丢失。
		// 必须真正做 base64 编码：直接 string(v.Y) 会让非 UTF-8 的二进制在 JSON 编码时
		// 被替换成 U+FFFD，往返后数据损坏且不可逆。
		return compactBytesPrefix + base64.StdEncoding.EncodeToString(v.Y)
	case TypeObject:
		return v.O.String()
	default:
		return nil
	}
}

// inferValue 从紧凑 JSON 原始片段尽力推断一个 Value（数字 / 字符串 / 布尔 / 对象串）。
func inferValue(raw json.RawMessage) (Value, bool) {
	// 去掉首尾空白
	s := string(raw)
	// null：还原为 nil 类型字段。compactValue 对 TypeNil 输出 null，
	// 必须能回读，否则 nil 字段经紧凑 JSON 往返被静默丢弃。
	if s == "null" {
		return Value{}, true
	}
	// 布尔
	if s == "true" {
		return NewBool(true), true
	}
	if s == "false" {
		return NewBool(false), true
	}
	// 字符串（含引号）
	if len(s) >= 2 && s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return Value{}, false
		}
		// 紧凑 bytes 标记：避免 base64 字串被误判为普通 string 导致类型丢失。
		if after, ok := strings.CutPrefix(str, compactBytesPrefix); ok {
			raw, err := base64.StdEncoding.DecodeString(after)
			if err != nil {
				// 前缀匹配但不是合法 base64，退化为普通字符串而非丢弃该字段。
				return NewString(str), true
			}
			return NewBytes(raw), true
		}
		// 仅当字符串符合 "type:seq" 格式（含冒号）时才按对象引用解析，
		// 否则一律作为普通字符串——避免把纯数字字符串（如 "123"）经 ParseObjectID
		// 误判为对象引用（type=0, seq=123），导致本应是字符串的字段被存成对象引用。
		if strings.Contains(str, ":") {
			if id, err := ParseObjectID(str); err == nil && !id.IsZero() {
				return NewObject(id), true
			}
		}
		return NewString(str), true
	}
	// 数字
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		// 检查是否在 int64 范围内且可以精确表示。
		// 上界必须用严格小于：float64(math.MaxInt64) 实际是 2^63（> MaxInt64），
		// 用 <= 会把恰好 2^63 的浮点送进越界的 int64 转换（结果实现相关）。
		// 用 math.Trunc 判整而不是 f == float64(int64(f))：后者本身要先做越界转换。
		if f == math.Trunc(f) && f >= minInt64AsFloat && f < maxInt64AsFloatExcl {
			return NewInt(int64(f)), true
		}
		return NewFloat(f), true
	}
	return Value{}, false
}
