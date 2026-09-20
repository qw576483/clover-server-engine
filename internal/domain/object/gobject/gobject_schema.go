package gobject

import (
	"errors"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
)

// errNoSchema 在调用需要 schema 的方法但未绑定 schema 时返回。
var errNoSchema = errors.New("gobject: no schema bound (call SetSchema first)")

// 可选：绑定标准化数据格式 schema（客户端 / 服务器共用同一份字段定义）
//
// 绑定 schema 后，GameObject 在既有「按字段名读写 + 变更追踪」之上，额外获得：
//   - 按序号紧凑编码/解码（MMO「线上按序号同步」精髓，双端仅凭同一份 schema 即互通，无字段名冗余）；
//   - 按字段 Flag 精确控制同步/存盘范围（只推标 Sync 的脏字段、只存标 Save 的字段、公开视图剔除 Private）。
//
// schema 为可选：不绑定时 GameObject 行为完全不变（既有整对象 JSON 线化 / Save 广播照旧）。

// SetSchema 绑定标准化属性 schema（nil 表示解绑）。
func (g *GameObject) SetSchema(s *object.Schema) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.schema = s
}

// Schema 返回已绑定的 schema（未绑定为 nil）。
func (g *GameObject) Schema() *object.Schema {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.schema
}

// ValidateProps 依据已绑定 schema 校验 props 字段类型；未绑定 schema 时视为通过。
func (g *GameObject) ValidateProps() error {
	// schema 与 props 必须在同一把读锁里取快照：UnmarshalJSON 会整体替换 g.props，
	// 分两次读会拿到「旧 schema + 新 Bag」或对已被替换的 Bag 操作。
	g.mu.RLock()
	s := g.schema
	p := g.props
	g.mu.RUnlock()
	if s == nil {
		return nil
	}
	return s.Validate(p)
}

// MarshalPropsIndexed 把 props 按 schema 编码为「序号→紧凑值」JSON（客户端整对象快照，双端互通）。
// 需先绑定 schema。
func (g *GameObject) MarshalPropsIndexed() ([]byte, error) {
	g.mu.RLock()
	s := g.schema
	p := g.props
	g.mu.RUnlock()
	if s == nil {
		return nil, errNoSchema
	}
	return object.MarshalIndexed(s, p)
}

// ApplyPropsIndexed 用「序号→紧凑值」JSON 按 schema 回写 props（收到双端互通数据时用）。
// 需先绑定 schema。
func (g *GameObject) ApplyPropsIndexed(data []byte) error {
	g.mu.RLock()
	s := g.schema
	p := g.props
	g.mu.RUnlock()
	if s == nil {
		return errNoSchema
	}
	return object.ApplyIndexed(s, p, data)
}

// SyncPropsPatch 依据 schema 生成「脏且需同步」字段的按序号增量补丁。
// public=true 时剔除私有字段（向他人广播）。第二个返回值为 false 表示无需推送。需先绑定 schema。
func (g *GameObject) SyncPropsPatch(public bool) ([]byte, bool, error) {
	g.mu.RLock()
	s := g.schema
	p := g.props
	g.mu.RUnlock()
	if s == nil {
		return nil, false, errNoSchema
	}
	data, names, ok, err := object.SyncPatch(s, p, public)
	if err != nil || !ok {
		return data, ok, err
	}
	// 仅清除已同步字段的脏标记，保留 FlagSync=false 的未同步脏字段。
	p.MarkCleanNames(names)
	return data, true, nil
}

// SavePropsSnapshot 依据 schema 生成仅含「需持久化」字段的按序号快照。需先绑定 schema。
func (g *GameObject) SavePropsSnapshot() ([]byte, error) {
	g.mu.RLock()
	s := g.schema
	p := g.props
	g.mu.RUnlock()
	if s == nil {
		return nil, errNoSchema
	}
	return object.SaveSnapshot(s, p)
}
