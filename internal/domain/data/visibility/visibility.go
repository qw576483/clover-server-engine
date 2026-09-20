// Package visibility 定义「数据对客户端可见性」的引擎级契约。
//
// 三级可见性：
// - ClientVisible：所有人可见（默认）。进视野 / 变动推给视野内所有人。
// - ClientSelfOnly：仅自己可见。进视野不推给他人；变更只推给自己。
// - ServerOnly：纯服务器，任何同步都不下发到客户端。
//
// 业务在启动期 Register：
//
// visibility.Register(data.OwnerPlayer, "bag", visibility.ClientSelfOnly)
// visibility.Register(data.OwnerPlayer, "ai_state", visibility.ServerOnly)
// // "profile" "appearance" 未注册 → 默认 ClientVisible
//
// SnapshotClient / FilterPublicVisible 在「下发源头」自动裁剪。
package visibility

import (
	"clover-server-engine/internal/domain/data"
	"encoding/json"
	"sync"
)

// Visibility 数据对客户端的可见性类别（本体定义在 pkg/domain/data）。
type Visibility = data.Visibility

const (
	ClientVisible  = data.ClientVisible
	ClientSelfOnly = data.ClientSelfOnly
	ServerOnly     = data.ServerOnly
)

// Registry 可见性注册表（type 级）。
type Registry struct {
	mu     sync.RWMutex
	m      map[string]Visibility
	defVis Visibility // 未注册类型的默认可见性
	strict bool       // 严格模式下未注册类型按 ServerOnly（不下发）处理
}

// NewRegistry 构造空注册表，默认可见性为 ClientVisible。
func NewRegistry() *Registry {
	return &Registry{m: make(map[string]Visibility), defVis: ClientVisible}
}

// SetDefault 设置未注册类型的默认可见性。
func (r *Registry) SetDefault(v Visibility) {
	r.mu.Lock()
	r.defVis = v
	r.mu.Unlock()
}

// SetStrict 开启/关闭严格模式。
//
// 默认（strict=false）下，未 Register 的 (ownerType, typ) 走 defVis（初始 ClientVisible=公开广播），
// 一旦漏注册敏感数据便会被广播给所有玩家，属于「默认不安全」。开启严格模式后，未注册类型
// 一律按最保守的 ServerOnly 处理（任何同步都不下发），迫使敏感/需下发字段必须显式 Register，
// 把「漏配置」的后果从「意外泄露」变为「意外不可见」，是更安全的失败方向。
func (r *Registry) SetStrict(strict bool) {
	r.mu.Lock()
	r.strict = strict
	r.mu.Unlock()
}

func regKey(ownerType data.OwnerType, typ string) string {
	return string(ownerType) + "/" + typ
}

// Register 声明某 (ownerType, typ) 的可见性类别。
func (r *Registry) Register(ownerType data.OwnerType, typ string, v Visibility) {
	r.mu.Lock()
	r.m[regKey(ownerType, typ)] = v
	r.mu.Unlock()
}

// Lookup 返回 (ownerType, typ) 已注册的可见性；未注册时：严格模式返回 ServerOnly（不下发），
// 否则返回 SetDefault 配置的默认值（初始 ClientVisible）。
func (r *Registry) Lookup(ownerType data.OwnerType, typ string) Visibility {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v, ok := r.m[regKey(ownerType, typ)]; ok {
		return v
	}
	return r.effectiveDefaultLocked()
}

// effectiveDefaultLocked 返回未注册类型的生效默认可见性（调用方须持有 r.mu 读/写锁）。
// 严格模式下未注册即最保守的 ServerOnly。
func (r *Registry) effectiveDefaultLocked() Visibility {
	if r.strict {
		return ServerOnly
	}
	return r.defVis
}

// IsClientVisible 返回 typ 是否对客户端可见（含 SelfOnly）。
func (r *Registry) IsClientVisible(ownerType data.OwnerType, typ string) bool {
	return r.Lookup(ownerType, typ) != ServerOnly
}

// IsPublicVisible 返回 typ 是否对所有人可见（不包含 SelfOnly 和 ServerOnly）。
func (r *Registry) IsPublicVisible(ownerType data.OwnerType, typ string) bool {
	return r.Lookup(ownerType, typ) == ClientVisible
}

// IsClientVisible 包级快捷调用。
func IsClientVisible(ownerType data.OwnerType, typ string) bool {
	return Default.IsClientVisible(ownerType, typ)
}

// IsPublicVisible 包级快捷调用。
func IsPublicVisible(ownerType data.OwnerType, typ string) bool {
	return Default.IsPublicVisible(ownerType, typ)
}

// FilterClientVisible 剔除 ServerOnly，保留 ClientVisible 和 ClientSelfOnly。
// 用于"推给自己"的路径（全量同步）。
func (r *Registry) FilterClientVisible(ownerType data.OwnerType, snap map[string]json.RawMessage) map[string]json.RawMessage {
	if len(snap) == 0 {
		return snap
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]json.RawMessage, len(snap))
	for typ, raw := range snap {
		v := r.effectiveDefaultLocked()
		if rv, ok := r.m[regKey(ownerType, typ)]; ok {
			v = rv
		}
		if v == ServerOnly {
			continue
		}
		out[typ] = raw
	}
	return out
}

// FilterPublicVisible 仅保留 ClientVisible，剔除 ClientSelfOnly 和 ServerOnly。
// 用于"推给别人"的路径（进视野快照）。
func (r *Registry) FilterPublicVisible(ownerType data.OwnerType, snap map[string]json.RawMessage) map[string]json.RawMessage {
	if len(snap) == 0 {
		return snap
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]json.RawMessage, len(snap))
	for typ, raw := range snap {
		v := r.effectiveDefaultLocked()
		if rv, ok := r.m[regKey(ownerType, typ)]; ok {
			v = rv
		}
		if v != ClientVisible {
			continue // 只保留 ClientVisible
		}
		out[typ] = raw
	}
	return out
}

// Default 全局默认注册表（业务启动期 Register 到此）。
var Default = NewRegistry()

// Register 在 Default 上声明 (ownerType, typ) 可见性。
func Register(ownerType data.OwnerType, typ string, v Visibility) {
	Default.Register(ownerType, typ, v)
}

// Lookup 在 Default 上查询可见性。
func Lookup(ownerType data.OwnerType, typ string) Visibility {
	return Default.Lookup(ownerType, typ)
}

// FilterClientVisible 在 Default 上过滤（剔除 ServerOnly）。
func FilterClientVisible(ownerType data.OwnerType, snap map[string]json.RawMessage) map[string]json.RawMessage {
	return Default.FilterClientVisible(ownerType, snap)
}

// FilterPublicVisible 在 Default 上过滤（仅 ClientVisible）。
func FilterPublicVisible(ownerType data.OwnerType, snap map[string]json.RawMessage) map[string]json.RawMessage {
	return Default.FilterPublicVisible(ownerType, snap)
}

// SnapshotScope 快照用途。
type SnapshotScope int

const (
	// ScopePublic 给别人看（进视野：只推 ClientVisible，剔除 SelfOnly）。
	ScopePublic SnapshotScope = iota
	// ScopeSelf 给自己看（全量同步：推全部非 ServerOnly）。
	ScopeSelf
)
