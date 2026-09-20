package data

import (
	"reflect"
	"sort"
	"sync"
)

// typeRegistry 编译期注册 ownerType→type 映射，取代表级 SQL ListTypes。
var typeRegistry = &schemaReg{
	m:     map[OwnerType]map[string]struct{}{},
	tiers: map[string]StorageTier{},
}

type schemaReg struct {
	mu    sync.RWMutex
	m     map[OwnerType]map[string]struct{}
	tiers map[string]StorageTier // "Kind/Type"→Tier
}

// TypeSchema 统一 StructSchema / RecordSchema 的 Kind+Type 提取接口。
type TypeSchema interface {
	SchemaOwnerType() OwnerType
	SchemaType() string
}

// TierSchema 扩展接口：Tier-aware Schema（StructSchema / RecordSchema 均实现）。
type TierSchema interface {
	TypeSchema
	SchemaTier() StorageTier
}

// tierKey 生成 {Kind}/{Type} 查找键。
func tierKey(ownerType OwnerType, typ string) string {
	return string(ownerType) + "/" + typ
}

// RegisterType 注册 ownerType 下的类型名。
func RegisterType(ownerType OwnerType, typ string) {
	typeRegistry.mu.Lock()
	defer typeRegistry.mu.Unlock()
	k := OwnerType(ownerType)
	if typeRegistry.m[k] == nil {
		typeRegistry.m[k] = map[string]struct{}{}
	}
	typeRegistry.m[k][typ] = struct{}{}
}

// RegisterTypeBySchema 传入 StructSchema / RecordSchema 变量自动提取 Kind+Type+Tier 注册。
// nil（含装箱 nil 接口）直接 panic：下一步就会调 s.SchemaOwnerType() 解引用 nil 指针，
// 在这里显式拦下能给出准确原因，而不是在方法调用处报无上下文的空指针。
func RegisterTypeBySchema(s TypeSchema) {
	if s == nil {
		panic("data: RegisterTypeBySchema: nil schema")
	}
	if v := reflect.ValueOf(s); v.Kind() == reflect.Pointer && v.IsNil() {
		panic("data: RegisterTypeBySchema: nil schema (typed-nil)")
	}
	typeRegistry.mu.Lock()
	defer typeRegistry.mu.Unlock()
	k := s.SchemaOwnerType()
	t := s.SchemaType()
	if typeRegistry.m[k] == nil {
		typeRegistry.m[k] = map[string]struct{}{}
	}
	typeRegistry.m[k][t] = struct{}{}
	// 自动提取 Tier
	if ts, ok := s.(TierSchema); ok {
		typeRegistry.tiers[tierKey(k, t)] = ts.SchemaTier()
	}
}

// SchemaTypes 返回 ownerType 下已注册的全部类型名，未注册返回 nil。
func SchemaTypes(ownerType OwnerType) []string {
	typeRegistry.mu.RLock()
	defer typeRegistry.mu.RUnlock()
	set := typeRegistry.m[OwnerType(ownerType)]
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	// 排序：该结果用于 SQL ListTypes / 迁移遍历，map 迭代顺序随机会让跨次运行
	// 顺序不稳定（迁移/DDL 顺序漂移，排查时无法复现）。
	sort.Strings(out)
	return out
}

// get 返回 ownerType/typ 对应的 StorageTier，未注册返回 (0, false)。
func (r *schemaReg) get(ownerType OwnerType, typ string) (StorageTier, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tiers[tierKey(ownerType, typ)]
	return t, ok
}

// hasPersistent 是否有任何 Schema 需要 MySQL 持久化（TierRedisMySQL 或 TierSnapshot）。
func (r *schemaReg) hasPersistent() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.tiers {
		if t == TierRedisMySQL || t == TierSnapshot {
			return true
		}
	}
	return false
}

// 导出包装：供 internal Store 等实现层调用
// TierNotSet 返回零值 tier sentinel（供 internal NewStore 兜底判断）。
func TierNotSet() StorageTier { return tierNotSet }

// TierGet 返回 ownerType/typ 对应的 StorageTier，未注册返回 (0, false)。
// 供 internal Store.getTier 使用。
func TierGet(ownerType OwnerType, typ string) (StorageTier, bool) {
	return typeRegistry.get(ownerType, typ)
}

// TierHasPersistent 是否有任何 Schema 需要 MySQL 持久化。
// 供 internal Store 周期落盘 / 终落盘判断。
func TierHasPersistent() bool {
	return typeRegistry.hasPersistent()
}
