package data

import pdata "clover-server-engine/pkg/domain/data"

// TypeSchema 统一 StructSchema / RecordSchema 的 Kind+Type 提取接口（本体在 pkg/domain/data）。
type TypeSchema = pdata.TypeSchema

// TierSchema 扩展接口：Tier-aware Schema（本体在 pkg/domain/data）。
type TierSchema = pdata.TierSchema

// SchemaTypes 返回 ownerType 下已注册的全部类型名，未注册返回 nil。
// 直接委托 pkg 层的 schema 注册表。
var SchemaTypes = pdata.SchemaTypes

// RegisterTypeBySchema 传入 StructSchema / RecordSchema 变量自动提取 Kind+Type+Tier 注册。
// 直接委托 pkg 层的 schema 注册表。
var RegisterTypeBySchema = pdata.RegisterTypeBySchema

// RegisterType 注册 ownerType 下的类型名。
var RegisterType = pdata.RegisterType
