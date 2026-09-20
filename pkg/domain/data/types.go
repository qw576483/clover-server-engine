package data

import (
	"errors"
	"fmt"
	"strings"

	"clover-server-engine/pkg/domain/object"
)

// StorageTier 数据存储等级——由每个 StructSchema / RecordSchema 独立声明，
// 同 Store 可混用多种存储策略，彻底取代旧全局 Mode。
// 零值（0）表示「未声明」，getTier 会兜底到 Store 默认 Tier。
type StorageTier uint8

const (
	tierNotSet StorageTier = iota
	// TierMemory 纯进程内存（Go map），不依赖 Redis/MySQL，进程退出即丢失。
	// 适合本地开发、单测、临时缓存，以及房间/对局内热数据。
	TierMemory
	// TierRedis 纯 Redis 存储：Save 写 Redis，Load 读 Redis，不依赖 MySQL。
	// 适合每日缓存、会话数据、临时状态等不需要 MySQL 持久化的场景。
	// 数据驻留 Redis，可通过 CacheTTL 设置过期时间，过期后自动清除。
	TierRedis
	// TierRedisMySQL Redis 热缓存 + MySQL 冷持久化：Save 标脏 → 写 Redis，Load 优先 Redis → 回源 MySQL，
	// Flush 将脏数据批量落 MySQL。适合玩家存档、装备、道具等需持久化的数据。
	TierRedisMySQL
	// TierSnapshot 内存主存 + MySQL 快照落盘：内存为权威源，Load 内存优先 → 回源 MySQL 并回填；
	// Flush 将内存快照写入 MySQL。适合 MMO 实体（高频读写、需崩溃恢复但不需逐帧持久化）。
	TierSnapshot
)

// String 返回 Tier 的字符串名，与配置文件中的写法一致。
func (t StorageTier) String() string {
	switch t {
	case TierMemory:
		return "TierMemory"
	case TierRedis:
		return "TierRedis"
	case TierRedisMySQL:
		return "TierRedisMySQL"
	case TierSnapshot:
		return "TierSnapshot"
	default:
		return ""
	}
}

// MarshalText 实现 encoding.TextMarshaler，序列化为字符串名而非数字，
// 保证「写出去的配置」与「文档里的写法」一致。
//
// 未知 tier 必须返回错误而不是静默写空串：空串经 UnmarshalText 会被解释为
// tierNotSet（未声明），把「未知值」悄悄改成「未声明」，配置往返即丢失。
func (t StorageTier) MarshalText() ([]byte, error) {
	s := t.String()
	if s == "" {
		return nil, fmt.Errorf("data: unknown storage tier value %d (cannot marshal)", uint8(t))
	}
	return []byte(s), nil
}

// UnmarshalText 实现 encoding.TextUnmarshaler，让配置文件可以直接写
// tier: "TierRedisMySQL" 这样的字符串。
//
// 支持两种写法：字符串名（TierMemory，大小写与 Tier 前缀均可省略）、
// 空串（视为未声明，由 NewStore 兜底为 TierRedisMySQL）。
func (t *StorageTier) UnmarshalText(text []byte) error {
	s := strings.TrimSpace(string(text))
	if s == "" {
		*t = tierNotSet
		return nil
	}
	// 归一化：忽略大小写与可选的 "tier" 前缀，允许 "memory" / "TierMemory" 等写法。
	norm := strings.ToLower(s)
	norm = strings.TrimPrefix(norm, "tier")
	norm = strings.ReplaceAll(norm, "_", "")
	norm = strings.ReplaceAll(norm, "-", "")
	switch norm {
	case "memory", "mem":
		*t = TierMemory
		return nil
	case "redis":
		*t = TierRedis
		return nil
	case "redismysql", "mysql":
		*t = TierRedisMySQL
		return nil
	case "snapshot", "mmo":
		*t = TierSnapshot
		return nil
	case "notset", "0":
		*t = tierNotSet
		return nil
	}
	return fmt.Errorf("data: unknown storage tier %q (valid: TierMemory/TierRedis/TierRedisMySQL/TierSnapshot)", s)
}

// ErrNotFound 表示指定的 owner + type 数据不存在。
var ErrNotFound = errors.New("data: not found")

// OwnerType 数据归属实体的类型。任何业务实体都只是其中一个取值。
type OwnerType string

// 预置实体类型常量。业务也可自定义，只要满足 OwnerType 即可。
const (
	OwnerAccount OwnerType = "account" // 账号（登录身份，其他账号数据经 data 绑定在它身上）
	OwnerPlayer  OwnerType = "player"  // 玩家
	OwnerServer  OwnerType = "server"  // 服务器（区服广播数据，推送到 NATS 通配 subject）
	OwnerObject  OwnerType = "object"  // 场景中的游戏对象（NPC、道具等）
	OwnerMeta    OwnerType = "meta"    // 内部簿记（idgen 高水位），非业务实体
)

// Key 组合主键：归属实体类型 + 归属实体 ID + 数据类型。
// 对应数据表 data 的联合主键 (owner_type, owner_id, type)。
// 例：玩家背包 = Key{OwnerPlayer, "1001", "bag"}；道具属性 = Key{OwnerObject, "1:1001", "props"}。
type Key struct {
	Owner        OwnerType // 归属实体类型（player/scene/legion/...）
	ID           string    // 归属实体 ID（玩家 UID / 场景 ID / 军团 ID ...）
	Type         string    // 业务数据类型（bag/mail/pos/state ...）
	NoLocalCache bool      // 跳过本地内存缓存，直读持久层（用于跨区镜像等场景）
}

// Visibility 数据对客户端的可见性类别。
type Visibility uint8

const (
	// ClientVisible 所有人可见（默认）。
	ClientVisible Visibility = iota
	// ClientSelfOnly 仅自己可见。
	ClientSelfOnly
	// ServerOnly 纯服务器数据。
	ServerOnly
)

// RecordSchema 预定义一条 Record 的完整 Schema。
// 业务层声明实体时一并定义，避免每次 LoadRecord 重复手写 cols / colTypes。
type RecordSchema struct {
	Type       string        // 数据唯一标识，如 "bag"
	OwnerType  OwnerType     // owner 类型：OwnerPlayer / OwnerAccount / ...
	Cols       []string      // 列名
	ColTypes   []object.Type // 列类型
	Visibility Visibility    // 客户端可见性
	Tier       StorageTier   // 存储等级：TierMemory / TierRedisMySQL / TierSnapshot
}

// SchemaOwnerType 实现 TypeSchema 接口。
func (s RecordSchema) SchemaOwnerType() OwnerType { return s.OwnerType }

// SchemaType 实现 TypeSchema 接口。
func (s RecordSchema) SchemaType() string { return s.Type }

// SchemaTier 实现 TierSchema 接口。
func (s RecordSchema) SchemaTier() StorageTier { return s.Tier }

// Key 用实体 ID 构造在存储层定位该记录实例的 Key。
func (s RecordSchema) Key(id string) Key {
	return Key{Owner: s.OwnerType, ID: id, Type: s.Type}
}

// StructSchema 预定义一条 Struct 数据的 Schema。
// 业务层声明 Struct 实体时一并定义 Type/Kind/Visibility/Tier，配合 Ctx.LoadStruct 一行加载。
type StructSchema struct {
	Type       string      // 数据唯一标识，如 "player_profile"
	OwnerType  OwnerType   // owner 类型：OwnerPlayer / OwnerAccount / ...
	Visibility Visibility  // 客户端可见性
	Tier       StorageTier // 存储等级：TierMemory / TierRedisMySQL / TierSnapshot
}

// SchemaOwnerType 实现 TypeSchema 接口。
func (s StructSchema) SchemaOwnerType() OwnerType { return s.OwnerType }

// SchemaType 实现 TypeSchema 接口。
func (s StructSchema) SchemaType() string { return s.Type }

// SchemaTier 实现 TierSchema 接口。
func (s StructSchema) SchemaTier() StorageTier { return s.Tier }

// Key 用实体 ID 构造在存储层定位该 Struct 实例的 Key。
func (s StructSchema) Key(id string) Key {
	return Key{Owner: s.OwnerType, ID: id, Type: s.Type}
}
