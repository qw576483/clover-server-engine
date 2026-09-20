package data

import (
	"context"
	"time"

	imysql "github.com/qw576483/clover-server-engine/internal/domain/data/store/mysql"
	iredis "github.com/qw576483/clover-server-engine/internal/domain/data/store/redis"
	pdata "github.com/qw576483/clover-server-engine/pkg/domain/data"
)

// StorageTier 数据存储等级（本体定义在 pkg/domain/data）。
type StorageTier = pdata.StorageTier

// tierNotSet 零值 sentinel（与 pkg 层 tierNotSet 一致），供 NewStore 兜底判断。
const tierNotSet StorageTier = 0

const (
	TierMemory     = pdata.TierMemory
	TierRedis      = pdata.TierRedis
	TierRedisMySQL = pdata.TierRedisMySQL
	TierSnapshot   = pdata.TierSnapshot
)

// OwnerType 数据归属实体的类型（本体定义在 pkg/domain/data）。
type OwnerType = pdata.OwnerType

const (
	OwnerAccount = pdata.OwnerAccount
	OwnerPlayer  = pdata.OwnerPlayer
	OwnerServer  = pdata.OwnerServer
	OwnerObject  = pdata.OwnerObject
	OwnerMeta    = pdata.OwnerMeta
)

// Key 组合主键（本体定义在 pkg/domain/data）。
type Key = pdata.Key

// Visibility 数据对客户端的可见性类别（本体定义在 pkg/domain/data）。
type Visibility = pdata.Visibility

const (
	ClientVisible  = pdata.ClientVisible
	ClientSelfOnly = pdata.ClientSelfOnly
	ServerOnly     = pdata.ServerOnly
)

// ErrNotFound 表示指定的 owner + type 数据不存在（本体定义在 pkg/domain/data）。
var ErrNotFound = pdata.ErrNotFound

// RecordSchema Record 的完整 Schema（本体定义在 pkg/domain/data）。
type RecordSchema = pdata.RecordSchema

// StructSchema Struct 数据的 Schema（本体定义在 pkg/domain/data）。
type StructSchema = pdata.StructSchema

// Config data 模块配置。兼容 yaml + mapstructure 双 tag，可经 pkg/config 反序列化。
// 保留定义在 internal，因为它依赖 internal/domain/data/store/redis 和 store/mysql 的配置类型。
type Config struct {
	Tier            StorageTier        `yaml:"tier" mapstructure:"tier"`                           // 默认存储等级（Schema 未声明 Tier 时的兜底），零值时默认为 TierRedisMySQL
	Redis           iredis.RedisConfig `yaml:"redis" mapstructure:"redis"`                         // redis 连接（TierRedisMySQL 必填）
	MySQL           imysql.MySQLConfig `yaml:"mysql" mapstructure:"mysql"`                         // mysql 连接（TierRedisMySQL / TierSnapshot 必填）
	RedisKeyPrefix  string             `yaml:"redis_key_prefix" mapstructure:"redis_key_prefix"`   // redis key 前缀，默认 clover:data
	CacheTTL        time.Duration      `yaml:"cache_ttl" mapstructure:"cache_ttl"`                 // redis 缓存过期；0 表示不过期（持久驻留 redis）
	Table           string             `yaml:"table" mapstructure:"table"`                         // 表名，默认 data
	AutoCreateTable bool               `yaml:"auto_create_table" mapstructure:"auto_create_table"` // 启动时自动执行 CREATE TABLE IF NOT EXISTS
	FlushInterval   time.Duration      `yaml:"flush_interval" mapstructure:"flush_interval"`       // 周期落库间隔（TierRedisMySQL / TierSnapshot）；<=0 不启动周期落盘（仅在进程 Close 时终落）；内存模式忽略此字段
	MaxMemory       int                `yaml:"max_memory" mapstructure:"max_memory"`               // 最大驻留条目数；>0 时启用 LRU 淘汰（TierSnapshot 脏数据淘汰前先落库），0=不限（默认）
	ShardCount      int                `yaml:"shard_count" mapstructure:"shard_count"`             // 内存储存/脏标记分片数（向上取 2 的幂，默认 64）；缓解高并发全局锁串行
	SqlsDir         string             `yaml:"sqls_dir" mapstructure:"sqls_dir"`                   // SQL 迁移目录（相对于工作目录），空=不启用；目录内文件按 V{版本}__{描述}.sql 命名
}

// ListTypes 列出已注册的 ownerType 下全部数据类型。
func (s *Store) ListTypes(ctx context.Context, ownerType OwnerType) ([]string, error) {
	// 此前 `_ = ctx` 完全忽略取消：调用方带着已取消的 ctx 也会得到「成功」。
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return SchemaTypes(ownerType), nil
}
