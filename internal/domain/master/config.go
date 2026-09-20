// Package master 汇总 master 相关的公共配置。
//
// 本文件只定义配置结构与默认值回落逻辑，不含任何运行时实现；
// 具体能力分别落在子包：
//   - failover：节点健康探测（心跳超时判定）与死节点摘除
//   - state：权威状态（节点表 / 玩家定位表 / 排行榜 / session token）与存储后端
package master

import (
	"time"

	"clover-server-engine/internal/domain/master/state"
	"clover-server-engine/internal/shared/config"
	"clover-server-engine/pkg/foundation/logger"
)

// 默认值：全部可通过配置覆盖，零值自动回落到这里。
const (
	// DefaultHeartbeatInterval 节点向 master 上报心跳的周期。
	// 与客户端上报的默认周期同源（协议定义在 state，避免两边各写一个 3s）。
	DefaultHeartbeatInterval = state.DefaultHeartbeatInterval
	// DefaultSuspectTimeout 超过该时长未收到心跳，节点标记为 Suspect。
	DefaultSuspectTimeout = 9 * time.Second
	// DefaultDeadTimeout 超过该时长未收到心跳，节点标记为 Dead 并摘除。
	DefaultDeadTimeout = 15 * time.Second
	// DefaultProbeInterval 健康探测器扫描节点表的周期。
	DefaultProbeInterval = 1 * time.Second

	// DefaultSessionTokenTTL session token 默认有效期。
	DefaultSessionTokenTTL = 24 * time.Hour
	// DefaultSessionTokenKeyPrefix session token 在 Redis 中的 key 前缀
	// （多游戏服共用 Redis 时通过 key_prefix 覆盖以隔离，避免相同 playerID 串号）。
	DefaultSessionTokenKeyPrefix = "session:token:"
)

// SessionToken 后端常量。
const (
	// SessionTokenBackendMemory 内存后端（默认）：只在 master 进程内保存，
	// master 重启后全部失效，玩家需重新登录。
	SessionTokenBackendMemory = "memory"
	// SessionTokenBackendRedis Redis 后端：由 Redis 自身 TTL 持久化，
	// master 重启后 token 仍有效（需配置 data.redis.addr）。
	SessionTokenBackendRedis = "redis"
)

// HealthConfig 是节点健康探测的配置。
//
// 判定链路：节点每 HeartbeatInterval 上报一次心跳；探测器每 ProbeInterval
// 扫描一次节点表，静默超过 SuspectTimeout 标记 Suspect，超过 DeadTimeout
// 标记 Dead 并触发摘除。
type HealthConfig struct {
	// HeartbeatInterval 节点上报心跳的周期（client 侧使用）。
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" mapstructure:"heartbeat_interval"`
	// SuspectTimeout 进入 Suspect 的静默阈值（server 侧使用）。
	SuspectTimeout time.Duration `yaml:"suspect_timeout" mapstructure:"suspect_timeout"`
	// DeadTimeout 进入 Dead 并摘除的静默阈值（server 侧使用）。
	DeadTimeout time.Duration `yaml:"dead_timeout" mapstructure:"dead_timeout"`
	// ProbeInterval 探测器扫描周期（server 侧使用）。
	ProbeInterval time.Duration `yaml:"probe_interval" mapstructure:"probe_interval"`
}

// Normalize 把零值/非法值回落到默认值，并修正阈值间的偏序关系。
// 返回修正后的副本，不修改接收者，便于配置对象复用。
func (c HealthConfig) Normalize() HealthConfig {
	// 注意用 normHealthDuration 而非裸 config.DefDuration：后者只把 0 换成默认值、
	// 负值原样放行——负的 probe_interval 会一路传到 time.NewTicker 触发 panic，
	// 负的 suspect/dead 会让所有节点在同一轮扫描里被判死摘除。
	c.HeartbeatInterval = normHealthDuration("heartbeat_interval", c.HeartbeatInterval, DefaultHeartbeatInterval)
	c.SuspectTimeout = normHealthDuration("suspect_timeout", c.SuspectTimeout, DefaultSuspectTimeout)
	c.DeadTimeout = normHealthDuration("dead_timeout", c.DeadTimeout, DefaultDeadTimeout)
	c.ProbeInterval = normHealthDuration("probe_interval", c.ProbeInterval, DefaultProbeInterval)
	// Suspect 必须严格早于 Dead，否则节点会直接跳过 Suspect 阶段；
	// 配置写反时按 DeadTimeout/3 兜底，保证状态机仍是 Alive→Suspect→Dead。
	if c.SuspectTimeout >= c.DeadTimeout {
		adjusted := c.DeadTimeout / 3
		if adjusted <= 0 {
			// DeadTimeout 过小（<3ns）：无法同时满足 Suspect<Dead，退化为与 Dead 相等（无 Suspect 阶段）。
			adjusted = c.DeadTimeout
		}
		logger.Warnf("master: health.suspect_timeout=%s >= dead_timeout=%s, adjusted to %s",
			c.SuspectTimeout, c.DeadTimeout, adjusted)
		c.SuspectTimeout = adjusted
	}
	// 扫描周期比 Suspect 阈值还长会导致判定严重滞后，收敛到阈值本身。
	if c.ProbeInterval > c.SuspectTimeout {
		c.ProbeInterval = c.SuspectTimeout
	}
	return c
}

// normHealthDuration 归一化健康探测时长：<0 是非法配置，回落到默认值并告警；
// 0 视为未设置，回落到默认值（与 config.DefDuration 的约定一致）。
func normHealthDuration(name string, v, def time.Duration) time.Duration {
	if v < 0 {
		logger.Warnf("master: health.%s=%s is negative, fallback to %s", name, v, def)
		return def
	}
	return config.DefDuration(v, def)
}

// ShardConfig 是 master 分片配置。
//
// 分片规则（两端唯一契约）：shard = Xxhash64(uid) % Total（见 pkg/shared/util）。
// Total=1（默认）即单分片，行为与未启用分片完全一致；多分片时各 master 只持有
// 归属自己那一段 key 的数据，不需要主备，也不需要 leader 选举。
//
// 本期不支持热扩容：改 Total 会使 key 重新映射，必须停服迁移。
type ShardConfig struct {
	// Index 本实例的分片序号，取值范围 [0, Total)。
	Index int `yaml:"index" mapstructure:"index"`
	// Total 集群分片总数（>=1，默认 1）。
	Total int `yaml:"total" mapstructure:"total"`
}

// Normalize 把非法值收敛到可用范围：Total<1 视为单分片，Index 越界收敛到 0。
// 返回修正后的副本，不修改接收者。
func (c ShardConfig) Normalize() ShardConfig {
	if c.Total < 1 {
		// 静默收敛会让写错的配置毫无痕迹：Total<1 与「单分片默认值」行为相同，无法区分。
		if c.Total != 0 {
			logger.Warnf("master: shard.total=%d invalid, fallback to 1 (single shard)", c.Total)
		}
		c.Total = 1
	}
	if c.Index < 0 {
		logger.Warnf("master: shard.index=%d negative, fallback to 0", c.Index)
		c.Index = 0
	}
	if c.Index >= c.Total {
		// 越界的 index 会让本实例既不服务任何分片、又占着一个地址，必须显式告警后再回落。
		logger.Warnf("master: shard.index=%d out of range [0,%d), fallback to 0", c.Index, c.Total)
		c.Index = 0
	}
	return c
}

// Sharded 报告是否处于多分片模式。
func (c ShardConfig) Sharded() bool { return c.Total > 1 }

// SessionTokenConfig 是 session token 存储的配置。
type SessionTokenConfig struct {
	// Backend 存储后端：memory（默认）| redis。
	Backend string `yaml:"backend" mapstructure:"backend"`
	// TTL token 有效期；0=回落默认 24h；负值（如 -1）永不过期。
	TTL time.Duration `yaml:"ttl" mapstructure:"ttl"`
	// KeyPrefix Redis 后端 key 前缀（仅 backend=redis 生效）；多游戏服共用 Redis 时覆盖以隔离。
	KeyPrefix string `yaml:"key_prefix" mapstructure:"key_prefix"`
}

// Normalize 把零值/非法值回落到默认值。
func (c SessionTokenConfig) Normalize() SessionTokenConfig {
	c.Backend = config.DefString(c.Backend, SessionTokenBackendMemory)
	c.TTL = config.DefDuration(c.TTL, DefaultSessionTokenTTL)
	c.KeyPrefix = config.DefString(c.KeyPrefix, DefaultSessionTokenKeyPrefix)
	if c.Backend != SessionTokenBackendMemory && c.Backend != SessionTokenBackendRedis {
		// 非法 backend 静默回落 memory：此前的实现无任何告警，配置写错时
		// 表现为「token 重启即失效」，排查无痕。
		logger.Warnf("master: session_token.backend=%q invalid (want memory/redis), fallback to memory", c.Backend)
		c.Backend = SessionTokenBackendMemory
	}
	return c
}
