// Package data 通用数据存储抽象层。

// 每个 Schema 独立声明存储等级（Tier），同一 Store 可混用四种策略：
//   - TierMemory：纯进程内存（Go map），不依赖 Redis/MySQL，进程退出即丢失。适合本地开发、单测、房间/对局内热数据。
//   - TierRedis：纯 Redis 存储，不依赖 MySQL。Save 写 Redis，Load 读 Redis。适合每日缓存、会话数据、临时状态等不需要 MySQL 持久化的场景。
//   - TierRedisMySQL：Redis 热缓存 + MySQL 冷持久化：Save 标脏 → 写 Redis，Load 优先 Redis → 回源 MySQL；
//     Flush 将脏数据批量落 MySQL。适合玩家存档、装备、道具等需持久化的数据。
//   - TierSnapshot：内存主存 + MySQL 快照落盘：内存为权威源，Load 内存优先 → 回源 MySQL 并回填；
//     Flush 将内存快照写入 MySQL。适合 MMO 实体（高频读写、需崩溃恢复但不需逐帧持久化）。

// 数据以 (owner_type, owner_id, type) 为联合主键，对应表结构

//	data(owner_type, owner_id, type, data LONGBLOB, updated_at)

// 任何业务实体（账号 / 玩家 / 服务器 / 场景 / 对象 / 军团 / 分组 / 房间 / 虚拟服 ...）都只是
// owner_type 的一个取值，统一用 Key{Owner, ID, Type} 表达，SQL 与 Redis 键自动派生。
package data

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	imysql "github.com/qw576483/clover-server-engine/internal/domain/data/store/mysql"
	iredis "github.com/qw576483/clover-server-engine/internal/domain/data/store/redis"

	pdata "github.com/qw576483/clover-server-engine/pkg/domain/data"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/validate"
)

// P3 注册：将工厂函数注册到 pkg/domain/data 门面
func init() {
	pdata.RegisterStoreFactory(func(cfg pdata.Config) (pdata.Store, error) {
		return NewStore(cfg.(Config))
	})
	pdata.RegisterMemoryConfig(func() pdata.Config { return MemoryConfig() })
	pdata.RegisterDefaultConfig(func() pdata.Config { return DefaultConfig() })
	pdata.RegisterMMOConfig(func() pdata.Config { return MMOConfig() })
	pdata.RegisterRedisConfig(func() pdata.Config { return RedisConfig() })
}

// persistentStore 持久层抽象（MySQL 实现），便于依赖注入与单测替换。
type persistentStore interface {
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	Get(ctx context.Context, dest any, query string, args ...any) error
	Select(ctx context.Context, dest any, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Close() error
}

// memEntry 进程内缓存条目（memory / mmo 模式共用）。挂在 LRU 链表节点上，避免二次映射表。
type memEntry struct {
	key  Key
	data []byte
}

// Store 数据存储抽象。业务层只依赖此类型，不感知底层是 Redis 还是 MySQL。

// 每个 Schema 独立声明 Tier，同一 Store 可混用 TierMemory/TierRedisMySQL/TierSnapshot：
// - TierMemory：纯进程内存，不连 Redis/MySQL。
// - TierRedisMySQL：Redis 热缓存 + MySQL 冷持久化，Flush/周期落盘。
// - TierSnapshot：内存主存 + MySQL 快照落盘 + LRU 淘汰。
type Store struct {
	cfg      Config
	tier     StorageTier     // 默认存储等级（Schema 未注册/未声明 Tier 时的兜底）
	redis    *iredis.Client  // TierMemory 可为 nil
	mysql    persistentStore // TierMemory 可为 nil
	prefix   string
	table    string
	cacheTTL time.Duration

	// 分片：进程内存储（memory / mmo 模式）与脏标记按 Key 哈希分散到多个分片，
	// 每片独立 RWMutex，缓解 MMO 高并发下的全局锁串行。
	shards    []storeShard
	numShards uint64

	// memory / mmo 模式的全局 LRU 容量（仅 mmo 模式生效，>0 时超出即按分片 LRU 淘汰，
	// 脏数据淘汰前先落库）。容量均摊到各分片（见 storeShard.shardCapacity）。
	maxMemory int

	flushMu   sync.Mutex    // 保护 flushStop 的取出/置空，防止并发 Close 重复 close
	flushStop chan struct{} // mmo 周期落盘停止信号（nil=未启用）
	flushDone chan struct{} // mmo 周期落盘 goroutine 退出确认

	online    playerOnlineSet // 本节点在线玩家集合，用于离线写自动切 Redis
	closeOnce sync.Once       // Close 幂等：重复 / 并发关闭只真正执行一次
	closeErr  error           // 首次 Close 的返回值，供后续重复调用复用
}

// NewStore 根据配置创建 Store。按需建立底层连接（含连通性 Ping 校验）。
// 所有 Schema 均支持三种 Tier 混用，具体存储链路由各 Schema.Tier 决定。
func NewStore(cfg Config) (*Store, error) {
	if cfg.Tier == tierNotSet {
		cfg.Tier = TierRedisMySQL
		logger.Infof("data: tier not specified, defaulting to TierRedisMySQL")
	}

	if cfg.RedisKeyPrefix == "" {
		cfg.RedisKeyPrefix = defaultRedisKeyPrefix
	}
	cfg.Table = sanitizeTable(cfg.Table)

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	s := &Store{
		cfg:       cfg,
		prefix:    cfg.RedisKeyPrefix,
		table:     cfg.Table,
		cacheTTL:  cfg.CacheTTL,
		maxMemory: cfg.MaxMemory,
		tier:      cfg.Tier,
	}
	s.initShards()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if s.tier != TierMemory {
		rdb, err := iredis.NewClient(cfg.Redis)
		if err != nil {
			return nil, err
		}
		s.redis = rdb

		// TierRedis 只需要 Redis，不需要 MySQL
		if s.tier != TierRedis {
			if err := s.initMySQL(ctx); err != nil {
				// MySQL 初始化失败时关闭已创建的 Redis 连接，防止泄漏。
				// 注意：initMySQL 的建表失败分支可能已把 s.redis 置 nil，必须先判空——
				// 否则会以 nil 接收者调用 Close（panic）。
				if s.redis != nil {
					_ = s.redis.Close()
				}
				return nil, err
			}
		}
	}

	// 以本 Store 是否具备 MySQL 后端决定启用周期落盘。此前用全局
	// pdata.TierHasPersistent() 判断：Schema 注册表是全局的，与单个 Store 的
	// Tier/配置解耦——TierRedisMySQL 的 Store 在「无任何 Schema 注册持久 Tier」时
	// 周期落盘不会启动，脏数据只能等 Close 终落（叠加 Flush 的同类门控即彻底不落库）。
	if s.mysql != nil {
		s.initPeriodicFlush(&cfg)
	}
	return s, nil
}

// validateConfig 校验必需后端连接参数是否齐备。
// TierMemory 不需要 Redis/MySQL，跳过校验；TierRedis 只需要 Redis；其余 Tier 必须配置 Redis 和 MySQL。
func validateConfig(cfg Config) error {
	if cfg.Tier == TierMemory {
		return nil
	}

	// tag 级校验（required/min 等）。
	if err := validate.Struct(cfg.Redis); err != nil {
		return fmt.Errorf("data: redis config invalid: %w", err)
	}
	// TierRedis 只需要 Redis，不需要 MySQL
	if cfg.Tier != TierRedis {
		if err := validate.Struct(cfg.MySQL); err != nil {
			return fmt.Errorf("data: mysql config invalid: %w", err)
		}
	}

	// 业务语义校验（validate tag 无法表达的逻辑约束）
	// Redis 配置校验
	if cfg.Redis.Addr == "" && len(cfg.Redis.Addrs) == 0 && cfg.Redis.MasterName == "" {
		return fmt.Errorf("data: redis config required (redis.addr/addrs/master_name), none set")
	}
	// Sentinel 模式下 MasterName 必须非空
	if cfg.Redis.MasterName != "" && len(cfg.Redis.Addrs) == 0 {
		return fmt.Errorf("data: sentinel mode requires redis.addrs (sentinel addresses)")
	}
	if cfg.Redis.DB < 0 || cfg.Redis.DB > 15 {
		return fmt.Errorf("data: redis.db must be 0-15, got %d", cfg.Redis.DB)
	}
	if cfg.Redis.DialTimeout > 0 && cfg.Redis.DialTimeout < 100*time.Millisecond {
		return fmt.Errorf("data: redis.dial_timeout too small (%v), minimum 100ms", cfg.Redis.DialTimeout)
	}
	// MySQL 配置校验（TierRedis 不需要 MySQL）
	if cfg.Tier != TierRedis {
		if cfg.MySQL.Port > 65535 {
			return fmt.Errorf("data: mysql.port must be 1-65535, got %d", cfg.MySQL.Port)
		}
		if cfg.MySQL.DialTimeout > 0 && cfg.MySQL.DialTimeout < 100*time.Millisecond {
			return fmt.Errorf("data: mysql.dial_timeout too small (%v), minimum 100ms", cfg.MySQL.DialTimeout)
		}
	}
	return nil
}

// initMySQL 建立 MySQL 连接 + auto-create table。
func (s *Store) initMySQL(ctx context.Context) error {
	mdb, err := newMySQLBackend(s.cfg.MySQL)
	if err != nil {
		return err
	}
	s.mysql = mdb
	if s.cfg.AutoCreateTable {
		if err := s.CreateTable(ctx); err != nil {
			// 直接关闭已建立的连接，不走 s.Close() 以免消耗 closeOnce，
			// 确保调用方的 defer Close 仍能执行完整的 doClose（含终落库）。
			// 将已关闭字段置 nil，防止后续被误用导致 panic。
			if s.mysql != nil {
				if err := s.mysql.Close(); err != nil {
					logger.Errorf("data: close mysql failed: %v", err)
				}
				s.mysql = nil
			}
			if s.redis != nil {
				if err := s.redis.Close(); err != nil {
					logger.Errorf("data: close redis failed: %v", err)
				}
				s.redis = nil
			}
			return err
		}
	}
	return nil
}

// initPeriodicFlush 按配置启动周期落盘。FlushInterval<=0 时不启动周期 goroutine，
// 仅依赖 Store.Close → doClose → 终落盘（适合 MMO 下线落模式 + 容灾回调）。
func (s *Store) initPeriodicFlush(cfg *Config) {
	s.startPeriodicFlush(cfg.FlushInterval)
}

// defaultRedisKeyPrefix 引擎默认 Redis key 前缀。
// 所有默认配置经此常量取值——此前同一字符串在本文件里写了五遍，改前缀时漏改一处
// 就会让不同 tier 的 key 空间分裂。
const defaultRedisKeyPrefix = "clover:data"

// DefaultConfig 返回本地开发可用的默认配置（TierRedisMySQL），Redis 为热缓存，
// 每 30 秒将脏数据刷入 MySQL，避免 Redis 故障丢数据。
func DefaultConfig() Config {
	return Config{
		Tier:            TierRedisMySQL,
		Redis:           iredis.DefaultConfig(),
		MySQL:           imysql.DefaultConfig(),
		RedisKeyPrefix:  defaultRedisKeyPrefix,
		CacheTTL:        0,
		Table:           "data",
		AutoCreateTable: false,
		FlushInterval:   30 * time.Second,
	}
}

// RedisConfig 返回「纯 Redis」的默认配置（TierRedis，不依赖 MySQL，适合每日缓存）。
func RedisConfig() Config {
	return Config{
		Tier:           TierRedis,
		Redis:          iredis.DefaultConfig(),
		RedisKeyPrefix: defaultRedisKeyPrefix,
		CacheTTL:       24 * time.Hour, // 默认 24 小时过期
	}
}

// MemoryConfig 返回「纯进程内存」的默认配置（TierMemory，不依赖 Redis/MySQL，进程退出即丢失）。
func MemoryConfig() Config {
	return Config{
		Tier:           TierMemory,
		RedisKeyPrefix: defaultRedisKeyPrefix,
		Table:          "data",
	}
}

// MMOConfig 返回「内存为主 + MySQL 持久化」的默认配置（TierSnapshot）。
// 内存为权威源，MySQL 仅作快照容灾。默认每 15 分钟落一次快照；
// 设 FlushInterval=0 可完全关闭周期落盘（仅依赖进程 Close 终落，即「下线落」模式）。
//
// 必须带 Redis 配置：TierSnapshot 的离线写路径（saveRedis/getMMO 冷加载）依赖 Redis，
// validateConfig 也要求 Redis（含 PoolSize>=1）；此前产出零值 Redis 配置会让
// NewStore(MMOConfig()) 必然报 "redis config invalid" —— 该「默认配置」不可用。
func MMOConfig() Config {
	return Config{
		Tier:           TierSnapshot,
		Redis:          iredis.DefaultConfig(),
		MySQL:          imysql.DefaultConfig(),
		RedisKeyPrefix: defaultRedisKeyPrefix,
		Table:          "data",
		FlushInterval:  15 * time.Minute,
	}
}

// Tier 返回 Store 默认的存储等级。
func (s *Store) Tier() StorageTier { return s.tier }

// getTier 返回 key 对应 Schema 的存储等级。Schema 已注册且 Tier>0 → 使用声明值，否则兜底 s.tier。
func (s *Store) getTier(key Key) StorageTier {
	if t, ok := pdata.TierGet(key.Owner, key.Type); ok && t != tierNotSet {
		return t
	}
	return s.tier
}

// needMySQLPersist 是否需要 MySQL 持久化（TierRedisMySQL 或 TierSnapshot）。
func (s *Store) needMySQLPersist(key Key) bool {
	t := s.getTier(key)
	return t == TierRedisMySQL || t == TierSnapshot
}

// Table 返回当前生效的表名。
func (s *Store) Table() string { return s.table }

// RedisClient 返回底层 iredis.Client（供 Redis 原生操作使用）。
// 注意：TierMemory 模式不连 Redis，返回 nil，调用方需判空。
func (s *Store) RedisClient() *iredis.Client { return s.redis }

// MySQLClient 返回底层 imysql.Client（供业务做原生 SQL 操作）。
// TierMemory 模式没有持久层，返回 nil；调用方必须判空（此前注释声称「所有模式均非 nil」，
// 按注释直接解引用会 nil panic）。
func (s *Store) MySQLClient() *imysql.Client {
	c, ok := s.mysql.(*imysql.Client)
	if !ok {
		return nil
	}
	return c
}

// requireRedis 返回 Redis 客户端；Store 未配置 Redis（TierMemory，或按 Schema 混用
// 却声明了 Redis 系 Tier）或客户端已被 Close 时返回明确错误，避免调用方对 nil 解引用 panic。
func (s *Store) requireRedis() (*iredis.Client, error) {
	if s.redis == nil {
		return nil, fmt.Errorf("data: redis not configured (schema tier requires redis)")
	}
	if s.redis.Raw() == nil {
		// Client.Close 会把底层句柄置 nil：此时继续发命令就是 nil 解引用 panic。
		return nil, fmt.Errorf("data: redis client is closed")
	}
	return s.redis, nil
}

// redisKey 组合 Redis 键：前缀:kind:id:type。
func (s *Store) redisKey(k Key) string {
	return s.prefix + ":" + string(k.Owner) + ":" + k.ID + ":" + k.Type
}

// Close 释放底层连接（Redis / MySQL）。mmo 模式先停止周期落盘并终落脏数据，避免进程退出丢数据。
// 幂等且并发安全：重复调用（如 initMySQL 失败内部已 Close，调用方又 defer Close）
// 只会真正释放一次，避免二次 Flush / 重复 close(channel) panic。
func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.doClose() })
	return s.closeErr
}

func (s *Store) doClose() error {
	var firstErr error
	// 停止周期落盘 goroutine。
	// stopPeriodicFlush 内部对未启用的情况幂等空操作，故无副作用。
	s.stopPeriodicFlush()
	// 退出前需终落库避免丢数据。以本 Store 是否具备 MySQL 后端为准：
	// 此前用全局 pdata.TierHasPersistent() 门控，与 Store 自身 Tier 解耦——
	// 默认 Tier 的 Store 在无任何 Schema 注册持久 Tier 时，进程退出也不落盘脏数据。
	if s.mysql != nil {
		// 终落库最多重试 3 次（指数退避），避免单次网络抖动导致进程退出时丢脏数据。
		for retry := 0; retry < 3; retry++ {
			// Flush 可能因底层连接已关闭而 panic，必须 recover 兜住，
			// 否则关闭期 panic 直接崩进程（与 stopPeriodicFlush 保持一致）。
			err := func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("data: close flush panic: %v", r)
					}
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				return s.Flush(ctx)
			}()
			if err == nil {
				firstErr = nil // 重试成功须清除前次失败，避免 Close 误报错误
				break
			}
			firstErr = err
			logger.Errorf("data: close flush attempt %d: %v", retry+1, err)
			time.Sleep(time.Duration(200<<uint(retry)) * time.Millisecond) // 200/400/800ms
		}
		// 终落库全部失败：统计未落库脏键数并记录日志，供运维手动恢复参考。
		if firstErr != nil {
			dirtyCount := 0
			for i := range s.shards {
				sh := &s.shards[i]
				sh.mu.RLock()
				dirtyCount += len(sh.dirty)
				sh.mu.RUnlock()
			}
			if dirtyCount > 0 {
				logger.Errorf("data: close final flush failed after 3 retries, %d dirty keys permanently lost", dirtyCount)
			}
		}
	}
	if s.redis != nil {
		if err := s.redis.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.mysql != nil {
		if err := s.mysql.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
