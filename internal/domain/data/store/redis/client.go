package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/qw576483/clover-server-engine/internal/shared/config"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// 错误变量
var (
	// ErrNil 表示 Redis 返回空结果（类似 redis.Nil）
	ErrNil = redis.Nil
	// errClosed 客户端已关闭（Close 后调用命令返回该错误而不是 nil 指针 panic）。
	errClosed = errors.New("redis: client is closed")
)

// Client 封装 go-redis/v9，统一支持单实例/集群/哨兵三种拓扑与基础 CRUD 操作。
// 底层持有 redis.UniversalClient 接口，三种拓扑均实现该接口，命令用法一致。

// 各数据类型的命令实现拆分到同包的 string.go / hash.go / list.go / set.go /
// key.go / json.go，本文件只负责连接生命周期与通用辅助。
type Client struct {
	conf    RedisConfig
	mode    string // 实际生效的连接模式
	rdb     redis.UniversalClient
	closeMu sync.RWMutex // 保护 rdb 指针：Close 与并发命令 / 重复 Close 的竞争
}

// NewClient 根据配置创建 Redis 客户端，按模式分派到单实例/集群/哨兵工厂，
// 自动完成连接池初始化与连通性验证。
func NewClient(conf RedisConfig) (*Client, error) {
	conf.PoolSize = config.DefInt(conf.PoolSize, 20)
	conf.MinIdleConns = config.DefInt(conf.MinIdleConns, 5)

	addrs := conf.resolveAddrs()
	mode := conf.resolveMode()
	dial := config.DefDuration(conf.DialTimeout, 5*time.Second)
	read := config.DefDuration(conf.ReadTimeout, 3*time.Second)
	write := config.DefDuration(conf.WriteTimeout, 3*time.Second)
	retries := config.DefInt(conf.MaxRetries, 3)

	var rdb redis.UniversalClient
	switch mode {
	case ModeCluster:
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			Username:     conf.User,
			Password:     conf.Pass,
			PoolSize:     conf.PoolSize,
			MinIdleConns: conf.MinIdleConns,
			DialTimeout:  dial,
			ReadTimeout:  read,
			WriteTimeout: write,
			MaxRetries:   retries,
		})
	case ModeSentinel:
		if conf.MasterName == "" {
			return nil, fmt.Errorf("redis sentinel mode requires master_name")
		}
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       conf.MasterName,
			SentinelAddrs:    addrs,
			SentinelPassword: conf.SentinelPass,
			Username:         conf.User,
			Password:         conf.Pass,
			DB:               conf.DB,
			PoolSize:         conf.PoolSize,
			MinIdleConns:     conf.MinIdleConns,
			DialTimeout:      dial,
			ReadTimeout:      read,
			WriteTimeout:     write,
			MaxRetries:       retries,
		})
	case ModeStandalone:
		rdb = redis.NewClient(&redis.Options{
			Addr:         addrs[0],
			Username:     conf.User,
			Password:     conf.Pass,
			DB:           conf.DB,
			PoolSize:     conf.PoolSize,
			MinIdleConns: conf.MinIdleConns,
			DialTimeout:  dial,
			ReadTimeout:  read,
			WriteTimeout: write,
			MaxRetries:   retries,
		})
	default:
		return nil, fmt.Errorf("redis unknown mode %q (want standalone/cluster/sentinel)", mode)
	}

	c := &Client{conf: conf, mode: mode, rdb: rdb}

	// 验证连通性
	ctx, cancel := context.WithTimeout(context.Background(), dial)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping %v (mode=%s, db=%d): %w", addrs, mode, conf.DB, err)
	}

	logger.Infof("redis connected: mode=%s addrs=%v db=%d pool_size=%d", mode, addrs, conf.DB, conf.PoolSize)
	return c, nil
}

// Mode 返回实际生效的连接模式（standalone/cluster/sentinel）。
func (c *Client) Mode() string { return c.mode }

// Close 关闭连接池，释放所有资源。线程安全，可安全并发调用。
// nil 接收者安全：调用方在构造失败路径上可能持有 nil 客户端。
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeMu.Lock()
	if c.rdb == nil {
		c.closeMu.Unlock()
		return nil
	}
	rdb := c.rdb
	c.rdb = nil
	c.closeMu.Unlock()
	err := rdb.Close()
	logger.Infof("redis client closed: %s (db=%d)", c.conf.Addr, c.conf.DB)
	return err
}

// Raw 返回底层 redis.UniversalClient，供高级场景直接调用原生命令。
// 客户端已关闭时返回 nil，调用方必须判空。
// 需要具体类型（如 *redis.ClusterClient 的 ForEachMaster）时可再类型断言。
func (c *Client) Raw() redis.UniversalClient {
	if c == nil {
		return nil
	}
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	return c.rdb
}

// Config 返回客户端配置副本。
func (c *Client) Config() RedisConfig { return c.conf }

// Ping 执行健康检查。
func (c *Client) Ping(ctx context.Context) error {
	rdb := c.Raw()
	if rdb == nil {
		return errClosed
	}
	return rdb.Ping(ctx).Err()
}

// 内部辅助函数
