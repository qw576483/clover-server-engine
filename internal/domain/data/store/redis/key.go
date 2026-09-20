package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Keys 返回匹配模式的所有 key（生产环境慎用大数据集，会阻塞 Redis）。
// 集群模式下会遍历所有 master 节点，保证跨 slot 的 key 不遗漏。
func (c *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	// 集群模式：key 按 slot 分布在多个 master，需逐节点 KEYS 再汇总。
	if cc, ok := c.rdb.(*redis.ClusterClient); ok {
		var (
			keys []string
			mu   sync.Mutex
		)
		err := cc.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			local, err := master.Keys(ctx, pattern).Result()
			if err != nil {
				return err
			}
			mu.Lock()
			keys = append(keys, local...)
			mu.Unlock()
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("redis cluster keys %s: %w", pattern, err)
		}
		return keys, nil
	}
	// 单实例/哨兵：单节点直接匹配。
	return c.rdb.Keys(ctx, pattern).Result()
}

// Type 返回 key 的类型（string/hash/list/set/zset/none）。
func (c *Client) Type(ctx context.Context, key string) (string, error) {
	return c.rdb.Type(ctx, key).Result()
}

// Persist 移除 key 的过期时间，使其永久保存。返回 false 表示 key 无过期或不存在。
func (c *Client) Persist(ctx context.Context, key string) (bool, error) {
	return c.rdb.Persist(ctx, key).Result()
}
