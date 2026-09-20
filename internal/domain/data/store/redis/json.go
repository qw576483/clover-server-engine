package redis

import (
	ujson "clover-server-engine/pkg/shared/json"
	"context"
	"fmt"
	"time"
)

// GetJSON 获取字符串值并反序列化到 out。key 不存在返回 ErrNil。
func (c *Client) GetJSON(ctx context.Context, key string, out any) error {
	data, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	return ujson.Unmarshal([]byte(data), out)
}

// SetJSON 序列化 value 后存入 Redis。
func (c *Client) SetJSON(ctx context.Context, key string, value any, expiration time.Duration) error {
	data, err := ujson.Marshal(value)
	if err != nil {
		return fmt.Errorf("redis setjson marshal: %w", err)
	}
	return c.Set(ctx, key, data, expiration)
}
