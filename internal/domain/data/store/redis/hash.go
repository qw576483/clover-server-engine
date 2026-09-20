package redis

import (
	"context"
	"fmt"
)

// HSet 设置 hash 字段。value 可以是 map 或 field/value 交替参数。
func (c *Client) HSet(ctx context.Context, key string, values ...any) error {
	return c.rdb.HSet(ctx, key, values...).Err()
}

// HGet 获取 hash 字段值。字段不存在返回 ErrNil。
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	return c.rdb.HGet(ctx, key, field).Result()
}

// HGetAll 获取 hash 所有字段和值。
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.rdb.HGetAll(ctx, key).Result()
}

// HDel 删除一个或多个 hash 字段，返回删除数量。
func (c *Client) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.rdb.HDel(ctx, key, fields...).Result()
}

// HMSet 批量设置 hash 字段（map 形式）。
func (c *Client) HMSet(ctx context.Context, key string, mapVal map[string]any) error {
	return c.rdb.HSet(ctx, key, mapVal).Err()
}

// HMGet 批量获取 hash 字段值，顺序对应 fields 参数。缺失字段返回空字符串。
// Redis HMGet 返回 []interface{}，其中每个元素为 string 或 []byte（取决于编码）。
// 本方法统一转为 []string；nil 元素（字段不存在）置为空串 ""。
func (c *Client) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	vals, err := c.rdb.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, err
	}
	result := make([]string, len(vals))
	for i, v := range vals {
		if v != nil {
			if b, ok := v.([]byte); ok {
				result[i] = string(b)
			} else {
				result[i] = fmt.Sprint(v)
			}
		}
	}
	return result, nil
}

// HExists 判断 hash 中字段是否存在。
func (c *Client) HExists(ctx context.Context, key, field string) (bool, error) {
	return c.rdb.HExists(ctx, key, field).Result()
}

// HLen 返回 hash 字段数。
func (c *Client) HLen(ctx context.Context, key string) (int64, error) {
	return c.rdb.HLen(ctx, key).Result()
}
