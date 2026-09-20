package redis

import (
	"context"
)

// LPush 将一个或多个值插入列表头部。
func (c *Client) LPush(ctx context.Context, key string, values ...any) error {
	return c.rdb.LPush(ctx, key, values...).Err()
}

// RPush 将一个或多个值插入列表尾部。
func (c *Client) RPush(ctx context.Context, key string, values ...any) error {
	return c.rdb.RPush(ctx, key, values...).Err()
}

// LPop 移除并返回列表头部元素。空列表返回 ErrNil。
func (c *Client) LPop(ctx context.Context, key string) (string, error) {
	return c.rdb.LPop(ctx, key).Result()
}

// RPop 移除并返回列表尾部元素。
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	return c.rdb.RPop(ctx, key).Result()
}

// LLen 返回列表长度。
func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	return c.rdb.LLen(ctx, key).Result()
}

// LRange 获取列表指定范围内的元素（支持负索引）。
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.rdb.LRange(ctx, key, start, stop).Result()
}
