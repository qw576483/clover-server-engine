package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// DelByPattern 按前缀/模式批量删除 key。
// 返回删除的总数量和遇到的第一个错误（继续执行不中断）。
func (c *Client) DelByPattern(ctx context.Context, pattern string) (int64, error) {
	keys, err := c.Keys(ctx, pattern)
	if err != nil {
		return 0, fmt.Errorf("redis delbypattern keys %s: %w", pattern, err)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	var total int64
	var firstErr error
	// 每 100 个一批删除，避免单次 DEL 过大
	for i := 0; i < len(keys); i += 100 {
		end := i + 100
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]
		n, delErr := c.Del(ctx, batch...)
		total += n
		if delErr != nil && firstErr == nil {
			firstErr = delErr
		}
	}
	return total, firstErr
}

// ExpireByPattern 按模式批量设置过期时间。返回设置成功的总数量与遇到的第一个错误
// （单 key 失败继续执行不中断，与 DelByPattern 口径一致）。
// 仅对存在的 key 生效，已过期的自动跳过。
func (c *Client) ExpireByPattern(ctx context.Context, pattern string, expiration time.Duration) (int64, error) {
	keys, err := c.Keys(ctx, pattern)
	if err != nil {
		return 0, fmt.Errorf("redis expirebypattern keys %s: %w", pattern, err)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	var total int64
	var firstErr error
	for _, k := range keys {
		ok, expErr := c.Expire(ctx, k, expiration)
		if expErr != nil {
			// 单个失败不中断其余，但必须同时留日志并上报首个错误：
			// NOAUTH/READONLY 等失败只表现为 total 偏小（整体返回 nil 时故障被吞）。
			logger.Warnf("redis: expirebypattern set expire %s failed: %v", k, expErr)
			if firstErr == nil {
				firstErr = expErr
			}
			continue
		}
		if ok {
			total++
		}
	}
	return total, firstErr
}

// GetDel 原子性地获取并删除 key（Redis 原生 GETDEL 单命令，无并发重复消费风险）。
// 适用于一次性消费队列场景；key 不存在返回 ErrNil。
func (c *Client) GetDel(ctx context.Context, key string) (string, error) {
	return c.rdb.GetDel(ctx, key).Result()
}

// GetSetJSON 获取并反序列化 JSON 对象后立即删除 key。
// 适用于一次性读取配置或临时数据。
// 删除失败必须返回错误：静默吞掉删除错误时，key 残留会让下次重复读到
// 同一份数据（一次性消费语义被破坏，且调用方无从察觉）。
func (c *Client) GetSetJSON(ctx context.Context, key string, out any) error {
	err := c.GetJSON(ctx, key, out)
	if err != nil {
		return err
	}
	if _, err := c.Del(ctx, key); err != nil {
		return fmt.Errorf("redis getsetjson del %s: %w", key, err)
	}
	return nil
}

// Touch 更新 key 的过期时间为当前时间 + expiration（类似 UNIX touch）。
// 不存在则返回 false。
func (c *Client) Touch(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	exists, err := c.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	if exists == 0 {
		return false, nil
	}
	return c.Expire(ctx, key, expiration)
}

// 计数器辅助（游戏常用：限流、排行榜、在线统计）
// IncrAndGet 自增并返回新值（原子操作）。
func (c *Client) IncrAndGet(ctx context.Context, key string) (int64, error) {
	return c.Incr(ctx, key)
}

// setIfGreaterScript 原子 CAS：仅当当前值不存在或小于新值时设置；返回 1 表示已更新，0 表示未更新。
// 显式检查 tonumber 返回值——若已存值为非数字串（如被其他调用方非预期写入），
// tonumber 返回 nil 导致比较静默跳过；现返回 -1 表示"值非法"，Go 侧据此记录错误。
//
// ARGV[1]=新值，ARGV[2]=TTL 毫秒（>0 时首次写入即带 TTL，否则不过期）。
// 「key 不存在」分支必须**同样**带上 TTL：只 set 会让首次写入的键永久驻留，
// 而同键的后续更新会保留剩余 TTL —— 同一个键一会儿永久、一会儿过期。
var setIfGreaterScript = redis.NewScript(`
	local cur = redis.call('get', KEYS[1])
	if cur == false then
		local ttl = tonumber(ARGV[2])
		if ttl and ttl > 0 then
			redis.call('set', KEYS[1], ARGV[1], 'PX', ttl)
		else
			redis.call('set', KEYS[1], ARGV[1])
		end
		return 1
	end
	cur = tonumber(cur)
	if cur == nil then
		return -1
	end
	local new = tonumber(ARGV[1])
	if new == nil then
		return -1
	end
	if new > cur then
		-- 保留原 key 的 TTL（若存在），避免 CAS 成功后 key 变为永久驻留。
		local ttl = redis.call('pttl', KEYS[1])
		if ttl > 0 then
			redis.call('set', KEYS[1], ARGV[1], 'PX', ttl)
		else
			redis.call('set', KEYS[1], ARGV[1])
		end
		return 1
	end
	return 0
`)

// setIfLessScript 原子 CAS：仅当当前值不存在或大于新值时设置；返回 1 表示已更新，0 表示未更新。
// tonumber nil check 同上；首次写入带 TTL 的理由同 setIfGreaterScript（ARGV[2]=TTL 毫秒）。
var setIfLessScript = redis.NewScript(`
	local cur = redis.call('get', KEYS[1])
	if cur == false then
		local ttl = tonumber(ARGV[2])
		if ttl and ttl > 0 then
			redis.call('set', KEYS[1], ARGV[1], 'PX', ttl)
		else
			redis.call('set', KEYS[1], ARGV[1])
		end
		return 1
	end
	cur = tonumber(cur)
	if cur == nil then
		return -1
	end
	local new = tonumber(ARGV[1])
	if new == nil then
		return -1
	end
	if new < cur then
		-- 保留原 key 的 TTL（若存在），避免 CAS 成功后 key 变为永久驻留。
		local ttl = redis.call('pttl', KEYS[1])
		if ttl > 0 then
			redis.call('set', KEYS[1], ARGV[1], 'PX', ttl)
		else
			redis.call('set', KEYS[1], ARGV[1])
		end
		return 1
	end
	return 0
`)

// ttlMillis 把 TTL 归一为脚本参数（毫秒）。
// 语义：0 或负值 = 不过期。负值属调用方编程错误（Redis 的 `PX -1` 会**直接删除** key），
// 这里夹紧为 0（不过期）并留痕，绝不把负数透传给脚本。
// 不足 1ms 的正值向上取整为 1ms：向下取整会变成 0（= 不过期），与本意相反。
func ttlMillis(key string, expiration time.Duration) int64 {
	if expiration <= 0 {
		if expiration < 0 {
			n := ttlNegCount.Add(1)
			if n == 1 || n%1000 == 0 {
				logger.Warnf("redis: CAS 收到负 TTL %v（key=%s），已按「不过期」处理（累计 %d 次）", expiration, key, n)
			}
		}
		return 0
	}
	ms := expiration.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return ms
}

// ttlNegCount 负 TTL 的降频计数（见 ttlMillis）。
var ttlNegCount atomic.Uint64

// SetIfGreater 仅当新值大于当前值时才设置（用于记录最大值/最高分）。
// 通过 Lua 脚本原子完成「读取-比较-写入」，消除并发下的丢失更新。返回是否执行了更新。
// Lua 返回 -1 表示已存值非法（非数字），Go 侧包装为错误。
//
// expiration 为 key 的存活时长（**首次写入同样生效**，0 或负值 = 不过期）；
// key 已存在时脚本保留其**原有剩余 TTL**（不按 expiration 重设）。
func (c *Client) SetIfGreater(ctx context.Context, key string, value int64, expiration time.Duration) (bool, error) {
	res, err := setIfGreaterScript.Run(ctx, c.rdb, []string{key}, value, ttlMillis(key, expiration)).Int64()
	if err != nil {
		return false, fmt.Errorf("redis setifgreater %s: %w", key, err)
	}
	if res == -1 {
		return false, fmt.Errorf("redis setifgreater %s: existing value is not a number", key)
	}
	return res == 1, nil
}

// SetIfLess 仅当新值小于当前值时才设置（用于记录最小值/最低分）。
// 通过 Lua 脚本原子完成「读取-比较-写入」，消除并发下的丢失更新。返回是否执行了更新。
// Lua 返回 -1 表示已存值非法（非数字），Go 侧包装为错误。
// expiration 语义同 SetIfGreater（首次写入即带 TTL；已存在时保留原有剩余 TTL）。
func (c *Client) SetIfLess(ctx context.Context, key string, value int64, expiration time.Duration) (bool, error) {
	res, err := setIfLessScript.Run(ctx, c.rdb, []string{key}, value, ttlMillis(key, expiration)).Int64()
	if err != nil {
		return false, fmt.Errorf("redis setifless %s: %w", key, err)
	}
	if res == -1 {
		return false, fmt.Errorf("redis setifless %s: existing value is not a number", key)
	}
	return res == 1, nil
}
