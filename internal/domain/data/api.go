package data

import (
	"context"
	"fmt"
	"time"

	"clover-server-engine/internal/shared/retry"
	ujson "clover-server-engine/pkg/shared/json"
)

// // 读写 API（核心存二进制 []byte，JSON 为封装）
// 分派逻辑已从全局 Mode 迁移至各 Schema.Tier。
// // Save 写入一条数据。data 为原始字节（二进制或 JSON 原文均可）。
func (s *Store) Save(ctx context.Context, key Key, data []byte) error {
	if key.Owner == "" || key.ID == "" || key.Type == "" {
		return fmt.Errorf("data: owner kind, id and type must not be empty")
	}
	switch s.getTier(key) {
	case TierRedis:
		rc, err := s.requireRedis()
		if err != nil {
			return err
		}
		if err := rc.Raw().Set(ctx, s.redisKey(key), data, s.cacheTTL).Err(); err != nil {
			return err
		}
		return nil
	case TierRedisMySQL:
		rc, err := s.requireRedis()
		if err != nil {
			return err
		}
		// 先写值、后标脏：反过来（先标脏、后写值）时，并发 Flush 可在 Set 完成前
		// 取走并清除脏标记、再读到 Redis 里的旧值落库 → 本次新值因脏标记已清而永不落库。
		if err := rc.Raw().Set(ctx, s.redisKey(key), data, s.cacheTTL).Err(); err != nil {
			// Set 失败：不清脏标记（此前一次成功写入可能仍待落库），原样返回错误。
			return err
		}
		s.markDirty(key)
		return nil
	case TierMemory:
		s.memSet(ctx, key, data)
		return nil
	case TierSnapshot:
		if !s.isOnline(key.Owner, key.ID) {
			// 玩家不在本节点在线：写 Redis（离线事件跨节点共享）
			return s.saveRedis(ctx, key, data)
		}
		return s.saveMMO(ctx, key, data)
	default:
		return fmt.Errorf("data: unknown tier for key %s/%s/%s", key.Owner, key.ID, key.Type)
	}
}

// SaveJSON 将 v 经 ujson.Marshal 序列化后写入。
func (s *Store) SaveJSON(ctx context.Context, key Key, v any) error {
	b, err := ujson.Marshal(v)
	if err != nil {
		return fmt.Errorf("data: marshal json: %w", err)
	}
	return s.Save(ctx, key, b)
}

// Load 读取一条数据，返回原始字节。不存在返回 ErrNotFound。
func (s *Store) Load(ctx context.Context, key Key) ([]byte, error) {
	if key.Owner == "" || key.ID == "" || key.Type == "" {
		return nil, fmt.Errorf("data: owner kind, id and type must not be empty")
	}
	switch s.getTier(key) {
	case TierRedis:
		return s.getCache(ctx, key)
	case TierRedisMySQL:
		return s.getCache(ctx, key)
	case TierMemory:
		return s.getMemory(ctx, key)
	case TierSnapshot:
		if !s.isOnline(key.Owner, key.ID) {
			// 玩家不在本节点在线：读 Redis → MySQL 兜底
			return s.getCache(ctx, key)
		}
		return s.getMMO(ctx, key)
	default:
		return nil, fmt.Errorf("data: unknown tier for key %s/%s/%s", key.Owner, key.ID, key.Type)
	}
}

// LoadJSON 读取并反序列化 JSON 到 v。不存在返回 ErrNotFound。
func (s *Store) LoadJSON(ctx context.Context, key Key, v any) error {
	b, err := s.Load(ctx, key)
	if err != nil {
		return err
	}
	if err := ujson.Unmarshal(b, v); err != nil {
		return fmt.Errorf("data: unmarshal json: %w", err)
	}
	return nil
}

// LoadDirect 绕过所有缓存层，直读持久层。
func (s *Store) LoadDirect(ctx context.Context, key Key) ([]byte, error) {
	if key.Owner == "" || key.ID == "" || key.Type == "" {
		return nil, fmt.Errorf("data: owner kind, id and type must not be empty")
	}
	switch s.getTier(key) {
	case TierRedis:
		return s.getCache(ctx, key)
	case TierRedisMySQL:
		if s.mysql == nil {
			return s.Load(ctx, key)
		}
		if s.isDirty(key) {
			// 与 TierSnapshot 分支同一防护口径：脏键（Redis 里存在比 MySQL 新的
			// 待落库值）时直读 MySQL 会拿到 stale 值，显式报错好过静默返回旧数据。
			return nil, fmt.Errorf("data: LoadDirect key %s is dirty, cannot bypass cache", s.redisKey(key))
		}
		return s.getMySQL(ctx, key)
	case TierMemory:
		return s.getMemory(ctx, key)
	case TierSnapshot:
		if !key.NoLocalCache {
			if b, err := s.getMemory(ctx, key); err == nil {
				return b, nil
			}
		}
		if s.mysql == nil {
			return s.Load(ctx, key)
		}
		if s.isDirty(key) {
			return nil, fmt.Errorf("data: LoadDirect key %s is dirty, cannot bypass in-memory store", s.redisKey(key))
		}
		return s.getMySQL(ctx, key)
	default:
		return nil, fmt.Errorf("data: unknown tier for key %s/%s/%s", key.Owner, key.ID, key.Type)
	}
}

// ReceiveMirror 跨服镜像：将远端数据写入本地（TierMemory 写内存；TierSnapshot 写内存 + Redis + 标脏）。
//
// TierSnapshot 必须同时写 Redis：离线玩家的 Load 走 getCache（Redis→MySQL、从不读进程内内存），
// 只 memSet 会让镜像数据对离线玩家完全不可见（静默失效）。
func (s *Store) ReceiveMirror(ctx context.Context, key Key, data []byte) error {
	switch s.getTier(key) {
	case TierSnapshot:
		s.memSet(ctx, key, data)
		rc, err := s.requireRedis()
		if err != nil {
			return err
		}
		if err := rc.Raw().Set(ctx, s.redisKey(key), data, s.cacheTTL).Err(); err != nil {
			return fmt.Errorf("data: receive mirror %s: %w", s.redisKey(key), err)
		}
		// 标脏：让后续 Flush / 周期落盘把镜像落到 MySQL（与 saveRedis 同语义）。
		s.markDirty(key)
		return nil
	case TierMemory:
		s.memSet(ctx, key, data)
		return nil
	default:
		// TierRedisMySQL / TierRedis 无本地内存，跨服镜像无意义，静默跳过。
		return nil
	}
}

// delCache 删除 Redis 缓存键，带 3 次指数退避重试（50ms 起步、倍率 2）。
//
// 抽出来是因为 TierRedis / TierRedisMySQL 两个分支**逐字复制**了同一段重试循环，
// 而退避曲线又由 retry 包统一提供（不再手写 `*= 2`）。
func (s *Store) delCache(ctx context.Context, key Key) error {
	rc, err := s.requireRedis()
	if err != nil {
		return err
	}
	rk := s.redisKey(key)
	policy := retry.Policy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, Multiplier: 2}
	if err := retry.Do(ctx, policy, func(int) error {
		return rc.Raw().Del(ctx, rk).Err()
	}); err != nil {
		return fmt.Errorf("data: cache delete: redis del %s after 3 retries: %w", rk, err)
	}
	return nil
}

// Delete 删除一条数据。
func (s *Store) Delete(ctx context.Context, key Key) error {
	if key.Owner == "" || key.ID == "" || key.Type == "" {
		return fmt.Errorf("data: owner kind, id and type must not be empty")
	}
	switch s.getTier(key) {
	case TierRedis:
		// 纯 Redis：直接删除 Redis 键
		return s.delCache(ctx, key)
	case TierRedisMySQL:
		// 先删 MySQL 源，再删 Redis 缓存。反向（先删缓存）的失败模式更糟：
		// 缓存删除成功、源删除失败时 MySQL 行永久残留，下次 Load 回源会把旧值回填 Redis
		// ——删除被「复活」，且没有任何重试路径；而本顺序下源删除失败时缓存仍在，
		// 数据原样保留（删除完全未发生），调用方可安全重试。
		if s.mysql != nil {
			if _, err := s.mysql.Exec(ctx, s.deleteSQL(), key.Owner, key.ID, key.Type); err != nil {
				// 不再 markDirty：脏标记语义是「Redis 里存在比 MySQL 新的待落库值」，
				// 与「删除待落库」完全相反——标脏只会让 persistDirty 读不到值并报
				// data lost（误导），删除也永远不会重试。这里原样返回错误由调用方重试。
				return fmt.Errorf("data: delete %s: mysql delete failed (retry the delete): %w", s.redisKey(key), err)
			}
		}
		if err := s.delCache(ctx, key); err != nil {
			// 源已删除、缓存删除失败：必须清脏，否则残留脏标记会让后续 Flush
			// 把 Redis 里的旧值 upsert 回 MySQL（删除复活）。
			s.unmarkDirty(key)
			return err
		}
		// 脏标记**最后**才清：它记的是「Redis 里存在比 MySQL 新的待落库值」。
		// 在前面任何一步失败时就清掉，等于把那次更新的落库义务一起丢掉——
		// Redis 值随 TTL 过期后，MySQL 侧就永久落后了（且没有任何路径能发现）。
		s.unmarkDirty(key)
		return nil
	case TierMemory:
		s.memDel(key)
		return nil
	case TierSnapshot:
		return s.delMMO(ctx, key)
	default:
		return fmt.Errorf("data: unknown tier for key %s/%s/%s", key.Owner, key.ID, key.Type)
	}
}

// saveRedis 写入 Redis 并标脏（不写内存）。用于 TierSnapshot 模式的离线写路径：
// 玩家不在线时事件写入走 Redis，确保跨节点 LoadAll / Load 可读。
func (s *Store) saveRedis(ctx context.Context, key Key, data []byte) error {
	rc, err := s.requireRedis()
	if err != nil {
		return err
	}
	// 先写值、后标脏：标脏早于 Set 会让并发 Flush 读到旧值落库并清掉脏标记（丢更新）。
	if err := rc.Raw().Set(ctx, s.redisKey(key), data, s.cacheTTL).Err(); err != nil {
		return err
	}
	s.markDirty(key)
	return nil
}
