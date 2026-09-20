package data

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	imysql "clover-server-engine/internal/domain/data/store/mysql"
	"clover-server-engine/pkg/foundation/logger"
)

// 手动落库（simple / mmo 模式）
// persistDirty 把单条脏键的当前值落库到 MySQL，成功清脏标记；读/写失败时重标脏以便重试。
// readValue 由各模式提供：cache 从 Redis 读，mmo 从进程内 map 读。

// 并发安全要点：进入时先清除脏标记并捕获"是否原本脏"，再做网络 I/O。
// 这样若并发 Save 在"读源后、落库前"写入了新值并重标脏，我们的读会拿到最新值落库，
// 且脏标记被并发 Save 重新置位，不会因本函数末尾的 unmark 而丢失。
// 源已无数据（缓存失效 / 内存已被删）时返回 found=false，无内容可落，脏标记已清。

// mmo 模式在分片锁内捕获内存值快照，避免 unlock 后 evictLRU 摘除导致 readValue
// 漏读（拿不到数据 / 拿到被覆盖的旧值）。
func (s *Store) persistDirty(ctx context.Context, k Key, readValue func() ([]byte, bool, error)) error {
	sh := s.shardOf(k)
	sh.mu.Lock()
	_, isDirty := sh.dirty[dirtyKey(k)]
	delete(sh.dirty, dirtyKey(k))

	// 在锁内捕获 TierSnapshot 内存值快照，防止 unlock 后并发 evictLRU 摘除导致漏读。
	// mmoCaptured 区分「捕获到（可能是空值）」与「内存中已无该条目」：
	// 仅凭 mmoSnap != nil 判断会让空值被误当成快照（make 出的空切片也是非 nil）。
	var mmoSnap []byte
	var mmoCaptured bool
	if isDirty && s.getTier(k) == TierSnapshot {
		rk := s.redisKey(k)
		if el, ok := sh.mem[rk]; ok {
			e := el.Value.(*memEntry)
			mmoSnap = make([]byte, len(e.data))
			copy(mmoSnap, e.data)
			mmoCaptured = true
		}
	}
	sh.mu.Unlock()
	if !isDirty {
		return nil
	}

	var b []byte
	var found bool
	var err error
	if mmoCaptured {
		// 锁内已捕获快照，直接使用
		b = mmoSnap
		found = true
	} else {
		b, found, err = readValue()
		if err != nil {
			s.markDirty(k) // 读失败，重新标记以便重试
			return err
		}
	}
	if !found {
		// 值在落库前已消失（Redis TTL 过期/被逐出，或内存条目已被淘汰）。
		// 脏标记已清（不做重标脏以免对不存在的数据无限重试），但必须留日志：
		// 这是「本次更新永久丢失」的唯一现场。
		logger.Errorf("data: persistDirty cache miss for dirty key %s (value expired/evicted before flush); update lost", s.redisKey(k))
		if s.getTier(k) == TierRedisMySQL {
			return fmt.Errorf("data: persistDirty cache miss for dirty key %s (redis expired/evicted before flush); data lost", s.redisKey(k))
		}
		return nil
	}
	if s.mysql == nil {
		return fmt.Errorf("data: mysql not initialized, cannot persist dirty key %s", s.redisKey(k))
	}
	// 空数据（nil 或零长）统一落库为空 BLOB：列定义是 data LONGBLOB NOT NULL，
	// 写 NULL 会报 1048；此前对 nil 重标脏并返回错误，会让空值脏键在每次 Flush 都失败重试。
	if b == nil {
		b = []byte{}
	}
	if _, err := s.mysql.Exec(ctx, s.upsertSQL(), k.Owner, k.ID, k.Type, b); err != nil {
		s.markDirty(k) // 落库失败，重新标记以便重试
		return fmt.Errorf("data: flush upsert %s: %w", s.redisKey(k), err)
	}
	return nil
}

// Flush 将当前所有脏数据批量落库到 MySQL。无 MySQL 后端时为空操作。
func (s *Store) Flush(ctx context.Context) error {
	// 门控只看本 Store 是否具备 MySQL 后端：此前叠加全局 pdata.TierHasPersistent()，
	// 在「Schema 未注册持久 Tier 但实际存在脏数据」时直接空操作，脏数据永不落库。
	if s.mysql == nil {
		return nil
	}
	var firstErr error
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		keys := make([]Key, 0, len(sh.dirty))
		for k := range sh.dirty {
			keys = append(keys, k)
		}
		sh.mu.RUnlock()

		for _, k := range keys {
			if err := s.flushOne(ctx, k); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				// 单条落库失败不中断：尽力落库其余脏键，避免 Close 终落库时其余脏数据丢失。
				continue
			}
		}
	}
	return firstErr
}

// FlushOne 仅将单条 Key 的缓存落库。非持久化 Tier 为空操作。
func (s *Store) FlushOne(ctx context.Context, key Key) error {
	if s.mysql == nil || !s.needMySQLPersist(key) {
		return nil
	}
	if key.Owner == "" || key.ID == "" || key.Type == "" {
		return fmt.Errorf("data: owner kind, id and type must not be empty")
	}
	return s.flushOne(ctx, key)
}

// flushOne 按 Key 的 Tier 选择值读取器，把单条脏键落库。
func (s *Store) flushOne(ctx context.Context, k Key) error {
	if s.getTier(k) == TierSnapshot {
		return s.flushKeyMMO(ctx, k)
	}
	return s.flushKey(ctx, k)
}

// flushKey 将单键缓存值（simple 模式，从 Redis 读）持久化到 MySQL。
// Redis 客户端缺失/已关闭（Close 会置空底层句柄）时返回错误而非对 nil 解引用 panic：
// 持久化失败会由 persistDirty 重新标脏，数据不会静默丢失。
func (s *Store) flushKey(ctx context.Context, k Key) error {
	return s.persistDirty(ctx, k, func() ([]byte, bool, error) {
		if s.redis == nil {
			return nil, false, fmt.Errorf("data: flush read cache %s: redis not configured", s.redisKey(k))
		}
		rdb := s.redis.Raw()
		if rdb == nil {
			return nil, false, fmt.Errorf("data: flush read cache %s: redis client closed", s.redisKey(k))
		}
		b, err := rdb.Get(ctx, s.redisKey(k)).Bytes()
		if errors.Is(err, goredis.Nil) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("data: flush read cache %s: %w", s.redisKey(k), err)
		}
		return b, true, nil
	})
}

// FlushPlayer 将指定玩家的所有脏键立即落地到 MySQL。
// 用于 OnPlayerLeave 流程：玩家下线时确保残留内存脏数据已被持久化，
// 避免其他节点 LoadAll 读到过期值。
func (s *Store) FlushPlayer(ctx context.Context, owner OwnerType, id string) error {
	if s.mysql == nil {
		return nil
	}
	// 收集所有属于该玩家的脏键
	var entries []Key
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k := range sh.dirty {
			if k.Owner == owner && k.ID == id {
				entries = append(entries, k)
			}
		}
		sh.mu.RUnlock()
	}
	var firstErr error
	for _, k := range entries {
		var err error
		switch s.getTier(k) {
		case TierSnapshot:
			err = s.flushKeyMMO(ctx, k)
		case TierRedisMySQL:
			err = s.flushKey(ctx, k)
		}
		if err != nil {
			// 单条失败不中断：继续落其余脏键，避免一条坏键让该玩家全部脏数据都不落库。
			logger.Errorf("data: FlushPlayer flush key %s failed: %v", s.redisKey(k), err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// getCache 先读 Redis；未命中则回源 MySQL 并回写缓存。
func (s *Store) getCache(ctx context.Context, key Key) ([]byte, error) {
	rc, err := s.requireRedis()
	if err != nil {
		return nil, err
	}
	b, err := rc.Raw().Get(ctx, s.redisKey(key)).Bytes()
	if err == nil {
		return b, nil
	}
	if !errors.Is(err, goredis.Nil) {
		return nil, err
	}
	// 缓存未命中 → 回源 MySQL
	if s.mysql != nil {
		var row struct {
			Data []byte `db:"data"`
		}
		if gerr := s.mysql.Get(ctx, &row, s.selectSQL(), key.Owner, key.ID, key.Type); gerr == nil {
			// 若 key 已标脏（Redis 在 Flush 前过期/TTL 剔除），MySQL
			// 内数据可能是旧值。先 flushOne 落库后再回源，避免返回已知陈旧数据。
			if s.isDirty(key) {
				if err := s.flushOne(ctx, key); err != nil {
					logger.Errorf("data: getCache flushOne dirty key %s before backfill failed: %v", s.redisKey(key), err)
					// 不回填 Redis：flushOne 失败（常见于脏值已丢失）时把 MySQL 当前值
					// 写回缓存会把可能陈旧的值固化（直到下次写/TTL），直接返回，
					// 留给后续写刷新缓存。
					return row.Data, nil
				}
				// 落库后重读 MySQL 获取最新数据（flushOne 已 upsert）
				if gerr2 := s.mysql.Get(ctx, &row, s.selectSQL(), key.Owner, key.ID, key.Type); gerr2 != nil {
					return nil, fmt.Errorf("data: getCache re-read after flush %s: %w", s.redisKey(key), gerr2)
				}
			}
			// 回灌失败用 Errorf 打印，不再 Warnf 静默忽略。
			if err := rc.Raw().Set(ctx, s.redisKey(key), row.Data, s.cacheTTL).Err(); err != nil {
				logger.Errorf("data: getCache backfill redis %s: %v", s.redisKey(key), err)
			}
			return row.Data, nil
		} else if !errors.Is(gerr, imysql.ErrNotFound) {
			// 用 errors.Is 而非 !=：后端错误一旦被包装（fmt.Errorf %w / 换实现），
			// != 会把「未命中」当真实错误上抛（与 sql.go 的写法保持一致）。
			return nil, gerr
		}
	}
	return nil, ErrNotFound
}

// 脏标记管理。所有入口统一把 Key 归一化（清 NoLocalCache 位）：
// NoLocalCache 只是「本次访问是否绕过本地缓存」的标记，不参与数据身份；
// 用它参与 dirty map 的键会让同一份数据在 NoLocalCache 切换时产生两条脏键，
// unmarkDirty / isDirty 判不到另一条 → 脏标记残留（反复 upsert）或误判「已落库」。
func (s *Store) markDirty(k Key) {
	sh := s.shardOf(k)
	sh.mu.Lock()
	sh.dirty[dirtyKey(k)] = struct{}{}
	sh.mu.Unlock()
}

func (s *Store) unmarkDirty(k Key) {
	sh := s.shardOf(k)
	sh.mu.Lock()
	delete(sh.dirty, dirtyKey(k))
	sh.mu.Unlock()
}
