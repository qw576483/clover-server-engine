package data

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// MMO 模式（内存为主 + 脏标记 + 周期落 MySQL）

// MMO 模式下，进程内 map 是一级缓存，所有读写都走它；脏数据经 s.dirty 标记，
// 由周期落盘 goroutine 或业务显式 Flush / FlushOne 批量落到 MySQL。Load 未命中内存时
// 回源 MySQL 并回填内存（冷加载 / 重启后部分数据未驻留）。Delete 同时清内存与落库。

// saveMMO 写入进程内存储并标记脏（脏数据稍后经 Flush 落 MySQL）。
//
// NoLocalCache 的 Key 明确声明「跳过本地内存缓存」：此时不能写内存——
// getMMO / LoadDirect 对同一 Key 都跳过内存，写进去的值永不被读；
// 直接同步直写 MySQL（其语义就是「直读持久层」的对偶），不标脏。
func (s *Store) saveMMO(ctx context.Context, key Key, data []byte) error {
	if key.NoLocalCache {
		if s.mysql == nil {
			return fmt.Errorf("data: saveMMO NoLocalCache requires mysql backend for key %s", s.redisKey(key))
		}
		if _, err := s.mysql.Exec(ctx, s.upsertSQL(), key.Owner, key.ID, key.Type, data); err != nil {
			return fmt.Errorf("data: saveMMO NoLocalCache upsert %s: %w", s.redisKey(key), err)
		}
		return nil
	}
	s.memSet(ctx, key, data)
	s.markDirty(key)
	return nil
}

// getMMO 内存优先；未命中依次查 Redis → MySQL 并回填内存，返回副本避免外部修改底层切片。
func (s *Store) getMMO(ctx context.Context, key Key) ([]byte, error) {
	if !key.NoLocalCache {
		if b, ok := s.memGet(key); ok {
			return b, nil
		}
	}
	// 冷加载：先查 Redis。离线事件可能已将最新数据写入 Redis 但尚未 flush 到 MySQL，
	// 此时 Redis 中的数据比 MySQL 更权威。命中后回填内存以减少后续 MySQL 回源。
	if s.redis != nil {
		if rdb := s.redis.Raw(); rdb != nil {
			b, err := rdb.Get(ctx, s.redisKey(key)).Bytes()
			if err == nil {
				if !key.NoLocalCache {
					s.memSet(ctx, key, b)
				}
				return b, nil
			}
			if !errors.Is(err, goredis.Nil) {
				// 非「未命中」的真实错误（连接失败/超时）不能静默：留日志后再回源 MySQL，
				// 否则 Redis 故障被掩盖成正常的冷加载路径。
				logger.Warnf("data: getMMO redis read %s failed: %v (fallback to mysql)", s.redisKey(key), err)
			}
		} else {
			// 客户端已 Close（底层句柄置空）：不能对 nil 解引用 panic，留痕后回源 MySQL。
			logger.Warnf("data: getMMO redis client closed for %s (fallback to mysql)", s.redisKey(key))
		}
	}
	// 冷加载（或 NoLocalCache 跳过内存缓存）：回源 MySQL。
	b, err := s.getMySQL(ctx, key)
	if err != nil {
		return nil, err
	}
	if key.NoLocalCache {
		// NoLocalCache 路径跳过了内存缓存，返回的是 MySQL 直接读取的值。
		// 若 key 已标脏，MySQL 中数据为旧值：先 flushOne 确保最新数据已落库再回源。
		if s.isDirty(key) {
			if err := s.flushOne(ctx, key); err != nil {
				return nil, fmt.Errorf("data: getMMO flush dirty before NoLocalCache read %s: %w", s.redisKey(key), err)
			}
			// 落库后重读 MySQL 获取最新数据。
			b2, err := s.getMySQL(ctx, key)
			if err != nil {
				return nil, err
			}
			return b2, nil
		}
		return b, nil
	}
	// 回填前重查内存：并发写入可能已在 MySQL 回源期间完成，避免用旧值覆盖新值。
	if cur, ok := s.memGet(key); ok {
		return cur, nil
	}
	// 冷加载前检查 dirty 标记——若 key 已标脏说明有未落库数据，
	// 此时 MySQL 值为旧值，跳过回填以避免用旧值覆盖内存。调用方可稍后重试或等待 Flush 完成。
	if s.isDirty(key) {
		logger.Warnf("data: getMMO cold-load skip cache fill, key is dirty (MySQL may be stale): %s", s.redisKey(key))
		return b, nil
	}
	s.memSet(ctx, key, b)
	return b, nil
}

// delMMO 同时删除进程内存储与 MySQL 记录，并清除脏标记。
// 锁内操作：仅移除内存条目+脏标记；MySQL 删除放锁外执行，避免 I/O 阻塞整个分片。
// 锁释到 MySQL 删除间的并发 Save 会重新写入内存并标脏→后续 Flush 即恢复，无数据丢失。
//
// MySQL 删除失败时回滚内存删除（数据保持原样，删除完全未发生），避免
// 「内存已删、MySQL 未删」的半删状态：下一次 Load 会把 MySQL 旧值读回来，
// 表现为「删除静默失效/数据复活」，而内存里的删除意图已经丢失。
func (s *Store) delMMO(ctx context.Context, key Key) error {
	sh := s.shardOf(key)
	rk := s.redisKey(key)
	// Phase 1: 锁内移除内存条目与脏标记，保留被删条目的值与原脏状态用于失败回滚。
	sh.mu.Lock()
	var removed *memEntry
	if el, ok := sh.mem[rk]; ok {
		if sh.lru != nil {
			sh.lru.Remove(el)
		}
		removed = el.Value.(*memEntry)
		delete(sh.mem, rk)
	}
	_, wasDirty := sh.dirty[dirtyKey(key)]
	delete(sh.dirty, dirtyKey(key))
	sh.mu.Unlock()
	// Phase 2: 锁外执行 MySQL 删除（权威持久层）。
	if s.mysql != nil {
		if _, err := s.mysql.Exec(ctx, s.deleteSQL(), key.Owner, key.ID, key.Type); err != nil {
			// 回滚：仅当期间没有并发 Save 写入新值时才恢复内存条目（不覆盖更新的数据）。
			if removed != nil {
				if _, ok := s.memGet(key); !ok {
					s.memSet(ctx, key, removed.data)
				}
			}
			if wasDirty {
				s.markDirty(key)
			}
			return err
		}
	}
	// Phase 3: 删除 Redis 副本。TierSnapshot 下 Redis 存有「离线写入 / 冷加载回填」的
	// 副本，只删内存+MySQL 时离线玩家的 Load 走 getCache 会从 Redis 把旧值读回来（删除复活）。
	// 此时 MySQL 已删，Redis 删除失败不回滚内存（删除意图已生效于持久层），
	// 由调用方重试删除或等 TTL 清除残留。
	if s.redis != nil {
		rc, rcErr := s.requireRedis()
		if rcErr != nil {
			return rcErr // 客户端已 Close：无法确认副本已删，返回错误由调用方重试
		}
		if err := rc.Raw().Del(ctx, rk).Err(); err != nil {
			return fmt.Errorf("data: delMMO delete redis replica %s: %w (retry the delete)", rk, err)
		}
	}
	return nil
}

// flushKeyMMO 把单条脏键的当前值（mmo 模式）持久化到 MySQL。
// 值优先从内存读；内存未命中时回退 Redis —— 离线写入（saveRedis）标脏的键
// 值只在 Redis，不回退读会把「脏数据未落库」误报为丢失（且永不落库）。
func (s *Store) flushKeyMMO(ctx context.Context, k Key) error {
	return s.persistDirty(ctx, k, func() ([]byte, bool, error) {
		if b, ok := s.memGet(k); ok {
			return b, true, nil
		}
		if s.redis != nil {
			rdb := s.redis.Raw()
			if rdb == nil {
				// 客户端已 Close：不能对 nil 解引用 panic；报错误让 persistDirty 重标脏。
				return nil, false, fmt.Errorf("data: flushKeyMMO read redis %s: client closed", s.redisKey(k))
			}
			b, err := rdb.Get(ctx, s.redisKey(k)).Bytes()
			if err == nil {
				return b, true, nil
			}
			if !errors.Is(err, goredis.Nil) {
				return nil, false, fmt.Errorf("data: flushKeyMMO read redis %s: %w", s.redisKey(k), err)
			}
		}
		return nil, false, nil
	})
}

// startPeriodicFlush 启动周期落盘 goroutine（仅在 FlushInterval>0 时）。
// 每隔 FlushInterval 把当前脏数据批量落 MySQL；收到 stopPeriodicFlush 信号后退出。
func (s *Store) startPeriodicFlush(interval time.Duration) {
	if interval <= 0 {
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	s.flushMu.Lock()
	s.flushStop = stop
	s.flushDone = done
	s.flushMu.Unlock()
	// goroutine 捕获局部 stop/done：stopPeriodicFlush 会把 s.flushStop 置 nil，
	// 若这里读字段会 select 到 nil channel 永久阻塞、收不到停止信号。
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := s.Flush(ctx); err != nil {
					logger.Errorf("data: mmo periodic flush: %v", err)
				}
				cancel()
			}
		}
	}()
}

// stopPeriodicFlush 停止周期落盘 goroutine，并做一次最终 Flush 清空残余脏数据。
// 最终 Flush 失败不阻塞退出流程（进程正在停止），但升级日志级别让运维感知数据丢失风险。
func (s *Store) stopPeriodicFlush() {
	// 取出并置空 flushStop 必须原子完成：否则两个并发 Close 都能通过 nil 检查，
	// 双双 close 同一 channel → panic。（Close 已用 sync.Once 兜底，这里再加一层。）
	s.flushMu.Lock()
	stop, done := s.flushStop, s.flushDone
	s.flushStop, s.flushDone = nil, nil
	s.flushMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	if done != nil {
		<-done
	}
	// 最终清空残余脏数据：周期性 Flush 的 ticker 可能在两次触发之间有新增脏数据
	// 但 goroutine 已退出，这些脏数据将永远不会写入 MySQL。
	// Flush 内部可能因底层连接已关闭而 panic，必须 recover 兜住，否则停机期 panic
	// 会直接崩进程且残余脏数据永不上落库。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	func() {
		defer func() {
			if r := recover(); r != nil {
				// 带堆栈：终落库 panic 意味着脏数据可能丢失，必须留下可定位的现场。
				logger.Errorf("data: mmo final flush panic on stop: %v — dirty data may be lost\n%s", r, debug.Stack())
			}
		}()
		if err := s.Flush(ctx); err != nil {
			logger.Errorf("data: mmo final flush on stop failed: %v — dirty data may be lost", err)
		}
	}()
}
