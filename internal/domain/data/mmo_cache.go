// #nosec G115 -- numShards 在构造期已规整为正数（见同文件 normalize），maxMemory 亦为配置的非负值：转换范围受限于分片数。

package data

import (
	"context"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// 进程内存储（memory / mmo 模式共用）+ MMO 分片 LRU 淘汰

// mem 以 redisKey 为键映射到 LRU 链表节点（节点值 *memEntry），访问时 MoveToFront，
// 淘汰时取 Back（最久未访问）。mmo 模式启用 MaxMemory>0 后，写入超出单分片容量即按分片 LRU
// 淘汰，脏条目淘汰前先落库（persistDirty），避免崩溃丢数据 & OOM。
// memory 模式不淘汰（默认 MaxMemory=0），数据完整驻留内存直至进程退出。

// 内存以「分片」组织（见 shard.go）：每个 Key 按哈希路由到固定分片，单 Key 的读写 / 标脏
// 只持本分片锁；热点 Key 之间不再互相阻塞。

// getMemory 从进程内存储读取单条数据（memory / mmo 模式共用）；键不存在返回 ErrNotFound。
func (s *Store) getMemory(_ context.Context, key Key) ([]byte, error) {
	b, ok := s.memGet(key)
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

// memSet 写入进程内存储（memory / mmo 模式）。命中已有键则原地更新并提到 LRU 队首；
// 否则新建条目入队首。容量判定在锁内完成：新建条目超出分片容量时再触发分片 LRU 淘汰，
// 避免每次写入都调用 evictLRU 引入与并发 getMMO 冷加载之间的竞态窗口。
func (s *Store) memSet(ctx context.Context, key Key, data []byte) {
	rk := s.redisKey(key)
	sh := s.shardOf(key)
	sh.mu.Lock()
	if el, ok := sh.mem[rk]; ok {
		el.Value.(*memEntry).data = append([]byte(nil), data...)
		if sh.lru != nil {
			sh.lru.MoveToFront(el)
		}
		sh.mu.Unlock()
		return // 已有键已提到队首，容量未变，无需 evict
	}
	// initShards 已为所有分片预建 LRU 链表（sh.lru 恒非 nil），无需再分支；
	// 原 else 分支（新建不入链表的裸 list.Element）不可达，已删除。
	el := sh.lru.PushFront(&memEntry{key: key, data: append([]byte(nil), data...)})
	sh.mem[rk] = el
	// 在锁内判定容量，仅超限时才触发淘汰，减小窗口。
	needEvict := sh.lru != nil && s.maxMemory > 0 &&
		sh.lru.Len() > sh.shardCapacity(s.maxMemory, int(s.numShards))
	sh.mu.Unlock()
	if needEvict {
		s.evictLRU(ctx, sh)
	}
}

// memGet 读取进程内存储单条数据，返回副本避免外部修改底层切片；命中时更新 LRU 访问顺序。

// 注意：整个「查表 → 提到队首 → 拷贝值」必须在同一把分片写锁内完成：若在 MoveToFront 与
// 读 el.Value 之间释放锁，并发的 LRU 淘汰（evictLRU）可能把该节点 Remove 掉并置
// el.Value=nil，后续类型断言 el.Value.(*memEntry) 直接 panic；拷贝出独立切片后释放，
// 也保证「读到的快照」与「被置前」的节点一致。
func (s *Store) memGet(key Key) ([]byte, bool) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	el, ok := sh.mem[s.redisKey(key)]
	if !ok {
		sh.mu.Unlock()
		return nil, false
	}
	if sh.lru != nil {
		sh.lru.MoveToFront(el)
	}
	e := el.Value.(*memEntry)
	out := make([]byte, len(e.data))
	copy(out, e.data)
	sh.mu.Unlock()
	return out, true
}

// memDel 删除进程内存储单条数据（memory / mmo 模式），仅持所属分片锁。
func (s *Store) memDel(key Key) {
	rk := s.redisKey(key)
	sh := s.shardOf(key)
	sh.mu.Lock()
	if el, ok := sh.mem[rk]; ok {
		if sh.lru != nil {
			sh.lru.Remove(el)
		}
		delete(sh.mem, rk)
	}
	sh.mu.Unlock()
}

// evictLRU 当本分片驻留条目超过单分片容量（全局 MaxMemory 均摊）时，按分片 LRU 顺序淘汰
// 最久未访问条目；脏条目在淘汰前先落库，避免崩溃丢数据。仅 mmo 模式（sh!=nil 且 MaxMemory>0）生效。

// 注意：「判断容量 → 取队尾 → 读 entry → 摘除」必须在同一把分片临界区内完成，否则与并发的
// memSet/memGet/memDel 产生数据竞争：后者会 MoveToFront/PushFront/Remove 改变 lru 结构，
// 无锁读取 Len/Back/Back().Value 可能拿到正被 Remove 的节点（其 Value 已被置 nil），触发
// nil 解引用 panic（与 memGet 同一类并发问题）。落库网络 I/O 在锁外执行。
// ctx 刻意不使用：淘汰落库统一走独立后台 ctx（见下方注释），保留参数以维持调用点签名不变。
func (s *Store) evictLRU(_ context.Context, sh *storeShard) {
	if sh == nil || sh.lru == nil || s.maxMemory <= 0 {
		return
	}
	capacity := sh.shardCapacity(s.maxMemory, int(s.numShards))
	for {
		sh.mu.Lock()
		if sh.lru.Len() <= capacity {
			sh.mu.Unlock()
			return
		}
		back := sh.lru.Back()
		if back == nil {
			sh.mu.Unlock()
			return
		}
		e := back.Value.(*memEntry)
		rk := s.redisKey(e.key)
		_, dirty := sh.dirty[dirtyKey(e.key)]
		if !dirty {
			// 干净条目（已落库/从未变脏）直接淘汰：MySQL 已有等值数据，后续 Load 可回源。
			// 注意不能 MoveToFront+continue「保留」：若分片超容量且条目全为干净，
			// Len 永不减少，本循环将永不退出（100% CPU 持锁死循环）。
			// persistDirty 已在分片锁内原子完成「清脏+捕获值快照」，
			// 不存在「脏标记刚清但值未读完」需要保留条目的竞态。
			sh.lru.Remove(back)
			delete(sh.mem, rk)
			sh.mu.Unlock()
			continue
		}
		// 脏数据在锁内拷贝快照并清理标记，避免与并发 Flush 竞态导致双落或漏落。
		// 必须用 make+copy 而非 append([]byte(nil), ...)：后者在 e.data 为 nil 时得到
		// nil 快照，upsert 会把 NULL 写进 data LONGBLOB NOT NULL 列报 1048 →
		// 恢复条目并中止淘汰 → 该条目每次淘汰都失败（活锁 + 反复失败日志）。
		flushData := make([]byte, len(e.data))
		copy(flushData, e.data)
		delete(sh.dirty, dirtyKey(e.key))
		sh.lru.Remove(back)
		delete(sh.mem, rk)
		sh.mu.Unlock()

		// dirty=true 时即使 data 为空也要落库（如清零操作），仅 dirty=false 才保留。
		// 脏数据淘汰前直接落库（不经过 persistDirty，避免其重复检查 dirty 标记导致跳过）。
		if s.mysql != nil {
			k := e.key
			// 淘汰落库不能挂在调用方 ctx 上：请求结束 ctx 被取消后，
			// 所有淘汰落库都会失败并回滚，缓存永远降不下来。用独立后台 ctx。
			c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if _, err := s.mysql.Exec(c, s.upsertSQL(), k.Owner, k.ID, k.Type, flushData); err != nil {
				logger.Errorf("data: mmo evict flush %s: %v", rk, err)
				// 落库失败：恢复脏标记与内存条目，避免数据丢失（否则条目已被摘除且
				// 脏标记已清 → 永久丢失）。下次 Flush/淘汰仍有机会重试落库。
				// 放回队尾（PushBack）而非队首：放队首会把它变成"最近使用"，
				// 打乱 LRU 语义；且 MySQL 持续不可用时条目只搬不减，
				// Len 永远降不到 capacity 之下 → 本循环活锁。故恢复后直接中止本轮淘汰。
				sh.mu.Lock()
				if _, exists := sh.mem[rk]; !exists {
					sh.mem[rk] = sh.lru.PushBack(&memEntry{key: e.key, data: flushData})
				}
				sh.dirty[dirtyKey(e.key)] = struct{}{}
				sh.mu.Unlock()
				cancel()
				return
			}
			cancel()
		} else {
			// mysql 未初始化但 dirty 为 true（mmo 模式正常不可达，防御分支）：
			// 条目已在锁内被摘除，必须放回内存并恢复脏标记才真正「防丢数据」，
			// 然后停止本轮淘汰（无处落库，继续淘汰只会空转）。
			sh.mu.Lock()
			if _, exists := sh.mem[rk]; !exists {
				sh.mem[rk] = sh.lru.PushBack(&memEntry{key: e.key, data: flushData})
			}
			sh.dirty[dirtyKey(e.key)] = struct{}{}
			sh.mu.Unlock()
			logger.Errorf("data: mmo evict dirty key %s without mysql — data restored, eviction aborted", rk)
			return
		}
	}
}
