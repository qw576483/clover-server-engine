package data

import (
	"container/list"
	"sync"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/util"
)

// defaultShardCount 默认分片数（2 的幂）。
//
// data.Store 的进程内存储（memory / mmo 模式）与脏标记按 Key 哈希分散到多个分片，
// 每个分片持有独立 RWMutex，热点 Key 之间不再互相阻塞；分片数可通过
// Config.ShardCount 配置（向上取 2 的幂，默认 defaultShardCount）。
const defaultShardCount = 64

// storeShard 单分片：持有独立锁、脏标记集与（memory / mmo 模式的）进程内 map + LRU 链表。
// 同一 Key 永远路由到同一分片，故单 Key 的读写 / 标脏 / 淘汰都在同一把锁内，正确性不变。
type storeShard struct {
	mu    sync.RWMutex
	idx   int                      // 分片下标（用于容量余数分配，见 shardCapacity）
	dirty map[Key]struct{}         // cache / mmo 模式下待落库的脏键
	mem   map[string]*list.Element // 进程内存储：redisKey -> LRU 节点（memory / mmo 模式非空）
	lru   *list.List               // 访问顺序（front=最近，back=最久）；memory / mmo 模式非空
}

// initShards 按 ShardCount 初始化分片（向上取 2 的幂）；memory / mmo 模式预建 mem / lru。
func (s *Store) initShards() {
	n := s.cfg.ShardCount
	if n <= 0 {
		n = defaultShardCount
	}
	const maxShards = 1 << 20
	if n > maxShards {
		// 误配大值会在这里一次性按值预分配全部分片（每片 2 map + list），
		// 必须显式告警，不能静默截断让人以为配置已生效。
		logger.Warnf("data: shard_count=%d exceeds max %d, clamped (check config)", n, maxShards)
		n = maxShards
	}
	n = util.NextPow2(n)
	if n != s.cfg.ShardCount && s.cfg.ShardCount > 0 {
		logger.Warnf("data: shard_count=%d normalized to %d (power of two)", s.cfg.ShardCount, n)
	}
	// #nosec G115 -- n 已限幅到 [1, maxShards] 且 nextPow2 保证为 2 的幂，在 uint64 范围内。
	s.numShards = uint64(n)
	s.shards = make([]storeShard, n)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.idx = i
		sh.dirty = make(map[Key]struct{})
		sh.mem = make(map[string]*list.Element)
		sh.lru = list.New()
	}
}

// shardOf 返回某 Key 所属分片（按 Key 哈希，稳定映射）。
func (s *Store) shardOf(key Key) *storeShard {
	h := shardHash(key)
	return &s.shards[h&(s.numShards-1)]
}

// shardHash 对 Key 做 xxHash-64 哈希，作为分片路由依据。
func shardHash(key Key) uint64 {
	return util.Xxhash64Key(string(key.Owner), key.ID, key.Type)
}

// shardCapacity 计算单分片的 LRU 容量：全局 MaxMemory 精确均摊到各分片，各分片容量之和
// 恰好等于 MaxMemory。单分片超出容量即按本分片 LRU 淘汰（脏数据淘汰前先落库），整体容量=MaxMemory。
//
// 容量必须精确均摊：MaxMemory<numShards 时若每分片强制返回 1，实际总量会远超 MaxMemory，
// LRU 永不触发 → OOM。故按 base=MaxMemory/numShards 均摊，并把余数 rem=MaxMemory%numShards
// 只分给前 rem 个分片（idx<rem 时 +1）。这样：
// - 求和 = base*numShards + rem = MaxMemory（精确不溢出）；
// - 当 MaxMemory<numShards 时 base=0，仅前 MaxMemory 个分片容量为 1，其余为 0（该分片不缓存，
// 写入后立即淘汰落库），总量严格受限于 MaxMemory。
func (sh *storeShard) shardCapacity(maxMemory, numShards int) int {
	if maxMemory <= 0 || numShards <= 0 {
		return 0
	}
	cap := maxMemory / numShards
	if sh.idx < maxMemory%numShards {
		cap++
	}
	return cap
}

// isDirty 测试 / 内部辅助：返回某 Key 当前是否标记脏。
func (s *Store) isDirty(k Key) bool {
	sh := s.shardOf(k)
	sh.mu.RLock()
	_, ok := sh.dirty[dirtyKey(k)]
	sh.mu.RUnlock()
	return ok
}

// dirtyKey 归一化脏键：NoLocalCache 是「本次访问是否绕过本地缓存」的标记，
// 不参与数据身份。dirty map 以归一化 Key 为键，避免同一份数据在 NoLocalCache
// 切换时分裂成两条脏键（unmarkDirty/isDirty 判不到另一条 → 脏标记残留或误判已落库）。
func dirtyKey(k Key) Key {
	k.NoLocalCache = false
	return k
}
