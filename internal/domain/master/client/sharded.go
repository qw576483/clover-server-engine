package client

import (
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// shardDialCooldown 建连失败后的冷却窗口：窗口内 ForKey/All 直接返回 nil，
// 不再重复 TCP dial。失败不缓存失败态时，master 宕机时每一次高频路由调用
// 都会重新拨号并把调用阻塞在 dial 超时上。
const shardDialCooldown = time.Second

// ShardedClient 按 key 归属在多个 master 分片之间路由的连接池。
//
// 分片键由调用方给出：玩家定位用 uid、排行榜用榜名、session token 用 playerID。
// 归属由 pick 计算 —— pick(key) 返回该 key 所属分片的可调用地址，返回空串表示
// 「无可用分片」。空串必须按错误处理，**不能**退到别的分片（会把请求发给不持有
// 该 key 的 master，读到错误数据）。
//
// 连接按地址惰性建立并缓存；分片总数固定（本期不支持热扩容），因此池大小有界。
type ShardedClient struct {
	pick func(key string) string
	// all 返回全部分片的可调用地址，供 BackupAll / RestoreAll 这类「所有分片都要做一遍」
	// 的广播操作使用；为 nil 时 All 返回 nil。
	all func() []string
	mu  sync.Mutex
	// conns 已建立的分片连接；failed 记录「上次建连失败的地址」，用于把失败日志
	// 收敛为「状态变化时一条」——ForKey 在跨节点投递的高频路径上，master 宕机时
	// 每次调用都会失败，不能每次都打日志。
	// failedAt 记录失败时刻，用于 dial 冷却（避免每次调用都重新拨号）。
	conns    map[string]*Client
	failed   map[string]bool
	failedAt map[string]time.Time
	// token master 内部 RPC 的共享密钥，建连时透传给每个分片（见 NewClient 的 WithToken）。
	token string
}

// NewShardedClient 创建分片路由客户端。pick 为 nil 时 ForKey 恒返回 nil。
// opts 与 NewClient 同源（如 WithToken），分片连接必须带上同一份凭据。
func NewShardedClient(pick func(key string) string, all func() []string, opts ...Option) *ShardedClient {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	return &ShardedClient{
		pick:     pick,
		all:      all,
		conns:    make(map[string]*Client),
		failed:   make(map[string]bool),
		failedAt: make(map[string]time.Time),
		token:    o.token,
	}
}

// ForKey 返回 key 归属分片的客户端；无可用分片或建连失败时返回 nil。
// 返回 nil 由调用方按错误处理（不会静默投到错误的分片）。
func (s *ShardedClient) ForKey(key string) *Client {
	if s == nil || s.pick == nil {
		return nil
	}
	addr := s.pick(key)
	if addr == "" {
		// 分片缺失的原因已在 resolver.pick 里打过日志（同样是收敛后的），此处不重复打。
		return nil
	}
	return s.dial(addr)
}

// All 返回全部分片的客户端，供广播类操作（BackupAll / RestoreAll）使用。
// 未配置 all 时返回 nil；单个分片建连失败会被跳过（已告警），不阻断其他分片。
func (s *ShardedClient) All() []*Client {
	if s == nil || s.all == nil {
		return nil
	}
	addrs := s.all()
	out := make([]*Client, 0, len(addrs))
	for _, addr := range addrs {
		if addr == "" {
			continue
		}
		if c := s.dial(addr); c != nil {
			out = append(out, c)
		}
	}
	return out
}

// dial 返回 addr 对应分片的连接；不存在则建连。
// 失败返回 nil（日志按状态变化收敛），并在冷却窗口内不再重试拨号。
func (s *ShardedClient) dial(addr string) *Client {
	s.mu.Lock()
	cached := s.conns[addr]
	if cached == nil {
		if at, ok := s.failedAt[addr]; ok && time.Since(at) < shardDialCooldown {
			// 冷却窗口内：不重新拨号（否则 master 宕机时高频路由路径每次调用
			// 都被 dial 超时拖住；窗口过后自动允许重试以支持 master 恢复自愈）。
			s.mu.Unlock()
			return nil
		}
	}
	s.mu.Unlock()
	if cached != nil {
		return cached
	}
	// 建连（TCP dial）在锁外执行：否则首个请求会阻塞所有并发分片查询。
	nc, err := NewClient(addr, WithToken(s.token))
	if err != nil {
		s.mu.Lock()
		first := !s.failed[addr]
		s.failed[addr] = true
		s.failedAt[addr] = time.Now()
		s.mu.Unlock()
		if first {
			logger.Warnf("master client: dial shard %s failed: %v (同类错误不再重复打印, cooldown=%s)", addr, err, shardDialCooldown)
		}
		return nil
	}
	s.mu.Lock()
	delete(s.failed, addr)
	delete(s.failedAt, addr)
	if old := s.conns[addr]; old != nil { // 并发建连：保留先到的，丢弃本次
		s.mu.Unlock()
		_ = nc.Close()
		return old
	}
	s.conns[addr] = nc
	s.mu.Unlock()
	logger.Infof("master client: shard %s connected", addr)
	return nc
}

// BroadcastTargets 返回可调用的全部分片客户端与建连失败/缺失的分片地址，
// 供 BackupAll / RestoreAll 这类广播操作识别「部分成功」：
// All 会静默跳过失败分片，只凭它无法区分「全部成功」与「漏做了某个分片」。
func (s *ShardedClient) BroadcastTargets() (clients []*Client, missing []string) {
	if s == nil || s.all == nil {
		return nil, nil
	}
	for _, addr := range s.all() {
		if addr == "" {
			missing = append(missing, addr)
			continue
		}
		if c := s.dial(addr); c != nil {
			clients = append(clients, c)
		} else {
			missing = append(missing, addr)
		}
	}
	return clients, missing
}

// Close 关闭全部已建立的分片连接（幂等）。
func (s *ShardedClient) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	conns := s.conns
	s.conns = make(map[string]*Client)
	s.mu.Unlock()
	var firstErr error
	for addr, c := range conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		logger.Infof("master client: shard %s closed", addr)
	}
	return firstErr
}
