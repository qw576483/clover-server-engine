// Package client 提供 master 协作服的 TCP 客户端。
// 通过 TCP 直连 master，替代原 NATS Request/Reply 模式。
//
// Client 是纯 TCP 管道（NewClient / Close / Call），
// 各领域功能由独立的 *Client 类型组装：
//   - RankClient      → state.RankService 接口
//   - AuthorityClient → state.Authority   接口
//   - SessionClient   → session token 管理
//   - PlayerClient    → 玩家定位
//   - HeartbeatClient → 节点心跳上报与健康视图
//
// 用法：
//
//	mc, _ := client.NewClient(masterAddr)
//	rc := client.NewRankClient(mc)
//	rc.Add(ctx, "leaderboard", "player-1", 1000, nil)
//
// 单 master 部署：Client 只连一个地址，失败不做故障转移。
// 多 master 分片部署：调 UseShardRouting 后按 key 归属路由
// （定位=uid、排行榜=榜名、session=playerID），各分片只持有自己的数据；
// 未启用时 ForKey 返回自身，所有领域客户端行为不变。
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
	"github.com/qw576483/clover-server-engine/internal/transport/tcpmsg"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// ErrClientUnavailable master 客户端不可用。
var ErrClientUnavailable = errors.New("master: unavailable")

// ErrPlayerNotFound master 查无此玩家。
var ErrPlayerNotFound = errors.New("master: player not found")

// Client master 协作服 TCP 客户端——纯传输层，不含任何领域方法。
//
// 并发安全：连接指针的读写由 mu 保护，网络调用不持锁。
//
// 多 master 分片：启用 UseShardRouting 后，领域客户端统一经 ForKey(key) 取连接，
// 定位按 uid、排行榜按榜名、session token 按 playerID 落到各自属主分片；
// 未启用时 ForKey 返回自身，行为与单 master 部署完全一致。
type Client struct {
	mu    sync.RWMutex
	addr  string
	conn  *tcpmsg.Client
	shard *ShardedClient // nil = 单连接模式
	// token master 内部 RPC 的共享密钥（配置 master_token）；空 = 不做握手
	//（只绑回环的默认部署）。启用分片路由时透传给各分片连接。
	token string
}

// options NewClient 的可选参数。
type options struct {
	token string
}

// Option NewClient 的可选项。
type Option func(*options)

// WithToken 让客户端在连接建立后立即与 master 完成 MsgAuth 握手（共享密钥）。
// master 绑非回环地址时**必须**用它，否则首个业务请求会被服务端拒绝并断开连接
// （服务端侧行为见 domain/master/server 的 installConnAuth）。
func WithToken(token string) Option {
	return func(o *options) { o.token = token }
}

// NewClient 创建 master TCP 客户端。
func NewClient(addr string, opts ...Option) (*Client, error) {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	dialOpts := make([]tcpmsg.DialOption, 0, 1)
	if o.token != "" {
		dialOpts = append(dialOpts, tcpmsg.WithAuth(state.MsgAuth, o.token))
	}
	conn, err := tcpmsg.Dial(addr, dialOpts...)
	if err != nil {
		// 底层 err 用 %w 而非 %v：否则调用方 errors.Is(err, tcpmsg.ErrTimeout/ErrDisconnected)
		// 全部失效，无法区分超时与断连。
		return nil, fmt.Errorf("%w: %s: %w", ErrClientUnavailable, addr, err)
	}
	return &Client{addr: addr, conn: conn, token: o.token}, nil
}

// Addr 返回当前连接的 master 地址，便于日志与排障。
func (c *Client) Addr() string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.addr
}

// Close 关闭到 master 的 TCP 连接（含全部分片连接）。幂等：重复调用返回 nil。
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	conn := c.conn
	shard := c.shard
	c.conn = nil
	c.shard = nil
	c.mu.Unlock()
	var firstErr error
	if shard != nil {
		if err := shard.Close(); err != nil {
			firstErr = err
		}
	}
	if conn != nil {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// UseShardRouting 启用按 key 分片路由（多 master 部署）。
//
// pick 计算 key 归属分片的地址；all 返回全部分片地址（供 BackupAll 这类广播操作）。
// 未调用时 Client 保持单连接行为（单 master / 静态地址部署零感知）。
func (c *Client) UseShardRouting(pick func(key string) string, all func() []string) {
	if c == nil || pick == nil {
		return
	}
	// token 一并透传：分片连接走的是同一条受鉴权保护的 master 内部 RPC，
	// 漏传会让「单 master 直连正常、启用分片后全部被拒」这种配置级故障极难排查。
	sc := NewShardedClient(pick, all, WithToken(c.token))
	c.mu.Lock()
	old := c.shard
	c.shard = sc
	c.mu.Unlock()
	if old != nil {
		// 重复调用（重配分片表）时必须关闭旧分片连接，否则旧连接（含其 TCP 缓冲与
		// 心跳 goroutine）永久泄漏。在途请求可能被中断——配置级操作，可接受。
		if err := old.Close(); err != nil {
			logger.Warnf("master client: close old sharded connections: %v", err)
		}
	}
	logger.Infof("master client: shard routing enabled (primary=%s)", c.Addr())
}

// ForKey 返回 key 归属分片的客户端；未启用分片路由时返回自身。
//
// 领域客户端（PlayerClient / RankClient / SessionClient）统一经此完成按 key 路由。
// 返回 nil 表示「无可用分片」，调用方必须按错误处理：**不能**回落到自身连接，
// 否则会把请求发给不持有该 key 的 master。
func (c *Client) ForKey(key string) *Client {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	shard := c.shard
	c.mu.RUnlock()
	if shard == nil {
		return c
	}
	return shard.ForKey(key)
}

// AllShards 返回全部分片客户端；未启用分片路由时返回 nil（调用方回落到自身连接）。
func (c *Client) AllShards() []*Client {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	shard := c.shard
	c.mu.RUnlock()
	if shard == nil {
		return nil
	}
	return shard.All()
}

// Call 统一 RPC 调用：序列化 req → TCP Call → 反序列化到 resp。
// 领域 Client（RankClient 等）内部通过此方法完成实际的网络通信。
//
// 失败一律不重试——无法判断请求是否已在服务端执行过，盲目重试可能重复生效。
func (c *Client) Call(ctx context.Context, msgID uint32, req, resp any) error {
	if c == nil {
		return ErrClientUnavailable
	}
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return ErrClientUnavailable
	}
	if err := conn.CallContext(ctx, msgID, req, resp); err != nil {
		// 根因错误用 %w 传递（超时/断连等类型可被 errors.Is 识别）。
		return fmt.Errorf("%w: msgID=%d: %w", ErrClientUnavailable, msgID, err)
	}
	return nil
}
