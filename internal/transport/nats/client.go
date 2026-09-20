package nats

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/qw576483/clover-server-engine/internal/shared/config"
	"github.com/qw576483/clover-server-engine/internal/shared/tlsutil"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
	"github.com/qw576483/clover-server-engine/pkg/shared/id"
	"github.com/qw576483/clover-server-engine/pkg/shared/timeutil"
)

// 错误变量
var (
	// ErrNotConnected 表示客户端未建立连接
	ErrNotConnected = errors.New("nats: client not connected")
	// ErrJetStreamOff 表示 JetStream 未开启，无法执行持久化操作
	ErrJetStreamOff = errors.New("nats: jetstream disabled")
)

// Client 封装底层 nats.Conn 与 JetStream 上下文，维护连接状态与订阅集合。
type Client struct {
	conf NatsConfig
	conn atomic.Pointer[nats.Conn]
	js   nats.JetStreamContext
	mu   sync.Mutex
	// subs 按**登记键**保存订阅句柄，使订阅可以被单独反注册（不再只能靠 Close 一刀切）。
	// 登记键与 subTopics 同一套：
	//   - 普通订阅      → subject 本身
	//   - 队列订阅      → "queue:<group>:<subject>"
	// 用 map 而非切片，是为了让 Unsubscribe(subject) 能 O(1) 取到句柄；
	// 切片形态下「哪个句柄对应哪个 subject」这一信息根本没被保存（无从反注册）。
	subs      map[string]*nats.Subscription
	subTopics map[string]bool // 记录已注册的订阅主题防止重复注册
	closed    bool
}

// subKeyQueuePrefix 队列订阅登记键的前缀（与 subTopics 共用，避免与普通订阅的 key 冲突）。
const subKeyQueuePrefix = "queue:"

// orDuration 使用统一的配置默认值逻辑。
func orDuration(v, def time.Duration) time.Duration {
	return config.DefDuration(v, def)
}

// resolveMaxReconnect 归一 MaxReconnect 的三种语义（见 NatsConfig.MaxReconnect）：
// nil = 未设置 → defaultMaxReconnect（无限重连）；显式 0 → 0（不重连）；正数 → 原值。
// 抽成纯函数是为了能单测：NewClient 要真实 NATS 服务才能跑起来。
func resolveMaxReconnect(v *int) int {
	if v == nil {
		return defaultMaxReconnect
	}
	return *v
}

// NewClient 根据配置创建 NATS 客户端，自动完成连接与断线重连注册。
func NewClient(conf NatsConfig) (*Client, error) {
	if conf.Addr == "" {
		conf.Addr = "nats://127.0.0.1:4222"
	}
	// 未设置（nil）才按默认无限重连；显式 0 是「不重连」，不再被改写成 -1。
	maxReconnect := resolveMaxReconnect(conf.MaxReconnect)
	opts := []nats.Option{
		nats.Name(conf.ClientName),
		nats.MaxReconnects(maxReconnect),
		nats.ReconnectWait(orDuration(conf.ReconnectDelay, time.Second)),
		nats.Timeout(orDuration(conf.DialTimeout, 2*time.Second)),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Infof("nats reconnected: %s", nc.ConnectedUrl())
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.Warnf("nats disconnected: %v", err)
		}),
	}
	if conf.User != "" {
		opts = append(opts, nats.UserInfo(conf.User, conf.Pass))
	}
	// TLS 配置：当 TLS 配置非 nil 时启用 TLS 加密连接。
	if conf.TLS != nil {
		tlsCfg, terr := buildTLSConfig(conf.TLS)
		if terr != nil {
			return nil, fmt.Errorf("nats tls config: %w", terr)
		}
		opts = append(opts, nats.Secure(tlsCfg))
	}

	conn, err := nats.Connect(conf.Addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect %s: %w", conf.Addr, err)
	}

	c := &Client{conf: conf}
	c.conn.Store(conn)
	// 连接彻底关闭时把 c.conn 置 nil，使后续操作能通过 Load()==nil 快速失败，
	// 避免继续向已关闭连接投递消息。
	// 注意：Close() 方法也会 Store(nil)，此处使用 CompareAndSwap 避免重复置 nil。
	conn.SetClosedHandler(func(nc *nats.Conn) {
		logger.Errorf("nats connection closed (status=%s) client=%s", nc.Status(), conf.ClientName)
		c.conn.CompareAndSwap(conn, nil)
	})
	if conf.JetStream.Enable {
		js, jerr := conn.JetStream()
		if jerr != nil {
			conn.Close()
			return nil, fmt.Errorf("nats jetstream init: %w", jerr)
		}
		c.js = js
	}

	logger.Infof("nats connected: %s (jetstream=%v, client=%s)", conn.ConnectedUrl(), conf.JetStream.Enable, conf.ClientName)
	return c, nil
}

// Close 释放资源：取消全部订阅、关闭连接、释放协程。
// 使用 DrainTimeout 排空未处理消息再关闭，避免丢数据。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	subs := c.subs
	c.subs = nil
	c.subTopics = nil
	conn := c.conn.Load()
	c.conn.Store(nil)
	c.js = nil
	c.mu.Unlock()

	for _, s := range subs {
		if s != nil {
			_ = s.Unsubscribe()
		}
	}
	if conn != nil {
		// Drain() 是异步的、不会等待排空完成，飞行中的消息可能丢失；
		// 因此配合有界等待，尽量让飞行中的消息处理完成再 Close。
		_ = conn.Drain()
		// 有界等待连接进入 CLOSED（Drain 完成后底层会自动 Close），避免无限阻塞。
		deadline := time.Now().Add(5 * time.Second)
		for conn.Status() != nats.CLOSED && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if conn.Status() != nats.CLOSED {
			conn.Close()
		}
	}
	logger.Infof("nats client closed (client=%s)", c.conf.ClientName)
	return nil
}

// track 登记订阅句柄（key 见 Client.subs 的说明）。
// 调用方须确保该 key 已写入 subTopics（否则退订路径取不到句柄）。
func (c *Client) track(key string, sub *nats.Subscription) {
	c.mu.Lock()
	if c.closed {
		// Subscribe/QueueSubscribe 的 conn.Load() 与本处 track 之间存在与 Close() 的
		// 竞态窗口：Close 已清空 subs 后再写入，订阅将永不被清理（泄漏且 Close 语义失效）。
		// 已关闭则立即退订兜底。
		c.mu.Unlock()
		if sub != nil {
			_ = sub.Unsubscribe()
		}
		return
	}
	if c.subs == nil {
		c.subs = make(map[string]*nats.Subscription)
	}
	c.subs[key] = sub
	c.mu.Unlock()
}

// Unsubscribe 取消某个 subject 的**普通订阅**（与 Subscribe 成对）。
//
// 需要它是因为订阅必须能对称反注册：此前 Client 只保存 []*nats.Subscription、
// 不保存「句柄↔subject」的对应关系，上层（mmo/entitysync 等）模块 Stop 时
// 无法只退自己那几条订阅，只能等到整个客户端 Close —— 长跑进程里「模块已停、
// 回调仍在跑」就是这么来的。
//
// 语义：
//   - 幂等：该 subject 未订阅（含重复调用 / Stop 被调两次）时返回 nil，不算错误；
//   - 只移除**普通**订阅；队列订阅的登记键带 "queue:" 前缀，不会误伤。
func (c *Client) Unsubscribe(subject string) error {
	c.mu.Lock()
	sub, ok := c.subs[subject]
	delete(c.subs, subject)
	delete(c.subTopics, subject)
	c.mu.Unlock()
	if !ok || sub == nil {
		return nil
	}
	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("nats: unsubscribe %q: %w", subject, err)
	}
	return nil
}

// JS 返回 JetStream 上下文；未开启时返回 ErrJetStreamOff。
func (c *Client) JS() (nats.JetStreamContext, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.js == nil {
		return nil, ErrJetStreamOff
	}
	return c.js, nil
}

// Config 返回客户端配置副本。
func (c *Client) Config() NatsConfig { return c.conf }

// ClientName 返回客户端标识。
func (c *Client) ClientName() string { return c.conf.ClientName }

// Raw 返回底层 *nats.Conn，供高级场景使用。
func (c *Client) Raw() *nats.Conn { return c.conn.Load() }

// JetStreamEnabled 返回 JetStream 是否开启。
func (c *Client) JetStreamEnabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.js != nil
}

// Flush 强制将待发送数据刷到服务端，常用于测试前等待订阅就绪。
func (c *Client) Flush() error {
	if conn := c.conn.Load(); conn == nil {
		return ErrNotConnected
	} else {
		return conn.Flush()
	}
}

// injectHeaders 向消息注入公共字段：服务名、时间戳、traceID。
func (c *Client) injectHeaders(m *nats.Msg) {
	m.Header.Set("service", c.conf.ClientName)
	m.Header.Set("timestamp", conv.ToString(timeutil.NowMS()))
	m.Header.Set("trace_id", id.GenTraceID())
}

// buildTLSConfig 根据 TLSConfig 构建 crypto/tls.Config。
//
// 构建逻辑在 internal/shared/tlsutil（与 etcd 共用一份）——此前两边各写一遍，
// 校验细节与错误文案已经漂移。本函数只做「本包 TLSConfig 字段 → 三个路径」的适配。
func buildTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	return tlsutil.Build(cfg.CertFile, cfg.KeyFile, cfg.CAFile)
}
