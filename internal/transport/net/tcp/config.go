package tcp

import (
	"crypto/tls"
	"fmt"
	"time"

	"clover-server-engine/internal/shared/config"
	"clover-server-engine/internal/transport/net/session"
)

const (
	// defaultMaxMsgSize 默认单帧 payload 上限 10MiB。
	//
	// 值取自 `session.MaxFrameSize`（**服务端帧上限的唯一来源**，含 TCP/WS/QUIC 与网关
	// 配置默认值；旧写法是本文件各写一份字面量，值会漂移）。
	// **与客户端一致**：客户端 `Runtime/Network/Connection.cs:111` 的 `ClientFrame.MaxBodySize`
	// 与 `:190/:195` 的 `MaxFramePayload` / `HardMaxFramePayload` 同为 `10 << 20`。
	// 此前这里是 1<<18（256KiB）而网关又未把 `MaxFrameSize` 下传到本配置：客户端按
	// 上限构造的帧会被本层判超限并直接断连（表现为「连得上→秒断」，网关日志里看不到）。
	defaultMaxMsgSize     = session.MaxFrameSize // 10MiB，与客户端一致
	defaultSendBufferSize = 256
	defaultDialTimeout    = 5 * time.Second
	// defaultMaxConns 默认最大并发连接数上限，缓解恶意连接耗尽 fd/goroutine 资源。
	// 业务可按承载规模显式调小；=0 取本默认值；<0 表示不限制（不推荐用于公网）。
	defaultMaxConns = 100000
)

// ServerConfig TCP 服务器配置。
type ServerConfig struct {
	ListenAddr string // 监听地址，如 ":8002"
	// HeartbeatInterval 服务端写 ping 的间隔（0=关闭），**语义是「服务端主动探测」**。
	//
	// 与客户端心跳不是一回事，两端默认值不同是**有意为之**、不是漂移：
	//   - 客户端（Unity）：15s 主动上行心跳（Net.HeartbeatInterval）——客户端保活用；
	//   - 服务端（本字段）：30s 写 ping + 按读空闲判活——服务端侧「读超时」保底。
	// 二者互不依赖：客户端心跳停了服务端仍会按自己的读空闲超时回收连接。
	HeartbeatInterval time.Duration
	MaxMsgSize        int         // 单条 payload 上限
	SendBufferSize    int         // 发送队列缓冲
	MaxConns          int         // 最大并发连接数；=0 用默认 defaultMaxConns；<0 不限制
	TLSConfig         *tls.Config // 可选 TLS 配置；nil=明文
	// NoDelay 禁用 Nagle 算法（默认 true）。设为 true 降低延迟，false 允许底层合并小包。
	NoDelay bool
	// KeepAlivePeriod TCP KeepAlive 探测间隔（默认 0=使用系统默认值）。
	KeepAlivePeriod time.Duration
	// ReadBufferSize 操作系统接收缓冲区字节数（0=系统默认）。
	ReadBufferSize int
	// WriteBufferSize 操作系统发送缓冲区字节数（0=系统默认）。
	WriteBufferSize int
}

// ClientConfig TCP 客户端配置。
type ClientConfig struct {
	Address           string
	HeartbeatInterval time.Duration
	DialTimeout       time.Duration // 拨号超时
	MaxMsgSize        int
	SendBufferSize    int
	// NoDelay 禁用 Nagle 算法（默认 true）。
	NoDelay bool
	// KeepAlivePeriod TCP KeepAlive 探测间隔（默认 0=使用系统默认值）。
	KeepAlivePeriod time.Duration
	// ReadBufferSize 操作系统接收缓冲区字节数（0=系统默认）。
	ReadBufferSize int
	// WriteBufferSize 操作系统发送缓冲区字节数（0=系统默认）。
	WriteBufferSize int
}

// PoolConfig 网关 ↔ 逻辑服连接池配置。
type PoolConfig struct {
	Address           string
	MinConns          int // 预热连接数
	MaxConns          int // 连接池上限
	HeartbeatInterval time.Duration
	DialTimeout       time.Duration
	MaxMsgSize        int
	SendBufferSize    int
	Handler           Handler // 逻辑服 → 网关推送回调
}

// Validate 校验 PoolConfig。MinConns > MaxConns 为语义混乱。
func (c PoolConfig) Validate() error {
	if c.MaxConns > 0 && c.MinConns > c.MaxConns {
		return fmt.Errorf("tcp pool: MinConns(%d) > MaxConns(%d)", c.MinConns, c.MaxConns)
	}
	return nil
}

func (c ServerConfig) normalize() ServerConfig {
	c.MaxMsgSize = config.DefInt(c.MaxMsgSize, defaultMaxMsgSize)
	c.SendBufferSize = config.DefInt(c.SendBufferSize, defaultSendBufferSize)
	c.MaxConns = config.DefInt(c.MaxConns, defaultMaxConns)
	return c
}

func (c ClientConfig) normalize() ClientConfig {
	c.MaxMsgSize = config.DefInt(c.MaxMsgSize, defaultMaxMsgSize)
	c.SendBufferSize = config.DefInt(c.SendBufferSize, defaultSendBufferSize)
	c.DialTimeout = config.DefDuration(c.DialTimeout, defaultDialTimeout)
	return c
}

func (c PoolConfig) normalize() PoolConfig {
	c.MaxMsgSize = config.DefInt(c.MaxMsgSize, defaultMaxMsgSize)
	c.SendBufferSize = config.DefInt(c.SendBufferSize, defaultSendBufferSize)
	c.MaxConns = config.DefInt(c.MaxConns, 4)
	c.DialTimeout = config.DefDuration(c.DialTimeout, defaultDialTimeout)
	return c
}

// DefaultServerConfig 默认服务器配置。
// ⚠️ 仅限本地开发/测试环境使用，生产环境务必通过 yaml 覆盖 ListenAddr。
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ListenAddr:        "",
		HeartbeatInterval: 30 * time.Second,
		NoDelay:           true,
	}
}

// DefaultClientConfig 默认客户端配置。
// ⚠️ 仅限本地开发/测试环境使用，生产环境务必通过 yaml 覆盖 Address。
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Address:           "",
		HeartbeatInterval: 30 * time.Second,
		NoDelay:           true,
	}
}
