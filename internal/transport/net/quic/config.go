package quic

import (
	"crypto/tls"
	"time"

	"clover-server-engine/internal/transport/net/session"
)

// defaultMaxStream 默认最大流数。
const defaultMaxStream = 100

// defaultIdleTimeout 默认空闲超时（30s），是 QUIC 传输的**服务端判活口径**。
//
// 心跳语义（与客户端不冲突）：
//   - 服务端 30s = 读超时口径：按应用层流读写活跃度判死（见 conn.go 的 touch()/touchWrite()
//     与 server.go 的 cleanLoop，空闲本值即关连接）；
//   - 客户端 15s = 客户端主动心跳口径（Runtime/Network/Quic/QuicConnection.cs 的
//     HeartbeatIntervalMs = 15_000，无发送时补一帧保活）。
//
// 二者不冲突：客户端更频繁地上报，服务端按 30s 判死。
const defaultIdleTimeout = 30 * time.Second

// defaultAcceptStreamTimeout 等待对端在新建连接上打开首条流的默认超时。
// 每条连接固定一条双向流（见 README 规则 2），超时仍不开流的连接按异常连接关闭，
// 以免它长期占住唯一的 accept 循环。
const defaultAcceptStreamTimeout = 10 * time.Second

const quicFrameLenSize = 4

// maxQUICFrameSize 单帧 body 上限 10MiB，**与客户端一致**
// （客户端 `Runtime/Network/Quic/QuicConnection.cs:50` 的 `MaxFrameSize` 同为 `10 << 20`），
// 且取自 `session.MaxFrameSize`（**服务端帧上限的唯一来源**，与 tcp/ws 传输层、
// 网关配置默认值、会话加密密文上限同值）。
// 注意本值不来自 ServerConfig：QUIC 帧长由 4B 前缀自带，服务端按此常量校验。
const maxQUICFrameSize = session.MaxFrameSize

// defaultMaxConns 默认最大并发连接数。QUIC 连接建立即占用内存与若干协程，
// 不设上限时洪水建连可持续消耗资源；与 tcp/udp 的默认上限保持同一量级。
const defaultMaxConns = 100000

// ServerConfig QUIC 服务器配置。
type ServerConfig struct {
	ListenAddr  string        // 监听地址，如 ":8003"
	MaxConns    int           // 最大连接数；=0 取 defaultMaxConns，<0 不限制
	MaxStreams  int           // 最大并发流数，<=0 取 defaultMaxStream
	IdleTimeout time.Duration // 空闲超时，<=0 取 defaultIdleTimeout
	// AcceptStreamTimeout 等待对端在新建连接上打开首条流的超时，<=0 取 defaultAcceptStreamTimeout。
	AcceptStreamTimeout time.Duration
	// TLSConfig TLS 配置，必填：为 nil 时 Start / StartWithPacketConn 返回错误，本模块不生成自签名证书。
	TLSConfig *tls.Config
}

// ClientConfig QUIC 客户端配置。
type ClientConfig struct {
	Address            string        // 目标地址，如 "127.0.0.1:8003"
	MaxStreams         int           // 最大并发流数，<=0 取 defaultMaxStream
	IdleTimeout        time.Duration // 空闲超时，<=0 取 defaultIdleTimeout
	ConnectTimeout     time.Duration // 连接超时，<=0 取 10s
	TLSConfig          *tls.Config   // TLS 配置（可选，为 nil 时按 InsecureSkipVerify 构造默认配置）
	InsecureSkipVerify bool          // 是否跳过证书验证（仅用于测试）
}

func (c ServerConfig) normalize() ServerConfig {
	// 0 取默认上限（DoS 防护）；负值表示显式不限制。
	if c.MaxConns == 0 {
		c.MaxConns = defaultMaxConns
	}
	if c.MaxStreams <= 0 {
		c.MaxStreams = defaultMaxStream
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.AcceptStreamTimeout <= 0 {
		c.AcceptStreamTimeout = defaultAcceptStreamTimeout
	}
	return c
}

func (c ClientConfig) normalize() ClientConfig {
	if c.MaxStreams <= 0 {
		c.MaxStreams = defaultMaxStream
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	return c
}

// DefaultServerConfig 默认服务器配置。
func DefaultServerConfig() ServerConfig {
	return ServerConfig{ListenAddr: ""}
}

// DefaultClientConfig 默认客户端配置。
func DefaultClientConfig() ClientConfig {
	return ClientConfig{Address: ""}
}
