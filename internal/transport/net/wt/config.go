package wt

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/net/session"
)

// defaultIdleTimeout 默认空闲超时。
const defaultIdleTimeout = 30 * time.Second

// wtFrameLenSize 可靠流帧长前缀字节数（4 字节大端）。与 quic / ws / tcp 的线格式一致：
// 长度前缀让对端能按帧切分流，而不是把每次 Read 当成一条完整消息。
const wtFrameLenSize = 4

// maxWTFrameSize 单帧 body 上限，取自 session.MaxFrameSize（服务端帧上限的唯一来源，
// 与 tcp / ws / quic 传输层、网关配置默认值、会话加密密文上限同值）。
const maxWTFrameSize = session.MaxFrameSize

// ServerConfig WebTransport 服务器配置。
type ServerConfig struct {
	ListenAddr  string        // 监听地址，如 ":8001"（复用 WS 端口）
	MaxConns    int           // 最大连接数，<=0 不限
	IdleTimeout time.Duration // 空闲超时，<=0 取 defaultIdleTimeout
	TLSConfig   *tls.Config   // TLS 配置，必填：为 nil 时 Start 返回错误，本模块不生成自签名证书
	// CheckOrigin 校验请求 Origin；nil 时用 webtransport-go 内置同源校验（拒绝跨源）。
	// 网页端通常从独立端口（如 msg-web 的 3020）连接网关，Origin 与 Host 不同源，
	// 需显式放开（与 WS 的 CheckOrigin 对齐），否则浏览器 WebTransport 升级被拒。
	CheckOrigin func(r *http.Request) bool
}

// ClientConfig WebTransport 客户端配置。
type ClientConfig struct {
	Address            string        // 目标地址，如 "127.0.0.1:8001"（复用 WS 端口）
	IdleTimeout        time.Duration // 空闲超时，<=0 取 defaultIdleTimeout
	TLSConfig          *tls.Config   // TLS 配置（可选，为 nil 时跳过证书验证）
	InsecureSkipVerify bool          // 是否跳过证书验证（仅用于测试）
}

func (c ServerConfig) normalize() ServerConfig {
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
