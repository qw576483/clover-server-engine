package udp

import "time"

// defaultMaxPacketSize 默认接收缓冲区。取 UDP 单包理论上限（65507 = 65535 - 8(UDP头) - 20(IP头)），
// 确保正常包不会被静默截断；上层仍可在 ServerConfig/ClientConfig 中按需覆盖。
const defaultMaxPacketSize = 65507

// defaultMaxConns 默认最大并发会话数。UDP 无握手，源地址可任意伪造，
// 不设上限时洪水包会为每个地址建会话直到 IdleTimeout，造成内存放大。
const defaultMaxConns = 100000

// ServerConfig UDP 服务器配置。
type ServerConfig struct {
	ListenAddr    string        // 监听地址，如 ":8003"
	MaxPacketSize int           // 单包最大字节数
	IdleTimeout   time.Duration // 会话空闲超时，<=0 默认 60s，到时清理
	MaxConns      int           // 最大并发会话数，<=0 取 defaultMaxConns；超限丢包
}

// ClientConfig UDP 客户端配置。
type ClientConfig struct {
	Address       string // 目标地址，如 "127.0.0.1:8003"
	MaxPacketSize int    // 接收缓冲区
	// LocalAddr 本地绑定源地址。为空时使用 ":0" 让 OS 按路由表自动选择正确
	// 出口网卡（多网卡环境正确性），而非固定 127.0.0.1:0 强制走回环。
	// 需指定特定网卡出口时显式设置，如 "192.168.1.10:0"。
	LocalAddr string
}

func (c ServerConfig) normalize() ServerConfig {
	if c.MaxPacketSize <= 0 {
		c.MaxPacketSize = defaultMaxPacketSize
	}
	if c.MaxConns <= 0 {
		c.MaxConns = defaultMaxConns
	}
	return c
}

func (c ClientConfig) normalize() ClientConfig {
	if c.MaxPacketSize <= 0 {
		c.MaxPacketSize = defaultMaxPacketSize
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
