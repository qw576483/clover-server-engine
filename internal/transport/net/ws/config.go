package ws

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"slices"
	"time"

	"clover-server-engine/internal/transport/net/session"
)

const (
	defaultReadBufferSize  = 4096
	defaultWriteBufferSize = 4096
	// defaultMaxMsgSize 默认单条消息上限 10MiB。值取自 `session.MaxFrameSize`
	// （**服务端帧上限的唯一来源**，与 tcp/quic 传输层、网关配置默认值、
	// 会话加密密文上限同值）。
	//
	// **与客户端一致**：客户端 `Runtime/Network/WebSocketConnection.cs:33` 的 `MaxMsgPayload`
	// 同为 `10 << 20`，此前服务端这里是 1<<16（64KiB）而网关又未下发 `MaxFrameSize`：
	// 客户端认为合法的 >64KiB 消息被 gorilla `SetReadLimit` 判超限、连接被直接关闭。
	defaultMaxMsgSize       = session.MaxFrameSize // 10MiB，与客户端一致
	defaultHandshakeTimeout = 10 * time.Second
	defaultSendBufferSize   = 256
	// maxMsgSizeHardCap 单消息硬上限：钳到 32MB 而非 1GB，
	// 防止恶意单帧近 GB 分配导致内存放大 DoS，与网关侧上限对齐。
	// 注意它与 defaultMaxMsgSize 是两件事：本值是**可配置值的天花板**，
	// 实际单帧上限仍是 `session.MaxFrameSize`（10 MiB）——若把 max_frame_size
	// 调到 10 MiB 以上，必须同时抬高 session 的加密上限，否则启用通道加密的帧仍发不出。
	maxMsgSizeHardCap = 32 << 20 // 32MB
	// defaultMaxConns 默认最大并发连接数上限（与 tcp/udp 的默认上限同量级）。
	// 升级成功即注册进 mgr，只建连不发帧的连接不进入网关会话计数；
	// 不设上限时可被无限建连耗尽 goroutine/内存。
	defaultMaxConns = 100000
)

// ServerConfig WebSocket 服务器配置。
type ServerConfig struct {
	Addr            string // HTTP 监听地址，如 ":8001"
	Path            string // 升级路径，默认 "/ws"
	ReadBufferSize  int
	WriteBufferSize int
	// HeartbeatInterval 服务端写 ping 的间隔（0=关闭），**语义是「服务端主动探测」**。
	// 客户端侧 15s 主动上行心跳与它互不依赖，默认值不同是有意为之（见 tcp/config.go 同名字段注释）。
	HeartbeatInterval time.Duration
	// MaxMsgSize 单条消息上限（默认 10MiB，与客户端一致；硬上限 32MiB）。
	MaxMsgSize int64
	// MaxConns 最大并发连接数；=0 取 defaultMaxConns（10 万），<0 不限制。
	MaxConns         int
	HandshakeTimeout time.Duration
	CheckOrigin      func(*http.Request) bool
	// AllowedOrigins 允许的跨域 Origin host 白名单（如 "game.example.com"）。
	// 仅当 CheckOrigin 为 nil 且本字段非空时生效：自动构造同源/白名单校验。
	// 生产必须显式配置，切勿将 CheckOrigin 设为 `return true`（完全放开 CSWSH 风险）。
	AllowedOrigins []string
	// TLSConfig 可选 TLS 配置；nil=明文。启用后 WS 以 wss:// 提供。
	TLSConfig *tls.Config
	// WTCertHash 服务器证书 DER 的 SHA-256（hex）。非空时额外暴露 GET /wt-cert-hash，
	// 供浏览器以 serverCertificateHashes 方式建立 WebTransport。
	//
	// 背景：Chromium 对 WebTransport 的证书校验独立于普通 HTTPS，既不接受本机
	// 自建根证书，--ignore-certificate-errors 也对其无效。自签名场景下唯一可用
	// 的通路是 W3C 规定的 serverCertificateHashes（证书哈希固定）。该方式下浏览器
	// 不再走 PKI，改由服务端通过已受信任的通道下发哈希，因此本端点与 WS 共用
	// 同一张证书、同一端口，安全性由 wss 通道本身保证。
	WTCertHash string
	// WTCertHashFunc 动态读取当前证书哈希；非 nil 时优先于 WTCertHash。
	// 用于证书运行期间自动轮换：重签后哈希变化，每次请求都应返回最新值，
	// 保证前端每次建连前拉到的哈希与实际证书一致。
	WTCertHashFunc func() string
}

func (c ServerConfig) normalize() ServerConfig {
	if c.Path == "" {
		c.Path = "/ws"
	}
	if c.ReadBufferSize <= 0 {
		c.ReadBufferSize = defaultReadBufferSize
	}
	if c.WriteBufferSize <= 0 {
		c.WriteBufferSize = defaultWriteBufferSize
	}
	if c.MaxMsgSize <= 0 {
		c.MaxMsgSize = defaultMaxMsgSize
	}
	// 防止 MaxMsgSize 超大导致内存放大 DoS，钳到 32MB 硬上限（与网关对齐）。
	if c.MaxMsgSize > maxMsgSizeHardCap {
		c.MaxMsgSize = maxMsgSizeHardCap
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = defaultHandshakeTimeout
	}
	// 0 取默认上限（DoS 防护）；负值表示显式不限制。
	if c.MaxConns == 0 {
		c.MaxConns = defaultMaxConns
	}
	// 默认安全策略：未显式设置 CheckOrigin 时，若配置了 AllowedOrigins 白名单则按白名单校验，
	// 否则维持 gorilla 默认（仅允许同源）。业务切勿将 CheckOrigin 设为 `return true` 完全放开。
	if c.CheckOrigin == nil && len(c.AllowedOrigins) > 0 {
		allowed := c.AllowedOrigins
		c.CheckOrigin = func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true // 非浏览器同源请求（本地工具/内网）放行
			}
			u, err := url.Parse(origin)
			if err != nil {
				return false
			}
			return slices.Contains(allowed, u.Host)
		}
	}
	return c
}

// DefaultServerConfig 默认服务器配置。
// ⚠️ 仅限本地开发/测试环境使用，生产环境务必通过 yaml 覆盖 Addr。
func DefaultServerConfig() ServerConfig {
	return ServerConfig{Addr: "", HeartbeatInterval: 30 * time.Second}
}
