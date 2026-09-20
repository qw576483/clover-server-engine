package http

import (
	"crypto/tls"
	"errors"
	"time"
)

// 默认超时（与 ws/tcp 保持一致量级）。
const (
	defaultReadTimeout    = 15 * time.Second
	defaultWriteTimeout   = 15 * time.Second
	defaultIdleTimeout    = 60 * time.Second
	defaultMaxHeaderBytes = 1 << 20 // 1MB，限制 HTTP 请求头大小防 Slowloris
)

// DefaultTLSMinVersion HTTP 传输层 TLS 的最低版本（TLS 1.2）。
// 与 gwcore 的网关 TLS 口径一致：1.0/1.1 已被各大浏览器与合规基线废弃。
const DefaultTLSMinVersion = tls.VersionTLS12

// TLSConfig HTTP 传输层的 TLS 服务端配置。
//
// 为什么是「证书文件对」而不是 *tls.Config：本层是**传输原语**，不持有密钥材料，
// 密钥只在进程启动时被标准库读一次（ServeTLS 内部加载）；业务若需要自定义
// （内存证书 / 动态轮换）可再传 *tls.Config，那是后续扩展点，暂不做。
type TLSConfig struct {
	// CertFile PEM 证书链路径。
	CertFile string
	// KeyFile PEM 私钥路径。**必须与 CertFile 同时配置**。
	KeyFile string
}

// Enabled 是否启用 TLS（只配一半时算「配置错误」，由 Validate 报错）。
func (c TLSConfig) Enabled() bool { return c.CertFile != "" || c.KeyFile != "" }

// Validate 校验 TLS 配置成对出现：只配证书不配私钥（或反之）无法提供服务，
// 静默回落成明文是**最坏的失败方式**（运维以为链路已加密，实际裸奔）——故启动期直接报错。
func (c TLSConfig) Validate() error {
	if (c.CertFile == "") != (c.KeyFile == "") {
		return errors.New("http: TLS 配置不完整：cert_file 与 key_file 必须成对配置")
	}
	return nil
}

// Config HTTP 服务器配置。
type Config struct {
	Addr           string        // TCP 监听地址，如 ":9102" 或 "127.0.0.1:9102"
	ReadTimeout    time.Duration // 读取整个请求体的超时，0 用默认
	WriteTimeout   time.Duration // 写入响应的超时，0 用默认
	IdleTimeout    time.Duration // keep-alive 空闲超时，0 用默认
	MaxHeaderBytes int           // 最大请求头大小（字节），0 用默认 1MB
	// TLS 非空（CertFile/KeyFile 均配置）时以 HTTPS 提供服务；
	// 未配置 = 明文 HTTP（是否允许明文由**上层**的配置策略决定，见 domain/auth）。
	TLS TLSConfig
}

func (c Config) normalize() Config {
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = defaultMaxHeaderBytes
	}
	return c
}
