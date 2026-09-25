package types

import (
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	// DefaultListenAddr admin HTTP 控制面默认监听地址。
	//
	// 必须是**回环地址**：admin 挂着 /admin/shutdown、/admin/gateway/upstream、/metrics、
	// /debug/pprof 等高危端点，绑到通配地址等于把运维面暴露到公网。
	// net.Listen("tcp", "") 会监听**所有网卡 + 随机端口**，不是「只绑定回环」。
	// 需要关闭请设 admin.disable=true，不要靠留空来表达。
	DefaultListenAddr      = "127.0.0.1:8041"
	DefaultShutdownTimeout = 5 * time.Second
)

// IsLoopbackAddr 判断 `host:port` / `host` 是否只绑定回环地址。
//
// 语义保守：拿不准（空 host = 所有网卡、无法解析的主机名）一律返回 false（按非回环处理）。
// 供 AdminConfig.Normalize 的「无令牌必须只绑回环」校验使用。
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// 没有端口（如 "127.0.0.1"）时按整串判断，避免把「明显是回环」写成非回环。
		host = addr
	}
	if host == "" {
		// net.Listen("tcp", "") / ":8041" 监听**所有网卡**，不是回环。
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// 主机名解析需要 DNS，此处不做（启动期阻塞 + 结果不可判定）；按非回环处理更安全。
		return false
	}
	return ip.IsLoopback()
}

type AdminConfig struct {
	Disable    bool   `yaml:"disable" mapstructure:"disable"`
	ListenAddr string `yaml:"listen_addr" mapstructure:"listen_addr"`
	// Token admin 控制面鉴权令牌。**空 = 不启用鉴权**（默认，兼容既有部署），
	// 此时 Normalize 要求 ListenAddr 必须是回环（见下），否则**拒绝启动**；
	// 非空时 /admin/*、/deadletter、/log/level、/debug/pprof 一律要求请求带同一令牌
	//（请求头 X-Admin-Token 或 Authorization: Bearer）。
	//
	// 为什么默认空而不是随机生成：控制面是**本地运维面**，随机令牌等于要求运维先去
	// 进程里找密码，反而会把「绑回环 + 防火墙」这条标准做法挤掉。需要内网多机访问时再显式配置。
	Token string `yaml:"token" mapstructure:"token"`
	// ShutdownTimeout 关闭时等待在途请求的超时上限。
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" mapstructure:"shutdown_timeout"`
	// Pprof 是否启用 /debug/pprof/*（默认关闭）。
	Pprof bool `yaml:"pprof" mapstructure:"pprof"`
}

// ErrNonLoopbackWithoutToken 未配置 Token 却把 admin 控制面绑到非回环地址。
//
// 这是**启动期硬错误**（调用方必须中止启动），不是可忽略的告警：admin 控制面挂着
// /admin/shutdown、/admin/drain、/admin/gateway/upstream、/deadletter、/log/level、/debug/pprof
// 等高危端点，无鉴权 + 非回环 = 任何能连到该地址的来源都能关服 / 切上游 / 改日志级别 / 改死信队列。
var ErrNonLoopbackWithoutToken = errors.New("admin: listen_addr is not loopback but admin.token is empty")

// Normalize 回落零值，并校验安全边界：**未配置 Token 时只允许绑回环**。
//
// 返回非 nil 表示配置不安全 —— 调用方**必须中止启动**（不是改写成回环继续跑）：
// 静默改写会让运维以为「已按配置绑到内网」，实际根本没生效，等到排查连通性时才发现，
// 属于「用一次假成功换一次真事故」。要绑内网就必须显式配 admin.token，
// 两者是**同时满足**关系（无 token ⇒ 只回环；有 token ⇒ 可非回环）。
func (c *AdminConfig) Normalize() error {
	if c.ListenAddr == "" {
		c.ListenAddr = DefaultListenAddr
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	// 已禁用 admin 时不校验：此配置根本不监听，端口无关安全。
	if !c.Disable && c.Token == "" && !IsLoopbackAddr(c.ListenAddr) {
		return fmt.Errorf("%w (listen_addr=%q)", ErrNonLoopbackWithoutToken, c.ListenAddr)
	}
	return nil
}

type TableLoadedEvent struct {
	Count   int
	Elapsed int
	Names   []string
}

type TableLoader interface {
	LoadAll(dir string) error
}

type ConnDisconnectEvent struct {
	ConnID   string
	Owner    string
	PlayerID string
}
