// Package auth 汇总账号服（server_type=auth）的域级公共物：配置与业务扩展点。
//
// 本文件只定义配置结构与默认值回落逻辑，不含任何运行时实现；
// 具体能力分别落在子包：
//   - state：账号服核心能力（注册 / 登录 / 验签 / 渠道登录 + 撞库防护）与线协议契约
//   - server：HTTP 路由与传输适配（把 state 挂到角色内核的控制面）
//   - client：调用侧（game 校验登录凭证时调账号服 /auth/verify）
//
// channel.go 定义账号体系唯一需要业务实现的扩展点（渠道票据校验器）。
//
// 角色宿主（AuthGame）与进程装配留在 internal/app —— 与 master / log 的分工一致：
// domain 只管本域能力，宿主只管 Core 接线与生命周期。
package auth

import (
	"errors"
	"fmt"
	"time"

	"clover-server-engine/internal/shared/config"
	"clover-server-engine/pkg/foundation/logger"
)

// 默认值：全部可通过配置覆盖，零值自动回落到这里。
const (
	// DefaultTokenTTL JWT 默认有效期。
	DefaultTokenTTL = 2 * time.Hour
	// DefaultIssuer JWT 默认签发者（iss 声明）。
	DefaultIssuer = "clover-auth"
	// DefaultRPCListenAddr 账号服**消息通道**默认监听地址。
	//
	// 说明：master / log 的消息通道默认监听地址是**空串**（不启用，需显式配置），
	// 只有账号服有默认值——它必须在 auth 单独部署时也能被 game 连上
	// （game 经 Game.CallAuth 走本通道发业务消息；登录校验不在此通道，固定走 HTTP）。
	DefaultRPCListenAddr = "127.0.0.1:8061"
)

// AuthConfig 账号服与登录链路配置。
//
// 引擎只有**一种**登录模式：账号体系外置到账号服（server_type=auth），
// 由它校验账号密码并签发 JWT；游戏服每次登录都调账号服的 /auth/verify 换取 owner。
// 因此游戏服不碰账号表、不存密码，也不持有签名密钥。
//
// 同一个结构在两种角色下各取所需字段（缺项在启动期即报错，不做静默降级）：
//   - 账号服角色（auth / all）：Listen、JWTSecret 必填；
//   - 游戏服角色（game / gateway / all）：VerifyAddr 必填。
type AuthConfig struct {
	// Listen 账号服的 HTTP 监听地址（server_type=auth 必填，如 "127.0.0.1:8051"）。
	// 传输安全由 TLS / InsecurePlaintext 决定（见 ValidateAuthServer）：默认要求 TLS。
	Listen string `yaml:"listen" mapstructure:"listen"`
	// JWTSecret JWT 签名密钥（HS256，字节串）。账号服签发与验签都用它。
	// 生产务必使用足够长的随机串，并按密钥管理流程轮换。
	JWTSecret string `yaml:"jwt_secret" mapstructure:"jwt_secret"`
	// TokenTTL 签发 token 的有效期；<=0 时用内置默认 2h。
	// 短 TTL 更安全（泄露窗口小），但需要客户端按时重新登录换取。
	TokenTTL time.Duration `yaml:"token_ttl" mapstructure:"token_ttl"`
	// Issuer JWT 签发者（iss 声明）；空=内置默认 "clover-auth"。
	// 用于多环境共用密钥时区分签发来源。
	Issuer string `yaml:"issuer" mapstructure:"issuer"`

	// VerifyAddr 游戏服校验登录凭证的账号服地址（server_type=game/gateway/all 必填），
	// 如 "https://127.0.0.1:8051"（账号服默认要求 TLS，故通常是 https；明文部署见 insecure_plaintext）。
	// 自签 / 内网 CA 用 VerifyCAFile。登录时游戏服会 POST {VerifyAddr}/auth/verify 换取 owner，
	// 所以账号服必须处于可访问状态——这是「账号服必须启动」的落点。
	VerifyAddr string `yaml:"verify_addr" mapstructure:"verify_addr"`
	// VerifyTimeout 调用 /auth/verify 的单次超时；<=0 时由调用侧（client 包）回落默认 3s。
	// 登录是交互式操作：超时过长玩家干等，过短会在账号服抖动时误报不可用。
	VerifyTimeout time.Duration `yaml:"verify_timeout" mapstructure:"verify_timeout"`

	// RpcListen 账号服**消息通道**监听地址（server_type=auth / all 时生效）；空=用内置默认。
	// 与 master / log 的 *_listen_addr 对称：game 经 Game.CallAuth 走本通道发业务消息
	// （auth 侧 app.Mount(app.RoleAuth, ...) + AuthGame.OnMsg 收）。**登录校验不在此通道**，
	// 固定走 HTTP POST {VerifyAddr}/auth/verify。
	RpcListen string `yaml:"rpc_listen" mapstructure:"rpc_listen"`
	// RpcAddr 游戏服连接账号服消息通道的地址（server_type=game / all 时生效）；空=用内置默认。
	RpcAddr string `yaml:"rpc_addr" mapstructure:"rpc_addr"`

	// RateLimitPerSec 账号服 HTTP 凭据端点的 **per-IP** 每秒请求上限
	// （/auth/signup、/auth/login、/auth/verify）；<=0 用内置默认 5。
	// 域内的 per-account 撞库防护只按账号计数，换账号名即可绕过，故必须有来源维度限流。
	RateLimitPerSec int `yaml:"rate_limit_per_sec" mapstructure:"rate_limit_per_sec"`
	// RateLimitBurst 上者的突发桶容量；<=0 用内置默认 10。
	// NAT / 同网段多玩家共享出口 IP 时会被算作同一来源，按承载规模调大即可。
	RateLimitBurst int `yaml:"rate_limit_burst" mapstructure:"rate_limit_burst"`

	// TLS 账号服 HTTP 的证书对（server_type=auth 生效）。配置后以 HTTPS 提供服务。
	// **默认要求 TLS**：账号服 POST 的是明文口令与 JWT，走明文 HTTP 等于把凭据与
	// 登录态直接摆在网络上（同网可达者抓包即得）。未配置证书时见 InsecurePlaintext。
	TLS TLSServerConfig `yaml:"tls" mapstructure:"tls"`
	// InsecurePlaintext 显式放行明文 HTTP（默认 false）。仅供本机 / 完全隔离的内网联调：
	// 置 true 时**不校验**证书配置并打印一条 Warn（明文告警）。这是「可配置回退」的口，
	// 不是推荐值——生产环境请配 TLS。
	InsecurePlaintext bool `yaml:"insecure_plaintext" mapstructure:"insecure_plaintext"`

	// VerifyCAFile 游戏服校验账号服证书所用的 CA 证书包（server_type=game/all 生效）。
	// 空 = 用系统根证书。自签 / 内网 CA 签发时填它，**不要**靠 VerifyInsecureSkipVerify 绕过。
	VerifyCAFile string `yaml:"verify_ca_file" mapstructure:"verify_ca_file"`
	// VerifyInsecureSkipVerify 跳过账号服证书校验（server_type=game/all 生效，默认 false）。
	// ⚠️ 置 true 会让 TLS 退化成「只防被动嗅探、不防中间人」；仅在内网自签且来不及配
	// VerifyCAFile 时临时使用，并且调用侧会打印一条 Warn。
	VerifyInsecureSkipVerify bool `yaml:"verify_insecure_skip_verify" mapstructure:"verify_insecure_skip_verify"`
}

// TLSServerConfig 账号服 HTTP 服务端的证书配置。
type TLSServerConfig struct {
	// CertFile PEM 证书链路径。
	CertFile string `yaml:"cert_file" mapstructure:"cert_file"`
	// KeyFile PEM 私钥路径；必须与 CertFile 成对配置。
	KeyFile string `yaml:"key_file" mapstructure:"key_file"`
}

// Enabled 是否配置了 TLS 证书对。
func (c TLSServerConfig) Enabled() bool { return c.CertFile != "" || c.KeyFile != "" }

// ValidateAuthServer 校验「账号服角色」的安全边界，返回非 nil 表示**不允许启动**。
//
// 两条硬规则（行业默认实践，二者取其一，不能不选）：
//  1. 配了证书对 ⇒ 以 HTTPS 提供服务（传输层负责，见 internal/transport/net/http）；
//  2. 没配证书对 ⇒ 必须显式 InsecurePlaintext=true 才放行明文（并 Warn），
//     否则**拒绝启动**——静默明文上线 = 口令与 token 在网络中裸奔，且事后无人知情。
//
// 只配一半（有证书无私钥 / 反之）一律报错，不回落成明文。
func (c AuthConfig) ValidateAuthServer() error {
	switch {
	case c.TLS.CertFile != "" && c.TLS.KeyFile != "":
		logger.Infof("auth: 账号服 HTTP 已启用 TLS (cert=%s)", c.TLS.CertFile)
		return nil
	case c.TLS.Enabled():
		return fmt.Errorf("auth: TLS 配置不完整：tls.cert_file 与 tls.key_file 必须成对配置（cert=%q key=%q）",
			c.TLS.CertFile, c.TLS.KeyFile)
	case c.InsecurePlaintext:
		logger.Warnf("auth: 账号服 HTTP 以**明文**提供服务（insecure_plaintext=true）—— " +
			"口令与 JWT 会在网络中以明文传输，仅限本机 / 完全隔离的内网联调")
		return nil
	default:
		return errors.New("auth: 账号服 HTTP 默认要求 TLS：请配置 auth.tls.cert_file / auth.tls.key_file；" +
			"若确实只能明文（本机联调 / 隔离内网），显式设置 auth.insecure_plaintext=true")
	}
}

// Normalize 把零值回落到默认值，返回修正后的副本（不修改接收者，便于配置对象复用）。
//
// VerifyTimeout 刻意不在这里回落：它的消费者是调用侧（client 包，超时属于调用行为），
// 在那边回落可避免同一个默认值在配置层与调用层各写一份。
func (c AuthConfig) Normalize() AuthConfig {
	c.TokenTTL = config.DefDuration(c.TokenTTL, DefaultTokenTTL)
	c.Issuer = config.DefString(c.Issuer, DefaultIssuer)
	c.RpcListen = config.DefString(c.RpcListen, DefaultRPCListenAddr)
	return c
}
