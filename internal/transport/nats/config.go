package nats

import "time"

// NatsConfig NATS 连接配置，对应 yaml 中 nats 节点，
// 支持经 config.Loader 自动反序列化（yaml + mapstructure 双 tag）。
type NatsConfig struct {
	Addr       string `yaml:"addr" mapstructure:"addr"`               // 集群地址，多地址逗号分隔
	User       string `yaml:"user" mapstructure:"user"`               // 账号，空表示匿名
	Pass       string `yaml:"pass" mapstructure:"pass"`               // 密码
	ClientName string `yaml:"client_name" mapstructure:"client_name"` // 客户端标识，日志区分服务实例
	// MaxReconnect 最大重连次数。**指针是为了区分「未设置」与「显式配 0」**，两者语义相反：
	//   - nil（yaml 里没有 max_reconnect 键）→ 未设置 ⇒ 按 defaultMaxReconnect(-1) 处理 = 无限重连；
	//   - 显式 0 → 不重连（nats.MaxReconnects(0) 的语义，断线即断开）；
	//   - 正数 → 最多重连该次数。
	// 历史形态是 `int` + `if conf.MaxReconnect == 0 { conf.MaxReconnect = -1 }`：零值被当成
	// 「未设置」，于是「显式配 0 = 不重连」被静默改写成无限重连，调用方无法表达「不重连」。
	MaxReconnect   *int            `yaml:"max_reconnect" mapstructure:"max_reconnect"`
	ReconnectDelay time.Duration   `yaml:"reconnect_delay" mapstructure:"reconnect_delay"` // 重连间隔
	DialTimeout    time.Duration   `yaml:"dial_timeout" mapstructure:"dial_timeout"`       // 初次连接超时
	MsgTimeout     time.Duration   `yaml:"msg_timeout" mapstructure:"msg_timeout"`         // 同步 Request 应答超时
	JetStream      JetStreamConfig `yaml:"jetstream" mapstructure:"jetstream"`             // JetStream 持久化配置
	TLS            *TLSConfig      `yaml:"tls" mapstructure:"tls"`                         // TLS 配置，nil 表示不启用 TLS
}

// TLSConfig NATS TLS 配置。
type TLSConfig struct {
	CertFile string `yaml:"cert_file" mapstructure:"cert_file"` // 客户端证书路径
	KeyFile  string `yaml:"key_file" mapstructure:"key_file"`   // 客户端私钥路径
	CAFile   string `yaml:"ca_file" mapstructure:"ca_file"`     // CA 证书路径，用于验证服务端
}

// JetStreamConfig JetStream 持久化配置。
type JetStreamConfig struct {
	Enable bool `yaml:"enable" mapstructure:"enable"` // 游戏必须开启持久化，防止消息丢失
}

// defaultMaxReconnect 是「未设置 max_reconnect」时的默认值：-1 = 无限重连，仅 Close() 才断开。
const defaultMaxReconnect = -1

// MaxReconnectPtr 返回指向 v 的指针，便于业务/默认配置显式表达 max_reconnect（含显式 0 = 不重连）。
func MaxReconnectPtr(v int) *int { return &v }

// DefaultConfig 返回本地单实例可用的默认配置。
func DefaultConfig() NatsConfig {
	return NatsConfig{
		Addr:           "nats://127.0.0.1:4222",
		ClientName:     "clover",
		MaxReconnect:   MaxReconnectPtr(defaultMaxReconnect),
		ReconnectDelay: time.Second,
		DialTimeout:    2 * time.Second,
		MsgTimeout:     500 * time.Millisecond,
		JetStream: JetStreamConfig{
			Enable: true,
		},
	}
}
