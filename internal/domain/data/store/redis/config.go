package redis

import (
	"strings"
	"time"

	"clover-server-engine/pkg/foundation/logger"
)

// RedisConfig Redis 连接配置，对应 yaml 中 redis 节点，
// 支持经 config.Loader 自动反序列化（yaml + mapstructure 双 tag）。
type RedisConfig struct {
	Mode         string        `yaml:"mode" mapstructure:"mode"`                            // 连接模式：standalone/cluster/sentinel，空则按 addrs/master_name 自动推断
	Addr         string        `yaml:"addr" mapstructure:"addr"`                            // 单实例地址 "host:port"（standalone 用），也可逗号分隔多节点
	Addrs        []string      `yaml:"addrs" mapstructure:"addrs"`                          // 多节点地址：cluster 各节点 / sentinel 哨兵列表，优先于 addr
	MasterName   string        `yaml:"master_name" mapstructure:"master_name"`              // sentinel 模式的 master 名称，非空即视为哨兵
	User         string        `yaml:"user" mapstructure:"user"`                            // 用户名（Redis 6+ ACL），空则不认证
	Pass         string        `yaml:"pass" mapstructure:"pass"`                            // 密码
	SentinelPass string        `yaml:"sentinel_pass" mapstructure:"sentinel_pass"`          // 哨兵节点密码（仅 sentinel 模式，可选）
	DB           int           `yaml:"db" mapstructure:"db"`                                // 逻辑数据库编号，默认 0（cluster 模式忽略）
	PoolSize     int           `yaml:"pool_size" mapstructure:"pool_size" validate:"min=1"` // 连接池大小，默认 20
	MinIdleConns int           `yaml:"min_idle_conns" mapstructure:"min_idle_conns"`        // 最小空闲连接数，默认 5
	DialTimeout  time.Duration `yaml:"dial_timeout" mapstructure:"dial_timeout"`            // 拨号超时，默认 5s
	ReadTimeout  time.Duration `yaml:"read_timeout" mapstructure:"read_timeout"`            // 读超时，默认 3s
	WriteTimeout time.Duration `yaml:"write_timeout" mapstructure:"write_timeout"`          // 写超时，默认 3s
	MaxRetries   int           `yaml:"max_retries" mapstructure:"max_retries"`              // 命令最大重试次数，默认 3
}

// 连接模式常量。
const (
	ModeStandalone = "standalone"
	ModeCluster    = "cluster"
	ModeSentinel   = "sentinel"
)

// DefaultConfig 返回本地开发可用的默认配置（单实例）。
// ⚠️ 仅限本地开发/测试环境使用，生产环境务必通过 yaml 覆盖 Addr/Mode。
func DefaultConfig() RedisConfig {
	return RedisConfig{
		Mode:         ModeStandalone,
		Addr:         "127.0.0.1:6379",
		DB:           0,
		PoolSize:     20,
		MinIdleConns: 5,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxRetries:   3,
	}
}

// resolveAddrs 归一化节点地址：优先 Addrs，其次拆分 Addr（逗号分隔），都为空则回退本地。
func (conf RedisConfig) resolveAddrs() []string {
	if len(conf.Addrs) > 0 {
		return conf.Addrs
	}
	if conf.Addr != "" {
		var out []string
		for _, p := range strings.Split(conf.Addr, ",") {
			if s := strings.TrimSpace(p); s != "" {
				out = append(out, s)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return []string{"127.0.0.1:6379"}
}

// resolveMode 归一化连接模式：显式 Mode 优先，空值按 master_name/多节点自动推断。
func (conf RedisConfig) resolveMode() string {
	switch m := strings.ToLower(strings.TrimSpace(conf.Mode)); m {
	case ModeCluster:
		return ModeCluster
	case ModeSentinel:
		return ModeSentinel
	case "single":
		// 显式声明的单实例：不再自动推断（此前 single 与空值同分支，导致
		// 配了 master_name / 多 addrs 时被静默当成 standalone 连 addrs[0]，
		// 用户以为在走 cluster/sentinel）。配置互相矛盾时告警。
		if conf.MasterName != "" || len(conf.resolveAddrs()) > 1 {
			logger.Warnf("redis: mode=single but master_name=%q addrs=%v indicate cluster/sentinel; connecting standalone",
				conf.MasterName, conf.resolveAddrs())
		}
		return ModeStandalone
	case ModeStandalone, "":
		// 空模式下按拓扑自动推断
		if m == "" {
			if conf.MasterName != "" {
				return ModeSentinel
			}
			if len(conf.resolveAddrs()) > 1 {
				return ModeCluster
			}
		}
		return ModeStandalone
	default:
		return m // 交由 NewClient 报未知模式错误
	}
}
