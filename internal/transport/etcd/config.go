package etcd

import (
	"errors"
	"time"
)

// ErrKeyNotFound 表示请求的 key 在 etcd 中不存在
var ErrKeyNotFound = errors.New("etcd: key not found")

const (
	defaultDialTimeout  = 5 * time.Second  // 建立连接超时
	defaultOpTimeout    = 3 * time.Second  // 单次 KV 操作超时
	watchRetryInterval  = 2 * time.Second  // watch 断线重连间隔
	defaultRegisterTTL  = 10 * time.Second // 服务注册租约 TTL，<=0 时使用
	defaultCloseTimeout = 5 * time.Second  // Close 等待后台 goroutine 退出的上限
)

// TLSConfig ETCD TLS 双向认证证书路径配置
// CertFile/KeyFile 均为空时跳过客户端认证；CAFile 为空使用系统根证书池
type TLSConfig struct {
	CertFile string `yaml:"cert_file" mapstructure:"cert_file"` // 客户端证书 PEM 路径
	KeyFile  string `yaml:"key_file"  mapstructure:"key_file"`  // 客户端私钥 PEM 路径
	CAFile   string `yaml:"ca_file"   mapstructure:"ca_file"`   // CA 根证书 PEM 路径，为空使用系统根证书池
}

// EtcdConfig ETCD 连接参数，含认证与 TLS 可选配置
// 字段均带 yaml tag，可从 etcd.yaml 反序列化（经 viper/config 加载）
type EtcdConfig struct {
	Endpoints   []string      `yaml:"endpoints"    mapstructure:"endpoints" validate:"required"` // ETCD 集群节点地址列表，元素格式 ip:port
	ConfigKey   string        `yaml:"config_key"   mapstructure:"config_key"`                    // 业务配置/数据在 ETCD 中的存储 key 路径
	Username    string        `yaml:"username"     mapstructure:"username"`                      // ETCD 用户名，为空跳过认证
	Password    string        `yaml:"password"     mapstructure:"password"`                      // ETCD 密码，为空跳过认证
	TLS         *TLSConfig    `yaml:"tls"          mapstructure:"tls"`                           // TLS 双向认证配置，nil 跳过 TLS
	DialTimeout time.Duration `yaml:"dial_timeout" mapstructure:"dial_timeout"`                  // 连接超时，<=0 使用默认 5s
	RegisterTTL time.Duration `yaml:"register_ttl" mapstructure:"register_ttl"`                  // 服务注册租约 TTL，<=0 使用默认 10s
}
