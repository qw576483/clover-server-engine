package mysql

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	mysqlerr "github.com/go-sql-driver/mysql"

	"github.com/qw576483/clover-server-engine/internal/shared/config"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// MySQLConfig MySQL 连接配置，对应 yaml 中 mysql 节点，
// 支持经 config.Loader 自动反序列化（yaml + mapstructure 双 tag）。
type MySQLConfig struct {
	Host            string        `yaml:"host" mapstructure:"host" validate:"required"`
	Port            int           `yaml:"port" mapstructure:"port" validate:"min=1"`
	User            string        `yaml:"user" mapstructure:"user" validate:"required"`
	Pass            string        `yaml:"pass" mapstructure:"pass"`
	DBName          string        `yaml:"db_name" mapstructure:"db_name" validate:"required"`
	Charset         string        `yaml:"charset" mapstructure:"charset"`
	ParseTime       bool          `yaml:"parse_time" mapstructure:"parse_time"`   // 解析 DATETIME/DATE 为 time.Time
	Loc             string        `yaml:"loc" mapstructure:"loc"`                 // 时区，默认 Local
	AutoCreate      bool          `yaml:"auto_create" mapstructure:"auto_create"` // 启动自动 CREATE DATABASE IF NOT EXISTS（仅开发环境建议开启）
	MaxOpenConns    int           `yaml:"max_open_conns" mapstructure:"max_open_conns" validate:"min=1"`
	MaxIdleConns    int           `yaml:"max_idle_conns" mapstructure:"max_idle_conns" validate:"min=0"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime" mapstructure:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time" mapstructure:"conn_max_idle_time"`
	DialTimeout     time.Duration `yaml:"dial_timeout" mapstructure:"dial_timeout"`
	ReadTimeout     time.Duration `yaml:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout" mapstructure:"write_timeout"`
}

// 默认值表：DefaultConfig（开发基线）与 normalize（补零值）都从这里取。
//
// 此前两处各写一份（3306 / utf8mb4 / Local / 20 / 10 / 1h / 30m / 5s / 3s / 3s 共十项），
// 改一处漏一处就会让「显式走 DefaultConfig」与「走 normalize 的 yaml 配置」行为不一致。
const (
	defaultPort            = 3306
	defaultCharset         = "utf8mb4"
	defaultLoc             = "Local"
	defaultMaxOpenConns    = 20
	defaultMaxIdleConns    = 10
	defaultConnMaxLifetime = time.Hour
	defaultConnMaxIdleTime = 30 * time.Minute
	defaultDialTimeout     = 5 * time.Second
	defaultReadTimeout     = 3 * time.Second
	defaultWriteTimeout    = 3 * time.Second
)

// DefaultConfig 返回本地开发可用的默认配置。
// 注意：ParseTime 默认 true（否则 DATETIME/DATE 无法映射为 time.Time，需显式置 true）。
// DBName 默认为 "clover"（与 NewClient 校验要求一致），生产环境务必通过 yaml 覆盖。
func DefaultConfig() MySQLConfig {
	logger.Warnf("mysql: DefaultConfig uses user=root with EMPTY password - DO NOT use in production; set mysql.user/pass via yaml")
	// 只写「与 normalize 不同」的字段，其余（端口 / 字符集 / 池大小 / 各项超时）走同一张默认值表。
	return MySQLConfig{
		Host:      "127.0.0.1",
		User:      "root",
		Pass:      "",
		DBName:    "clover",
		ParseTime: true,
	}.normalize()
}

// normalize 补全零值字段的默认配置（不改变 Host/User/DBName，这些由调用方保证非空）。
func (conf MySQLConfig) normalize() MySQLConfig {
	c := conf
	c.Port = config.DefInt(c.Port, defaultPort)
	c.Charset = config.DefString(c.Charset, defaultCharset)
	c.Loc = config.DefString(c.Loc, defaultLoc)
	c.MaxOpenConns = config.DefInt(c.MaxOpenConns, defaultMaxOpenConns)
	c.MaxIdleConns = config.DefInt(c.MaxIdleConns, defaultMaxIdleConns)
	// 确保 MaxIdleConns <= MaxOpenConns，否则连接池行为异常。
	if c.MaxIdleConns > c.MaxOpenConns {
		c.MaxIdleConns = c.MaxOpenConns
	}
	c.ConnMaxLifetime = config.DefDuration(c.ConnMaxLifetime, defaultConnMaxLifetime)
	c.ConnMaxIdleTime = config.DefDuration(c.ConnMaxIdleTime, defaultConnMaxIdleTime)
	c.DialTimeout = config.DefDuration(c.DialTimeout, defaultDialTimeout)
	c.ReadTimeout = config.DefDuration(c.ReadTimeout, defaultReadTimeout)
	c.WriteTimeout = config.DefDuration(c.WriteTimeout, defaultWriteTimeout)
	return c
}

// DSN 生成 go-sql-driver/mysql 连接串（带 db_name）。Host/User/DBName 必须非空，否则报错。
func (conf MySQLConfig) DSN() (string, error) {
	if strings.TrimSpace(conf.DBName) == "" {
		return "", fmt.Errorf("mysql DSN: db_name is required")
	}
	return conf.dsn(conf.DBName)
}

// rootDSN 生成不含 db_name 的连接串（连 root/system 库用），用于建库前探活。
func (conf MySQLConfig) rootDSN() (string, error) { return conf.dsn("") }

// dsn 是 DSN / rootDSN 的共体：dbName 为空表示「不选库」。
//
// 两个公开方法此前各抄了一份校验与参数拼装，唯一差别是路径段的 "/<db>" 与 "/"；
// 合并后只剩一处需要维护（如新增连接参数时不会漏改其中一个）。
func (conf MySQLConfig) dsn(dbName string) (string, error) {
	if strings.TrimSpace(conf.Host) == "" {
		return "", fmt.Errorf("mysql DSN: host is required")
	}
	if conf.Port <= 0 {
		return "", fmt.Errorf("mysql DSN: port must be positive")
	}
	if strings.TrimSpace(conf.User) == "" {
		return "", fmt.Errorf("mysql DSN: user is required")
	}
	// 用驱动官方 Config.FormatDSN 组装：手写 `user:pass@tcp(...)` 时密码含 @ / : / ? / /
	// 等字符会把 DSN 截断成错误的 host/db（连接失败且报错误导）。
	cfg := mysqlerr.NewConfig()
	cfg.User = conf.User
	cfg.Passwd = conf.Pass
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(conf.Host, strconv.Itoa(conf.Port))
	cfg.DBName = dbName
	cfg.ParseTime = conf.ParseTime
	cfg.Timeout = conf.DialTimeout
	cfg.ReadTimeout = conf.ReadTimeout
	cfg.WriteTimeout = conf.WriteTimeout
	cfg.Loc = time.UTC // 与驱动 NewConfig 的默认一致；conf.Loc 非空时按配置覆盖
	if conf.Loc != "" {
		l, err := time.LoadLocation(conf.Loc)
		if err != nil {
			return "", fmt.Errorf("mysql DSN: invalid loc %q: %w", conf.Loc, err)
		}
		cfg.Loc = l
	}
	if conf.Charset != "" {
		// charset 是自定义参数，经 Params 透传（FormatDSN 会做 url 转义）。
		cfg.Params = map[string]string{"charset": conf.Charset}
	}
	return cfg.FormatDSN(), nil
}
