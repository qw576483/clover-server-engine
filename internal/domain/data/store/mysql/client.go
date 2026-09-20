package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	// 导入 mysql 驱动并命名，以便识别 MySQL 错误号；init 仍会注册驱动。
	mysqlerr "github.com/go-sql-driver/mysql"

	"clover-server-engine/pkg/foundation/logger"
)

// 错误变量
var (
	// ErrNotFound 表示查询未返回任何行。
	ErrNotFound = fmt.Errorf("mysql: no rows in result set")
	// errClosed 客户端已关闭：Close 后所有命令返回该错误而不是 nil 指针 panic。
	errClosed = errors.New("mysql: client is closed")
)

// Client MySQL 客户端封装，持有 *sql.DB 连接池，对外提供查询/事务能力。
// 连接生命周期见本文件；事务封装见 tx.go；反射扫描见 scan.go。
type Client struct {
	conf    MySQLConfig
	closeMu sync.RWMutex // 保护 db：Close 与并发命令 / 重复 Close 的竞争
	db      *sql.DB
}

// NewClient 根据配置创建 MySQL 客户端：构造 DSN、初始化连接池并 Ping 验证连通性。
func NewClient(conf MySQLConfig) (*Client, error) {
	if strings.TrimSpace(conf.Host) == "" {
		return nil, fmt.Errorf("mysql: host is required")
	}
	if strings.TrimSpace(conf.User) == "" {
		return nil, fmt.Errorf("mysql: user is required")
	}
	if strings.TrimSpace(conf.DBName) == "" {
		return nil, fmt.Errorf("mysql: db_name is required")
	}
	// 校验库名字符合法性，防止拼接 SQL 时的注入风险。
	if err := sanitizeDBName(conf.DBName); err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}

	c := conf.normalize()
	dsn, err := c.DSN()
	if err != nil {
		return nil, err
	}

	// 开发期可选：连接前自动建库（幂等，连 root 库执行 CREATE DATABASE IF NOT EXISTS）。
	if c.AutoCreate {
		if err := ensureDatabase(context.Background(), c); err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql open %s:%d/%s: %w", c.Host, c.Port, c.DBName, err)
	}
	db.SetMaxOpenConns(c.MaxOpenConns)
	db.SetMaxIdleConns(c.MaxIdleConns)
	db.SetConnMaxLifetime(c.ConnMaxLifetime)
	db.SetConnMaxIdleTime(c.ConnMaxIdleTime)

	dctx, cancel := context.WithTimeout(context.Background(), c.DialTimeout)
	defer cancel()
	if err := db.PingContext(dctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql ping %s:%d/%s: %w", c.Host, c.Port, c.DBName, err)
	}

	logger.Infof("mysql connected: %s:%d db=%s max_open=%d max_idle=%d",
		c.Host, c.Port, c.DBName, c.MaxOpenConns, c.MaxIdleConns)
	return &Client{conf: c, db: db}, nil
}

// Close 关闭连接池。幂等且并发安全：重复 / 并发调用只真正关闭一次。
func (c *Client) Close() error {
	c.closeMu.Lock()
	if c.db == nil {
		c.closeMu.Unlock()
		return nil
	}
	db := c.db
	c.db = nil
	c.closeMu.Unlock()
	err := db.Close()
	logger.Infof("mysql client closed: %s:%d/%s", c.conf.Host, c.conf.Port, c.conf.DBName)
	return err
}

// dbHandle 返回连接池句柄；客户端已关闭时返回 errClosed。
// 所有命令统一经此获取句柄：Close 置 nil 后并发命令直接解引用会 panic。
func (c *Client) dbHandle() (*sql.DB, error) {
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	if c.db == nil {
		return nil, errClosed
	}
	return c.db, nil
}

// Raw 返回底层 *sql.DB，供高级场景直接调用原生命令。
// 客户端已关闭时返回 nil，调用方必须判空（本方法不做守卫）。
func (c *Client) Raw() *sql.DB {
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	return c.db
}

// NewClientFromDB 用已有 *sql.DB 构造客户端，主要用于单测注入 go-sqlmock。
func NewClientFromDB(db *sql.DB) *Client { return &Client{db: db} }

// Config 返回客户端配置副本。
func (c *Client) Config() MySQLConfig { return c.conf }

// isSafeCharset 校验字符集名（仅允许 [a-zA-Z0-9_]），防 DDL 注入。
func isSafeCharset(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '_') {
			return false
		}
	}
	return true
}

// sanitizeDBName 校验库名字符（仅允许 [a-zA-Z0-9_-]），防 SQL 注入。
// 直接拼接 DBName 到 SQL 字符串存在注入风险，先校验字符白名单。
func sanitizeDBName(name string) error {
	for _, ch := range name {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return fmt.Errorf("mysql: invalid db_name character %q", ch)
		}
	}
	return nil
}

// rootDSN 已挪到 config.go（与 DSN 共用同一份校验与参数拼装）。

// ensureDatabase 连 root 库执行 CREATE DATABASE IF NOT EXISTS（幂等，不破坏既有数据）。
func ensureDatabase(ctx context.Context, c MySQLConfig) error {
	// 建库前校验库名白名单，防止注入。
	if err := sanitizeDBName(c.DBName); err != nil {
		return err
	}
	root, err := c.rootDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", root)
	if err != nil {
		return fmt.Errorf("mysql open (system) %s:%d: %w", c.Host, c.Port, err)
	}
	// 本连接仅用于一次连通性探测，函数返回前必须释放；关闭失败无补救动作，显式忽略。
	defer func() { _ = db.Close() }()

	dctx, cancel := context.WithTimeout(ctx, c.DialTimeout)
	defer cancel()
	if err := db.PingContext(dctx); err != nil {
		return fmt.Errorf("mysql ping (system) %s:%d: %w", c.Host, c.Port, err)
	}
	// Charset 直接拼进 DDL（标识符无法参数化）：与 DBName 同样做字符白名单校验，
	// 防止配置写入含分号/反引号的值（当前 DSN 未开 multiStatements 时危害有限，
	// 但配置面一旦可外部改写即成注入面）。
	if !isSafeCharset(c.Charset) {
		return fmt.Errorf("mysql: invalid charset %q (want [A-Za-z0-9_]+)", c.Charset)
	}
	q := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET %s",
		c.DBName, c.Charset)
	if _, err := db.ExecContext(dctx, q); err != nil {
		return fmt.Errorf("mysql create database %s: %w", c.DBName, err)
	}
	logger.Infof("mysql ensure database: %s (created or already exists)", c.DBName)
	return nil
}

// IsDuplicateError 判断 err 是否为 MySQL 唯一键冲突（错误号 1062）。
// 真实驱动走类型断言；测试桩（sqlmock/文本模拟）走字符串兜底。
func IsDuplicateError(err error) bool {
	var e *mysqlerr.MySQLError
	if errors.As(err, &e) && e.Number == 1062 {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "Error 1062")
}

// Ping 执行健康检查。
func (c *Client) Ping(ctx context.Context) error {
	db, err := c.dbHandle()
	if err != nil {
		return err
	}
	return db.PingContext(ctx)
}

// Exec 执行写操作（INSERT/UPDATE/DELETE/DDL），返回 sql.Result。
func (c *Client) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	db, err := c.dbHandle()
	if err != nil {
		return nil, err
	}
	return db.ExecContext(ctx, query, args...)
}

// Query 执行查询并返回 *sql.Rows，调用方需自行关闭 rows（或用 Get/Select 简化）。
func (c *Client) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db, err := c.dbHandle()
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, query, args...)
}

// QueryWith 执行查询并在回调中处理结果，自动关闭 rows，避免连接泄漏。
func (c *Client) QueryWith(ctx context.Context, query string, args []any, fn func(rows *sql.Rows) error) error {
	db, err := c.dbHandle()
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	// 行集由 fn 消费（其内部读完并回传错误），此处只保证释放；关闭失败显式忽略。
	defer func() { _ = rows.Close() }()
	return fn(rows)
}

// Get 查询单行并反射到 dest（结构体指针或基础类型指针）。无结果返回 ErrNotFound。
func (c *Client) Get(ctx context.Context, dest any, query string, args ...any) error {
	db, err := c.dbHandle()
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return scanOne(rows, dest)
}

// Select 查询多行并反射到 dest（切片指针）。
func (c *Client) Select(ctx context.Context, dest any, query string, args ...any) error {
	db, err := c.dbHandle()
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return scanSlice(dest, rows)
}

// ScanOne 从 rows 读取单行并反射到 dest，然后关闭 rows。无结果返回 ErrNotFound。
// 一般在配合 Query 手动管理 rows 时使用。
func (c *Client) ScanOne(dest any, rows *sql.Rows) error {
	return scanOne(rows, dest)
}

// ScanSlice 从 rows 读取所有行并反射到 dest（切片指针），然后关闭 rows。
func (c *Client) ScanSlice(dest any, rows *sql.Rows) error {
	return scanSlice(dest, rows)
}

// Begin 开启事务。
func (c *Client) Begin(ctx context.Context, opts ...*sql.TxOptions) (*Tx, error) {
	db, err := c.dbHandle()
	if err != nil {
		return nil, err
	}
	var opt *sql.TxOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	tx, err := db.BeginTx(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("mysql begin tx: %w", err)
	}
	return &Tx{tx: tx}, nil
}
