// Package account 账号通用业务逻辑（登录身份层）。

// 设计要点：
// - 账号基础信息（账号 + 密码哈希 + token + 时间戳）存放于引擎数据层表 account，
// 主键 id 为 16 位全大写英文+数字字符串（由 id.GenUID 生成），account 字段唯一。
// - 账号渠道绑定（账号 + 渠道类型 + 渠道账号信息）存放于 account_channel，主键 id 同为 GenUID。
// - 登录支持密码 + token 双模式：token 校验通过后直接登录，无需密码；密码登录成功后
// 自动刷新 token 与 login_time。
// - 其余"挂在账号上的数据"（背包/邮件/设置/角色...）不进这两张表，而是复用通用的
// data 三元键存储层，归属实体用 OwnerAccount —— 通过 AccountKey 构造 data.Key
// 即可把任意类型的数据绑定到某个账号，保持账号表的简洁与通用。

// 本包实现见 internal/domain/data/account，pkg/domain/data/account 仅做对外透传（符合仓库强制规则）。
package account

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"clover-server-engine/internal/domain/data"
	imysql "clover-server-engine/internal/domain/data/store/mysql"
	paccount "clover-server-engine/pkg/domain/data/account"
	"clover-server-engine/pkg/shared/id"
	"clover-server-engine/pkg/shared/timeutil"
)

// 业务错误：指向 pkg 层本体（确保与 pkg.ErrAccountExists 等是同一个 error 值）。
var (
	ErrAccountExists   = paccount.ErrAccountExists
	ErrAccountNotFound = paccount.ErrAccountNotFound
	ErrWrongPassword   = paccount.ErrWrongPassword
	ErrInvalidToken    = paccount.ErrInvalidToken
	ErrTokenExpired    = paccount.ErrTokenExpired
)

// ErrInvalidAccountParam 账号/密码不符合格式要求。
// 调用方（账号服）据此把「请求参数错误」与「存储/驱动故障」区分开映射，
// 避免把 MySQL 故障错报成 400，也避免把 err.Error() 原文回给客户端。
var ErrInvalidAccountParam = errors.New("account: invalid account or password param")

// parseMySQLTime 依次尝试常见 MySQL DATETIME 格式，兼容 DATETIME(6) 带小数秒。
func parseMySQLTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.000000", // DATETIME(6)
		"2006-01-02 15:04:05",        // DATETIME
		"2006-01-02",                 // DATE
	}
	var lastErr error
	for _, f := range formats {
		t, err := time.Parse(f, s)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("account: unsupported time format %q: %w", s, lastErr)
}

// sqlExec 持久层抽象（*imysql.Client 实现），便于依赖注入与单测替换（go-sqlmock）。
// 复用 data.SQLExec 以避免 account/player/order 三重定义。
type sqlExec = data.SQLExec

// EAccount 账号基础信息（本体定义在 pkg/domain/data/account）。
type EAccount = paccount.EAccount

// HashPassword / VerifyPassword 透传 pkg 层的 bcrypt 实现，
// 供账号服构造「时序对齐」用的占位哈希（登录路径对不存在账号做等量运算）。
var (
	HashPassword   = paccount.HashPassword
	VerifyPassword = paccount.VerifyPassword
)

// VerifyPassword 校验明文密码是否匹配本账号（委托给 pkg 方法）。
// 实际调用链: internal account.Store → *EAccount（即 paccount.EAccount）.VerifyPassword。

// Store 账号业务存储（account 表）。
type Store struct {
	db    sqlExec
	table string
}

// NewStore 根据 MySQL 配置创建账号存储，并建立连接（含 Ping 校验）。
// table 可选，默认 "account"。
func NewStore(cfg imysql.MySQLConfig, table ...string) (*Store, error) {
	cli, err := imysql.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return NewStoreWithClient(cli, table...), nil
}

// NewStoreWithClient 用已存在的 MySQL 客户端（或测试 mock）构造 Store。
func NewStoreWithClient(db sqlExec, table ...string) *Store {
	tbl := "account"
	if len(table) > 0 && table[0] != "" {
		tbl = data.SanitizeTable(table[0], "account")
	}
	return &Store{db: db, table: tbl}
}

// Table 返回当前表名。
func (s *Store) Table() string { return s.table }

// CreateTable 执行 CREATE TABLE IF NOT EXISTS（account）。
func (s *Store) CreateTable(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id          VARCHAR(16) PRIMARY KEY,
    account     VARCHAR(64) NOT NULL,
    password    VARCHAR(255) NOT NULL,
    token       VARCHAR(255) DEFAULT '',
    token_expire DATETIME NULL,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    login_time  DATETIME NULL,
    UNIQUE KEY uk_account (account)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
	if _, err := s.db.Exec(ctx, q); err != nil {
		return fmt.Errorf("account: create table %s: %w", s.table, err)
	}
	return nil
}

// Register 注册新账号（校验账号不存在后写入密码哈希）。
// 密码以 bcrypt 哈希存储，不落明文。
func (s *Store) Register(ctx context.Context, account, password string) error {
	if account == "" {
		return fmt.Errorf("%w: account must not be empty", ErrInvalidAccountParam)
	}
	if err := validateAccountName(account); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAccountParam, err)
	}
	if password == "" {
		// 空密码必须拒绝：否则空密码哈希入库后 Authenticate("") 恒通过（该账号无凭据可守）。
		return fmt.Errorf("%w: password must not be empty", ErrInvalidAccountParam)
	}
	// INSERT ... ON DUPLICATE KEY：唯一键碰撞不抛错，通过 RowsAffected 区分新建/已存在，
	// 避免 GenUID 浪费且在多实例并发下更精确区分 id 碰撞与 account 碰撞。
	hash, err := paccount.HashPassword(password)
	if err != nil {
		return fmt.Errorf("account: hash password: %w", err)
	}
	uid, err := id.GenUID()
	if err != nil {
		return fmt.Errorf("account: gen uid: %w", err)
	}
	q := fmt.Sprintf(
		"INSERT INTO %s (id, account, password) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id=id",
		s.table,
	)
	res, err := s.db.Exec(ctx, q, uid, account, hash)
	if err != nil {
		// 防御映射：ON DUPLICATE KEY 常规下不抛 1062，但异常场景（其它唯一索引、
		// 部分代理/驱动行为差异）仍可能返回重复键错误，统一映射为 ErrAccountExists，
		// 让调用方（signup handler）稳定识别「账号已存在」而非报内部错误。
		if imysql.IsDuplicateError(err) {
			return ErrAccountExists
		}
		return fmt.Errorf("account: insert: %w", err)
	}
	n, raErr := res.RowsAffected()
	if raErr != nil {
		// 驱动异常时不能把 n 恒为 0 误判为「账号已存在」。
		return fmt.Errorf("account: rows affected: %w", raErr)
	}
	if n == 0 {
		// 受影响行数为 0 有两种可能：account 唯一键冲突，或 id（GenUID）碰撞而 account 未占用。
		// 用一次存在性查询区分，避免把 uid 碰撞误报为「账号已存在」。
		exist, exErr := s.Exists(ctx, account)
		if exErr != nil {
			return fmt.Errorf("account: probe existence: %w", exErr)
		}
		if exist {
			return ErrAccountExists
		}
		// uid 碰撞：换一个 uid 重试一次（36^16 空间中碰撞概率极低，一次重试足够）。
		uid2, uErr := id.GenUID()
		if uErr != nil {
			return fmt.Errorf("account: gen uid (uid collision retry): %w", uErr)
		}
		res2, err2 := s.db.Exec(ctx, q, uid2, account, hash)
		if err2 != nil {
			if imysql.IsDuplicateError(err2) {
				return ErrAccountExists
			}
			return fmt.Errorf("account: insert (uid collision retry): %w", err2)
		}
		n2, raErr2 := res2.RowsAffected()
		if raErr2 != nil {
			return fmt.Errorf("account: rows affected (uid collision retry): %w", raErr2)
		}
		if n2 == 0 {
			return fmt.Errorf("account: insert returned 0 rows for new account %q (uid collision retry exhausted)", account)
		}
	}
	return nil
}

// Authenticate 校验账号 + 密码，成功返回 (true, nil)。
// 账号不存在返回 (false, ErrAccountNotFound)；密码错误返回 (false, ErrWrongPassword)。
func (s *Store) Authenticate(ctx context.Context, account, password string) (bool, error) {
	a, err := s.Load(ctx, account)
	if err != nil {
		return false, err // ErrAccountNotFound 透传
	}
	if !a.VerifyPassword(password) {
		return false, ErrWrongPassword
	}
	return true, nil
}

// AuthenticateByToken 校验账号 + token，成功返回 (true, nil)。
// 账号不存在或 token 不匹配返回 (false, ErrInvalidToken)。
func (s *Store) AuthenticateByToken(ctx context.Context, account, token string) (bool, error) {
	if token == "" {
		return false, ErrInvalidToken
	}
	a, err := s.Load(ctx, account)
	if err != nil {
		return false, ErrInvalidToken // 不暴露账号是否存在
	}
	if a.Token != token {
		return false, ErrInvalidToken
	}
	if a.TokenExpire.Valid {
		// 依次尝试常见 MySQL 时间格式：带小数秒 → 标准格式 → 纯日期。
		// MySQL DATETIME(6) 输出 "2006-01-02 15:04:05.000000"，DATETIME 输出 "2006-01-02 15:04:05"。
		exp, perr := parseMySQLTime(a.TokenExpire.String)
		if perr != nil {
			return false, ErrInvalidToken // 时间格式非法，拒绝解析
		}
		if time.Now().After(exp) {
			return false, ErrTokenExpired
		}
	}
	return true, nil
}

// Login 执行登录成功后的公共逻辑：刷新 token 与 login_time。
// 账号不存在返回 ErrAccountNotFound；成功返回生成的 token 字符串。
// 若当前 token_expire 为 NULL（永久 token），保留 NULL 不覆写过期时间。
func (s *Store) Login(ctx context.Context, account string) (string, error) {
	token, err := genToken()
	if err != nil {
		return "", fmt.Errorf("account: generate token: %w", err)
	}
	a, loadErr := s.Load(ctx, account)
	// 账号不存在直接返回，避免对不存在的账号执行无意义的 UPDATE。
	if loadErr != nil {
		if errors.Is(loadErr, ErrAccountNotFound) {
			return "", ErrAccountNotFound
		}
		return "", fmt.Errorf("account: load: %w", loadErr)
	}
	var q string
	var ra sql.Result
	if !a.TokenExpire.Valid {
		// 当前为永久 token（NULL），保留 NULL
		q = fmt.Sprintf("UPDATE %s SET token=?, login_time=NOW() WHERE account=?", s.table)
		ra, err = s.db.Exec(ctx, q, token, account)
	} else {
		q = fmt.Sprintf("UPDATE %s SET token=?, token_expire=?, login_time=NOW() WHERE account=?", s.table)
		ra, err = s.db.Exec(ctx, q, token, nowSQLExpire(), account)
	}
	if err != nil {
		return "", fmt.Errorf("account: update token: %w", err)
	}
	if affected, _ := ra.RowsAffected(); affected == 0 {
		return "", ErrAccountNotFound
	}
	return token, nil
}

// Load 读取账号。不存在返回 ErrAccountNotFound。
func (s *Store) Load(ctx context.Context, account string) (*EAccount, error) {
	var a EAccount
	q := fmt.Sprintf("SELECT id, account, password, token, token_expire, create_time, login_time FROM %s WHERE account=?", s.table)
	if err := s.db.Get(ctx, &a, q, account); err != nil {
		if errors.Is(err, imysql.ErrNotFound) {
			return nil, ErrAccountNotFound
		}
		return nil, fmt.Errorf("account: load: %w", err)
	}
	return &a, nil
}

// Exists 判断账号是否存在。
func (s *Store) Exists(ctx context.Context, account string) (bool, error) {
	var a EAccount
	q := fmt.Sprintf("SELECT id FROM %s WHERE account=?", s.table)
	err := s.db.Get(ctx, &a, q, account)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, imysql.ErrNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("account: exists: %w", err)
}

// Delete 删除账号（仅 account 表；其渠道绑定与绑定的其他数据请另行清理）。
func (s *Store) Delete(ctx context.Context, account string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE account=?", s.table)
	if _, err := s.db.Exec(ctx, q, account); err != nil {
		return fmt.Errorf("account: delete: %w", err)
	}
	return nil
}

// ChangePassword 修改密码（先校验账号存在）。
func (s *Store) ChangePassword(ctx context.Context, account, newPassword string) error {
	if _, err := s.Load(ctx, account); err != nil {
		return err
	}
	hash, err := paccount.HashPassword(newPassword)
	if err != nil {
		return fmt.Errorf("account: hash password: %w", err)
	}
	q := fmt.Sprintf("UPDATE %s SET password=? WHERE account=?", s.table)
	if _, err := s.db.Exec(ctx, q, hash, account); err != nil {
		return fmt.Errorf("account: update password: %w", err)
	}
	return nil
}

// Close 释放底层连接。
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// 辅助函数
// tokenTTL 登录 token 有效期（默认 7 天）。
const tokenTTL = 7 * 24 * time.Hour

// accountNameRe 账号名白名单：3-64 位，仅允许字母/数字/_ . @ -。
var accountNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.@-]{3,64}$`)

// validateAccountName 校验账号名格式（防注入/超长/特殊字符）。
func validateAccountName(name string) error {
	if !accountNameRe.MatchString(name) {
		return fmt.Errorf("account: invalid account name (3-64 chars, [a-zA-Z0-9_.@-])")
	}
	return nil
}

// nowSQLExpire 返回 token 过期时间的 DATETIME 字符串。
//
// 必须走 timeutil（引擎时区）而不是 time.Now().Format：后者用**系统本地时区**，
// 与 NowSQL 写入的时间列不同源，跨时区部署时会出现「过期时间比写入时间早/晚几小时」。
func nowSQLExpire() string {
	return timeutil.StrAfter(tokenTTL)
}

// Logout 主动登出：清空 token 与过期时间，使现有 token 立即失效。
func (s *Store) Logout(ctx context.Context, account string) error {
	q := fmt.Sprintf("UPDATE %s SET token='', token_expire=NULL WHERE account=?", s.table)
	if _, err := s.db.Exec(ctx, q, account); err != nil {
		return fmt.Errorf("account: logout: %w", err)
	}
	return nil
}

// genToken 生成 32 字节 hex 随机字符串（复用 pkg/shared/id 的统一实现，不再自写一遍）。
func genToken() (string, error) {
	return id.RandomHex(32)
}

// 密码哈希（bcrypt）与校验见 pkg/domain/data/account 的 HashPassword / VerifyPassword；
// 明文密码强度校验由调用方（账号服 Signup）负责，本层仅拒绝空密码。
