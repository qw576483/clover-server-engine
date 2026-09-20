// Package account 账号领域类型、错误与存储接口。
package account

import (
	"context"
	"database/sql"
)

// EAccount 账号基础信息（Password 为哈希串，用 VerifyPassword 校验）。
// 字段：
//   - ID：引擎数据层 16 位唯一 ID（0-9A-Z，由 id.GenUID 生成）。
//   - Account：账号名（唯一，3-64 位字母/数字/_ . @ -）。
//   - Password：密码哈希（bcrypt），不存明文。
//   - Token：登录 token（密码登录成功后刷新，32 字节 hex）。
//   - CreateTime：创建时间（DB 维护，sql.NullString 因可为 NULL）。
//   - LoginTime：最近一次登录时间（sql.NullString）。
//   - TokenExpire：token 过期时间（NULL 表示未设置/永久）。
type EAccount struct {
	ID          string         `db:"id"`           // 引擎数据层 16 位唯一 ID
	Account     string         `db:"account"`      // 账号（唯一）
	Password    string         `db:"password"`     // 密码哈希（bcrypt）
	Token       string         `db:"token"`        // 登录 token（密码登录成功后刷新）
	CreateTime  sql.NullString `db:"create_time"`  // 创建时间（DB 维护）
	LoginTime   sql.NullString `db:"login_time"`   // 最近一次登录时间
	TokenExpire sql.NullString `db:"token_expire"` // token 过期时间（NULL 表示未设置/永久）
}

// VerifyPassword 校验明文密码是否匹配本账号（不暴露哈希）。
func (a *EAccount) VerifyPassword(plain string) bool {
	return verifyPassword(plain, a.Password)
}

// EChannel 账号渠道绑定。
// 一个账号可绑定多个渠道（微信/QQ/Apple...），每个渠道账号全局唯一，主键 id 为引擎数据层 16 位唯一字符串。
// 字段：
//   - ID：引擎数据层 16 位唯一 ID。
//   - Channel：渠道类型（如 wechat/qq/apple）。
//   - BindAccount：绑定的主账号。
//   - ChannelAccount：渠道账号信息（如 openid）。
type EChannel struct {
	ID             string `db:"id"`              // 引擎数据层 16 位唯一 ID
	Channel        string `db:"channel"`         // 渠道类型（如 wechat/qq/apple）
	BindAccount    string `db:"bind_account"`    // 绑定的主账号
	ChannelAccount string `db:"channel_account"` // 渠道账号信息（如 openid）
}

// 账号业务错误。
var (
	ErrAccountExists   = errAccountExists   // 注册时账号已存在
	ErrAccountNotFound = errAccountNotFound // 账号不存在
	ErrWrongPassword   = errWrongPassword   // 密码错误
	ErrInvalidToken    = errInvalidToken    // token 校验失败
	ErrTokenExpired    = errTokenExpired    // token 已过期
	ErrChannelNotFound = errChannelNotFound // 账号未绑定指定渠道
	ErrChannelExists   = errChannelExists   // 渠道账号已绑定（重复绑定）
)

// Store 账号存储的业务接口：只暴露注册 / 登录 / 查询 / 维护等业务能力，
// 建表（CreateTable）、关连接（Close）、表名（Table）等引擎装配细节不在此列。
// 由 internal 的 *account.Store 满足。
type Store interface {
	// Register 注册新账号（密码以 bcrypt 哈希存储）；已存在返回 ErrAccountExists。
	Register(ctx context.Context, account, password string) error
	// Authenticate 校验账号 + 密码；账号不存在返回 ErrAccountNotFound，密码错误返回 ErrWrongPassword。
	Authenticate(ctx context.Context, account, password string) (bool, error)
	// AuthenticateByToken 校验账号 + token；不匹配返回 ErrInvalidToken，过期返回 ErrTokenExpired。
	AuthenticateByToken(ctx context.Context, account, token string) (bool, error)
	// Login 登录成功后刷新 token 与 login_time，返回新 token。
	Login(ctx context.Context, account string) (string, error)
	// Logout 主动登出：清空 token 使其立即失效。
	Logout(ctx context.Context, account string) error
	// Load 读取账号；不存在返回 ErrAccountNotFound。
	Load(ctx context.Context, account string) (*EAccount, error)
	// Exists 判断账号是否存在。
	Exists(ctx context.Context, account string) (bool, error)
	// Delete 删除账号（其渠道绑定与绑定的其他数据需另行清理）。
	Delete(ctx context.Context, account string) error
	// ChangePassword 修改密码（先校验账号存在）。
	ChangePassword(ctx context.Context, account, newPassword string) error
}

// ChannelStore 账号渠道绑定的业务接口（account_channel 表）。
// 由 internal 的 *account.ChannelStore 满足。
type ChannelStore interface {
	// Bind 绑定渠道（channel + channel_account 唯一，重复绑定返回 ErrChannelExists）。
	Bind(ctx context.Context, channel, bindAccount, channelAccount string) error
	// Unbind 解绑渠道；不存在返回 ErrChannelNotFound。
	Unbind(ctx context.Context, channel, channelAccount string) error
	// Get 读取渠道绑定；不存在返回 ErrChannelNotFound。
	Get(ctx context.Context, channel, channelAccount string) (*EChannel, error)
	// ListByAccount 列出主账号绑定的所有渠道。
	ListByAccount(ctx context.Context, bindAccount string) ([]EChannel, error)
	// Exists 判断渠道账号是否已绑定。
	Exists(ctx context.Context, channel, channelAccount string) (bool, error)
	// FindByChannel 按渠道反查绑定（渠道登录用）；未找到返回 ErrChannelNotFound。
	FindByChannel(ctx context.Context, channel, channelAccount string) (*EChannel, error)
	// DeleteByAccount 删除账号的全部渠道绑定。
	DeleteByAccount(ctx context.Context, bindAccount string) error
}
