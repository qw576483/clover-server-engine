package account

import (
	"context"
	"errors"
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	imysql "github.com/qw576483/clover-server-engine/internal/domain/data/store/mysql"
	paccount "github.com/qw576483/clover-server-engine/pkg/domain/data/account"
	"github.com/qw576483/clover-server-engine/pkg/shared/id"
)

// EChannel 账号渠道绑定（本体定义在 pkg/domain/data/account）。
type EChannel = paccount.EChannel

// 渠道错误（指向 pkg 层本体）。
var (
	ErrChannelNotFound = paccount.ErrChannelNotFound
	ErrChannelExists   = paccount.ErrChannelExists
)

// ChannelStore 账号渠道业务存储（account_channel 表）。
type ChannelStore struct {
	db    sqlExec
	table string
}

// NewChannelStore 根据 MySQL 配置创建渠道存储，并建立连接（含 Ping 校验）。
// table 可选，默认 "account_channel"。
func NewChannelStore(cfg imysql.MySQLConfig, table ...string) (*ChannelStore, error) {
	cli, err := imysql.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return NewChannelStoreWithClient(cli, table...), nil
}

// NewChannelStoreWithClient 用已存在的 MySQL 客户端（或测试 mock）构造 ChannelStore。
func NewChannelStoreWithClient(db sqlExec, table ...string) *ChannelStore {
	tbl := "account_channel"
	if len(table) > 0 && table[0] != "" {
		tbl = data.SanitizeTable(table[0], "account_channel")
	}
	return &ChannelStore{db: db, table: tbl}
}

// Table 返回当前表名。
func (s *ChannelStore) Table() string { return s.table }

// CreateTable 执行 CREATE TABLE IF NOT EXISTS（account_channel）。
func (s *ChannelStore) CreateTable(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id               VARCHAR(16) PRIMARY KEY,
    channel          VARCHAR(32) NOT NULL,
    bind_account     VARCHAR(64) NOT NULL,
    channel_account  VARCHAR(255) NOT NULL,
    UNIQUE KEY uk_channel_account (channel, channel_account),
    KEY idx_bind_account (bind_account)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
	if _, err := s.db.Exec(ctx, q); err != nil {
		return fmt.Errorf("account: create channel table %s: %w", s.table, err)
	}
	return nil
}

// Bind 绑定账号的渠道（channel + channel_account 唯一，重复绑定拒绝不覆盖）。
func (s *ChannelStore) Bind(ctx context.Context, channel, bindAccount, channelAccount string) error {
	if channel == "" || bindAccount == "" || channelAccount == "" {
		return fmt.Errorf("account: channel/bind_account/channel_account must not be empty")
	}
	uid, err := id.GenUID()
	if err != nil {
		return fmt.Errorf("account: gen uid: %w", err)
	}
	q := fmt.Sprintf("INSERT INTO %s (id, channel, bind_account, channel_account) VALUES (?, ?, ?, ?)", s.table)
	if _, err := s.db.Exec(ctx, q, uid, channel, bindAccount, channelAccount); err != nil {
		if imysql.IsDuplicateError(err) {
			return fmt.Errorf("%w: %s/%s", ErrChannelExists, channel, channelAccount)
		}
		return fmt.Errorf("account: bind channel: %w", err)
	}
	return nil
}

// Unbind 解绑某个渠道账号。若绑定不存在返回 ErrChannelNotFound。
func (s *ChannelStore) Unbind(ctx context.Context, channel, channelAccount string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE channel=? AND channel_account=?", s.table)
	res, err := s.db.Exec(ctx, q, channel, channelAccount)
	if err != nil {
		return fmt.Errorf("account: unbind channel: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrChannelNotFound
	}
	return nil
}

// Get 读取指定渠道账号绑定。不存在返回 ErrChannelNotFound。
func (s *ChannelStore) Get(ctx context.Context, channel, channelAccount string) (*EChannel, error) {
	var c EChannel
	q := fmt.Sprintf("SELECT id, channel, bind_account, channel_account FROM %s WHERE channel=? AND channel_account=?", s.table)
	if err := s.db.Get(ctx, &c, q, channel, channelAccount); err != nil {
		if errors.Is(err, imysql.ErrNotFound) {
			return nil, ErrChannelNotFound
		}
		return nil, fmt.Errorf("account: get channel: %w", err)
	}
	return &c, nil
}

// ListByAccount 列出主账号绑定的所有渠道。
func (s *ChannelStore) ListByAccount(ctx context.Context, bindAccount string) ([]EChannel, error) {
	var cs []EChannel
	q := fmt.Sprintf("SELECT id, channel, bind_account, channel_account FROM %s WHERE bind_account=?", s.table)
	if err := s.db.Select(ctx, &cs, q, bindAccount); err != nil {
		return nil, fmt.Errorf("account: list channels: %w", err)
	}
	return cs, nil
}

// Exists 判断某个渠道账号是否已绑定。
func (s *ChannelStore) Exists(ctx context.Context, channel, channelAccount string) (bool, error) {
	var c EChannel
	q := fmt.Sprintf("SELECT id FROM %s WHERE channel=? AND channel_account=?", s.table)
	err := s.db.Get(ctx, &c, q, channel, channelAccount)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, imysql.ErrNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("account: channel exists: %w", err)
}

// FindByChannel 按渠道反查绑定（用于渠道登录：拿 channel_account 找到对应主账号）。
// 同一渠道信息理论上只绑定一个主账号，未找到返回 ErrChannelNotFound。
func (s *ChannelStore) FindByChannel(ctx context.Context, channel, channelAccount string) (*EChannel, error) {
	var c EChannel
	q := fmt.Sprintf("SELECT id, channel, bind_account, channel_account FROM %s WHERE channel=? AND channel_account=? LIMIT 1", s.table)
	if err := s.db.Get(ctx, &c, q, channel, channelAccount); err != nil {
		if errors.Is(err, imysql.ErrNotFound) {
			return nil, ErrChannelNotFound
		}
		return nil, fmt.Errorf("account: find by channel: %w", err)
	}
	return &c, nil
}

// DeleteByAccount 删除账号的全部渠道绑定（删除账号时级联调用）。
func (s *ChannelStore) DeleteByAccount(ctx context.Context, bindAccount string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE bind_account=?", s.table)
	if _, err := s.db.Exec(ctx, q, bindAccount); err != nil {
		return fmt.Errorf("account: delete channels: %w", err)
	}
	return nil
}

// Close 释放底层连接。
func (s *ChannelStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
