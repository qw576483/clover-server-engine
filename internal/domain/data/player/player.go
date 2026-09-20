// Package player 玩家（角色）通用业务存储。
//
// 设计要点：
// - 角色基础信息存放于引擎数据层表 player，主键 id 与游戏可见 player_id 均为
// 16 位全大写英文+数字字符串（由 id.GenUID 生成），二者值不同、独立生成。
// - 支持同一账号在同一区服下创建多个角色：唯一约束 uk_player_id (player_id)（player_id 全局唯一）。
// - 角色表只保留最小字段，其余等级/装备等业务数据走通用 data 三元键存储层。
//
// 关于 server_id（玩家侧概念，遵循仓库规则 7）：
// - server_id 是「玩家 / 客户端」侧概念：由客户端登录 / 创角请求带入，逻辑服仅做透传与查询，
// 绝不写入逻辑服自身的部署配置；一个玩家可用同一账号在不同 server_id 下创建多个角色。
// - 它与「逻辑服部署配置（监听地址 / 角色）」完全正交，请勿混淆：逻辑服配置里不含 server_id，
// 本包的 ServerID 字段专指客户端带入的区服 ID，而非本逻辑服实例的身份。
//
// 本包实现见 internal/domain/data/player，pkg/domain/data/player 仅做对外透传（符合仓库强制规则）。
package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"clover-server-engine/internal/domain/data"
	imysql "clover-server-engine/internal/domain/data/store/mysql"
	pplayer "clover-server-engine/pkg/domain/data/player"
	"clover-server-engine/pkg/shared/id"
)

// 业务错误（指向 pkg 层本体）。
var (
	ErrPlayerNotFound = pplayer.ErrPlayerNotFound
	ErrPlayerExists   = pplayer.ErrPlayerExists
)

// sqlExec 持久层抽象（复用 data.SQLExec，避免 account/player/order 三重定义）。
type sqlExec = data.SQLExec

// EPlayer 角色基础信息（本体定义在 pkg/domain/data/player）。
type EPlayer = pplayer.EPlayer

// Store 角色业务存储（player 表）。
type Store struct {
	db    sqlExec
	table string
}

// NewStore 根据 MySQL 配置创建角色存储，并建立连接（含 Ping 校验）。
// table 可选，默认 "player"。
func NewStore(cfg imysql.MySQLConfig, table ...string) (*Store, error) {
	cli, err := imysql.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return NewStoreWithClient(cli, table...), nil
}

// NewStoreWithClient 用已存在的 MySQL 客户端（或单测 mock）构造 Store。
func NewStoreWithClient(db sqlExec, table ...string) *Store {
	tbl := "player"
	if len(table) > 0 && table[0] != "" {
		tbl = data.SanitizeTable(table[0], "player")
	}
	return &Store{db: db, table: tbl}
}

// Table 返回当前表名。
func (s *Store) Table() string { return s.table }

// CreateTable 执行 CREATE TABLE IF NOT EXISTS（player）。
func (s *Store) CreateTable(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id          VARCHAR(16) PRIMARY KEY,
    player_id   VARCHAR(16) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    account     VARCHAR(64) NOT NULL,
    server_id   INT UNSIGNED NOT NULL,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    login_time  DATETIME NULL,
    UNIQUE KEY uk_player_id (player_id),
    KEY idx_account_server (account, server_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
	if _, err := s.db.Exec(ctx, q); err != nil {
		return fmt.Errorf("player: create table %s: %w", s.table, err)
	}
	return nil
}

// Create 创建角色。player_id 冲突（uk_player_id 唯一约束）时返回 ErrPlayerExists。
// id 与 player_id 分别由 id.GenUID 独立生成，二者不相同。
func (s *Store) Create(ctx context.Context, account, name string, serverID uint32) (*EPlayer, error) {
	if account == "" || name == "" || serverID == 0 {
		return nil, fmt.Errorf("player: account/name/server_id must be valid (non-empty and non-zero)")
	}
	if err := validatePlayerName(name); err != nil {
		return nil, err
	}
	uid, err := id.GenUID()
	if err != nil {
		return nil, fmt.Errorf("player: gen id: %w", err)
	}
	playerID, err := id.GenUID()
	if err != nil {
		return nil, fmt.Errorf("player: gen player_id: %w", err)
	}
	res, err := s.db.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (id, player_id, name, account, server_id) VALUES (?, ?, ?, ?, ?)", s.table),
		uid, playerID, name, account, serverID)
	if err != nil {
		if imysql.IsDuplicateError(err) {
			return nil, ErrPlayerExists
		}
		return nil, fmt.Errorf("player: insert: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fmt.Errorf("player: insert returned 0 rows")
	}
	return &EPlayer{
		ID:         uid,
		PlayerID:   playerID,
		Account:    account,
		Name:       name,
		ServerID:   serverID,
		CreateTime: sql.NullString{String: data.NowSQL(), Valid: true},
	}, nil
}

// GetByAccount 按账号 + 区服查角色列表；无角色返回空切片。
func (s *Store) GetByAccount(ctx context.Context, account string, serverID uint32) ([]EPlayer, error) {
	if serverID == 0 {
		return nil, fmt.Errorf("player: server_id must not be empty")
	}
	var ps []EPlayer
	q := fmt.Sprintf("SELECT id, player_id, name, account, server_id, create_time, login_time FROM %s WHERE account=? AND server_id=?", s.table)
	if err := s.db.Select(ctx, &ps, q, account, serverID); err != nil {
		return nil, fmt.Errorf("player: get by account: %w", err)
	}
	return ps, nil
}

// GetByID 按角色 ID 查角色；不存在返回 ErrPlayerNotFound。
func (s *Store) GetByID(ctx context.Context, playerID string) (*EPlayer, error) {
	var p EPlayer
	q := fmt.Sprintf("SELECT id, player_id, name, account, server_id, create_time, login_time FROM %s WHERE player_id=?", s.table)
	if err := s.db.Get(ctx, &p, q, playerID); err != nil {
		if errors.Is(err, imysql.ErrNotFound) {
			return nil, ErrPlayerNotFound
		}
		return nil, fmt.Errorf("player: get by id: %w", err)
	}
	return &p, nil
}

// GetByAccountAndID 按账号 + 区服 + player_id 查角色；不存在返回 ErrPlayerNotFound。
func (s *Store) GetByAccountAndID(ctx context.Context, account string, serverID uint32, playerID string) (*EPlayer, error) {
	var p EPlayer
	q := fmt.Sprintf("SELECT id, player_id, name, account, server_id, create_time, login_time FROM %s WHERE account=? AND server_id=? AND player_id=?", s.table)
	if err := s.db.Get(ctx, &p, q, account, serverID, playerID); err != nil {
		if errors.Is(err, imysql.ErrNotFound) {
			return nil, ErrPlayerNotFound
		}
		return nil, fmt.Errorf("player: get by account and id: %w", err)
	}
	return &p, nil
}

// UpdateName 修改角色名。
func (s *Store) UpdateName(ctx context.Context, playerID, name string) error {
	if name == "" {
		return fmt.Errorf("player: name must not be empty")
	}
	if err := validatePlayerName(name); err != nil {
		return err
	}
	q := fmt.Sprintf("UPDATE %s SET name=? WHERE player_id=?", s.table)
	if _, err := s.db.Exec(ctx, q, name, playerID); err != nil {
		return fmt.Errorf("player: update name: %w", err)
	}
	return nil
}

// UpdateLoginTime 更新角色登录时间。
func (s *Store) UpdateLoginTime(ctx context.Context, playerID string) error {
	q := fmt.Sprintf("UPDATE %s SET login_time=? WHERE player_id=?", s.table)
	if _, err := s.db.Exec(ctx, q, data.NowSQL(), playerID); err != nil {
		return fmt.Errorf("player: update login time: %w", err)
	}
	return nil
}

// Ensure 获取或创建角色：已存在返回现有；不存在以 name 创建。
// 同一账号同一服可创建多个角色，因此 Ensure 按 (account, server_id, name) 兜底创建。
func (s *Store) Ensure(ctx context.Context, account string, serverID uint32, name string) (*EPlayer, error) {
	ps, err := s.GetByAccount(ctx, account, serverID)
	if err != nil {
		return nil, err
	}
	if len(ps) > 0 {
		if name != "" {
			for i := range ps {
				if ps[i].Name == name {
					return &ps[i], nil
				}
			}
			// 同账号同服允许多角色：未找到同名角色则新建一个。
			return s.Create(ctx, account, name, serverID)
		}
		// name 为空时按 create_time 排序选取最早创建的角色（保证确定性）。
		// NULL create_time 的条目排在最后（视为最晚创建）。
		earliest := &ps[0]
		for i := 1; i < len(ps); i++ {
			if !ps[i].CreateTime.Valid {
				// NULL create_time 视为最晚，跳过
				continue
			}
			if !earliest.CreateTime.Valid {
				// earliest 是 NULL，当前非 NULL，当前更早
				earliest = &ps[i]
				continue
			}
			if ps[i].CreateTime.String < earliest.CreateTime.String {
				earliest = &ps[i]
			}
		}
		return earliest, nil
	}
	nm := name
	if nm == "" {
		nm = account
	}
	return s.Create(ctx, account, nm, serverID)
}

// Close 释放底层连接。
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// playerNameRe 角色名白名单：1-32 位，仅允许字母/数字/_ . @ - 与中文。
// 注意 `-` 必须放在字符类末尾：写作 `@-\x{4e00}` 会被解析为「U+0040..U+4E00 范围」，
// 意外放行 [ ] ^ { } | ~ 反引号、假名、希腊文等近两万个字符，白名单形同虚设。
var playerNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.@\x{4e00}-\x{9fa5}-]{1,32}$`)

// validatePlayerName 校验角色名格式（防注入/超长/特殊字符）。
func validatePlayerName(name string) error {
	if !playerNameRe.MatchString(name) {
		return fmt.Errorf("player: invalid player name (1-32 chars, letters/numbers/_ . @ - and Chinese)")
	}
	return nil
}
