// Package order 订单通用业务逻辑（底层）。
//
// 设计要点：
//   - 订单基础信息存放于引擎数据层表 orders，主键 id 为 16 位全大写英文+数字字符串。
//   - 状态流转：0 created → 1 paid → 2 done。
//   - ext_param 为渠道扩展 JSON，由业务/渠道层填充。
//
// 本包实现见 internal/domain/data/order，pkg/domain/data/order 仅做对外透传。
package order

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	imysql "github.com/qw576483/clover-server-engine/internal/domain/data/store/mysql"
	porder "github.com/qw576483/clover-server-engine/pkg/domain/data/order"
	"github.com/qw576483/clover-server-engine/pkg/shared/id"
)

// Status 订单状态（本体定义在 pkg/domain/data/order）。
type Status = porder.Status

const (
	StatusCreated = porder.StatusCreated
	StatusPaid    = porder.StatusPaid
	StatusDone    = porder.StatusDone
)

// 业务错误（指向 pkg 层本体）。
var (
	ErrOrderNotFound = porder.ErrOrderNotFound
)

// sqlExec 持久层抽象（*imysql.Client 实现），便于依赖注入与单测替换（go-sqlmock）。
type sqlExec = data.SQLExec

// EOrder 订单基础信息（本体定义在 pkg/domain/data/order）。
type EOrder = porder.EOrder

// Store 订单业务存储（orders 表）。
type Store struct {
	db    sqlExec
	table string
}

// NewStore 根据 MySQL 配置创建订单存储，并建立连接（含 Ping 校验）。
// table 可选，默认 "orders"。
func NewStore(cfg imysql.MySQLConfig, table ...string) (*Store, error) {
	cli, err := imysql.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return NewStoreWithClient(cli, table...), nil
}

// NewStoreWithClient 用已存在的 MySQL 客户端（或测试 mock）构造 Store。
func NewStoreWithClient(db sqlExec, table ...string) *Store {
	tbl := "orders"
	if len(table) > 0 && table[0] != "" {
		tbl = data.SanitizeTable(table[0], "orders")
	}
	return &Store{db: db, table: tbl}
}

// Table 返回当前表名。
func (s *Store) Table() string { return s.table }

// CreateTable 执行 CREATE TABLE IF NOT EXISTS（orders）。
func (s *Store) CreateTable(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id               VARCHAR(16) PRIMARY KEY,
    account          VARCHAR(64) NOT NULL,
    player_id        VARCHAR(16) NOT NULL,
    server_id        INT UNSIGNED NOT NULL,
    status           TINYINT NOT NULL DEFAULT 0,
    price            BIGINT NOT NULL DEFAULT 0,
    create_time      DATETIME DEFAULT CURRENT_TIMESTAMP,
    pay_time         DATETIME NULL,
    done_time        DATETIME NULL,
    channel_order_id VARCHAR(255) DEFAULT '',
    channel          VARCHAR(32) DEFAULT '',
    ext_param        TEXT,
    KEY idx_account (account),
    KEY idx_player_server (player_id, server_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
	if _, err := s.db.Exec(ctx, q); err != nil {
		return fmt.Errorf("order: create table %s: %w", s.table, err)
	}
	return nil
}

// Create 创建订单，返回订单对象。price 单位为分（cent），必须 > 0。
func (s *Store) Create(ctx context.Context, account string, playerID string, serverID uint32, price int64, channel, channelOrderID, extParam string) (*EOrder, error) {
	if account == "" || playerID == "" || serverID == 0 || price <= 0 {
		return nil, fmt.Errorf("order: account/player_id/server_id/price invalid")
	}
	uid, err := id.GenUID()
	if err != nil {
		return nil, fmt.Errorf("order: gen uid: %w", err)
	}
	res, err := s.db.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (id, account, player_id, server_id, status, price, channel, channel_order_id, ext_param) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", s.table),
		uid, account, playerID, serverID, StatusCreated, price, channel, channelOrderID, extParam)
	if err != nil {
		return nil, fmt.Errorf("order: insert: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fmt.Errorf("order: insert returned 0 rows")
	}
	return &EOrder{
		ID:             uid,
		Account:        account,
		PlayerID:       playerID,
		ServerID:       serverID,
		Status:         StatusCreated,
		Price:          price,
		Channel:        channel,
		ChannelOrderID: channelOrderID,
		ExtParam:       extParam,
		CreateTime:     sql.NullString{String: data.NowSQL(), Valid: true},
	}, nil
}

// GetByID 按订单 ID 查询。
func (s *Store) GetByID(ctx context.Context, orderID string) (*EOrder, error) {
	var o EOrder
	q := fmt.Sprintf("SELECT id, account, player_id, server_id, status, price, create_time, pay_time, done_time, channel_order_id, channel, ext_param FROM %s WHERE id=?", s.table)
	if err := s.db.Get(ctx, &o, q, orderID); err != nil {
		if errors.Is(err, imysql.ErrNotFound) {
			return nil, ErrOrderNotFound
		}
		return nil, fmt.Errorf("order: get by id: %w", err)
	}
	return &o, nil
}

// ListByAccount 按账号查询订单。
func (s *Store) ListByAccount(ctx context.Context, account string) ([]EOrder, error) {
	var os []EOrder
	q := fmt.Sprintf("SELECT id, account, player_id, server_id, status, price, create_time, pay_time, done_time, channel_order_id, channel, ext_param FROM %s WHERE account=? ORDER BY id DESC", s.table)
	if err := s.db.Select(ctx, &os, q, account); err != nil {
		return nil, fmt.Errorf("order: list by account: %w", err)
	}
	return os, nil
}

// ListByPlayer 按角色查询订单。
func (s *Store) ListByPlayer(ctx context.Context, playerID string, serverID uint32) ([]EOrder, error) {
	var os []EOrder
	q := fmt.Sprintf("SELECT id, account, player_id, server_id, status, price, create_time, pay_time, done_time, channel_order_id, channel, ext_param FROM %s WHERE player_id=? AND server_id=? ORDER BY id DESC", s.table)
	if err := s.db.Select(ctx, &os, q, playerID, serverID); err != nil {
		return nil, fmt.Errorf("order: list by player: %w", err)
	}
	return os, nil
}

// MarkPaid 标记订单已支付。仅允许从 StatusCreated 转换；已支付/已完成订单幂等返回 nil。
func (s *Store) MarkPaid(ctx context.Context, orderID string, channelOrderID string) error {
	q := fmt.Sprintf("UPDATE %s SET status=?, pay_time=?, channel_order_id=? WHERE id=? AND status=?", s.table)
	res, err := s.db.Exec(ctx, q, StatusPaid, data.NowSQL(), channelOrderID, orderID, StatusCreated)
	if err != nil {
		return fmt.Errorf("order: mark paid: %w", err)
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("order: mark paid rows affected: %w", err)
	}
	if ra == 0 {
		// 未匹配 StatusCreated：检查是否已终态（幂等成功）或不存在。
		o, err := s.GetByID(ctx, orderID)
		if err != nil {
			return err
		}
		if o.Status == StatusPaid || o.Status == StatusDone {
			return nil
		}
		return fmt.Errorf("order: can only mark paid from created status, current=%d", o.Status)
	}
	return nil
}

// MarkDone 标记订单已完成。仅允许从 StatusPaid 转换；已完成订单幂等返回 nil。
func (s *Store) MarkDone(ctx context.Context, orderID string) error {
	q := fmt.Sprintf("UPDATE %s SET status=?, done_time=? WHERE id=? AND status=?", s.table)
	res, err := s.db.Exec(ctx, q, StatusDone, data.NowSQL(), orderID, StatusPaid)
	if err != nil {
		return fmt.Errorf("order: mark done: %w", err)
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("order: mark done rows affected: %w", err)
	}
	if ra == 0 {
		o, err := s.GetByID(ctx, orderID)
		if err != nil {
			return err
		}
		if o.Status == StatusDone {
			return nil
		}
		return fmt.Errorf("order: can only mark done from paid status, current=%d", o.Status)
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
