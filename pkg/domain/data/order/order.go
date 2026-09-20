// Package order 订单领域类型、错误与存储接口。
//
// 归属决定：订单归**账号服**（auth）——账号是全局身份、公网 HTTP 只有账号服有，
// 渠道的异步支付回调天然落在那一层。发货仍由 game 执行：在线角色数据只在 owner
// 节点内存，别处直接改库会被内存覆盖。前提是单库部署（auth 与 game 共用同一
// `data.mysql`），orders 表同库可达——`server_id` 是玩家侧概念，不是分库标识。
//
// 现状：`pkg/app` 对**两个角色**都开放了本 Store —— game 的 `Game.OrderStore()`，
// 以及账号服的 `AuthGame.OrderStore()`（订单归属账号服，供支付回调建单 / 流转状态）。
// 支付结果**不回推**：game 侧用既有 `CallAuth` 查单后，走引擎已有的按玩家寻址事件通道
// （`SendQueueEventToPlayer`）通知自己发货。引擎侧已无待做，剩余全是业务逻辑。
package order

import (
	"context"
	"database/sql"
	"strconv"
)

// Status 订单状态。
type Status int8

const (
	// StatusCreated 已创建（待支付）。
	StatusCreated Status = 0
	// StatusPaid 已支付（待完成）。
	StatusPaid Status = 1
	// StatusDone 已完成。
	StatusDone Status = 2
)

// String 返回状态名（日志 / 回包可读；未知值返回 "Status(N)"）。
func (s Status) String() string {
	switch s {
	case StatusCreated:
		return "StatusCreated"
	case StatusPaid:
		return "StatusPaid"
	case StatusDone:
		return "StatusDone"
	default:
		return "Status(" + strconv.Itoa(int(s)) + ")"
	}
}

// EOrder 订单基础信息（Price 以分为单位存储）。
// 字段：
//   - ID：引擎数据层 16 位唯一 ID。
//   - Account：账号。
//   - PlayerID：角色 ID（16 位字符串）。
//   - ServerID：区服 ID。
//   - Status：状态（StatusCreated / StatusPaid / StatusDone）。
//   - Price：价格（分，int64 避免浮点误差）。
//   - CreateTime：创建时间（sql.NullString）。
//   - PayTime：支付时间（sql.NullString）。
//   - DoneTime：完成时间（sql.NullString）。
//   - ChannelOrderID：渠道订单号。
//   - Channel：渠道。
//   - ExtParam：扩展参数 JSON。
type EOrder struct {
	ID             string         `db:"id"`               // 引擎数据层 16 位唯一 ID
	Account        string         `db:"account"`          // 账号
	PlayerID       string         `db:"player_id"`        // 角色 ID（16 位字符串）
	ServerID       uint32         `db:"server_id"`        // 区服 ID
	Status         Status         `db:"status"`           // 状态
	Price          int64          `db:"price"`            // 价格（分）
	CreateTime     sql.NullString `db:"create_time"`      // 创建时间
	PayTime        sql.NullString `db:"pay_time"`         // 支付时间
	DoneTime       sql.NullString `db:"done_time"`        // 完成时间
	ChannelOrderID string         `db:"channel_order_id"` // 渠道订单号
	Channel        string         `db:"channel"`          // 渠道
	ExtParam       string         `db:"ext_param"`        // 扩展参数 JSON
}

// 编译期断言：EOrder 是纯数据结构（无接口）。
var _ = (*EOrder)(nil)

// 订单业务错误。
var (
	ErrOrderNotFound = errOrderNotFound // 订单不存在
)

// Store 订单存储的业务接口：只暴露下单 / 查单 / 状态流转等业务能力，
// 建表（CreateTable）、关连接（Close）、表名（Table）等引擎装配细节不在此列。
// 由 internal 的 *order.Store 满足。
type Store interface {
	// Create 创建订单（price 单位为分，必须 > 0），返回订单对象。
	Create(ctx context.Context, account string, playerID string, serverID uint32, price int64, channel, channelOrderID, extParam string) (*EOrder, error)
	// GetByID 按订单 ID 查询；不存在返回 ErrOrderNotFound。
	GetByID(ctx context.Context, orderID string) (*EOrder, error)
	// ListByAccount 按账号查询订单。
	ListByAccount(ctx context.Context, account string) ([]EOrder, error)
	// ListByPlayer 按角色查询订单。
	ListByPlayer(ctx context.Context, playerID string, serverID uint32) ([]EOrder, error)
	// MarkPaid 标记订单已支付（仅从 StatusCreated 转换，幂等）。
	MarkPaid(ctx context.Context, orderID string, channelOrderID string) error
	// MarkDone 标记订单已完成（仅从 StatusPaid 转换，幂等）。
	MarkDone(ctx context.Context, orderID string) error
}
