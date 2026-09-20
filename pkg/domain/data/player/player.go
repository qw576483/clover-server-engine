// Package player 玩家领域类型、错误与存储接口。
package player

import (
	"context"
	"database/sql"
)

// EPlayer 角色基础信息（ID 为 DB 主键，PlayerID 为游戏可见角色 ID，二者值不同）。
// 字段：
//   - ID：DB 主键，16 位唯一字符串。
//   - PlayerID：游戏可见角色 ID，16 位唯一字符串，与 ID 不同。
//   - Name：角色名（1-32 位字母/数字/_ . @ - 与中文）。
//   - Account：所属账号。
//   - ServerID：区服 ID（玩家侧概念：客户端登录/创角带入，逻辑服仅透传；非逻辑服部署身份）。
//   - CreateTime：创建时间（sql.NullString）。
//   - LoginTime：最近一次登录时间（sql.NullString）。
type EPlayer struct {
	ID         string         `db:"id"`          // DB 主键，16 位唯一字符串
	PlayerID   string         `db:"player_id"`   // 游戏可见角色 ID，16 位唯一字符串，与 ID 不同
	Name       string         `db:"name"`        // 角色名
	Account    string         `db:"account"`     // 所属账号
	ServerID   uint32         `db:"server_id"`   // 区服 ID（玩家侧概念）
	CreateTime sql.NullString `db:"create_time"` // 创建时间
	LoginTime  sql.NullString `db:"login_time"`  // 最近一次登录时间
}

// 编译期断言：EPlayer 是纯数据结构（无接口）。
var _ = (*EPlayer)(nil)

// 玩家业务错误。
var (
	ErrPlayerNotFound = errPlayerNotFound // 角色不存在
	// ErrPlayerExists：player_id 撞唯一约束（uk_player_id，player_id 全局唯一）。
	// 注意不是「同 (account, server_id) 已存在」——同一账号同一区服允许建多个角色。
	ErrPlayerExists = errPlayerExists
)

// Store 角色存储的业务接口：只暴露业务可达的查询与更新能力，
// 建表（CreateTable）、关连接（Close）等引擎装配细节不在此列。
// 由 internal 的 *player.Store 满足。
type Store interface {
	// Create 创建角色（id 与 player_id 独立生成）；player_id 撞唯一约束（uk_player_id）返回 ErrPlayerExists。
	Create(ctx context.Context, account, name string, serverID uint32) (*EPlayer, error)
	// GetByAccount 按账号 + 区服查角色列表；无角色返回空切片（非错误）。
	GetByAccount(ctx context.Context, account string, serverID uint32) ([]EPlayer, error)
	// GetByID 按角色 ID 查角色；不存在返回 ErrPlayerNotFound。
	GetByID(ctx context.Context, playerID string) (*EPlayer, error)
	// GetByAccountAndID 按账号 + 区服 + 角色 ID 查角色（用于归属校验，防越权）；不存在返回 ErrPlayerNotFound。
	GetByAccountAndID(ctx context.Context, account string, serverID uint32, playerID string) (*EPlayer, error)
	// UpdateName 修改角色名。
	UpdateName(ctx context.Context, playerID, name string) error
	// UpdateLoginTime 更新角色最近一次登录时间。
	UpdateLoginTime(ctx context.Context, playerID string) error
	// Ensure 获取或创建角色：已存在返回现有；不存在以 name（为空时用 account）创建。
	Ensure(ctx context.Context, account string, serverID uint32, name string) (*EPlayer, error)
}
