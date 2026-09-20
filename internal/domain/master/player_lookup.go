// 玩家定位查询模块：pkg/domain/master 的 PlayerLookup 底层实现。
//
// 以独立包模块的方式暴露：
//   - master.NewPlayerLookup(g) 创建模块句柄，业务层显式创建并持有。
//   - 业务只做查询（Locate）：定位表的写入由引擎在玩家上/下线时自动完成。
//
// 底层复用 state.PlayerNode（内存定位表），远程访问经 client.PlayerClient
// （master TCP 连接，多分片时按 uid 路由到属主分片）。
package master

import (
	"context"
	"errors"
	"sync"

	"clover-server-engine/internal/domain/master/client"
	"clover-server-engine/pkg/domain/master"
)

// PlayerLookupGame 玩家定位模块所需的 game 侧能力（内部接口）。
// app.Game 自动实现此接口。
type PlayerLookupGame interface {
	MasterClient() *client.Client
}

// MasterPlayerLookup 玩家定位查询句柄（master 玩家定位表客户端）。
// 业务层通过 NewPlayerLookup(g) 创建，持有句柄后调用 Locate。
type MasterPlayerLookup struct {
	g   PlayerLookupGame
	mu  sync.Mutex
	cli *client.PlayerClient
}

// init 注册工厂函数到 pkg/domain/master。
func init() {
	master.RegisterPlayerLookupFactory(func(g master.PlayerLookupGame) master.PlayerLookup {
		// 将 pkg 的 PlayerLookupGame 接口适配为 internal 的 PlayerLookupGame 接口
		return NewPlayerLookup(&playerLookupGameAdapter{g})
	})
}

// playerLookupGameAdapter 将 pkg 的 PlayerLookupGame 接口适配为 internal 的 PlayerLookupGame 接口。
type playerLookupGameAdapter struct {
	pkg master.PlayerLookupGame
}

// MasterClient 从 pkg 的 MasterClient 门面取出底层 *client.Client。
// 门面透传的实现始终是 *client.Client（见 pkg/app 的 NewMasterClient / Game.MasterClient），
// 直接断言即可；不是该具体类型时返回 nil，由调用方按「通道不可用」处理。
func (a *playerLookupGameAdapter) MasterClient() *client.Client {
	if a == nil || a.pkg == nil {
		return nil
	}
	c, _ := a.pkg.MasterClient().(*client.Client)
	return c
}

// NewPlayerLookup 创建玩家定位查询句柄。远程客户端（PlayerClient）延迟初始化。
func NewPlayerLookup(g PlayerLookupGame) *MasterPlayerLookup {
	return &MasterPlayerLookup{g: g}
}

// Locate 查询玩家当前所在节点。签名与 pkg/domain/master.PlayerLookup 一致。
//
// online=false 表示玩家不在线（未在 master 登记），此时 err 为 nil；
// 仅当查询通道本身不可用（master 连接未就绪）时才返回错误 —— 二者语义不同，
// 调用方不得把 error 当作「离线」。
func (m *MasterPlayerLookup) Locate(ctx context.Context, uid string) (string, bool, error) {
	pc := m.client()
	if pc == nil {
		return "", false, master.ErrPlayerLookupUnavailable
	}
	nodeID, err := pc.PlayerNode(ctx, uid)
	if err != nil {
		if errors.Is(err, client.ErrPlayerNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return nodeID, true, nil
}

// client 延迟初始化 PlayerClient（master 连接就绪后首次调用时创建）。
// master 连接尚未就绪时返回 nil（不缓存，下次调用重试）。
func (m *MasterPlayerLookup) client() *client.PlayerClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cli != nil {
		return m.cli
	}
	mc := m.g.MasterClient()
	if mc == nil {
		return nil
	}
	m.cli = client.NewPlayerClient(mc)
	return m.cli
}
