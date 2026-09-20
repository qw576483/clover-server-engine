// Package master 是 MMO 引擎顶层协调服公开 API。
//
// 本包暴露业务需要显式使用的两项能力：
//   - 排行榜模块：master.NewMasterRank(g) 创建句柄，之后调用 Add/Top/GetRank 等方法。
//   - 玩家定位查询：master.NewPlayerLookup(g) 创建句柄，调用 Locate 查询
//     「某玩家是否在线 / 在哪个节点」（跨服好友、邀请、观战寻址的基础）。
//
// master 的节点注册、玩家定位写入（玩家上下线由引擎自动登记）、排行榜、
// session token 等能力由引擎在 app 层自动装配，业务经 app.Game 与基础设施包访问：
//   - 自定义消息转发：app.Game.CallMaster
//   - master TCP 客户端：app.Game.MasterClient
//   - 配置热加载：foundation/config 的 Loader.Watch（本地 fsnotify / etcd 双通道）
//
// 这些装配细节不向业务暴露。
package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"clover-server-engine/pkg/foundation/logger"
)

// —— 排行榜模块 ——
//
// RankMember 排行榜单条记录。Rank 为 1-based（1 = 分数最高）。
// Extra 为绑定的额外数据（JSON），业务可存玩家名、等级等展示信息。
type RankMember struct {
	Member string          `json:"member"`
	Score  float64         `json:"score"`
	Rank   int             `json:"rank"`
	Extra  json.RawMessage `json:"extra,omitempty"`
}

// Threshold 段位门槛：排名区间 [MinRank,MaxRank] 的最低分数要求。
type Threshold struct {
	MinRank  int     `json:"min_rank"`
	MaxRank  int     `json:"max_rank"`
	MinScore float64 `json:"min_score"`
}

// Thresholds 门槛配置列表。
type Thresholds []Threshold

// maxGates 段位门槛展开后的名次条目总数上限，防止超大区间把内存打爆。
const maxGates = 100000

// Validate 校验段位门槛配置：每个区间必须满足 1 <= MinRank <= MaxRank，
// 且所有区间展开后的名次条目总数不得超过 maxGates。
//
// 校验只返回 error，绝不 panic：门槛通常来自配置或备份数据，
// 一条脏配置不应让查询路径的进程直接崩溃。
func (ts Thresholds) Validate() error {
	// 用 int64 累加：int 累加在「前段区间很大 + 后段 MaxRank≈MaxInt」时会溢出为负，
	// 直接绕过 total > maxGates 检查，随后 expandGates 用负数 make(map, total) panic。
	total := int64(0)
	for i, t := range ts {
		if t.MinRank < 1 {
			return fmt.Errorf("rank: thresholds[%d]: min_rank %d must be >= 1", i, t.MinRank)
		}
		if t.MaxRank < t.MinRank {
			return fmt.Errorf("rank: thresholds[%d]: max_rank %d must be >= min_rank %d", i, t.MaxRank, t.MinRank)
		}
		total += int64(t.MaxRank) - int64(t.MinRank) + 1
		if total > maxGates {
			return fmt.Errorf("rank: thresholds expand to %d gates, limit is %d", total, maxGates)
		}
	}
	return nil
}

// expandGates 展开段位门槛为「名次 → 最低分」映射。
func (ts Thresholds) expandGates() (map[int]float64, error) {
	if err := ts.Validate(); err != nil {
		return nil, err
	}
	// 与 Validate 相同的 int64 口径：Validate 已保证总量 <= maxGates，
	// 这里的重算绝不能再用 int 累加（否则同一算式会再溢出一次）。
	total := int64(0)
	for _, t := range ts {
		total += int64(t.MaxRank) - int64(t.MinRank) + 1
	}
	gates := make(map[int]float64, int(total))
	for _, t := range ts {
		for r := t.MinRank; r <= t.MaxRank; r++ {
			gates[r] = t.MinScore
		}
	}
	return gates, nil
}

// AdjustMembers 对原始排名列表应用门槛调整。
// 跳过不满足段位分数的排名位置（空位），返回调整 Rank 后的成员列表。
//
// 门槛配置非法时按「无门槛」返回原始排名并打印告警：查询路径不为脏配置买单，
// 同时留下线索供定位配置来源。
func (ts Thresholds) AdjustMembers(raw []RankMember) []RankMember {
	// 无门槛 / 门槛非法时返回**副本**：原样返回调用方切片（slice 别名）会让调用方
	// 修改结果时回写输入，调用点不知情时极易踩到共享底层数组的坑。
	if len(ts) == 0 {
		return append([]RankMember(nil), raw...)
	}
	gates, err := ts.expandGates()
	if err != nil {
		// 走 logger 而不是标准库 log：后者只到 stdout，不进日志系统。
		// （本方法在 Thresholds 上，拿不到榜单名；board 由调用方 SetRankThresholds 侧携带。）
		logger.Warnf("rank: invalid thresholds, return ranks without gate adjustment: %v", err)
		return append([]RankMember(nil), raw...)
	}
	result := make([]RankMember, 0, len(raw))
	displayRank := 1
	for _, m := range raw {
		for {
			gate, ok := gates[displayRank]
			if !ok || m.Score >= gate {
				break
			}
			displayRank++
		}
		m.Rank = displayRank
		result = append(result, m)
		displayRank++
	}
	return result
}

// MasterRank 排行榜模块句柄（master 排行榜客户端）。
// 业务层通过 master.NewMasterRank(g) 显式创建并持有。
type MasterRank interface {
	// Add 添加或更新排行项。
	Add(ctx context.Context, board, member string, score float64, extra any) error
	// Top 获取排行榜前 N 条。
	Top(ctx context.Context, board string, n int) ([]RankMember, error)
	// GetMember 获取指定成员的排名和分数。
	GetMember(ctx context.Context, board, member string) (RankMember, bool, error)
	// GetRank 获取指定成员的排名（1-based）。
	GetRank(ctx context.Context, board, member string) (int, bool, error)
	// GetByRankRange 获取 [start, stop] 排名区间的条目（含）。
	GetByRankRange(ctx context.Context, board string, start, stop int) ([]RankMember, error)
	// Len 获取排行榜总人数。
	Len(ctx context.Context, board string) (int, error)
	// Remove 移除排行榜中的成员。
	Remove(ctx context.Context, board, member string) error
	// AddOnlyUpdateScore 仅当成员已存在且新分更高时更新分数。
	AddOnlyUpdateScore(ctx context.Context, board, member string, score float64) error
	// Incr 无条件累加分数并返回新分数。
	Incr(ctx context.Context, board, member string, delta float64) (float64, error)
	// IncrOnlyUpdateScore 仅保留更高分。
	IncrOnlyUpdateScore(ctx context.Context, board, member string, delta float64) (float64, error)
	// GetByScoreRange 获取分数在 [min, max] 区间内的条目。
	GetByScoreRange(ctx context.Context, board string, min, max float64) ([]RankMember, error)
	// Clear 清空排行榜。
	Clear(ctx context.Context, board string, deleteBackup bool) error
	// BackupAll 备份所有排行榜到 Redis。
	BackupAll(ctx context.Context) error
	// Backup 备份指定排行榜到 Redis。
	Backup(ctx context.Context, board string) error
	// RestoreAll 从 Redis 恢复所有排行榜。
	RestoreAll(ctx context.Context) error
	// Restore 从 Redis 恢复指定排行榜。
	Restore(ctx context.Context, board string) error
	// SetRankThresholds 设置排行榜的段位门槛配置。
	SetRankThresholds(ctx context.Context, board string, ts Thresholds) error
}

// RankGame 排行榜模块所需的 game 侧能力。app.Game 自动实现此接口。
// 业务无需直接构造或实现此接口——引擎装配时由 app.Game 自动满足。
type RankGame interface {
	// RegisterEngineHandler 注册引擎级消息处理器（排行榜查询等）。
	// handler 签名：func(ctx context.Context, msgID uint32, data []byte) ([]byte, error)
	// 返回值为回包数据，nil 表示无回包。
	RegisterEngineHandler(msgID uint32, handler func(ctx context.Context, msgID uint32, data []byte) ([]byte, error))
	// MasterClient 返回 master TCP 客户端（排行榜远程调用通道）。
	MasterClient() MasterClient
}

// MasterClient master 协作服的纯 TCP 连接管道（不含任何领域方法）。
// 业务自己管理生命周期，创建后按需组装领域 client（如 RankClient）。
type MasterClient interface {
	// Addr 返回当前连接的 master 地址（拨号时配置的地址，便于日志与排障）。
	Addr() string
	// Close 关闭连接到 master 的 TCP 连接；幂等。
	Close() error
	// Call 统一 RPC 调用：序列化 req → TCP Call → 反序列化到 resp。
	Call(ctx context.Context, msgID uint32, req, resp any) error
}

// newMasterRankFn 由 internal/domain/master init() 注册的工厂函数。
var newMasterRankFn func(g RankGame) MasterRank

// RegisterMasterRankFactory 注册底层 NewMasterRank 实现（由 internal/domain/master init() 调用）。
// 注册 nil 视为装配错误，立即 panic（否则会与「未注册」无法区分）。
func RegisterMasterRankFactory(fn func(RankGame) MasterRank) {
	if fn == nil {
		panic("master: RegisterMasterRankFactory: nil factory")
	}
	newMasterRankFn = fn
}

// NewMasterRank 创建排行榜模块：注册引擎 handler，返回模块句柄。
// 业务层显式创建并持有句柄，调用 Add/Top/Get 等方法。
func NewMasterRank(g RankGame) MasterRank {
	if newMasterRankFn == nil {
		panic("master: rank factory not registered (internal/domain/master not linked?)")
	}
	return newMasterRankFn(g)
}

// —— 玩家定位模块 ——
//
// 玩家定位表由引擎自动维护（玩家上/下线经跨服总线向 master 登记与摘除），
// 业务只做查询：传入 uid，得到「是否在线 + 当前所在节点」。
// 跨服好友在线状态、跨服邀请、观战寻址都基于这一次查询。

// ErrPlayerLookupUnavailable 玩家定位查询通道不可用（master 客户端未就绪 / 已关闭）。
// 注意它与「玩家不在线」是两回事：后者返回 online=false 且 err=nil。
var ErrPlayerLookupUnavailable = errors.New("master: player lookup unavailable")

// PlayerLookup 玩家定位查询句柄。业务层通过 master.NewPlayerLookup(g) 创建并持有。
type PlayerLookup interface {
	// Locate 查询玩家当前所在节点。
	//
	// 返回 (nodeID, online, err)：
	//   - online=true：玩家在线，nodeID 为其当前所在 game 节点 ID；
	//   - online=false 且 err=nil：玩家不在线（未在任何节点登记），nodeID 为空串；
	//   - err!=nil：查询通道本身失败（如 master 不可用），此时不得当作「离线」处理。
	Locate(ctx context.Context, uid string) (nodeID string, online bool, err error)
}

// PlayerLookupGame 玩家定位模块所需的 game 侧能力。app.Game 自动实现此接口。
// 业务无需直接构造或实现——引擎装配时由 app.Game 自动满足。
type PlayerLookupGame interface {
	// MasterClient 返回 master TCP 客户端（玩家定位查询通道）。
	MasterClient() MasterClient
}

// newPlayerLookupFn 由 internal/domain/master init() 注册的工厂函数。
var newPlayerLookupFn func(g PlayerLookupGame) PlayerLookup

// RegisterPlayerLookupFactory 注册底层 NewPlayerLookup 实现（由 internal/domain/master init() 调用）。
// 注册 nil 视为装配错误，立即 panic（否则会与「未注册」无法区分）。
func RegisterPlayerLookupFactory(fn func(PlayerLookupGame) PlayerLookup) {
	if fn == nil {
		panic("master: RegisterPlayerLookupFactory: nil factory")
	}
	newPlayerLookupFn = fn
}

// NewPlayerLookup 创建玩家定位查询句柄。
// 业务层显式创建并持有句柄，调用 Locate 查询「某玩家是否在线 / 在哪个节点」。
func NewPlayerLookup(g PlayerLookupGame) PlayerLookup {
	if newPlayerLookupFn == nil {
		panic("master: player lookup factory not registered (internal/domain/master not linked?)")
	}
	return newPlayerLookupFn(g)
}
