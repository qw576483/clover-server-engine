// Package master 汇总 master 相关的公共配置与领域能力。
//
// 排行榜模块以独立包模块的方式暴露：
//   - master.NewMasterRank(g) 创建模块：注册引擎 EMsgRankQuery handler 并初始化 RankService。
//   - 业务层显式创建并持有句柄，直接调用 Add/Top/Get 等方法。
//
// 底层实现为 state.RankService，远程访问经 client.RankClient（master TCP 连接）。
package master

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/master/client"
	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/domain/master"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// RankGame 排行榜模块所需的 game 侧能力（内部接口）。
// app.Game 自动实现此接口。
type RankGame interface {
	RegisterEngineHandler(msgID uint32, handler func(ctx context.Context, msgID uint32, data []byte) ([]byte, error))
	MasterClient() *client.Client
}

// RankService 是排行榜服务接口（= state.RankService），对外暴露。
type RankService = state.RankService

// MasterRank 排行榜模块句柄（master 排行榜客户端）。
// 业务层通过 NewMasterRank(g) 创建，持有句柄后调用 Add/Top/Get 等方法。
type MasterRank struct {
	g     RankGame
	svcMu sync.Mutex // 保护 svc 的惰性初始化（Add/Top/handleRankQuery 可能并发首调）
	svc   RankService
}

// init 注册工厂函数到 pkg/domain/master。
func init() {
	master.RegisterMasterRankFactory(func(g master.RankGame) master.MasterRank {
		// 将 pkg 的 RankGame 接口适配为 internal 的 RankGame 接口
		ig := &rankGameAdapter{g}
		return NewMasterRank(ig)
	})
}

// rankGameAdapter 将 pkg 的 RankGame 接口适配为 internal 的 RankGame 接口。
type rankGameAdapter struct {
	pkg master.RankGame
}

func (a *rankGameAdapter) RegisterEngineHandler(msgID uint32, handler func(ctx context.Context, msgID uint32, data []byte) ([]byte, error)) {
	a.pkg.RegisterEngineHandler(msgID, handler)
}

func (a *rankGameAdapter) MasterClient() *client.Client {
	if a == nil || a.pkg == nil {
		return nil
	}
	// 从 pkg 的 MasterClient（纯接口门面）取出底层 *client.Client：
	// 门面透传的实现始终是 *client.Client（见 pkg/app 的 NewMasterClient / Game.MasterClient），
	// 直接断言即可。不是该具体类型（或为 nil 接口）时返回 nil，由调用方按「通道不可用」处理。
	c, _ := a.pkg.MasterClient().(*client.Client)
	return c
}

// NewMasterRank 创建排行榜模块：注册引擎 handler 并返回模块句柄。
// 排行榜服务（master TCP）由 lazySvc 延迟初始化。
func NewMasterRank(g RankGame) *MasterRank {
	m := &MasterRank{g: g}
	g.RegisterEngineHandler(proto.EMsgRankQuery, m.handleRankQuery)
	return m
}

// Add 添加或更新排行项。score 为排行分数，extra 为可选的附加 JSON 数据。
func (m *MasterRank) Add(ctx context.Context, board, member string, score float64, extra any) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	var raw json.RawMessage
	if extra != nil {
		b, err := json.Marshal(extra)
		if err != nil {
			// 不能把序列化失败的 Extra 静默变 nil 写进榜单。
			logger.Warnf("master: rank add marshal extra board=%s member=%s: %v", board, member, err)
			return fmt.Errorf("master: rank add marshal extra: %w", err)
		}
		raw = b
	}
	return s.Add(ctx, board, member, score, raw)
}

// Top 获取排行榜前 N 条。
func (m *MasterRank) Top(ctx context.Context, board string, n int) ([]master.RankMember, error) {
	s := m.lazySvc()
	if s == nil {
		return nil, nil
	}
	return s.Top(ctx, board, n)
}

// GetMember 获取指定成员的排名和分数。返回 (rankMember, found, error)。
func (m *MasterRank) GetMember(ctx context.Context, board, member string) (master.RankMember, bool, error) {
	s := m.lazySvc()
	if s == nil {
		return master.RankMember{}, false, nil
	}
	return s.GetMember(ctx, board, member)
}

// GetRank 获取指定成员的排名（1-based）。返回 (rank, found, error)。
func (m *MasterRank) GetRank(ctx context.Context, board, member string) (int, bool, error) {
	s := m.lazySvc()
	if s == nil {
		return 0, false, nil
	}
	return s.GetRank(ctx, board, member)
}

// GetByRankRange 获取 [start, stop] 排名区间的条目（含）。
func (m *MasterRank) GetByRankRange(ctx context.Context, board string, start, stop int) ([]master.RankMember, error) {
	s := m.lazySvc()
	if s == nil {
		return nil, nil
	}
	return s.GetByRankRange(ctx, board, start, stop)
}

// Len 获取排行榜总人数。
func (m *MasterRank) Len(ctx context.Context, board string) (int, error) {
	s := m.lazySvc()
	if s == nil {
		return 0, nil
	}
	return s.Len(ctx, board)
}

// Remove 移除排行榜中的成员。
func (m *MasterRank) Remove(ctx context.Context, board, member string) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.Remove(ctx, board, member)
}

// AddOnlyUpdateScore 仅当成员已存在且新分更高时更新分数；成员不存在返回 error。
func (m *MasterRank) AddOnlyUpdateScore(ctx context.Context, board, member string, score float64) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.AddOnlyUpdateScore(ctx, board, member, score)
}

// Incr 无条件累加分数并返回新分数。
func (m *MasterRank) Incr(ctx context.Context, board, member string, delta float64) (float64, error) {
	s := m.lazySvc()
	if s == nil {
		return 0, nil
	}
	return s.Incr(ctx, board, member, delta)
}

// IncrOnlyUpdateScore 仅保留更高分（累加后取 max），成员不存在返回 error。
func (m *MasterRank) IncrOnlyUpdateScore(ctx context.Context, board, member string, delta float64) (float64, error) {
	s := m.lazySvc()
	if s == nil {
		return 0, nil
	}
	return s.IncrOnlyUpdateScore(ctx, board, member, delta)
}

// GetByScoreRange 获取分数在 [min, max] 区间内的条目。
func (m *MasterRank) GetByScoreRange(ctx context.Context, board string, min, max float64) ([]master.RankMember, error) {
	s := m.lazySvc()
	if s == nil {
		return nil, nil
	}
	return s.GetByScoreRange(ctx, board, min, max)
}

// Clear 清空排行榜。deleteBackup=true 时连带删除 Redis 中该榜的备份 key。
func (m *MasterRank) Clear(ctx context.Context, board string, deleteBackup bool) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.Clear(ctx, board, deleteBackup)
}

// BackupAll 备份所有排行榜到 Redis。
func (m *MasterRank) BackupAll(ctx context.Context) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.BackupAll(ctx)
}

// Backup 备份指定排行榜到 Redis。
func (m *MasterRank) Backup(ctx context.Context, board string) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.Backup(ctx, board)
}

// RestoreAll 从 Redis 恢复所有排行榜。
func (m *MasterRank) RestoreAll(ctx context.Context) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.RestoreAll(ctx)
}

// Restore 从 Redis 恢复指定排行榜。
func (m *MasterRank) Restore(ctx context.Context, board string) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.Restore(ctx, board)
}

// SetRankThresholds 设置排行榜的段位门槛配置。
func (m *MasterRank) SetRankThresholds(ctx context.Context, board string, ts master.Thresholds) error {
	s := m.lazySvc()
	if s == nil {
		return nil
	}
	return s.SetRankThresholds(ctx, board, ts)
}

// lazySvc 延迟初始化 rankService（master 连接就绪后首次调用时创建）。
// 加锁保护：并发首调（Add/Top/handleRankQuery）会并发写 m.svc（数据竞争），
// 也可能竞态生成两个 RankClient（各自持有独立连接，泄漏一个）。
func (m *MasterRank) lazySvc() RankService {
	m.svcMu.Lock()
	defer m.svcMu.Unlock()
	if m.svc != nil {
		return m.svc
	}
	mc := m.g.MasterClient()
	if mc == nil {
		return nil
	}
	m.svc = client.NewRankClient(mc)
	return m.svc
}

// —— 引擎 handler：客户端排行榜统一查询 ——
//
// handleRankQuery 处理排行榜查询请求。
// 签名适配 pkg/domain/master.RankGame 接口。
func (m *MasterRank) handleRankQuery(ctx context.Context, msgID uint32, data []byte) ([]byte, error) {
	svc := m.lazySvc()
	if svc == nil {
		logger.Warnf("master: rank service unavailable")
		reply := proto.ERankQueryReply{Err: "rank service unavailable"}
		return json.Marshal(reply)
	}

	var req proto.ERankQueryRequest
	if err := json.Unmarshal(data, &req); err != nil {
		// 校验失败属非预期分支：回包之外必须留日志。
		logger.Warnf("master: rank query decode failed: %v", err)
		reply := proto.ERankQueryReply{Err: err.Error()}
		return json.Marshal(reply)
	}

	reply := proto.ERankQueryReply{Board: req.Board}
	if n, err := svc.Len(ctx, req.Board); err != nil {
		// 服务端故障不能当成「空榜」静默返回。
		logger.Warnf("master: rank query len board=%s: %v", req.Board, err)
	} else {
		reply.Total = n
	}

	// 查询指定成员
	if req.Member != "" {
		member, found, err := svc.GetMember(ctx, req.Board, req.Member)
		if err != nil {
			logger.Warnf("master: rank query member board=%s member=%s: %v", req.Board, req.Member, err)
		}
		if found {
			reply.Member = proto.ERankEntry{
				Member: member.Member,
				Score:  member.Score,
				Rank:   member.Rank,
				Extra:  member.Extra,
			}
			reply.MemberFound = true
		}
	}

	// 查询排名区间
	if req.Start > 0 || req.Stop > 0 {
		start := req.Start
		stop := req.Stop
		if start <= 0 {
			start = 1
		}
		if stop <= 0 {
			// start 接近 MaxInt 时 start+9 会溢出为负；溢出则退化为单条查询。
			if start > math.MaxInt-9 {
				stop = start
			} else {
				stop = start + 9 // 默认 10 条
			}
		}
		entries, err := svc.GetByRankRange(ctx, req.Board, start, stop)
		if err != nil {
			logger.Warnf("master: rank query range board=%s range=[%d,%d]: %v", req.Board, start, stop, err)
		}
		reply.Range = make([]proto.ERankEntry, 0, len(entries))
		for _, e := range entries {
			reply.Range = append(reply.Range, proto.ERankEntry{
				Member: e.Member,
				Score:  e.Score,
				Rank:   e.Rank,
				Extra:  e.Extra,
			})
		}
	}

	return json.Marshal(reply)
}
