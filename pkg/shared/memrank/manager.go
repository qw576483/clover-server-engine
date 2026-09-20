package memrank

import (
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/qw576483/clover-server-engine/pkg/domain/master"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// 榜名总数上限（0 表示不限）。
//
// 背景（缺陷）：GetOrCreate 对任意 key 自动建榜，唯一删除点是 Unregister；
// 榜名由业务/客户端传入（master TCP MsgRankAdd → state.GetOrCreate），
// 一旦含公会/赛季等动态维度即无界增长。
var maxBoards atomic.Int64

// DefaultMaxBoards 榜名总数上限的默认值。
const DefaultMaxBoards = 10_000

func init() { maxBoards.Store(DefaultMaxBoards) }

// SetMaxBoards 调整榜名总数上限（<=0 表示不限）。
func SetMaxBoards(n int64) { maxBoards.Store(n) }

// MaxBoards 返回当前生效的榜名总数上限。
func MaxBoards() int64 { return maxBoards.Load() }

// discardSet 容量触顶后 GetOrCreate 返回的「只丢弃」榜。
//
// 为什么不是返回 nil：GetOrCreate 的调用方（internal/domain/master/state）拿到后立刻
// 调 b.Set(...)，返回 nil 会把「容量保护」变成进程级 panic。
// 写入静默丢弃、读取恒为空，语义等价于「这个榜不存在且不会被创建」。
type discardSet struct{}

func (discardSet) Set(string, float64, json.RawMessage) {}
func (discardSet) Incr(string, float64) float64         { return 0 }
func (discardSet) AddOnlyUpdateScore(string, float64, json.RawMessage) (float64, bool) {
	return 0, false
}
func (discardSet) IncrOnlyUpdateScore(string, float64) (float64, bool) { return 0, false }
func (discardSet) Get(string) (master.RankMember, bool)                { return master.RankMember{}, false }
func (discardSet) Score(string) (float64, bool)                        { return 0, false }
func (discardSet) Rank(string) (int, bool)                             { return 0, false }
func (discardSet) Remove(string) bool                                  { return false }
func (discardSet) Len() int                                            { return 0 }
func (discardSet) Top(int) []master.RankMember                         { return nil }
func (discardSet) GetByRankRange(int, int) []master.RankMember         { return nil }
func (discardSet) GetByScoreRange(RangeOpts) []master.RankMember       { return nil }
func (discardSet) Members() []string                                   { return nil }
func (discardSet) All() []master.RankMember                            { return nil }
func (discardSet) Clear()                                              {}

// Manager 多榜注册表，按名字管理多个排行榜。
type Manager struct {
	mu         sync.RWMutex
	boards     map[string]SortedSet
	thresholds map[string]master.Thresholds
	// boardsRejectLogged 榜数触顶只报一次，避免每个新榜名打一条日志。
	boardsRejectLogged bool
}

func NewManager() *Manager {
	return &Manager{
		boards:     make(map[string]SortedSet),
		thresholds: make(map[string]master.Thresholds),
	}
}

func (m *Manager) Register(name string, b SortedSet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.boards[name] = b
}

func (m *Manager) Get(name string) (SortedSet, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.boards[name]
	return b, ok
}

func (m *Manager) MustGet(name string) SortedSet {
	b, ok := m.Get(name)
	if !ok {
		panic("rank: board not registered: " + name)
	}
	return b
}

// GetOrCreate 取榜，不存在则创建。
//
// 榜数达到 MaxBoards 时**不再创建**，返回只丢弃的 discardSet 并告警：
// 既不静默无界扩张，也不会把 nil 交给调用方引发 panic。
func (m *Manager) GetOrCreate(name string) SortedSet {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.boards[name]; ok {
		return b
	}
	if lim := MaxBoards(); lim > 0 && int64(len(m.boards)) >= lim {
		if !m.boardsRejectLogged {
			m.boardsRejectLogged = true
			logger.Warnf("memrank: board count limit reached (%d), reject new board %q (writes to it are dropped)", lim, name)
		}
		return discardSet{}
	}
	b := New()
	m.boards[name] = b
	return b
}

// Unregister 注销排行榜，并连带清理其段位门槛：
// 门槛与榜单必须同生共死，否则同名榜重新注册后会沿用上一轮的门槛配置。
func (m *Manager) Unregister(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.boards[name]; !ok {
		return false
	}
	delete(m.boards, name)
	delete(m.thresholds, name)
	return true
}

func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.boards))
	for n := range m.boards {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Len 返回指定排行榜内的成员数量。榜不存在时返回 0。
func (m *Manager) Len(board string) int {
	m.mu.RLock()
	b, ok := m.boards[board]
	m.mu.RUnlock()
	if !ok {
		return 0
	}
	return b.Len()
}

func (m *Manager) Boards() map[string]SortedSet {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]SortedSet, len(m.boards))
	for n, b := range m.boards {
		out[n] = b
	}
	return out
}

// Add 向排行榜写入分数。board 不存在时自动创建。
func (m *Manager) Add(board, member string, score float64, extra json.RawMessage) {
	b := m.GetOrCreate(board)
	b.Set(member, score, extra)
}

// GetAll 返回排行榜中所有成员（原始排名，仅供备份等内部使用）。不存在返回 false。
func (m *Manager) GetAll(board string) ([]master.RankMember, bool) {
	b, ok := m.Get(board)
	if !ok {
		return nil, false
	}
	return b.All(), true
}

// —— 段位门槛 ——
//
// SetRankThresholds 设置排行榜段位门槛。
// 配置非法（区间倒置或展开规模超限）时拒绝写入并打印告警，保留原有门槛，
// 让错误留在配置侧可见，而不是被带进查询路径。
func (m *Manager) SetRankThresholds(board string, ts master.Thresholds) {
	if err := ts.Validate(); err != nil {
		logger.Warnf("memrank: board %q: reject invalid thresholds: %v", board, err)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.thresholds[board] = ts
}

// GetThresholds 获取排行榜段位门槛。
func (m *Manager) GetThresholds(board string) master.Thresholds {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.thresholds[board]
}

// Top 获取前 n 名（自动应用门槛）。
func (m *Manager) Top(board string, n int) []master.RankMember {
	m.mu.RLock()
	b, ok := m.boards[board]
	ts := m.thresholds[board]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return ts.AdjustMembers(b.Top(n))
}

// GetMember 获取成员排名信息（自动应用门槛）。
func (m *Manager) GetMember(board, member string) (master.RankMember, bool) {
	m.mu.RLock()
	b, ok := m.boards[board]
	ts := m.thresholds[board]
	m.mu.RUnlock()
	if !ok {
		return master.RankMember{}, false
	}
	rm, ok := b.Get(member)
	if !ok {
		return master.RankMember{}, false
	}
	if len(ts) == 0 {
		return rm, true
	}
	if rm.Rank > 1 {
		above := b.GetByRankRange(1, rm.Rank-1)
		all := append(above, rm)
		adjusted := ts.AdjustMembers(all)
		return adjusted[len(adjusted)-1], true
	}
	adjusted := ts.AdjustMembers([]master.RankMember{rm})
	return adjusted[0], true
}

// GetByRankRange 按排名区间查询（自动应用门槛）。
// start/stop 为 1‑based 排名，底层 SortedSet.GetByRankRange 也统一为 1‑based。
func (m *Manager) GetByRankRange(board string, start, stop int) []master.RankMember {
	m.mu.RLock()
	b, ok := m.boards[board]
	ts := m.thresholds[board]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return ts.AdjustMembers(b.GetByRankRange(start, stop))
}

// All 返回全部成员（自动应用门槛）。
func (m *Manager) All(board string) []master.RankMember {
	m.mu.RLock()
	b, ok := m.boards[board]
	ts := m.thresholds[board]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return ts.AdjustMembers(b.All())
}

// —— 包级默认实例，rank.Add/rank.Top 直接调 ——

var defaultManager = &Manager{
	boards:     make(map[string]SortedSet),
	thresholds: make(map[string]master.Thresholds),
}

// Add 向排行榜写入分数。
func Add(board, member string, score float64) {
	defaultManager.Add(board, member, score, nil)
}

// Top 获取前 n 名（自动应用门槛）。
func Top(board string, n int) []master.RankMember { return defaultManager.Top(board, n) }

// GetMember 获取成员排名（自动应用门槛）。
func GetMember(board, member string) (master.RankMember, bool) {
	return defaultManager.GetMember(board, member)
}

// GetByRankRange 按排名区间查询（自动应用门槛）。
func GetByRankRange(board string, start, stop int) []master.RankMember {
	return defaultManager.GetByRankRange(board, start, stop)
}

// All 返回全部成员（自动应用门槛）。
func All(board string) []master.RankMember { return defaultManager.All(board) }

// SetRankThresholds 设置段位门槛。
func SetRankThresholds(board string, ts master.Thresholds) {
	defaultManager.SetRankThresholds(board, ts)
}
