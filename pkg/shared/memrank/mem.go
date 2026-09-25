package memrank

import (
	"encoding/json"
	"math"
	"sync"
	"sync/atomic"

	"github.com/qw576483/clover-server-engine/pkg/domain/master"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// 单个榜单的成员数上限（0 表示不限）。
//
// scores/extras/order 对任意新 member 直接插入，只有 Remove/Clear 会删；
// 与 Manager.GetOrCreate 叠加后形成「榜 × 成员」双层无界增长，榜名或成员名一旦
// 带公会/赛季等动态维度就会被外部输入撑爆。默认取 1e6：单榜 1e6 成员已是数十 MB 量级，
// 超过这个规模应改用 Redis 后端而不是继续吃进程内存。
var maxMembersPerBoard atomic.Int64

// DefaultMaxMembersPerBoard 单榜成员数上限的默认值。
const DefaultMaxMembersPerBoard = 1_000_000

func init() { maxMembersPerBoard.Store(DefaultMaxMembersPerBoard) }

// SetMaxMembersPerBoard 调整单榜成员数上限（<=0 表示不限）。
// 只影响之后新建的榜与之后的插入判定，已超限的榜不会被自动裁剪。
func SetMaxMembersPerBoard(n int64) { maxMembersPerBoard.Store(n) }

// MaxMembersPerBoard 返回当前生效的单榜成员数上限。
func MaxMembersPerBoard() int64 { return maxMembersPerBoard.Load() }

// MemSortedSet 内存有序集合：分数降序、同分按 member 字典序升序（确定性）。
// 内嵌读写锁，所有公开方法线程安全。
type MemSortedSet struct {
	mu     sync.RWMutex
	scores map[string]float64
	extras map[string]json.RawMessage
	order  []string
	// maxMembers 本榜成员数上限快照（构造时取自包级配置）；<=0 表示不限。
	maxMembers int64
	// rejectLogged 插入被拒只报一次，避免每个新成员打一条日志形成风暴。
	rejectLogged bool
}

// newMemSortedSet 新建空内存有序集合（并发安全），不导出，外部统一通过 NewManager 操作。
func newMemSortedSet() *MemSortedSet {
	return &MemSortedSet{
		scores:     make(map[string]float64),
		extras:     make(map[string]json.RawMessage),
		maxMembers: MaxMembersPerBoard(),
	}
}

func (s *MemSortedSet) less(x, y string) bool {
	a, b := s.scores[x], s.scores[y]
	// NaN 排在末尾：NaN 既不大于也不小于任何数，会导致二分查找定位错误，
	// 把 NaN 都归到末尾并保持一致性。
	na, nb := math.IsNaN(a), math.IsNaN(b)
	if na && nb {
		return x < y // 同为 NaN 按 member 字典序
	}
	if na {
		return false // NaN > 任何数 不成立
	}
	if nb {
		return true // 任何数 > NaN 成立（正常数排在 NaN 前面）
	}
	if a != b {
		return a > b
	}
	return x < y
}

func (s *MemSortedSet) findIndex(member string) int {
	lo, hi := 0, len(s.order)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.less(s.order[mid], member) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (s *MemSortedSet) insert(member string) {
	idx := s.findIndex(member)
	s.order = append(s.order, "")
	copy(s.order[idx+1:], s.order[idx:])
	s.order[idx] = member
}

func (s *MemSortedSet) removeFromOrder(member string) {
	idx := s.findIndex(member)
	if idx < len(s.order) && s.order[idx] == member {
		s.order = append(s.order[:idx], s.order[idx+1:]...)
	}
}

func (s *MemSortedSet) Set(member string, score float64, extra json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setLocked(member, score, extra)
}

func (s *MemSortedSet) Incr(member string, delta float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.scores[member]
	if !ok {
		// 成员不存在，按契约不创建新记录，返回 0。
		// 与 IncrOnlyUpdateScore 保持一致语义。
		return 0
	}
	ns := old + delta
	s.setLocked(member, ns, s.extras[member])
	return ns
}

// AddOnlyUpdateScore 仅在成员已存在 且 newScore > 旧分数时写入，否则保持原值。
// 返回 (最终生效的分数, 成员是否存在)。
// 成员不存在时返回 (0, false)，不创建新记录。
func (s *MemSortedSet) AddOnlyUpdateScore(member string, score float64, extra json.RawMessage) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.scores[member]
	if !ok {
		return 0, false
	}
	if score <= old {
		return old, true
	}
	s.setLocked(member, score, extra)
	return score, true
}

// IncrOnlyUpdateScore 仅在成员已存在 且 newScore > 旧分数时写入，否则保持原值。
// 返回 (最终生效的分数, 成员是否存在)。
// 成员不存在时返回 (0, false)，不创建新记录。
func (s *MemSortedSet) IncrOnlyUpdateScore(member string, delta float64) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.scores[member]
	if !ok {
		return 0, false
	}
	ns := old + delta
	if math.IsNaN(delta) || math.IsNaN(ns) || math.IsInf(ns, 0) {
		return old, true
	}
	if ns <= old {
		return old, true
	}
	s.setLocked(member, ns, s.extras[member])
	return ns, true
}

// setLocked 已持锁前提下设分和扩展数据，不额外加锁。
//
// 容量纪律：成员已存在时是「更新」（不增加条目数），只有**新成员**才受 maxMembers 约束；
// 超限时拒绝插入（保留既有的正常成员），不静默扩张。
func (s *MemSortedSet) setLocked(member string, score float64, extra json.RawMessage) {
	if _, ok := s.scores[member]; ok {
		s.removeFromOrder(member)
	} else if s.maxMembers > 0 && int64(len(s.scores)) >= s.maxMembers {
		if !s.rejectLogged {
			s.rejectLogged = true
			logger.Warnf("memrank: board member limit reached (%d), reject new member (existing members untouched)", s.maxMembers)
		}
		return
	}
	s.scores[member] = score
	s.extras[member] = extra
	s.insert(member)
}

// entryLocked 已持读锁前提下构造 RankMember，不额外加锁。
// Extra 字段做深拷贝，避免外部修改污染排行榜内部数据。
func (s *MemSortedSet) entryLocked(member string, i int) master.RankMember {
	extra := s.extras[member]
	if extra != nil {
		cp := make(json.RawMessage, len(extra))
		copy(cp, extra)
		extra = cp
	}
	return master.RankMember{
		Member: member,
		Score:  s.scores[member],
		Rank:   i + 1,
		Extra:  extra,
	}
}

func (s *MemSortedSet) Get(member string) (master.RankMember, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.scores[member]
	if !ok {
		return master.RankMember{}, false
	}
	return s.entryLocked(member, s.findIndex(member)), true
}

func (s *MemSortedSet) Score(member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sc, ok := s.scores[member]
	return sc, ok
}

func (s *MemSortedSet) Rank(member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.scores[member]; !ok {
		return 0, false
	}
	return s.findIndex(member) + 1, true
}

func (s *MemSortedSet) Remove(member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.scores[member]; !ok {
		return false
	}
	// 必须先从 order 中摘除：removeFromOrder 依赖 findIndex→less 读取
	// s.scores[member] 的旧分数定位下标，若先 delete 分数会变成 0 导致
	// 二分定位到错误位置，元素残留在 order 中。
	s.removeFromOrder(member)
	delete(s.scores, member)
	delete(s.extras, member)
	return true
}

func (s *MemSortedSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.scores)
}

func (s *MemSortedSet) Top(n int) []master.RankMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// n<=0 表示「不要任何成员」→ 空集。
	if n <= 0 {
		return nil
	}
	if n > len(s.order) {
		n = len(s.order)
	}
	out := make([]master.RankMember, n)
	for i := 0; i < n; i++ {
		out[i] = s.entryLocked(s.order[i], i)
	}
	return out
}

// GetByRankRange 按排名区间查询，start/stop 为 1-based 排名（与 Manager.GetByRankRange 接口一致）。
// stop 传负数（如 -1）表示取到末尾。
func (s *MemSortedSet) GetByRankRange(start, stop int) []master.RankMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// 1-based → 0-based index
	start = start - 1
	if start < 0 {
		start = 0
	}
	// stop < 0 表示到末尾
	if stop < 0 {
		stop = len(s.order) - 1
	} else {
		stop = stop - 1
	}
	if stop >= len(s.order) {
		stop = len(s.order) - 1
	}
	if start > stop || start >= len(s.order) {
		return nil
	}
	out := make([]master.RankMember, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, s.entryLocked(s.order[i], i))
	}
	return out
}

func inRange(sc float64, opts RangeOpts) bool {
	if opts.HasMin {
		if opts.MinExclusive && sc <= opts.Min {
			return false
		}
		if !opts.MinExclusive && sc < opts.Min {
			return false
		}
	}
	if opts.HasMax {
		if opts.MaxExclusive && sc >= opts.Max {
			return false
		}
		if !opts.MaxExclusive && sc > opts.Max {
			return false
		}
	}
	return true
}

func (s *MemSortedSet) GetByScoreRange(opts RangeOpts) []master.RankMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []master.RankMember
	for i, m := range s.order {
		sc := s.scores[m]
		if !inRange(sc, opts) {
			continue
		}
		out = append(out, s.entryLocked(m, i))
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if opts.Offset > 0 {
		if opts.Offset >= len(out) {
			return nil
		}
		out = out[opts.Offset:]
	}
	if opts.Limit > 0 && opts.Limit < len(out) {
		out = out[:opts.Limit]
	}
	return out
}

func (s *MemSortedSet) Members() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.order))
	copy(out, s.order)
	return out
}

// All 返回所有成员的完整信息（按排名升序）。
func (s *MemSortedSet) All() []master.RankMember {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]master.RankMember, len(s.order))
	for i, m := range s.order {
		out[i] = s.entryLocked(m, i)
	}
	return out
}

func (s *MemSortedSet) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scores = make(map[string]float64)
	s.extras = make(map[string]json.RawMessage)
	s.order = nil
}
