// Package memrank 通用有序榜（排行榜）引擎原语。
//
//	rankSet := memrank.New()
//	rankSet.Set("player:1001", 12345, nil)
//	top := rankSet.Top(100)
//
// 默认内存实现（MemSortedSet）已是线程安全。
// 如需 Redis 后端，实现 SortedSet 接口注入即可。
package memrank

import (
	"encoding/json"

	"github.com/qw576483/clover-server-engine/pkg/domain/master"
)

// RangeOpts 按分数区间查询的可选项。
// 零值（Min=0,Max=0 且 HasMin/HasMax 均为 false）时区间覆盖所有成员（-inf~+inf）。
// 如需精确指定 Min/Max，必须同时设置 HasMin/HasMax 为 true。
type RangeOpts struct {
	Min, Max      float64
	MinExclusive  bool
	MaxExclusive  bool
	HasMin        bool // 是否显式设置了 Min，false 则 Min 视为 -inf
	HasMax        bool // 是否显式设置了 Max，false 则 Max 视为 +inf
	Offset, Limit int
}

// SortedSet 有序集合后端接口。
type SortedSet interface {
	Set(member string, score float64, extra json.RawMessage)
	Incr(member string, delta float64) float64
	AddOnlyUpdateScore(member string, score float64, extra json.RawMessage) (float64, bool)
	IncrOnlyUpdateScore(member string, delta float64) (float64, bool)
	Get(member string) (master.RankMember, bool) // 查分+排名（一次调用）
	Score(member string) (float64, bool)         // 仅查分
	Rank(member string) (int, bool)
	Remove(member string) bool
	Len() int
	Top(n int) []master.RankMember
	GetByRankRange(start, stop int) []master.RankMember
	GetByScoreRange(opts RangeOpts) []master.RankMember
	Members() []string
	All() []master.RankMember
	Clear()
}

// New 创建线程安全的内存排行榜（默认后端）。
// 等价于 newMemSortedSet()。
func New() SortedSet { return newMemSortedSet() }

// —— 段位门槛 ——
