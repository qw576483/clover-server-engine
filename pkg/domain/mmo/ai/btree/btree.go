// Package btree 轻量行为树框架的公开 API 层。
//
// 提供 Status、Blackboard、Node 等基础类型定义，
// 以及控制节点 / 装饰节点 / 叶子节点的接口抽象。
// 具体实现位于 internal/domain/mmo/gameplay/ai/btree，
// 工厂函数位于 pkg/domain/mmo/mmo.go。
package btree

import "time"

// Status 是节点 tick 的返回状态。
type Status int

const (
	StatusSuccess Status = iota
	StatusFailure
	StatusRunning
)

func (s Status) String() string {
	switch s {
	case StatusSuccess:
		return "success"
	case StatusFailure:
		return "failure"
	default:
		return "running"
	}
}

// ParallelPolicy 并行完成策略。
type ParallelPolicy int

const (
	ParallelAllSuccess ParallelPolicy = iota
	ParallelOneSuccess
)

// 引擎占用的黑板键。定义在本包（而非 internal）是刻意的：
// 业务要用同一套时钟就得能引用同一套字面量，免得两处各写一次字符串、各错一次。
const (
	// KeyDT 是本帧步长（秒，float64）：Tree.Tick 每帧自动写入。
	KeyDT = "dt"
	// KeyNow 是当前逻辑时刻：**由驱动方注入**（见 Blackboard 的说明）。
	KeyNow = "now"
)

// Blackboard 是行为树的共享黑板接口：Agent 级状态 store。
//
// ★ 引擎占用的公共键（驱动方写入、节点只读；业务不要拿它们放别的东西）：
//
//   - "dt"  ：本帧步长（秒，float64）。Tree.Tick 每帧自动写入，Timeout 等节点读它。
//   - "now" ：当前逻辑时刻，供 Limiter / Cooldown 判窗口与冷却。
//     **Tree.Tick 保证它每帧存在且推进**，归属按**首帧**判定：
//     首次 Tick 前黑板已有该键 ⇒ 归驱动方（Tree 只读；写法 b.Set("now", mmo.LogicalTime(逻辑秒))，
//     或直接写逻辑秒数值 float64/int64，内部按 LogicalTime 换算）；
//     首次 Tick 前没有 ⇒ 归 Tree，由它按 dt 自累加推进
//     ⇒ Timeout / Limiter / Cooldown 在同一棵树里共用同一个逻辑时刻。
//     只有**直接 tick 节点、不经 Tree.Tick** 时才回落墙钟 time.Now()（并留一条降频 Warn）：
//     不会崩，但会与 dt 驱动的节点口径脱节（服务器暂停 / 变速时表现为"冷却照样走完、
//     限流窗口照样重置"）。
//
// ★ 另注意：行为树的节点持有可变状态（Selector 续跑位置 / Limiter 窗口 / Cooldown 时刻 /
// Repeater 计数 / Timeout 已耗时），因此**一棵树只服务一个 Agent** ——
// 多 Agent 共用一棵树会让决策状态互相串扰（典型症状：有目标却站着不攻击）。
type Blackboard interface {
	Set(key string, v any)
	Get(key string) any
	Del(key string)
	GetFloat64(key string) float64
	GetInt64(key string) int64
	GetString(key string) string
	GetBool(key string) bool
}

// Node 是行为树节点接口：输入黑板，返回执行状态。
type Node interface {
	Tick(b Blackboard) Status
}

// Sequence 是序列控制节点接口。
type Sequence interface {
	Node
}

// Selector 是选择器控制节点接口。
type Selector interface {
	Node
}

// Parallel 是并行控制节点接口。
type Parallel interface {
	Node
}

// Inverter 是取反装饰节点接口。
type Inverter interface {
	Node
}

// Repeater 是重复执行装饰节点接口。
type Repeater interface {
	Node
}

// UntilFailure 是"直到失败"装饰节点接口。
type UntilFailure interface {
	Node
}

// Limiter 是频次限制装饰节点接口。
type Limiter interface {
	Node
}

// Cooldown 是冷却装饰节点接口。
type Cooldown interface {
	Node
}

// Timeout 是超时装饰节点接口。
type Timeout interface {
	Node
}

// Condition 是条件叶子节点接口。
type Condition interface {
	Node
}

// Action 是行为叶子节点接口。
type Action interface {
	Node
}

// ActionFn 是执行即成功的行为叶子节点接口。
type ActionFn interface {
	Node
}

// Tree 是行为树接口：持有根节点，可绑定 dt 供 Timeout 等使用。
type Tree interface {
	Tick(b Blackboard, dt time.Duration) Status
}
