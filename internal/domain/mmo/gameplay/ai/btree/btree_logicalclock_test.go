package btree

import (
	"math"
	"testing"
	"time"

	pkgbtree "github.com/qw576483/clover-server-engine/pkg/domain/mmo/ai/btree"
)

// countingLeaf 是一个计数叶子：每被 tick 一次 +1，恒成功。
type countingLeaf struct{ n int }

func (c *countingLeaf) Tick(pkgbtree.Blackboard) pkgbtree.Status {
	c.n++
	return pkgbtree.StatusSuccess
}

// Cooldown / Limiter 必须使用**黑板注入的逻辑时刻**，而不是墙钟。
//
// 守的是本缺陷：这两个节点原先读黑板 "now"（但全仓没有生产端）+ 回落 time.Now()，
// 而同一棵树的 Timeout 用 dt 累加 —— 同一棵树里三套时间口径。
// 服务器暂停 / 变速（逻辑时钟不动）时，冷却会照墙钟走完，表现与设计不符。
//
// 判别方式：把逻辑时刻设为 Unix 纪元附近的**很小的值**（如 10 秒）。
// 用墙钟时 time.Now() 远大于任何逻辑窗口，节点会「永远已就绪」，
// 下面的"窗口内不许执行"断言会立刻失败 —— 即本用例对真缺陷有检出能力。
func TestCooldownUsesBlackboardLogicalNow(t *testing.T) {
	leaf := &countingLeaf{}
	cd := NewCooldown(5*time.Second, leaf)
	bb := NewBlackboard()

	// t=10s：首次执行，冷却到 15s。
	bb.Set(KeyNow, LogicalTime(10))
	if s := cd.Tick(bb); s != pkgbtree.StatusSuccess {
		t.Fatalf("首次执行应成功，实际 %v", s)
	}
	if leaf.n != 1 {
		t.Fatalf("首次执行应调用子节点，实际 %d 次", leaf.n)
	}

	// t=12s：仍在冷却内 → 不许执行（墙钟实现会在这里执行）。
	bb.Set(KeyNow, LogicalTime(12))
	if s := cd.Tick(bb); s != pkgbtree.StatusFailure {
		t.Fatalf("冷却中应 Failure，实际 %v（说明冷却没用逻辑时刻）", s)
	}
	if leaf.n != 1 {
		t.Fatalf("冷却中不该调用子节点，实际 %d 次", leaf.n)
	}

	// t=16s：冷却结束 → 重新执行。
	bb.Set(KeyNow, LogicalTime(16))
	if s := cd.Tick(bb); s != pkgbtree.StatusSuccess {
		t.Fatalf("冷却结束后应成功，实际 %v", s)
	}
	if leaf.n != 2 {
		t.Fatalf("冷却结束后应再次调用子节点，实际 %d 次", leaf.n)
	}
}

// Limiter 的窗口同样按逻辑时刻推进（窗口内限额、窗口结束后重置）。
func TestLimiterUsesBlackboardLogicalNow(t *testing.T) {
	leaf := &countingLeaf{}
	lim := NewLimiter(2, 10*time.Second, leaf)
	bb := NewBlackboard()

	bb.Set(KeyNow, LogicalTime(0))
	for i := 0; i < 2; i++ {
		if s := lim.Tick(bb); s != pkgbtree.StatusSuccess {
			t.Fatalf("窗口内第 %d 次应成功，实际 %v", i+1, s)
		}
	}
	if s := lim.Tick(bb); s != pkgbtree.StatusFailure {
		t.Fatalf("超出限额应 Failure，实际 %v", s)
	}
	if leaf.n != 2 {
		t.Fatalf("超限那次不该调用子节点，实际 %d 次", leaf.n)
	}

	// 同一逻辑时刻下，无论真实时间过多久，窗口都不该重置。
	bb.Set(KeyNow, LogicalTime(9))
	if s := lim.Tick(bb); s != pkgbtree.StatusFailure {
		t.Fatalf("窗口未结束（逻辑 9s < 10s）不该重置限额，实际 %v", s)
	}

	// 越过窗口末尾：窗口重置，重新放行。
	bb.Set(KeyNow, LogicalTime(11))
	if s := lim.Tick(bb); s != pkgbtree.StatusSuccess {
		t.Fatalf("窗口结束后应重置并放行，实际 %v", s)
	}
}

// 黑板里塞「逻辑秒」的裸数值（float64）也应被接受：驱动方可能只累加 seconds。
func TestLimiterAcceptsLogicalSecondsAsFloat64(t *testing.T) {
	leaf := &countingLeaf{}
	lim := NewLimiter(1, time.Second, leaf)
	bb := NewBlackboard()

	bb.Set(KeyNow, float64(0)) // 逻辑 0 秒
	if s := lim.Tick(bb); s != pkgbtree.StatusSuccess {
		t.Fatalf("首次应成功，实际 %v", s)
	}
	if s := lim.Tick(bb); s != pkgbtree.StatusFailure {
		t.Fatalf("窗口内超限应 Failure，实际 %v", s)
	}
	bb.Set(KeyNow, float64(2)) // 逻辑 2 秒，越过 1s 窗口
	if s := lim.Tick(bb); s != pkgbtree.StatusSuccess {
		t.Fatalf("窗口结束后应放行，实际 %v", s)
	}
}

// 黑板 "now" 类型不符 / 缺失时回落墙钟：不许 panic，也不许把窗口判成"永远过期"以外的东西。
func TestCooldownFallsBackToWallClock(t *testing.T) {
	leaf := &countingLeaf{}
	// 冷却取 1 小时：用例只关心「回落的是墙钟」这一事实，
	// 窗口必须远大于两次 Tick 的真实耗时，否则机器卡顿一下就会假红。
	cd := NewCooldown(time.Hour, leaf)
	bb := NewBlackboard()

	// 无 "now" 键：回落墙钟，首次应执行。
	if s := cd.Tick(bb); s != pkgbtree.StatusSuccess {
		t.Fatalf("无 now 键时应回落墙钟并执行，实际 %v", s)
	}
	// 类型不符（string）：同样回落墙钟，不该 panic（并留有降频 Warn）。
	bb.Set(KeyNow, "not-a-time")
	if s := cd.Tick(bb); s != pkgbtree.StatusFailure {
		t.Fatalf("墙钟下落 1h 冷却内应 Failure，实际 %v", s)
	}
}

// Tree.Tick 必须保证黑板 "now" 存在：驱动方不注入时按 **dt 自累加的逻辑钟**兜底，
// 从而 Limiter / Cooldown / Timeout 在同一棵树里共用一套时间源（历史缺陷：三套口径）。
//
// 判别方式：自累加的钟从 0 起算（每帧 +dt），若节点回落墙钟，其值会远大于逻辑时刻。
func TestTreeTickInjectsLogicalNow(t *testing.T) {
	leaf := &countingLeaf{}
	// 冷却 5s：只在逻辑 1s（首帧）与 7s 各执行一次（见下方帧循环）。
	tree := NewTree(NewCooldown(5*time.Second, leaf))
	bb := NewBlackboard()

	for i := 1; i <= 7; i++ {
		tree.Tick(bb, time.Second)
		got, ok := bb.Get(KeyNow).(time.Time)
		if !ok {
			t.Fatalf("第 %d 帧：Tree.Tick 未注入 %q（实际 %T）", i, KeyNow, bb.Get(KeyNow))
		}
		if want := LogicalTime(float64(i)); !got.Equal(want) {
			t.Fatalf("第 %d 帧注入的逻辑时刻 = %v，期望 %v（必须按 dt 自累加，不许用墙钟）", i, got, want)
		}
	}
	if leaf.n != 2 {
		t.Fatalf("7 帧 × 1s、冷却 5s 应执行 2 次，实际 %d 次（回落墙钟时窗口判定会与 dt 脱节）", leaf.n)
	}
}

// 首次 Tick **之前**已注入 "now" ⇒ 该键归驱动方，Tree.Tick 每帧都不得覆盖它
// （MobManager 在 Spawn 时预置、此后每帧自写，即此做法）。
func TestTreeTickKeepsInjectedLogicalNow(t *testing.T) {
	tree := NewTree(NewActionFn(func(pkgbtree.Blackboard) {}))
	bb := NewBlackboard()
	injected := LogicalTime(100)
	bb.Set(KeyNow, injected)

	for i := 1; i <= 3; i++ {
		// 驱动方每帧自写（同一个逻辑时刻）：Tree 接管的话这里会被自累加钟改写。
		bb.Set(KeyNow, injected)
		tree.Tick(bb, time.Second)
		got, ok := bb.Get(KeyNow).(time.Time)
		if !ok {
			t.Fatalf("第 %d 帧：注入的 %q 被覆盖（实际 %T）", i, KeyNow, bb.Get(KeyNow))
		}
		if !got.Equal(injected) {
			t.Fatalf("第 %d 帧：驱动方注入的逻辑时刻被覆盖：got %v，期望 %v", i, got, injected)
		}
	}
}

// LogicalTime 对非法值必须夹紧，不许让时间倒流 / NaN 参与比较（比较恒 false = 静默失效）。
func TestLogicalTimeClampsNonFinite(t *testing.T) {
	if got := LogicalTime(0); got.UnixNano() != 0 {
		t.Fatalf("LogicalTime(0) 应为纪元，实际 %v", got)
	}
	if got := LogicalTime(2.5); got.UnixNano() != int64(2.5*float64(time.Second)) {
		t.Fatalf("LogicalTime(2.5) 换算错误：%v", got.UnixNano())
	}
	nan := LogicalTime(math.NaN())
	if nan.UnixNano() != 0 {
		t.Fatalf("NaN 应夹紧为 0 时刻，实际 %v", nan.UnixNano())
	}
	inf := LogicalTime(math.Inf(1))
	if inf.UnixNano() != 0 {
		t.Fatalf("+Inf 应夹紧为 0 时刻，实际 %v", inf.UnixNano())
	}
	huge := LogicalTime(math.MaxFloat64)
	if huge.UnixNano() <= 0 {
		t.Fatalf("超大逻辑秒应夹紧为正的时刻（不许溢出成负值=时间倒流），实际 %v", huge.UnixNano())
	}
}
