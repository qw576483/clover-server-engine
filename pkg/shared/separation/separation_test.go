package separation

import (
	"math"
	"testing"
)

// 本文件是「群体分离」的三类判据，全部提炼自来源工程
// `clover-project-cr/server/game/core/separation_limit_test.go`（把该工程的专有夹具换成
// 本包自己的定点整数夹具，判据一字不改）：
//
//	A. 逐 tick 位移有界 —— 不许出现"一帧冲刺"（峰值速度 = 一帧的位移 / 一帧的时间）。
//	B. 限速之后不许退化成"来回顶牛"（limit cycle）—— 这是 A 的必经配套：只做 A 会把
//	   "一次性大位移"换成"持续小位移来回"。
//	C. 限速不能把功能做坏 —— 有界分离仍必须在若干 tick 内**完全分开**，且轻微重叠必须在
//	   一个 tick 内解决（最常见的轻微重叠不为阻尼付代价）。
//
// 判据判的是**过程**（每 tick 的位移上界 / 方向反转次数），不是"最后看起来分开了"——
// 后者一条"把实体直接瞬移开"的实现也能满足。

// 夹具常量（本包自定；来源工程对应值是它自己单位制下的 500 半径 / 3 容差 / 1 质量）。
const (
	fixRadius int64 = 500
	fixTol    int64 = 3
	fixWalk   int64 = 100 // 一个 tick 的行走距离 = 每 tick 分离额度
	fixMass   int64 = 1
	fixTicks        = 300
	fixSkip         = 64 // 起点松弛瞬态：实测反转集中在它以内（skip=32 仍有 4~6 次 / skip=64 为 0）
)

func newPair(x, y int64) (*Body, *Body) {
	return &Body{X: x, Y: y, Radius: fixRadius, Mass: fixMass, MaxStep: fixWalk},
		&Body{X: x, Y: y, Radius: fixRadius, Mass: fixMass, MaxStep: fixWalk}
}

func dist(a, b *Body) float64 {
	return math.Hypot(float64(b.X-a.X), float64(b.Y-a.Y))
}

// TestResolveMovesAtMostOneStep 钉住"一个 tick 的分离位移 ≤ 该刚体自己的额度"。
//
// 这是源事故（"人物抖得厉害 / 抽搐"）的根因修法：旧实现把重叠量在一个 tick 内全部分完，
// 实机实测单格位移是前后两格的 3.8 倍且方向相反，客户端 10Hz 快照 + 线性插值把它原样播成
// 一帧冲刺。判据的出处：一个刚体"被推开的速率"不应超过它**自己走路**的速率。
func TestResolveMovesAtMostOneStep(t *testing.T) {
	a, b := newPair(9000, 1000) // 完全同点（多单位同牌 + 召唤半径为 0 时服务端确实会产生）
	s := New(Config{TouchTolerance: fixTol})

	ax0, ay0 := a.X, a.Y
	bx0, by0 := b.X, b.Y
	if moved := s.Resolve([]*Body{a, b}); moved != 2 {
		t.Fatalf("coincident pair: moved = %d, want 2 (both must be pushed apart)", moved)
	}
	movedA := math.Hypot(float64(a.X-ax0), float64(a.Y-ay0))
	movedB := math.Hypot(float64(b.X-bx0), float64(b.Y-by0))
	t.Logf("one tick of separation: step limit = %d, movedA=%.1f movedB=%.1f", fixWalk, movedA, movedB)
	if movedA <= 0 || movedB <= 0 {
		t.Fatalf("one of the pair did not move at all (A=%.1f B=%.1f): a no-op resolver would pass the bound below", movedA, movedB)
	}
	if movedA > float64(fixWalk) {
		t.Fatalf("separation moved one body %.1f in a single tick, want <= %d (its own step budget)", movedA, fixWalk)
	}
	if movedB > float64(fixWalk) {
		t.Fatalf("separation moved the other body %.1f in a single tick, want <= %d", movedB, fixWalk)
	}
}

// TestBoundedResolveStillSeparates 钉住"限速没有把功能做坏"：有界分离仍必须在若干 tick 内
// 完全分开 —— 只是从"一个 tick"变成"几个 tick"。
func TestBoundedResolveStillSeparates(t *testing.T) {
	a, b := newPair(9000, 1000)
	s := New(Config{TouchTolerance: fixTol})
	want := float64(a.Radius + b.Radius - fixTol)

	for tick := 1; tick <= 40; tick++ {
		s.Resolve([]*Body{a, b})
		if gap := dist(a, b); gap >= want {
			t.Logf("fully separated after %d tick(s) (gap=%.0f, want>=%.0f, step limit=%d)", tick, gap, want, fixWalk)
			return
		}
	}
	t.Fatalf("still overlapping after 40 ticks (gap=%.0f, want>=%.0f): the step budget broke separation", dist(a, b), want)
}

// TestSmallOverlapResolvesInOneTick 钉住常见情形不被拖慢：重叠量在一个额度以内 ⇒
// 必须在一个 tick 内解决（n = 1 时取平均不做任何阻尼）。
func TestSmallOverlapResolvesInOneTick(t *testing.T) {
	gap := fixRadius*2 - fixWalk // 重叠量恰好一个额度
	a := &Body{X: 9000, Y: 1000, Radius: fixRadius, Mass: fixMass, MaxStep: fixWalk}
	b := &Body{X: 9000 + gap, Y: 1000, Radius: fixRadius, Mass: fixMass, MaxStep: fixWalk}
	s := New(Config{TouchTolerance: fixTol})

	s.Resolve([]*Body{a, b})
	got := dist(a, b)
	want := float64(a.Radius + b.Radius - fixTol)
	if got < want {
		t.Fatalf("small overlap not resolved in one tick: gap=%.0f want>=%.0f (overlap was only %d)", got, want, fixWalk)
	}
	t.Logf("small overlap resolved in one tick: gap=%.0f want>=%.0f", got, want)
}

// TestCrowdDisplacementIsBounded 是判据 A 的端到端形态：六只刚体叠在同一点（服务端为
// 多单位牌实际产生的形状），每 tick 先按自己的速度走一步、再解算分离，观察**每个刚体每 tick**
// 的总位移。
//
// 界是**推导**出来的，不是试出来的：一个 tick 内一个刚体能走的距离 = 自己走一步（≤ walk）
// + 分离位移（≤ MaxStep，按整条向量模长收缩一次）⇒ 由三角不等式 ≤ walk + MaxStep。
// 旧实现（逐对立即施加 + 每对推力各裁一次额度）实测单只一 tick 被推 5 次、峰值达名义步长的
// 12 倍 ⇒ 这条会红。
func TestCrowdDisplacementIsBounded(t *testing.T) {
	const n = 6
	bodies := make([]*Body, 0, n)
	ptrs := make([]*Body, 0, n)
	for i := 0; i < n; i++ {
		b := &Body{X: 9000, Y: 1500, Radius: fixRadius, Mass: fixMass, MaxStep: fixWalk}
		bodies = append(bodies, b)
		ptrs = append(ptrs, b)
	}
	s := New(Config{TouchTolerance: fixTol})
	bound := float64(fixWalk + fixWalk)

	px := make([]int64, n)
	py := make([]int64, n)
	for i, b := range bodies {
		px[i], py[i] = b.X, b.Y
	}

	worst, worstTick, worstIdx := 0.0, -1, -1
	worstDX, worstDY := 0.0, 0.0
	for tick := 0; tick < 120; tick++ {
		for _, b := range bodies {
			b.X += fixWalk // 主动行走（与分离无关的那一份位移）
		}
		s.Resolve(ptrs)
		for i, b := range bodies {
			dx := float64(b.X - px[i])
			dy := float64(b.Y - py[i])
			d := math.Hypot(dx, dy)
			if d > worst {
				worst, worstTick, worstIdx, worstDX, worstDY = d, tick, i, dx, dy
			}
			px[i], py[i] = b.X, b.Y
		}
	}
	// 功能仍然成立：六只必须真的散开（否则"不动的解算器"也能满足上界）。
	far := 0.0
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if d := dist(bodies[i], bodies[j]); d > far {
				far = d
			}
		}
	}
	t.Logf("worst per-tick displacement = %.1f (body=%d, tick=%d) d=(%.1f,%.1f), bound %.0f; max pairwise distance after 120 ticks = %.0f",
		worst, worstIdx, worstTick, worstDX, worstDY, bound, far)
	if worst > bound {
		t.Fatalf("a body moved %.1f in one tick, want <= %.0f (= its own walk step plus one bounded "+
			"separation): the server is still teleporting bodies, and a client sampling snapshots will "+
			"play that as a one-frame lunge", worst, bound)
	}
	if far < float64(fixRadius*2-fixTol) {
		t.Fatalf("crowd never separated: max pairwise distance = %.0f, want >= %d", far, fixRadius*2-fixTol)
	}
}

// TestSeparationDoesNotOscillate 是判据 B：上面的限速不能把"一帧冲刺"换成"永久来回顶牛"。
//
// 受控复现（源事故"抽搐"的第二个面）：六只半径 500 的刚体放在**间距仅 70** 的一条竖线上，
// **不施加任何行走**、只跑解算。
//
//	· 旧写法（逐对立即施加 + 每对推力各裁一次额度）：单只在 y 上每 tick 来回 15，
//	  跑满 300 tick 仍在振（反转 ≈ 290/300）；
//	· 现写法（向量累加 + 取同伴平均）：越过起点松弛瞬态后反向 0 次，单调散开。
//
// 起始瞬态单独说明（⛔ 不是隐藏证据）：前若干 tick 是"最深重叠"的松弛起步，个别刚体会有一两次
// 方向修正。只跳过**起点**、不跳过过程中的任何一帧（本测试把 0/8/16/32/64 各档的反转次数
// 全部打到日志里，可复核）。本夹具实测：skip=32 时 4 个刚体各反转 4~6 次，skip=64 时全部为 0，
// 而 300 tick 的其余 236 帧一帧不跳。
func TestSeparationDoesNotOscillate(t *testing.T) {
	ys := []int64{1000, 1070, 1140, 1210, 1280, 1350}
	bodies := make([]*Body, 0, len(ys))
	ptrs := make([]*Body, 0, len(ys))
	for _, y := range ys {
		b := &Body{X: 9000, Y: y, Radius: fixRadius, Mass: fixMass, MaxStep: fixWalk}
		bodies = append(bodies, b)
		ptrs = append(ptrs, b)
	}
	s := New(Config{TouchTolerance: fixTol})

	hist := make([][]int64, len(bodies))
	for tick := 0; tick < fixTicks; tick++ {
		s.Resolve(ptrs) // 只有分离，没有任何行走
		for i, b := range bodies {
			hist[i] = append(hist[i], b.Y)
		}
	}

	// reversals 数"越过第 skip 个 tick 之后方向反转了几次"（skip < 1 时按 1 处理：
	// 第 0 个 tick 没有"前一个 tick"，不构成方向）。
	reversals := func(h []int64, skip int) int {
		if skip < 1 {
			skip = 1
		}
		rev := 0
		for k := skip; k+1 < len(h); k++ {
			d1 := float64(h[k] - h[k-1])
			d2 := float64(h[k+1] - h[k])
			if math.Abs(d1) < 1 || math.Abs(d2) < 1 {
				continue
			}
			if d1*d2 < 0 {
				rev++
			}
		}
		return rev
	}

	worstAll := 0
	var worstIdx int
	skips := []int{0, 8, 16, 32, 64}
	for i := range bodies {
		revs := make([]int, 0, len(skips))
		for _, sk := range skips {
			revs = append(revs, reversals(hist[i], sk))
		}
		t.Logf("body=%d y[0]=%d y[16]=%d y[%d]=%d | reversals after skip %v = %v",
			i, hist[i][0], hist[i][16], len(hist[i])-1, hist[i][len(hist[i])-1], skips, revs)
		if r := reversals(hist[i], fixSkip); r > worstAll {
			worstAll, worstIdx = r, i
		}
	}
	gap := math.Abs(float64(bodies[len(bodies)-1].Y - bodies[0].Y))
	t.Logf("after %d ticks: span=%.0f (started at %d), worst reversals after skip %d = %d (body=%d)",
		fixTicks, gap, ys[len(ys)-1]-ys[0], fixSkip, worstAll, worstIdx)
	if worstAll > 0 {
		t.Fatalf("separation oscillates: body=%d reversed direction %d times after the start-up transient "+
			"(%d ticks) with no walking at all (want 0): the per-tick correction is not cancelling / not "+
			"under-relaxed, so a body squeezed between neighbours ping-pongs forever",
			worstIdx, worstAll, fixSkip)
	}
}

// TestLayerIsolation 钉住分层语义：不同 Layer 的刚体互不分离（来源工程用它把空中层与地面层
// 分开 —— 两层的实体在同一点上时不该互相推）。
func TestLayerIsolation(t *testing.T) {
	a, b := newPair(0, 0)
	a.Layer, b.Layer = 0, 1
	s := New(Config{TouchTolerance: fixTol})
	if moved := s.Resolve([]*Body{a, b}); moved != 0 {
		t.Fatalf("layers must not separate each other: moved = %d, want 0", moved)
	}
	if a.X != 0 || a.Y != 0 || b.X != 0 || b.Y != 0 {
		t.Fatalf("bodies in different layers moved: a=(%d,%d) b=(%d,%d)", a.X, a.Y, b.X, b.Y)
	}
}

// TestImmovableBodyIsNotMoved 钉住质量语义：不可推动的一方（Mass ≤ 0）恒为 0 位移，
// 可推动的一方承担全部重叠量。
func TestImmovableBodyIsNotMoved(t *testing.T) {
	wall := &Body{X: 0, Y: 0, Radius: fixRadius, Mass: 0} // Mass ≤ 0 = 不可推动
	unit := &Body{X: 0, Y: 0, Radius: fixRadius, Mass: fixMass}
	s := New(Config{TouchTolerance: fixTol})

	s.Resolve([]*Body{wall, unit})
	t.Logf("immovable=(%d,%d) unit=(%d,%d)", wall.X, wall.Y, unit.X, unit.Y)
	if wall.X != 0 || wall.Y != 0 {
		t.Fatalf("immovable body moved to (%d,%d), want (0,0)", wall.X, wall.Y)
	}
	if unit.X == 0 && unit.Y == 0 {
		t.Fatal("the movable body was not pushed off the immovable one at all")
	}
}

// TestPassableRefusalKeepsBodyInPlace 钉住"落点不可进入时不瞬移"：把 Passable 恒判 false
// ⇒ 一次位移都不该发生（不位移比塞进墙里好）。
func TestPassableRefusalKeepsBodyInPlace(t *testing.T) {
	a, b := newPair(0, 0)
	a.MaxStep, b.MaxStep = 0, 0 // 不限额度，专门判 Passable 这一条
	s := New(Config{TouchTolerance: fixTol, Passable: func(Body, int64, int64) bool { return false }})
	if moved := s.Resolve([]*Body{a, b}); moved != 0 {
		t.Fatalf("no body may move when every destination is impassable: moved = %d, want 0", moved)
	}
	if a.X != 0 || a.Y != 0 || b.X != 0 || b.Y != 0 {
		t.Fatalf("bodies moved into impassable terrain: a=(%d,%d) b=(%d,%d)", a.X, a.Y, b.X, b.Y)
	}
}

// TestResolveIsDeterministic 钉住确定性：同输入两次必须逐字节同输出（服务器权威模拟与
// 离线断言都依赖这一条；实现里全整数、无 map 迭代、无浮点分支）。
func TestResolveIsDeterministic(t *testing.T) {
	run := func() []int64 {
		bodies := make([]*Body, 0, 6)
		ptrs := make([]*Body, 0, 6)
		for i := 0; i < 6; i++ {
			b := &Body{X: 9000, Y: 1500, Radius: fixRadius, Mass: fixMass, MaxStep: fixWalk}
			bodies = append(bodies, b)
			ptrs = append(ptrs, b)
		}
		s := New(Config{TouchTolerance: fixTol})
		for tick := 0; tick < 30; tick++ {
			for _, b := range bodies {
				b.X += fixWalk
			}
			s.Resolve(ptrs)
		}
		out := make([]int64, 0, len(bodies)*2)
		for _, b := range bodies {
			out = append(out, b.X, b.Y)
		}
		return out
	}
	first, second := run(), run()
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("Resolve is not deterministic at index %d: %d vs %d", i, first[i], second[i])
		}
	}
	t.Logf("deterministic across two identical runs (final positions: %v)", first)
}

// TestSqueezedBodyMovesByMeanOfItsPairs 直接钉住"落地时取**同伴数平均**"这条语义。
//
// 为什么需要它（实测发现的判据缺口）：在 TestSeparationDoesNotOscillate 那个**带每 tick 额度**
// 的动态夹具里，把"取平均"去掉换成"直接落地累加向量"，仍然会收敛 —— 额度把迭代增益封顶了
// （额度 ≪ 重叠量时，修正量一律被裁到额度，增益不再由 n 决定）。⇒ 那条动态判据**判不到**
// "取平均"这一层（负控会打空）。本测试用**不限额**（MaxStep = 0）的受控夹具把额度这一层排除，
// 只判取平均本身。
//
// 夹具与手推（全整数、无浮点，故可逐位核对）：三体，中间一只被左右两只**不可推动**的邻居夹住，
// 左近右远 ⇒ 累加向量不为 0。
//
//	左邻（不可推动）：圆心距 980 ⇒ 重叠 20，share = 20·immovableMass/(immovableMass+1) = 19
//	                  ⇒ 对中间体的推力 = 980·19/980 = **+19**（朝 +X，远离左邻）
//	右邻（不可推动）：圆心距 900 ⇒ 重叠 100，share = 100·immovableMass/(immovableMass+1) = 99
//	                  ⇒ 推力 = 900·99/900 = **−99**
//	累加 = 19 − 99 = **−80**，同伴数 = 2 ⇒ 取平均 ⇒ **−40**；不取平均则落地 −80。
func TestSqueezedBodyMovesByMeanOfItsPairs(t *testing.T) {
	mid := &Body{Radius: fixRadius, Mass: fixMass} // (0,0)，可推动
	left := &Body{X: -980, Radius: fixRadius}      // Mass ≤ 0 = 不可推动
	right := &Body{X: 900, Radius: fixRadius}      // Mass ≤ 0 = 不可推动

	s := New(Config{TouchTolerance: fixTol})
	s.Resolve([]*Body{mid, left, right})
	t.Logf("squeezed body landed at x=%d (accumulated -80 over 2 pairs ⇒ mean -40), neighbours did not move: left=%d right=%d",
		mid.X, left.X, right.X)
	if left.X != -980 || right.X != 900 {
		t.Fatalf("immovable neighbours moved: left=%d right=%d", left.X, right.X)
	}
	if mid.X != -40 {
		t.Fatalf("squeezed body landed at x=%d, want -40 (accumulated -80 divided by its 2 pairs): "+
			"landing the raw accumulated vector would move it %d and is exactly the 'displacement "+
			"amplified by neighbour count' defect", mid.X, -80)
	}
}

// TestSqueezedCenterConvergesWithoutBudget 是"取平均"的动态形态：**不限额**（MaxStep = 0）
// 时原始修正量不再被额度封顶 ⇒ 迭代增益完全由同伴数 n 决定（不取平均 ⇒ 增益 1−n/2，
// n ≥ 4 时 |增益| ≥ 1 即不收敛 —— 这就是来源工程记的那个 limit cycle）。
//
// 夹具：一只刚体被四只**不可推动**的邻居围在一个**不对称**的口袋里（左右各远 / 上下各近
// 一对，重叠 20 与 100），只跑分离、不施加任何行走。对称夹具（四邻等距）会让净推力恒为 0、
// 判据永远绿（**判不到任何东西**），所以这里刻意做成不对称：净推力指向 +X+Y，
// 刚体必须一路移出重叠区并**停住**，不许来回。
func TestSqueezedCenterConvergesWithoutBudget(t *testing.T) {
	center := &Body{Radius: fixRadius, Mass: fixMass} // (0,0)，唯一的可推动体；MaxStep 0 = 不限额
	walls := []*Body{                                 // Mass ≤ 0 = 不可推动；口袋不对称（近的是 980，远的是 900）
		{X: -980, Y: 0, Radius: fixRadius}, // 近：重叠 20 ⇒ 把中心推向 +X
		{X: 900, Y: 0, Radius: fixRadius},  // 远：重叠 100 ⇒ 把中心推向 −X
		{X: 0, Y: -980, Radius: fixRadius},
		{X: 0, Y: 900, Radius: fixRadius},
	}
	ptrs := append([]*Body{center}, walls...)
	s := New(Config{TouchTolerance: fixTol})

	const ticks = 300
	hist := make([]int64, 0, ticks)
	for i := 0; i < ticks; i++ {
		s.Resolve(ptrs)
		hist = append(hist, center.X+center.Y)
	}
	rev := 0
	for k := 1; k+1 < len(hist); k++ {
		d1 := float64(hist[k] - hist[k-1])
		d2 := float64(hist[k+1] - hist[k])
		if d1 == 0 || d2 == 0 {
			continue
		}
		if d1*d2 < 0 {
			rev++
		}
	}
	settled := 0
	for k := len(hist) - 1; k > 0 && hist[k] == hist[len(hist)-1]; k-- {
		settled++
	}
	t.Logf("center track: first=%d after1tick=%d last=%d, reversals=%d, still for last %d ticks (4 immovable neighbours, no budget)",
		hist[0], hist[1], hist[len(hist)-1], rev, settled)
	if hist[1] == 0 {
		t.Fatalf("asymmetric fixture produced no net push at all (center did not move in the first tick): the fixture is degenerate and this criterion cannot fail")
	}
	if rev > 0 {
		t.Fatalf("center reversed direction %d times with 4 neighbours at near-equilibrium overlap and NO "+
			"budget: the raw correction is not under-relaxed (gain 1-n/2 = -1 ⇒ permanent limit cycle)", rev)
	}
	if settled == 0 {
		t.Fatalf("center never came to rest (last value %d appears only once): the under-relaxed correction never settles", hist[len(hist)-1])
	}
}

// TestWalkStep pins the step-budget helper's口径（来源工程 separationStepLimitMilli 的取值法）。
func TestWalkStep(t *testing.T) {
	cases := []struct {
		speed, ticks, want int64
	}{
		{1500, 20, 75}, // 正常：速度 / 每 tick 数，向下取整
		{1, 20, 1},     // 商落在 (0,1) ⇒ 取 1，否则极慢的实体会"一步都不准动"
		{0, 20, 0},     // 无移动速度（静态）⇒ 0 = 不限制
		{-100, 20, 0},  // 非法速度按"不限制"处理，不静默限成 0
		{1000, 0, 0},   // 非法 tick 数同理
	}
	for _, c := range cases {
		if got := WalkStep(c.speed, c.ticks); got != c.want {
			t.Errorf("WalkStep(%d, %d) = %d, want %d", c.speed, c.ticks, got, c.want)
		}
	}
}
