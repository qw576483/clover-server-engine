// Package separation 提供通用的 2D 圆形群体分离解算（定点整数、确定性、逐 tick 位移上限）。
//
// # 它解决什么
//
// 一批圆形刚体（有半径的实体）挤在一起时，需要把它们推开到"互不相交"。朴素写法有两种
// 都会出事：
//
//   - **一次性把重叠量推完** ⇒ 被推开的**速率**远超实体自己移动的速率。客户端按固定
//     频率接收快照并做线性插值，那一格会被原样播成一帧冲刺（源事故的正面）。
//   - **逐对立即施加** ⇒ 被夹在两侧同伴中间的实体会先被推向一侧、再被推向另一侧；
//     两次位移不抵消（再叠加"每对推力各裁一次额度"会把反向的那份裁掉），形成永久振荡
//     （源事故的第二个面，用户报"抖得厉害 / 抽搐"）。
//
// 本包给出第三种写法 —— 即来源工程在真实事故上收敛出来的形状，三步：
//
//  1. 一个 tick 内先把**所有**重叠对的分离位移按**向量累加**到各刚体头上（accumulate），
//     全过程不移动任何东西；
//  2. 落地时按**参与的同伴数取平均**做欠松弛（apply）。理由（可推导）：一只刚体被 n 个
//     同伴贴住时，它这一 tick 拿到的分离位移 ≈ Σ(overlap_i/2)，而
//     overlap_i = limit − |x − x_i| ⇒ ∂(位移)/∂x = −n/2 ⇒ 离散迭代的误差增益 = 1 − n/2，
//     n ≥ 4 时 |增益| ≥ 1 = **不收敛**；除以 n 后增益恒为 1 − 1/2 = 0.5，与 n 无关。
//     且 n = 1（最常见的轻微重叠）时仍是完整修正、一个 tick 解决，不为阻尼付代价。
//  3. 落地前把整条位移向量按**模长**收缩到该刚体的每 tick 额度（Body.MaxStep）。
//     额度按"整个 tick 的位移向量"计**一次**，**不是**每对推力各裁一次 —— 后者正是上面
//     "被夹住时反向那份被裁掉"的振荡来源。
//
// 收敛由**逐 tick 的松弛**负责（每 tick 一轮），tick 内不再迭代（历史写法是"固定 N 轮、
// 每轮逐对立即施加"，已验证会 produce 永久 limit cycle，已废弃）。
//
// # 来源与事故
//
// 提炼自来源工程 `clover-project-cr` 的服务端定点战斗内核（只读提炼，语义逐行对齐）：
//
//   - `server/game/core/combat.go:629` accumulateSeparation（向量累加、按质量分摊）
//   - `server/game/core/combat.go:677` applySeparation（取同伴平均 + 按模长收缩额度）
//   - `server/game/core/combat.go:724` separationStepLimitMilli（每 tick 位移额度）
//   - `server/game/core/combat.go:736` shiftEntity（落地位移；落点不可进入时退化为单轴）
//   - `server/game/core/battle.go:646` resolveCollisions（一个 tick 一轮的驱动形状）
//
// ⚠️ 行号取自 **2026-09-24 快照**；来源工程的 `combat.go` / `battle.go` 当日仍有改动
// （mtime 分别是 12:42 / 12:26），**行号会漂 ⇒ 定位以函数名为准**。
//
// 事故（来源工程 2026-09-24 逐帧实机取证，`.ai-tmp/test/D134-units.tsv`）：旧写法把某只
// 实体在**一个 tick 内**移动了 0.573 格，而它前后两格各约 0.15 格 ⇒ 单格快 3.8 倍、方向
// 还相反；客户端 10Hz 快照 + 线性插值把它原样播成一帧冲刺。离线仿真还证伪了"改客户端插值"
// 这条路（任何穿过快照点的插值都躲不开这段位移）⇒ 唯一无滞后的修法就是让源头的**逐 tick
// 位移**本身有界。取值的出处：实体**自己走路**一个 tick 的距离（本包的 `WalkStep`），
// 物理上自洽 —— "被推开的速率不超过主动移动的速率"。
//
// # 边界（什么不在这里）
//
//   - **不是 MMO 空间能力**：与 `pkg/domain/mmo/collide` 无 import 关系。本包只有"圆形群体
//     分离"，不做格子查询、不做宽相、不做射线、不做寻路。
//   - **不是浮点三维几何**：坐标 / 半径 / 额度一律是调用方自定的**定点整数**（来源工程用
//     1/1000 格）。全整数运算 ⇒ 同输入同输出，可直接进服务器权威模拟与离线断言，不引入
//     浮点在不同平台上的不确定。`pkg/shared/geom` 只收 float64 三维量（其 README 第 1、14
//     条），`collide` 只收 float64 米制二维原语，故本包独立存在。
//   - **不带生命周期、不带状态**：`Solver` 只持有可复用的临时缓冲，不持全局单例、不起
//     goroutine、不读时钟。
//   - **不做玩法判断**：半径多大、谁不可推动、每 tick 额度多少、地形能不能进，全部由调用方
//     经 `Body` 与 `Config.Passable` 给。
//
// # 复杂度
//
// `Resolve` 是 O(n²) 全对枚举（与来源工程同形状）。它是给"一屏内的近战拥挤"用的，**不含**
// 宽相；数量级上去时应由调用方先做空间划分再分组调用。
package separation

import "math"

// immovableMass 是"不可推动"刚体的等效质量。
//
// 出处：来源工程 `server/game/core/combat.go:24`（`immovableMass = 1 << 20`）——静态障碍
// 与带 ignore-pushback 标记的实体都用它，因此"一推不动"在质量分摊公式里自然成立，不需要
// 额外的 if 分支去特判方向。
const immovableMass int64 = 1 << 20

// Body 是一次群体分离解算中的圆形刚体，坐标为调用方的定点整数单位。
//
// 调用方通常把它**内嵌**进自己的实体结构，每 tick 把要参与解算的实体指针收进一个切片交给
// `Solver.Resolve`（切片元素为 nil 表示不参与）。
type Body struct {
	// X, Y 是圆心坐标（调用方自己的定点单位）。
	X, Y int64
	// Radius 是碰撞半径（同一单位）；双方半径之和 ≤ 0 的对直接跳过。
	Radius int64
	// Mass 是碰撞质量：> 0 = 可推动（来源工程对 < 1 也按 1 处理），≤ 0 = **不可推动**
	// （静态障碍、塔、带 ignore-pushback 标记的实体）。质量只用于分摊重叠量：
	// 一方越重，被推开的那一方承担越多；不可推动的一方恒为 0 位移。
	Mass int64
	// Layer 是分层标识：只有 Layer 相同的两个刚体才会互相分离（来源工程用它把空中层与
	// 地面层分开）。具体哪个值代表哪一层是调用方的语义，本包只比较相等。
	Layer int32
	// MaxStep 是该刚体**本 tick** 允许的分离位移上限（按整条位移向量的模长收缩），
	// 0 = 不限制。推荐值 = 它自己走路一个 tick 的距离，见 `WalkStep`。
	MaxStep int64
}

// Config 是 `Solver` 的构造参数。
type Config struct {
	// TouchTolerance 是忽略的重叠量：重叠 ≤ 它的对不产生分离（避免"只是挨着"时互推抖动）。
	// 0 = 不留容差。来源工程取 3（= 其参考实现的 60 subtiles）；本包**不设默认值**，
	// 不把某个项目的容差带进别的项目。
	TouchTolerance int64
	// Passable 判定"实体 b 能否移动到 (x, y)"，nil = 总是可以。
	//
	// 它存在的意义：分离位移落地时若把实体塞进不可进入的区域（墙 / 河 / 阻挡格），
	// 比"不分开"更糟。本包按来源工程的退化顺序处理：先试 (x+dx, y+dy)，不行退化为只走
	// 一轴 (x+dx, y)，再不行退化为 (x, y+dy)，都不行则**本次不位移**。
	Passable func(b Body, x, y int64) bool
}

// Solver 是可复用的解算工作区。它只持有临时缓冲（每次 Resolve 复用，避免每 tick 分配），
// 因此**不是**并发安全的：一个 goroutine 一个 Solver。
type Solver struct {
	cfg Config
	dx  []int64 // 每个刚体本 tick 累加的位移
	dy  []int64
	nbr []int32 // 每个刚体本 tick 参与了几个重叠对
}

// New 构造解算器。
func New(cfg Config) *Solver { return &Solver{cfg: cfg} }

// Resolve 解算一轮：把 bodies 里所有重叠对推开，**原地**更新各 Body 的 X / Y。
//
// 返回本 tick 真的发生了位移的刚体数。
//
// 同点重叠（两圆心完全重合、没有分离方向）按**切片下标**定方向：下标小的向 −X 推
// （来源工程按实体 ID 定，同样是"约定任意、但确定"——确定性才是判据）。
// 不同 Layer、Radius 和为 0、重叠 ≤ TouchTolerance 的对都不产生位移。
func (s *Solver) Resolve(bodies []*Body) int {
	n := len(bodies)
	s.ensure(n)
	d := s.dx[:n]
	e := s.dy[:n]
	cnt := s.nbr[:n]
	for i := range d {
		d[i], e[i], cnt[i] = 0, 0, 0
	}

	for i := 0; i < n; i++ {
		a := bodies[i]
		if a == nil {
			continue
		}
		for j := i + 1; j < n; j++ {
			b := bodies[j]
			if b == nil {
				continue
			}
			// 不同层互不分离（来源工程把空中层与地面层分开）。
			if a.Layer != b.Layer {
				continue
			}
			// 方向约定：accumulate 假定入参顺序就是调用方的稳定顺序（这里 i < j）,
			// 同点重叠时把第一个向 −X 推。
			if s.accumulate(a, b, &d[i], &e[i], &d[j], &e[j]) {
				cnt[i]++
				cnt[j]++
			}
		}
	}

	moved := 0
	for i := 0; i < n; i++ {
		if bodies[i] == nil {
			continue
		}
		if s.apply(bodies[i], d[i], e[i], int(cnt[i])) {
			moved++
		}
	}
	return moved
}

func (s *Solver) ensure(n int) {
	if cap(s.dx) >= n {
		s.dx = s.dx[:n]
		s.dy = s.dy[:n]
		s.nbr = s.nbr[:n]
		return
	}
	s.dx = make([]int64, n)
	s.dy = make([]int64, n)
	s.nbr = make([]int32, n)
}

// accumulate 把"把 b1 / b2 推开"所需的位移累加进 d1 / d2，**不移动任何东西**。
// 返回是否产生了位移（= 这一对是否算作 b1 / b2 的"同伴"）。
//
// 质量分摊：share1 = overlap · m2 / (m1 + m2)，share2 = overlap − share1（先算 share1 再相减，
// 保证 share1 + share2 == overlap 不丢整数量）；不可推动的一方（Mass ≤ 0）不位移。
// 出处：来源工程 `server/game/core/combat.go:629`。
func (s *Solver) accumulate(b1, b2 *Body, d1x, d1y, d2x, d2y *int64) bool {
	limit := b1.Radius + b2.Radius
	if limit <= 0 {
		return false
	}
	dx := b2.X - b1.X
	dy := b2.Y - b1.Y
	gap := isqrt(dx*dx + dy*dy)
	overlap := limit - gap
	if overlap <= s.cfg.TouchTolerance {
		return false
	}
	if gap == 0 {
		// 同点：没有分离方向，按调用方给的稳定顺序（下标小的向 −X 推）取一个。
		// 约定任意，但**确定**——确定性才是这里要的。
		dx, dy, gap = limit, 0, limit
	}
	m1, m2 := effMass(b1), effMass(b2)
	total := m1 + m2
	if total >= 2*immovableMass {
		return false // 两个都不可推动，无事可做
	}
	share1 := overlap * m2 / total
	share2 := overlap - share1
	if b1.Mass > 0 && share1 != 0 {
		*d1x -= dx * share1 / gap
		*d1y -= dy * share1 / gap
	}
	if b2.Mass > 0 && share2 != 0 {
		*d2x += dx * share2 / gap
		*d2y += dy * share2 / gap
	}
	return true
}

// apply 取"整条累加位移 / 同伴数"（欠松弛，推导见包文档第 2 条），按**向量模长**收缩到
// b.MaxStep，然后落地位移。返回是否真的动了。
//
// 额度按整条向量收缩一次，⛔ 不是"每对推力各裁一次"：后者在被夹住时会把反向的那一份裁掉，
// 正是源事故的振荡来源（出处：来源工程 `server/game/core/combat.go:677` 的注释）。
func (s *Solver) apply(b *Body, dxv, dyv int64, neighbours int) bool {
	if neighbours > 1 {
		dxv /= int64(neighbours)
		dyv /= int64(neighbours)
	}
	if dxv == 0 && dyv == 0 {
		return false
	}
	if b.MaxStep > 0 {
		mag := isqrt(dxv*dxv + dyv*dyv)
		if mag > b.MaxStep {
			dxv = dxv * b.MaxStep / mag
			dyv = dyv * b.MaxStep / mag
		}
	}
	return s.shift(b, dxv, dyv)
}

// shift 位移一步；落点不可进入时按"整体 → 只 X → 只 Y"退化，全不行则不位移。
// 出处：来源工程 `server/game/core/combat.go:736`（shiftEntity；行号见包文档的漂移说明）。
func (s *Solver) shift(b *Body, dx, dy int64) bool {
	if dx == 0 && dy == 0 {
		return false
	}
	if s.passable(b, b.X+dx, b.Y+dy) {
		b.X, b.Y = b.X+dx, b.Y+dy
		return true
	}
	if s.passable(b, b.X+dx, b.Y) {
		b.X += dx
		return true
	}
	if s.passable(b, b.X, b.Y+dy) {
		b.Y += dy
		return true
	}
	return false
}

func (s *Solver) passable(b *Body, x, y int64) bool {
	if s.cfg.Passable == nil {
		return true
	}
	return s.cfg.Passable(*b, x, y)
}

// effMass 返回用于分摊的等效质量：可推动体用自身质量，不可推动体用 immovableMass。
// 出处：来源工程 `server/game/core/combat.go:601`（effectiveMass）。
func effMass(b *Body) int64 {
	if b.Mass > 0 {
		return b.Mass
	}
	return immovableMass
}

// WalkStep 返回"该实体一个 tick 沿路线能前进的距离" —— 即每 tick 分离额度的推荐取值。
//
// 取值口径（与来源工程 `separationStepLimitMilli` / `units.go:SpeedMilliPerSec` 同式）：
// 速度是「定点单位 / 秒」，一个 tick 前进 speed / ticksPerSecond（**向下取整**）；
// 商落在 (0, 1) 时取 1，否则极慢的实体会被限成"一步都不准动"；商 ≤ 0 说明这个实体没有
// 移动速度（静态），返回 0 = **不限制**（它仍应被即时推开，只是不会被自己走路的额度约束）。
func WalkStep(speedPerSec, ticksPerSecond int64) int64 {
	if speedPerSec <= 0 || ticksPerSecond <= 0 {
		return 0
	}
	if step := speedPerSec / ticksPerSecond; step > 0 {
		return step
	}
	return 1
}

// isqrt 是精确整数平方根（种子取自 IEEE-754 开方，再做至多一步修正）。
//
// 相对来源工程 `server/game/core/units.go:417` 的 isqrt64 有两处收紧：
//   - 种子用 `math.Sqrt` 的**向下取整**，修正循环因此只需"向下纠正 + 向上补齐"两步；
//   - 上界用 3037000499（= ⌊√(2^63−1)⌋），避免 `(r+1)*(r+1)` 在 int64 上溢出。
func isqrt(v int64) int64 {
	if v <= 0 {
		return 0
	}
	r := int64(math.Sqrt(float64(v)))
	for r > 0 && r*r > v {
		r--
	}
	for r < 3037000499 && (r+1)*(r+1) <= v {
		r++
	}
	return r
}
