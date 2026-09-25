// 移动限频器：把「条数」与「位移」两条约束合成一次判定（防加速挂）。
package ratelimit

import "time"

// 移动判定的拒绝原因（空串 = 放行）。业务据此打不同的降频日志：
// rate = 刷包（消息频率超限），dist = 疑似加速挂（窗口内累计位移超限）。
const (
	// MoveRejectNone 放行。
	MoveRejectNone = ""
	// MoveRejectRate 消息频率超限（1 秒内条数超上限）。
	MoveRejectRate = "rate"
	// MoveRejectDist 窗口内累计位移超限（加速挂 / 瞬移）。
	MoveRejectDist = "dist"
)

// MoveGuard 移动限频器：一个玩家（或任何移动体）一个实例。
//
// 「服务端权威位置 + 客户端上行坐标」这一类同步模型，要防的是**两种**互相独立的作弊：
//   - 频率：无限刷位移包（把单步上限合法地反复发送 ⇒ 等效无限速度）；
//   - 位移：降频发包、每包一个大跳（1Hz 发 50 米 —— 条数完全不超，位移严重超标）。
//
// 两条约束必须**同时**存在，缺任一条都能被绕过：引擎现有的限流原语都是单维的
// （TokenBucket/FixedWindow/SlidingWindow 吃整数次数，SlidingSum 吃量）。
//
// ★ 两个阈值都是**业务数值**（客户端上行频率、角色跑速、留多少余量），引擎只给机制：
// 构造函数只做"取正"兜底，不给任何默认值猜测。
//
// 语义（与三处实现对齐，改动必须同步）：
//   - 先查**条数**再查**位移**；两者都超时只报 rate（条数超限更可能是刷包，先修这个）；
//   - 被拒时**位移量不累加**（连续超限不会把窗口越填越满，等窗口滑过即恢复），
//     但条数额度**会**被消耗 —— 与 SlidingWindow 自身语义一致；
//   - dist <= 0 视为"无位移"（站立不动不发包的业务不该被位移约束拦）。
//
// 线程安全：内部两条计数器各自加锁；同一个 MoveGuard 可被并发调用，
// 但**同一个玩家不该并发处理两条移动消息**（顺序语义由业务的消息处理保证）。
type MoveGuard struct {
	rateLimit int
	distLimit float64
	win       time.Duration
	rate      *SlidingWindow
	dist      *SlidingSum
}

// NewMoveGuard 构造移动限频器（默认时间源 time.Now，位移窗口 10 桶）。
// rateLimit = 窗口内允许的消息条数；distLimit = 窗口内允许的累计位移；window = 窗口长度。
// <=0 的入参按 1 / 1 / 1s 兜底（与 SlidingSum 同一套约定）。
func NewMoveGuard(rateLimit int, distLimit float64, window time.Duration) *MoveGuard {
	return NewMoveGuardWithClock(rateLimit, distLimit, window, 10, defaultClock())
}

// NewMoveGuardWithClock 注入自定义时间源与位移窗口桶数（测试用：固定时钟即可精确断言）。
func NewMoveGuardWithClock(rateLimit int, distLimit float64, window time.Duration, bucketCount int, clock Clock) *MoveGuard {
	if rateLimit <= 0 {
		rateLimit = 1
	}
	if distLimit <= 0 {
		distLimit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &MoveGuard{
		rateLimit: rateLimit,
		distLimit: distLimit,
		win:       window,
		rate:      NewSlidingWindowWithClock(rateLimit, window, clock),
		dist:      NewSlidingSumWithClock(distLimit, window, bucketCount, clock),
	}
}

// Allow 判定本次移动（位移为 dist 米）是否放行，返回 (是否放行, 拒绝原因)。
//
// ★ 零值 / 未构造的守卫**放行而不拦截**：宁可漏拦一个坏包，也不能把守门人自己变成故障源。
func (g *MoveGuard) Allow(dist float64) (bool, string) {
	if g == nil || g.rate == nil || g.dist == nil {
		return true, MoveRejectNone
	}
	if !g.rate.Allow() {
		return false, MoveRejectRate
	}
	if !g.dist.Allow(dist) {
		return false, MoveRejectDist
	}
	return true, MoveRejectNone
}

// Reset 清空两条窗口。**必须在这些时机调用**：
//   - 复活 / 重新进图（死亡或断线期间积累的历史位移不该继续占用复活后的额度，
//     否则刚复活就被"上一轮"卡住，玩家感受是"复活了但走不动"）；
//   - 传送 / 换图（瞬移本身不是作弊）。
func (g *MoveGuard) Reset() {
	if g == nil {
		return
	}
	if g.rate != nil {
		g.rate.Reset()
	}
	if g.dist != nil {
		g.dist.Reset()
	}
}

// Remaining 当前窗口还剩余多少条数额度、多少位移额度（排障 / 调参用）。
func (g *MoveGuard) Remaining() (msgs int, dist float64) {
	if g == nil || g.rate == nil || g.dist == nil {
		return 0, 0
	}
	return g.rate.Remaining(), g.dist.Remaining()
}

// Limits 返回构造时生效的三个阈值（条数 / 位移 / 窗口），供日志与面板自述口径。
func (g *MoveGuard) Limits() (rateLimit int, distLimit float64, window time.Duration) {
	if g == nil {
		return 0, 0, 0
	}
	return g.rateLimit, g.distLimit, g.win
}
