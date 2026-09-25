package state

import (
	"strings"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/runtime/ratelimit"
)

// 撞库防护参数（行业默认实践：按「账号」与「账号 + 来源」双维度计数，连续失败指数退避）。
//
// 为什么不是一个维度：
//   - **只按账号**：攻击者用一批账号名各试几次，任何单个账号都涨不到阈值，
//     防护形同不存在；反过来又容易被「一个来源恶意刷别人的账号」触发（锁人不锁来源）。
//   - **只按来源**：同一来源对同一账号慢速试（低于阈值）永远不触发；
//     NAT / 代理后的多个来源也可能共享出口。
//
// 故两个维度同时计数，任一维度达阈值即锁定；另加一个**全局失败预算**兜「大范围账号枚举」。
const (
	// maxAccountAttempts 单账号最大连续失败次数（跨全部来源累计）。
	maxAccountAttempts = 10
	// maxPairAttempts 同一「账号 + 来源」的最大连续失败次数（比账号维度更紧、更早锁）。
	maxPairAttempts = 5

	// lockBase 首次触发的锁定时长；每再犯一次翻倍（指数退避）。
	lockBase = 5 * time.Minute
	// lockCap 锁定时长上限（指数退避的封顶，避免退避到「永久锁定」）。
	lockCap = 30 * time.Minute

	// loginAttemptTTL 一条记录的惰性清扫 TTL。
	// **必须 > lockCap**：正在锁定中的条目不能被清扫掉，否则攻击者在锁定期内继续刷，
	// 条目被清扫后锁定立即失效（防护被自己清掉）。同时它也是指数退避的「衰减窗口」：
	// 条目被清扫即遗忘 strike 次数，长期安分的账号不会一直被最长的锁惩罚。
	loginAttemptTTL = 2 * lockCap
	// purgeEvery 每 N 次失败写入触发一次惰性清扫（摊还成本，避免每次都扫全表）。
	purgeEvery = 1024
	// purgeThreshold 条目数超过它立即清扫，防突发（攻击者用海量随机账号各试几次）。
	purgeThreshold = 4096
	// maxPurgePerPass 单次清扫最多检查的条目数。超阈值时不能整表全扫：
	// 海量随机账号场景下，条件「每次失败都扫全表」会把单次登录失败放大成 O(n) 遍历。
	maxPurgePerPass = 1024
)

// 全局失败预算（第三层：全局层）。
//
// 语义是**只按失败计**的令牌桶 + 有上限的熔断窗口：
// 失败速率持续超过预算 ⇒ 进入 globalShedDuration 的「全局卸载」期，期间登录一律拒绝。
//
// 为什么按「失败」而不是按「请求」计：正常玩家绝大多数请求是**成功**登录，
// 成功不消耗预算 ⇒ 就算有人刷失败，也不会顺带把正常玩家挡在门外（这是全局层最容易踩的坑：
// 按请求计的全局限流 = 一个有界的全局拒绝服务开关）。
// 阈值取向：globalFailBurst 次/分钟远超正常业务的失败量（正常失败是偶发的），
// 只有撞库 / 枚举洪泛这种量级才可能触到；卸载期有上限且自动恢复，不会永久锁死。
const (
	globalFailBurst    = 200
	globalFailWindow   = time.Minute
	globalShedDuration = 15 * time.Second
)

// 计数键前缀：三种维度共用一个 map（单锁、单次清扫），靠前缀区分，便于惰性清扫统一处理。
const (
	keyKindAccount = "a|"
	keyKindPair    = "p|"
)

// guardKey 归一化账号键。MySQL 侧账号比较大小写不敏感且忽略尾随空格
// （utf8mb4 默认排序规则），而 map 键完全敏感——不归一化时 "Alice"/"alice"/"alice "
// 各算一份计数，攻击者可用大小写/尾空格变体绕开同一账号的失败锁定。
func guardKey(account string) string {
	return strings.ToLower(strings.TrimSpace(account))
}

// guardSourceKey 归一化来源标识。空串（调用方未提供来源）归一到同一个键：
// 语义退化为「该账号的单一来源」，与不区分来源等价，不会凭空放大计数维度。
func guardSourceKey(source string) string {
	return strings.TrimSpace(source)
}

// accountCountKey 账号维度键。
func accountCountKey(account string) string { return keyKindAccount + guardKey(account) }

// pairCountKey 「账号 + 来源」维度键。
func pairCountKey(account, source string) string {
	return keyKindPair + guardKey(account) + "|" + guardSourceKey(source)
}

// lockDurationFor 指数退避：第 n 次触发锁定的时长 = lockBase << (n-1)，封顶 lockCap。
func lockDurationFor(strikes int) time.Duration {
	if strikes < 1 {
		strikes = 1
	}
	d := lockBase
	for i := 1; i < strikes; i++ {
		if d >= lockCap {
			return lockCap
		}
		d *= 2
	}
	if d > lockCap {
		d = lockCap
	}
	return d
}

// loginGuard 撞库防护（线程安全）：连续失败达阈值后锁定一段时间，再犯则指数退避加长。
//
// 放在账号服而非游戏服，是因为**只有这里能识别「谁在撞库」**：
// 账号密码在本域校验；游戏服只转发 token，拿不到账号名
// （详见 internal/transport/net/auth/auth.go 顶部说明）。
//
// 注意边界：状态是**进程内**的，账号服多实例横扩时各实例各算各的。
// 需要全局一致时再换成 Redis/共享后端，届时按 domain 规则在本包内替换实现即可。
type loginGuard struct {
	mu       sync.Mutex
	attempts map[string]*loginAttempt
	ops      int // 距上次清扫的失败写入次数
	// globalBudget 全局失败预算：只被**失败**消耗（成功登录不消耗，见上方常量说明）。
	globalBudget *ratelimit.TokenBucket
	// shedUntil 全局卸载期截止时间；zero 表示未处于卸载期。
	shedUntil time.Time
	// shedLogged 本轮卸载是否已记过日志（卸载期可能被成千上万次拒绝，日志必须只记一条）。
	shedLogged bool
}

// loginAttempt 单键登录失败追踪（账号维度 / 账号+来源维度共用同一结构）。
type loginAttempt struct {
	count       int
	strikes     int       // 已触发锁定的次数，指数退避的指数
	lockUntil   time.Time // zero = 未锁定
	lastFailure time.Time // 最后一次失败时间，用于惰性清扫
}

// newLoginGuard 构造一个空的失败防护器。
func newLoginGuard() *loginGuard {
	return &loginGuard{
		attempts:     make(map[string]*loginAttempt),
		globalBudget: ratelimit.NewTokenBucket(float64(globalFailBurst)/globalFailWindow.Seconds(), globalFailBurst),
	}
}

// shouldBlock 判断本次登录是否应被拒绝，并返回拒绝原因（供日志与文案区分）。
//
// 判定顺序：全局卸载 → 「账号+来源」锁定 → 账号锁定。
// 先看「账号+来源」（更紧的维度），让最可能的攻击来源先被点名；
// 两种锁定的对外响应完全相同（同一个 Kind 与文案），因此这个顺序不构成探测信道。
//
// 注意：**锁定判定的边界条件很关键** —— 必须显式判断 lockUntil 是否为零值。
// 反例：直接 `now.After(a.lockUntil)` 判断过期——未达阈值时
// lockUntil 是零值，任何时刻都被判为「已过期」，于是每次尝试都会把计数清零，
// 计数永远涨不到阈值，防护形同虚设。
func (g *loginGuard) shouldBlock(account, source string) (string, bool) {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if reason, ok := g.shedLocked(now); ok {
		return reason, true
	}
	if a, ok := g.attempts[pairCountKey(account, source)]; ok && lockedNow(a, now) {
		return "account+source locked", true
	}
	if a, ok := g.attempts[accountCountKey(account)]; ok && lockedNow(a, now) {
		return "account locked", true
	}
	return "", false
}

// lockedNow 该键当前是否处于锁定期（lockUntil 为零值 = 从未锁定）。
func lockedNow(a *loginAttempt, now time.Time) bool {
	if a.lockUntil.IsZero() {
		return false
	}
	if now.After(a.lockUntil) {
		// 锁定已过期：清掉锁定标记，让该键从 0 重新累计（strikes 保留，见条目 TTL）。
		a.lockUntil = time.Time{}
		return false
	}
	return true
}

// shedLocked 查询全局卸载期（调用方须持锁）。进入卸载期时记一条 Error（只记一条）。
func (g *loginGuard) shedLocked(now time.Time) (string, bool) {
	if g.shedUntil.IsZero() {
		return "", false
	}
	if !now.Before(g.shedUntil) {
		// 卸载期结束：清标记，恢复接收。
		g.shedUntil = time.Time{}
		g.shedLogged = false
		return "", false
	}
	if !g.shedLogged {
		g.shedLogged = true
		logger.Errorf("auth: 全局失败预算已耗尽，进入全局卸载 %v —— 说明正在发生大范围账号枚举 / 撞库洪泛"+
			"（阈值 %d 次/%v，只统计失败）；期间的登录一律拒绝，卸载期结束后自动恢复",
			globalShedDuration, globalFailBurst, globalFailWindow)
	}
	return "global failure budget exhausted", true
}

// recordFailure 记录一次登录失败（账号维度 + 「账号+来源」维度 + 全局预算）。
// 成功时调用 reset 清除计数。
func (g *loginGuard) recordFailure(account, source string) {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	// 全局预算只被失败消耗：耗尽即进入全局卸载期。
	if g.shedUntil.IsZero() && !g.globalBudget.Allow() {
		g.shedUntil = now.Add(globalShedDuration)
	}
	g.bumpLocked(accountCountKey(account), maxAccountAttempts, "account", account, source, now)
	g.bumpLocked(pairCountKey(account, source), maxPairAttempts, "account+source", account, source, now)
	g.purgeIdleLocked(now)
}

// bumpLocked 给一个计数键累加一次失败，达阈值即按指数退避锁定（调用方须持锁）。
//
// 达阈值时把 count 清零：锁定期间不再累计，锁定结束后从 0 重新数，否则
// 「锁定刚过期 --> 下一次失败立刻又达阈值」会让退避退化成一锁到底。
func (g *loginGuard) bumpLocked(key string, threshold int, dim, account, source string, now time.Time) {
	a, ok := g.attempts[key]
	if !ok {
		a = &loginAttempt{}
		g.attempts[key] = a
	}
	a.count++
	a.lastFailure = now
	if a.count < threshold {
		return
	}
	a.count = 0
	a.strikes++
	d := lockDurationFor(a.strikes)
	a.lockUntil = now.Add(d)
	logger.Warnf("auth: login locked by %s (account=%q source=%q strikes=%d duration=%s threshold=%d)",
		dim, account, source, a.strikes, d, threshold)
}

// purgeIdleLocked 惰性清扫长期未再失败的条目。
// 为什么必须清扫：失败 1~9 次的账号永远涨不到阈值，也就永远走不到 shouldBlock 里的
// 解锁分支——攻击者用海量随机账号各试几次，这张 map 就会一直涨下去（每条约几十字节，
// 千万条即几百 MB，且进程不重启不释放）。
// 摊还策略：每 purgeEvery 次失败写入扫一遍，或条目数超 purgeThreshold 立即扫，
// 单次写入的期望成本接近 O(1)。
func (g *loginGuard) purgeIdleLocked(now time.Time) {
	g.ops++
	if g.ops < purgeEvery && len(g.attempts) < purgeThreshold {
		return
	}
	g.ops = 0
	// 分批清扫：每轮最多检查 maxPurgePerPass 条，避免「条目数 ≥ 阈值后每次失败
	// 都全表扫描」把单次登录失败放大为 O(n) 遍历（攻击者用海量随机账号即可拖满 CPU）。
	// map 迭代起点随机，多轮清扫可覆盖不同部分；过期条目在 shouldBlock/reset
	// 被访问时也会被自然清理。
	scanned := 0
	for k, a := range g.attempts {
		// 正在锁定期内的条目不扫（TTL > lockCap 已保证不会被误扫，这里是纵深防御）。
		if !a.lockUntil.IsZero() && now.Before(a.lockUntil) {
			continue
		}
		if now.Sub(a.lastFailure) > loginAttemptTTL {
			delete(g.attempts, k)
		}
		scanned++
		if scanned >= maxPurgePerPass {
			break
		}
	}
}

// reset 登录成功时清除计数（两个维度一起清，同时清掉 strike 退避）。
func (g *loginGuard) reset(account, source string) {
	g.mu.Lock()
	delete(g.attempts, accountCountKey(account))
	delete(g.attempts, pairCountKey(account, source))
	g.mu.Unlock()
}
