package state

import (
	"testing"
	"time"
)

// TestGuardLocksPairDimBeforeAccountDim 「账号 + 来源」维度必须比账号维度更早锁：
// 单来源持续撞同一账号时，前者的阈值更低（5 < 10），因此先命中。
// 这是「双维度」最实际的价值——一个来源刷一个账号不必等到第 10 次才被挡。
func TestGuardLocksPairDimBeforeAccountDim(t *testing.T) {
	g := newLoginGuard()
	account, source := "alice", "10.0.0.1"

	for i := 0; i < maxPairAttempts; i++ {
		if _, blocked := g.shouldBlock(account, source); blocked {
			t.Fatalf("第 %d 次尝试前不应被锁（阈值 %d）", i+1, maxPairAttempts)
		}
		g.recordFailure(account, source)
	}
	reason, blocked := g.shouldBlock(account, source)
	if !blocked {
		t.Fatalf("%d 次失败后「账号+来源」维度应锁定", maxPairAttempts)
	}
	if reason != "account+source locked" {
		t.Errorf("拒绝原因 = %q, want %q（判定顺序：先更紧的维度）", reason, "account+source locked")
	}

	// 账号维度在此时**还未**达阈值（5 < 10）：换个来源仍应放行 ——
	// 证明两个维度是各自独立计数，不是一个阈值被抄成两份。
	if _, blocked := g.shouldBlock(account, "10.0.0.2"); blocked {
		t.Errorf("账号维度尚未达阈值（%d < %d），其它来源不该被同一把锁挡住", maxPairAttempts, maxAccountAttempts)
	}
	// 但该账号的其它来源试到账号维度阈值后也必须被锁（否则换 IP 即可绕过）。
	for i := 0; i < maxAccountAttempts; i++ {
		g.recordFailure(account, "10.0.0.2")
	}
	if reason, blocked := g.shouldBlock(account, "10.0.0.3"); !blocked {
		t.Errorf("账号维度达阈值后应锁定全部来源（换 IP 不能绕过），got reason=%q", reason)
	}
}

// TestGuardAccountDimIsSourceIndependent 账号维度跨来源累计：
// 攻击者用海量来源各试几次（每个来源都不到 pair 阈值）时，账号维度仍必须拦住。
func TestGuardAccountDimIsSourceIndependent(t *testing.T) {
	g := newLoginGuard()
	account := "bob"
	// 每个来源各失败 3 次（< maxPairAttempts=5，故 pair 维度不锁）。
	for s := 0; s < 10; s++ {
		source := "10.0.1." + string(rune('0'+s))
		for i := 0; i < 3; i++ {
			g.recordFailure(account, source)
		}
	}
	if _, blocked := g.shouldBlock(account, "10.0.1.99"); !blocked {
		t.Fatalf("分散来源累计 30 次失败后账号维度应锁定（threshold=%d）", maxAccountAttempts)
	}
}

// TestGuardLockExpiresAndBackoffGrows 锁定期必须到期自动解除，且再犯时长按指数退避增长。
func TestGuardLockExpiresAndBackoffGrows(t *testing.T) {
	g := newLoginGuard()
	account, source := "carol", "10.0.0.9"

	lockOnce := func() time.Duration {
		for i := 0; i < maxPairAttempts; i++ {
			g.recordFailure(account, source)
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		a := g.attempts[pairCountKey(account, source)]
		if a == nil {
			t.Fatalf("计数条目应当存在")
		}
		return a.lockUntil.Sub(time.Now())
	}

	first := lockOnce()
	if first <= 0 {
		t.Fatalf("首次触发的锁定时长 = %v, want > 0", first)
	}
	// 模拟锁定到期：直接把 lockUntil 拨到过去，再犯一次即进入第二轮退避。
	g.mu.Lock()
	g.attempts[pairCountKey(account, source)].lockUntil = time.Now().Add(-time.Second)
	g.mu.Unlock()
	if _, blocked := g.shouldBlock(account, source); blocked {
		t.Fatalf("锁定期已过，shouldBlock 应放行")
	}
	second := lockOnce()
	if second <= first {
		t.Errorf("指数退避未生效：首次 %v，第二轮 %v（应更长）", first, second)
	}
	// 封顶：退避不得无限增长。
	for i := 0; i < 10; i++ {
		g.mu.Lock()
		g.attempts[pairCountKey(account, source)].lockUntil = time.Now().Add(-time.Second)
		g.mu.Unlock()
		_ = lockOnce()
	}
	g.mu.Lock()
	strikes := g.attempts[pairCountKey(account, source)].strikes
	g.mu.Unlock()
	if got := lockDurationFor(strikes); got != lockCap {
		t.Errorf("退避封顶失效：strikes=%d 时长=%v, want %v", strikes, got, lockCap)
	}
}

// TestGuardGlobalShedAfterFailureBurst 全局层：失败量耗尽预算后进入全局卸载期，
// 期间任何账号的登录都被拒（兜「大范围账号枚举」），且卸载期有上限、会自动恢复。
func TestGuardGlobalShedAfterFailureBurst(t *testing.T) {
	g := newLoginGuard()
	// 用互不相同的账号 + 来源制造失败：单账号 / 单来源都不达阈值，
	// 只有全局预算能拦住这种「广撒网」形态。
	// 预算是令牌桶（容量 = globalFailBurst），故第 globalFailBurst 次失败仍可消耗，
	// 第 +1 次才耗尽 ⇒ 循环到 <= globalFailBurst。
	for i := 0; i <= globalFailBurst; i++ {
		g.recordFailure("u"+string(rune('a'+i%26))+string(rune('a'+i/26)), "10.9.0."+string(rune('0'+i%10)))
	}
	reason, blocked := g.shouldBlock("victim", "10.8.8.8")
	if !blocked {
		t.Fatalf("全局失败预算耗尽后应进入全局卸载（阈值 %d 次/%v）", globalFailBurst, globalFailWindow)
	}
	if reason != "global failure budget exhausted" {
		t.Errorf("拒绝原因 = %q, want %q", reason, "global failure budget exhausted")
	}

	// 卸载期到点必须自动恢复：把 shedUntil 拨到过去，下一次判定即放行。
	g.mu.Lock()
	g.shedUntil = time.Now().Add(-time.Millisecond)
	g.mu.Unlock()
	if reason, blocked := g.shouldBlock("victim", "10.8.8.8"); blocked {
		t.Errorf("卸载期已过应恢复放行，got reason=%q", reason)
	}
}

// TestGuardResetClearsBothDims 登录成功后两个维度都必须清零：
// 漏清一个会让「密码输错几次后输对」的玩家被残留计数误锁。
func TestGuardResetClearsBothDims(t *testing.T) {
	g := newLoginGuard()
	account, source := "dave", "10.0.0.7"
	for i := 0; i < maxAccountAttempts-1; i++ {
		g.recordFailure(account, source)
	}
	g.reset(account, source)
	if _, blocked := g.shouldBlock(account, source); blocked {
		t.Fatalf("reset 后不该被锁")
	}
	g.mu.Lock()
	n := len(g.attempts)
	g.mu.Unlock()
	if n != 0 {
		t.Errorf("reset 后残留 %d 条计数，want 0（账号维度与「账号+来源」维度都要清）", n)
	}
}

// TestGuardPurgeKeepsLockedEntries 惰性清扫不得清掉仍处于锁定期的条目：
// TTL 必须大于最长锁定时长（lockCap），否则攻击者可在锁定期内刷别的东西把锁定清掉。
func TestGuardPurgeKeepsLockedEntries(t *testing.T) {
	if loginAttemptTTL <= lockCap {
		t.Fatalf("loginAttemptTTL=%v 必须 > lockCap=%v，否则锁定中条目会被清扫", loginAttemptTTL, lockCap)
	}
	g := newLoginGuard()
	account, source := "erin", "10.0.0.6"
	for i := 0; i < maxPairAttempts; i++ {
		g.recordFailure(account, source)
	}
	// 把 lastFailure 拨到 TTL 之外（模拟「锁定期间再无失败」），但锁定仍在进行。
	g.mu.Lock()
	a := g.attempts[pairCountKey(account, source)]
	a.lastFailure = time.Now().Add(-loginAttemptTTL - time.Minute)
	g.ops = purgeEvery
	g.purgeIdleLocked(time.Now())
	_, still := g.attempts[pairCountKey(account, source)]
	g.mu.Unlock()
	if !still {
		t.Errorf("锁定中的条目被清扫掉了 —— 锁定立即失效")
	}
}
