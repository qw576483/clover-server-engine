package ratelimit

import (
	"testing"
	"time"
)

// 本类型存在的理由：**单条约束拦不住的那一类作弊**必须被抓住。
//
// 场景：1 秒窗口、上限 40 条 / 12 米。
//  1. 正常玩家（20Hz、每包 0.3 米）应当全程放行；
//  2. 降频大跳（1Hz、每包 50 米）条数完全不超 —— 只有位移约束能拦（= dist）；
//  3. 高频小步（100Hz、每包 0.5 米）位移总量 50 米 —— 只有条数约束能拦（= rate）。
func TestMoveGuard_RejectsBothAbuseShapes(t *testing.T) {
	now, clock := newTestClock()
	g := NewMoveGuardWithClock(40, 12, time.Second, 10, clock)
	rateLimit, distLimit, _ := g.Limits()
	if rateLimit != 40 || distLimit != 12 {
		t.Fatalf("Limits = (%v, %v), 期望 (40, 12)", rateLimit, distLimit)
	}

	// ① 正常玩家：20 条 × 0.3 米 = 6 米 < 12，全部放行。
	for i := 0; i < 20; i++ {
		if ok, reason := g.Allow(0.3); !ok {
			t.Fatalf("正常玩家第 %d 条被拒: reason=%s", i+1, reason)
		}
	}

	// ② 换个窗口重来，模拟降频大跳：条数只有 1 条，位移 50 米。
	*now = now.Add(2 * time.Second)
	g.Reset()
	if ok, reason := g.Allow(50); ok || reason != MoveRejectDist {
		t.Fatalf("1 帧 50 米应被位移约束拒绝（reason=%s, ok=%v）—— 只看条数的限流抓不到它", reason, ok)
	}

	// ③ 高频小步：每包 0.5 米，前 24 步累计 12 米正好到上限，第 25 步被位移拦。
	g.Reset()
	for i := 0; i < 24; i++ {
		if ok, reason := g.Allow(0.5); !ok {
			t.Fatalf("累计 12 米以内的第 %d 步不该被拒: reason=%s", i+1, reason)
		}
	}
	if ok, reason := g.Allow(0.5); ok || reason != MoveRejectDist {
		t.Fatalf("第 25 步累计 12.5 米应被拒: ok=%v reason=%s", ok, reason)
	}

	// ④ 满额刷包（0 位移）撞条数上限：第 41 条报 rate。
	g.Reset()
	for i := 0; i < 40; i++ {
		if ok, _ := g.Allow(0); !ok {
			t.Fatalf("0 位移的第 %d 条不该被拒（条数上限是 40）", i+1)
		}
	}
	if ok, reason := g.Allow(0); ok || reason != MoveRejectRate {
		t.Fatalf("第 41 条应报条数超限: ok=%v reason=%s", ok, reason)
	}
}

// 窗口滑过去之后自动恢复：限流不是惩罚，不该越填越满。
func TestMoveGuard_RecoversAfterWindow(t *testing.T) {
	now, clock := newTestClock()
	g := NewMoveGuardWithClock(2, 5, time.Second, 10, clock)

	if ok, _ := g.Allow(1); !ok {
		t.Fatal("第 1 条应放行")
	}
	if ok, _ := g.Allow(1); !ok {
		t.Fatal("第 2 条应放行")
	}
	if ok, reason := g.Allow(1); ok || reason != MoveRejectRate {
		t.Fatalf("第 3 条应报 rate: ok=%v reason=%s", ok, reason)
	}

	*now = now.Add(2 * time.Second)
	if ok, reason := g.Allow(1); !ok {
		t.Fatalf("窗口滑过后应恢复放行: reason=%s", reason)
	}
}

// Reset 必须同时清空两条窗口：复活 / 进图后不该被死亡期间的历史位移卡住。
func TestMoveGuard_ResetClearsBothWindows(t *testing.T) {
	_, clock := newTestClock()
	g := NewMoveGuardWithClock(1, 1, time.Second, 10, clock)

	if ok, _ := g.Allow(1); !ok {
		t.Fatal("首条应放行")
	}
	if ok, _ := g.Allow(1); ok {
		t.Fatal("额度用尽后应被拒")
	}
	g.Reset()
	if ok, reason := g.Allow(1); !ok {
		t.Fatalf("Reset 后应重新可放行（复活/进图场景）: reason=%s", reason)
	}
	msgs, dist := g.Remaining()
	if msgs != 0 || dist != 0 {
		t.Fatalf("Reset 后再用掉一次，剩余应为 (0,0)，实际 (%v,%v)", msgs, dist)
	}
}

// 零值守卫放行而不拦截：宁可漏拦一个坏包，也不能让"忘了构造"把正常玩家卡死。
func TestMoveGuard_ZeroValueAllows(t *testing.T) {
	var g *MoveGuard
	if ok, reason := g.Allow(100); !ok || reason != MoveRejectNone {
		t.Fatalf("nil 守卫应放行: ok=%v reason=%s", ok, reason)
	}
	var zero MoveGuard
	if ok, _ := zero.Allow(100); !ok {
		t.Fatal("零值守卫应放行（未构造不该变成故障源）")
	}
	zero.Reset() // 不该 panic
	if _, _, w := zero.Limits(); w != 0 {
		t.Fatalf("零值 Limits 应为 0，实际 %v", w)
	}
}
