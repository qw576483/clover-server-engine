package mob

import (
	"testing"
	"time"

	idata "clover-server-engine/internal/domain/data"
	pkgmob "clover-server-engine/pkg/domain/mmo/mob"
	"clover-server-engine/pkg/shared/geom"
)

// aggroScene 在 stubScene 基础上让 Neighbors 可配置（索敌用例要用）。
type aggroScene struct {
	*stubScene
	neighbors []uint64
}

func (s *aggroScene) Neighbors(uint64, float64) []uint64 { return s.neighbors }

// 仇恨时效：被激怒后 timeout 内没人再激它 → 忘掉目标与仇恨；
// 没到时间不许忘（否则怪会反复脱战）；刷新之后重新计时。
//
// 这条用例守的是实测事故：玩家离图 / 挂机后仇恨永久存在，
// 怪追着那个 objID 打，玩家一重新进图就被围殴（"进图即阵亡"）。
func TestMobAggroTimeoutForgetsStaleTarget(t *testing.T) {
	sc := &aggroScene{stubScene: newStubScene()}
	mgr := NewMobManager(sc)

	// 固定时钟：时效用真实时间衡量，必须能精确推进而不 sleep。
	now := time.Unix(1_700_000_000, 0)
	mgr.setClock(func() time.Time { return now })

	m, err := mgr.Spawn(pkgmob.SpawnConfig{
		ID:              1,
		OwnerType:       idata.OwnerObject,
		X:               0,
		Z:               0,
		Speed:           1,
		HP:              100,
		AggroRange:      5,
		ChaseRange:      8,
		AttackRange:     1,
		AggroTimeoutSec: 2,
		// 场景里没有邻居：遗忘之后不会立刻被重新索敌，能干净地断言"忘了"。
	})
	if err != nil {
		t.Fatalf("Spawn 失败: %v", err)
	}

	// 目标必须在场景里（否则 hasTargetOrFind 会因"查不到位置"主动清掉目标，
	// 那测的就是另一条分支了）；放得远远的，避免行为树自己走过去打它。
	if err := sc.EnterOwnerType(2, idata.OwnerPlayer, geom.Vec3{X: 100, Z: 100}); err != nil {
		t.Fatalf("登记目标失败: %v", err)
	}

	m.SetTarget(2)
	m.AddHate(2, 5)

	// 时效未到：不许忘。
	now = now.Add(1 * time.Second)
	mgr.Update(50 * time.Millisecond)
	if m.Target() != 2 || m.Hate(2) == 0 {
		t.Fatalf("时效未到不该遗忘：target=%d hate=%v", m.Target(), m.Hate(2))
	}

	// 挨打刷新时点：从"刚才"重新计时，1 秒后再走 1.5 秒仍不该忘。
	now = now.Add(500 * time.Millisecond)
	mgr.OnDamaged(m, 0)
	now = now.Add(1500 * time.Millisecond)
	mgr.Update(50 * time.Millisecond)
	if m.Target() != 2 {
		t.Fatal("挨打会刷新时效，刷新后 1.5s < 2s 不该遗忘")
	}

	// 超过时效：忘掉目标并清空仇恨。
	now = now.Add(1 * time.Second)
	mgr.Update(50 * time.Millisecond)
	if m.Target() != 0 {
		t.Fatalf("超过时效应清掉目标，实际 target=%d", m.Target())
	}
	if h := m.Hate(2); h != 0 {
		t.Fatalf("超过时效应清空仇恨，实际 hate=%v", h)
	}
}

// 时效关闭（<=0，默认）时行为与改动前一致：永不遗忘。
func TestMobAggroTimeoutDisabledKeepsHate(t *testing.T) {
	sc := &aggroScene{stubScene: newStubScene()}
	mgr := NewMobManager(sc)
	now := time.Unix(1_700_000_000, 0)
	mgr.setClock(func() time.Time { return now })

	m, err := mgr.Spawn(pkgmob.SpawnConfig{
		ID: 1, OwnerType: idata.OwnerObject, HP: 100,
		AggroRange: 5, ChaseRange: 8, AttackRange: 1,
	})
	if err != nil {
		t.Fatalf("Spawn 失败: %v", err)
	}
	if err := sc.EnterOwnerType(2, idata.OwnerPlayer, geom.Vec3{X: 100, Z: 100}); err != nil {
		t.Fatalf("登记目标失败: %v", err)
	}
	m.SetTarget(2)
	m.AddHate(2, 3)

	now = now.Add(1 * time.Hour)
	mgr.Update(50 * time.Millisecond)
	if m.Target() != 2 || m.Hate(2) == 0 {
		t.Fatal("AggroTimeoutSec<=0 表示永不忘，行为必须与改动前一致")
	}
}

// 被动单位（纯靶子）：即使身边有人、即使被显式设了目标，也**不索敌、不还手**。
//
// 守的是实测设计事故：训练靶子会还手时，近战玩家"砍靶子"变成"被靶子磨死"，
// 玩家感受是"我一挥剑自己就掉血"。
func TestPassiveMobNeverAttacks(t *testing.T) {
	sc := &aggroScene{stubScene: newStubScene(), neighbors: []uint64{2}}
	mgr := NewMobManager(sc)

	attacks := 0
	mgr.SetOnAttack(func(_, _ uint64) { attacks++ })

	m, err := mgr.Spawn(pkgmob.SpawnConfig{
		ID: 1, OwnerType: idata.OwnerObject, HP: 100,
		AggroRange: 5, ChaseRange: 8, AttackRange: 1, Speed: 1,
		Passive: true,
	})
	if err != nil {
		t.Fatalf("Spawn 失败: %v", err)
	}
	// 场景里必须有个"人"（位置表里登记），否则连索敌路径都进不去，用例就没有说服力。
	_ = sc.EnterOwnerType(2, idata.OwnerPlayer, geom.Vec3{X: 1, Z: 0})

	for i := 0; i < 20; i++ {
		mgr.Update(50 * time.Millisecond)
	}
	if m.Target() != 0 {
		t.Fatalf("被动单位不该索敌，实际 target=%d", m.Target())
	}
	if attacks != 0 {
		t.Fatalf("被动单位不该攻击，实际攻击 %d 次", attacks)
	}

	// 显式设目标也不还手（决策层直接屏蔽）。
	m.SetTarget(2)
	mgr.Update(50 * time.Millisecond)
	if attacks != 0 {
		t.Fatalf("被动单位被显式设目标也不该攻击，实际 %d 次", attacks)
	}
}

// 非被动单位（对照组）在同样场景下必须能索敌并攻击：
// 否则"被动不还手"可能只是因为链路根本没通，用例就成了假绿。
func TestHostileMobAttacksInSameScene(t *testing.T) {
	sc := &aggroScene{stubScene: newStubScene(), neighbors: []uint64{2}}
	mgr := NewMobManager(sc)

	var attacked uint64
	mgr.SetOnAttack(func(_, target uint64) { attacked = target })

	_, err := mgr.Spawn(pkgmob.SpawnConfig{
		ID: 1, OwnerType: idata.OwnerObject, HP: 100,
		AggroRange: 5, ChaseRange: 8, AttackRange: 1, Speed: 1,
	})
	if err != nil {
		t.Fatalf("Spawn 失败: %v", err)
	}
	_ = sc.EnterOwnerType(2, idata.OwnerPlayer, geom.Vec3{X: 1, Z: 0})

	// 攻击冷却默认 1s（逻辑时钟），50ms 一 tick 需要 20+ 次才会真的打出去。
	for i := 0; i < 30 && attacked == 0; i++ {
		mgr.Update(50 * time.Millisecond)
	}
	if attacked != 2 {
		t.Fatalf("对照组必须能攻击到 2，实际 %v（用例校准失败，先修这里）", attacked)
	}
}
