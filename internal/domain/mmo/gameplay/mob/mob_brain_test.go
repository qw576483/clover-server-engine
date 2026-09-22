package mob

import (
	"sync/atomic"
	"testing"
	"time"

	idata "github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/mmo/gameplay/ai/btree"
	pkgmob "github.com/qw576483/clover-server-engine/pkg/domain/mmo/mob"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// 每只怪必须有**自己的**行为树实例。
//
// 守的是本缺陷：MobManager 曾只构建一棵 *btree.Tree 供所有怪共用，
// 而 Selector/Limiter/Cooldown 的续跑位置、限流窗口、冷却时刻都挂在节点上
// —— 状态跨怪串扰。
//
// 这条是结构性断言（确定性，不依赖遍历顺序）：共用一棵树时两者相等，必红。
func TestMobBrainIsPerMob(t *testing.T) {
	sc := newStubScene()
	mgr := NewMobManager(sc)

	m1, err := mgr.Spawn(pkgmob.SpawnConfig{ID: 1, OwnerType: idata.OwnerObject, HP: 10, Speed: 1})
	if err != nil {
		t.Fatalf("Spawn #1 失败: %v", err)
	}
	m2, err := mgr.Spawn(pkgmob.SpawnConfig{ID: 2, OwnerType: idata.OwnerObject, HP: 10, Speed: 1})
	if err != nil {
		t.Fatalf("Spawn #2 失败: %v", err)
	}

	b1, b2 := m1.(*Mob).brain, m2.(*Mob).brain
	if b1 == nil || b2 == nil {
		t.Fatal("Spawn 必须为每只怪构建行为树/黑板（brain=nil 会在 Update 里被跳过）")
	}
	if b1 == b2 {
		t.Fatal("两只怪共用了同一棵行为树：节点上的续跑位置/限流窗口/冷却会跨怪串扰")
	}
}

// 行为层验证「一只怪的决策状态不会把另一只怪挤掉」。
//
// 场景（与症状同形）：
//   - 怪 1 的目标在**追击范围**内但不在攻击范围内 → Selector 停在索引 1（追击分支），
//     把节点的续跑位置留在 1；
//   - 怪 2 的目标在**攻击范围**内 → 它应该走索引 0（攻击分支）。
//
// 共用一棵树时，怪 1 留下的续跑位置会让怪 2 从索引 1 起跑、永久跳过攻击分支
// —— 症状就是「有目标却站着不攻击 / 只会追着走」。
func TestMobBrainStateDoesNotLeakBetweenMobs(t *testing.T) {
	sc := newStubScene()
	mgr := NewMobManager(sc)

	var attacked int64
	mgr.SetOnAttack(func(_, target uint64) {
		if target == 22 {
			atomic.AddInt64(&attacked, 1)
		}
	})

	// Speed=0（两只都冻住不动）：本用例只测决策树的续跑状态，
	// 位置一旦会变就会把「追到目标脚下 → 自然切回攻击」混进来，掩盖串扰。
	//
	// 怪 1：目标 11 在 5 米处（ChaseRange=8、AttackRange=1）→ 恒走追击分支且永不抵达
	// （Speed=0 ⇒ actChase 恒 Running），把 Selector 的续跑位置钉在索引 1。
	if _, err := mgr.Spawn(pkgmob.SpawnConfig{
		ID: 1, OwnerType: idata.OwnerObject, HP: 100, Speed: 0,
		AggroRange: 5, ChaseRange: 8, AttackRange: 1,
	}); err != nil {
		t.Fatalf("Spawn #1 失败: %v", err)
	}
	// 怪 2：目标 22 在 0.5 米处（攻击范围 1 内）→ 只要从索引 0 起跑就会攻击。
	if _, err := mgr.Spawn(pkgmob.SpawnConfig{
		ID: 2, OwnerType: idata.OwnerObject, HP: 100, Speed: 0,
		AggroRange: 5, ChaseRange: 8, AttackRange: 1,
	}); err != nil {
		t.Fatalf("Spawn #2 失败: %v", err)
	}

	if err := sc.EnterOwnerType(1, idata.OwnerObject, geom.Vec3{X: 0, Z: 0}); err != nil {
		t.Fatalf("登记怪 1 位置失败: %v", err)
	}
	if err := sc.EnterOwnerType(11, idata.OwnerPlayer, geom.Vec3{X: 5, Z: 0}); err != nil {
		t.Fatalf("登记目标 11 失败: %v", err)
	}
	if err := sc.EnterOwnerType(2, idata.OwnerObject, geom.Vec3{X: 100, Z: 0}); err != nil {
		t.Fatalf("登记怪 2 位置失败: %v", err)
	}
	if err := sc.EnterOwnerType(22, idata.OwnerPlayer, geom.Vec3{X: 100.5, Z: 0}); err != nil {
		t.Fatalf("登记目标 22 失败: %v", err)
	}

	// 目标必须显式设好（本用例只测决策树的续跑状态，不测索敌）。
	mobs := []*Mob{}
	mgr.mu.Lock()
	for _, m := range mgr.mobs {
		mobs = append(mobs, m)
	}
	mgr.mu.Unlock()
	for _, m := range mobs {
		switch m.ID {
		case 1:
			m.SetTarget(11)
		case 2:
			m.SetTarget(22)
		}
	}

	// 遍历顺序是 map 顺序（随机）：跑足多次，让"怪 1 先 tick"的场景必然出现。
	// 共用一棵树时，凡是怪 1 先跑的那一帧，怪 2 就跳过攻击分支。
	for i := 0; i < 200 && atomic.LoadInt64(&attacked) == 0; i++ {
		mgr.Update(20 * time.Millisecond)
	}
	if atomic.LoadInt64(&attacked) == 0 {
		t.Fatal("怪 2 的目标就在攻击范围内，却从未攻击：决策树状态疑似被怪 1 的续跑位置串扰")
	}
}

// 复活必须重置决策树的续跑状态。
//
// 守的是同一类症状的"第二现场"：一只怪死在追击途中时，Selector 的续跑位置停在
// 「追击」分支（索引 1）；若复活后沿用同一棵树，它本帧就从索引 1 起跑、
// 跳过索引 0 的攻击分支 —— 刚复活、贴着人却只追不打。
func TestReviveResetsBrainState(t *testing.T) {
	sc := newStubScene()
	mgr := NewMobManager(sc)

	m, err := mgr.Spawn(pkgmob.SpawnConfig{ID: 1, OwnerType: idata.OwnerObject, HP: 10, Speed: 0})
	if err != nil {
		t.Fatalf("Spawn 失败: %v", err)
	}
	im := m.(*Mob)
	im.mu.Lock()
	before := im.brain
	im.mu.Unlock()
	if before == nil {
		t.Fatal("Spawn 后 brain 不该为空")
	}

	m.Revive()

	im.mu.Lock()
	after := im.brain
	im.mu.Unlock()
	if after == nil {
		t.Fatal("Revive 后 brain 不该为空（否则 Update 会跳过这只怪）")
	}
	if after == before {
		t.Fatal("Revive 应重建决策树以清掉上一世的续跑位置/限流窗口/冷却（否则复活后可能只追不打）")
	}
}

// AI 的时间源必须是**逻辑时刻**：黑板 KeyNow 由 Update 按 mgr.clock（dt 累加）注入，
// 而不是留空让 Limiter/Cooldown 回落墙钟。
//
// 判别方式：逻辑时钟从 0 起算，因此注入值必须等于 LogicalTime(clock)；
// 回落墙钟时黑板里根本没有 KeyNow，或值是一个 2025 年级别的真实时刻。
func TestMobUpdateInjectsLogicalClockIntoBlackboard(t *testing.T) {
	sc := newStubScene()
	mgr := NewMobManager(sc)

	m, err := mgr.Spawn(pkgmob.SpawnConfig{ID: 1, OwnerType: idata.OwnerObject, HP: 10, Speed: 1})
	if err != nil {
		t.Fatalf("Spawn 失败: %v", err)
	}
	im := m.(*Mob)

	const dt = 20 * time.Millisecond
	// 用与 MobManager 相同的累加顺序（clock += dt.Seconds()）复算期望值，
	// 避免浮点结合律差异导致断言脆弱。
	var acc float64
	for i := 1; i <= 3; i++ {
		mgr.Update(dt)
		acc += dt.Seconds()
		got, ok := im.bb.Get(btree.KeyNow).(time.Time)
		if !ok {
			t.Fatalf("第 %d 帧：黑板 %q 未注入 time.Time（实际 %T）", i, btree.KeyNow, im.bb.Get(btree.KeyNow))
		}
		want := btree.LogicalTime(acc)
		if !got.Equal(want) {
			t.Fatalf("第 %d 帧：逻辑时刻应为 %v，实际 %v（回落墙钟会与 dt 口径脱节）", i, want, got)
		}
		if got.Year() > 2000 {
			t.Fatalf("第 %d 帧：黑板里是墙钟时刻 %v，不是逻辑时刻", i, got)
		}
	}
}

// 未经 Spawn 直接 Register 的 *Mob（字段导出，业务可自行拼装）不许在 Update 里 panic，
// 也不许静默不 tick：Register 应补建行为树/黑板并留痕。
func TestRegisterWithoutSpawnBuildsBrain(t *testing.T) {
	sc := newStubScene()
	mgr := NewMobManager(sc)

	raw := &Mob{ID: 7, OwnerType: idata.OwnerObject, HP: 10, Speed: 1, hate: map[uint64]float64{}}
	mgr.Register(raw)
	if raw.brain == nil || raw.bb == nil {
		t.Fatal("Register 必须为未 Spawn 的 Mob 补建行为树/黑板（否则 Update 会跳过或 panic）")
	}
	if err := sc.EnterOwnerType(7, idata.OwnerObject, geom.Vec3{}); err != nil {
		t.Fatalf("登记位置失败: %v", err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Update 不该 panic：%v", r)
		}
	}()
	mgr.Update(20 * time.Millisecond)
}
