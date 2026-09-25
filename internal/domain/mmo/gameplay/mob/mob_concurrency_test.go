package mob

import (
	"sync"
	"testing"
	"time"

	idata "github.com/qw576483/clover-server-engine/internal/domain/data"
	pkgcollide "github.com/qw576483/clover-server-engine/pkg/domain/mmo/collide"
	pkgmob "github.com/qw576483/clover-server-engine/pkg/domain/mmo/mob"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// stubScene 只维护位置表：本用例要守的是 Mob 的锁，不是 AOI 语义。
type stubScene struct {
	mu  sync.Mutex
	pos map[uint64]geom.Vec3
}

func newStubScene() *stubScene { return &stubScene{pos: make(map[uint64]geom.Vec3)} }

func (s *stubScene) EnterOwnerType(objID uint64, _ idata.OwnerType, pos geom.Vec3) error {
	s.mu.Lock()
	s.pos[objID] = pos
	s.mu.Unlock()
	return nil
}

func (s *stubScene) Move(objID uint64, pos geom.Vec3) {
	s.mu.Lock()
	s.pos[objID] = pos
	s.mu.Unlock()
}

func (s *stubScene) Leave(objID uint64) {
	s.mu.Lock()
	delete(s.pos, objID)
	s.mu.Unlock()
}

func (s *stubScene) Position(objID uint64) (geom.Vec3, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pos[objID]
	return p, ok
}

func (s *stubScene) Neighbors(uint64, float64) []uint64 { return nil }
func (s *stubScene) Members() []uint64                  { return nil }

// 主循环（刷怪 / 重生 / 行为树）、外部入口（打死 / 复活 / 仇恨）与读侧并发跑，
// 三方对同一 Mob 的 map / 状态字段构成真实的跨 goroutine 读写。
//
// 检出能力（如实说明，勿夸大）：
//   - 拆掉 map 类字段（hate 等）的锁：读侧与写侧对同一 map 的并发读-写会触发
//     runtime 的 "concurrent map read and map write"（无需 -race 也可检出）。
//     读侧 goroutine 专为检出拆锁而设（拆掉 AddHate 的锁后本用例必红）。
//   - 数值字段（respawnRetry / respawnAt / patrolIdx）的竞争在无 -race 环境不可从
//     行为层观测（对齐访存不会撕裂、也无 panic），需 `go test -race` 检出——
//     无 cgo 环境请在带 -race 的 CI 上跑本用例。
//   - 持怪锁走慢路径（如持 m.mu 调 scene）造成的锁序问题会以死锁 / 超时暴露。
//
// 结尾还有一组终态断言：并发压力之后，状态机必须仍能正确打死 / 复活 / 重生
// （锁协议被拆掉或状态串坏时，这里会失败而不是静默漂移）。
func TestMobRespawnAndPatrolUnderConcurrency(t *testing.T) {
	sc := newStubScene()
	mgr := NewMobManager(sc)

	m, err := mgr.Spawn(pkgmob.SpawnConfig{
		ID:          1,
		OwnerType:   idata.OwnerServer,
		Name:        "mob",
		X:           0,
		Z:           0,
		Speed:       5,
		HP:          10,
		AggroRange:  5,
		ChaseRange:  8,
		AttackRange: 1,
		Patrol:      []pkgcollide.Vec2{{X: 0, Y: 0}, {X: 10, Y: 0}},
		RespawnSec:  0.001, // 越小越快进入 respawn 分支
	})
	if err != nil {
		t.Fatalf("Spawn 失败: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 写侧 1：主循环（会走 actPatrol 读 patrolIdx、走 respawn 写 respawnRetry/respawnAt）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			mgr.Update(5 * time.Millisecond)
		}
	}()

	// 写侧 2：外部入口（打死 + 复活 + 仇恨）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < 2000; i++ {
			mgr.OnDamaged(m, 100)
			_ = m.IsDead()
			m.AddHate(2, 1)
			_ = m.Hate(2)
			m.SetTarget(2)
			_ = m.Target()
			m.DelHate(2)
			m.Revive()
		}
	}()

	// 读侧：持续读仇恨 / 目标 / 状态。与写侧的 map 写形成跨 goroutine 的读-写竞争，
	// 拆掉 mob 的锁后，runtime 的并发 map 检测会直接 fatal（本用例可检出）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = m.Hate(2)
			_ = m.Hate(3)
			_ = m.Target()
			_ = m.IsDead()
		}
	}()

	wg.Wait()

	// ── 终态断言：并发压力之后状态机必须仍正确工作 ──
	// （wg.Wait 已保证无并发写入，这里直接读字段是安全的。）
	im := m.(*Mob)
	m.Revive()
	if m.IsDead() {
		t.Fatal("Revive 后 IsDead 仍为 true")
	}
	if im.HP != im.MaxHP {
		t.Fatalf("Revive 后 HP=%v，期望 MaxHP=%v", im.HP, im.MaxHP)
	}
	mgr.OnDamaged(m, im.MaxHP+1)
	if !m.IsDead() {
		t.Fatalf("致命伤害后应死亡：HP=%v dead=%v", im.HP, im.dead)
	}
	// 重生：RespawnSec=0.001，逻辑时钟按 dt 累加，几帧内应恢复。
	deadline := time.Now().Add(2 * time.Second)
	for m.IsDead() && time.Now().Before(deadline) {
		mgr.Update(5 * time.Millisecond)
	}
	if m.IsDead() {
		t.Fatal("respawn 未在期限内恢复（重生 / 时钟逻辑异常）")
	}
}
