package frame

import (
	"sync"
	"testing"
	"time"
)

// ============================================================================
// 输入应用器（InputApplier）的并发契约回归
//
// 契约：三阶段 —— 锁内推进帧号并出队输入 → **放锁**调回调 → 回锁按玩家逐个合并。
// 回调在锁外执行，回调内回读/操作房间（Info / Join / Push…）不会卡死 tickLoop
// 与进房/离房；回调耗时（读库 / 计算 / 慢日志）不计入房锁持有时间。
// ============================================================================

// waitOrFatal 等一个信号，超时即失败 —— 死锁在测试里必须表现为"快速红"，
// 不能是"整包挂到 go test 超时"。
func waitOrFatal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s：等待超时（疑似自死锁 —— 回调是不是又在持房锁时被执行了？）", what)
	}
}

// 回调内调用房间公共方法不许死锁。
//
// 回调在锁外执行：applier 调 svc.Info → Info 再取 r.mu.RLock 不会永久阻塞。
// 本用例会在 5s 后判失败而不是挂死整包。
func TestInputApplierMayCallRoomMethodsWithoutDeadlock(t *testing.T) {
	rec := &recorder{}
	// 两段式声明是必需的（不是 S1021）：回调里要回读 svc，而 `svc := NewService(...)`
	// 的 svc 在右值内尚未进入作用域 —— 合并写法会让闭包捕获不到它。
	var svc *Service
	svc = NewService(
		WithBroadcaster(rec.push),
		WithInputApplier(func(roomID string, frame int64, inputs map[string]Input) map[string]*PlayerState {
			// 回调内回读房间。
			if _, err := svc.Info(roomID); err != nil {
				t.Errorf("回调内 svc.Info 失败: %v", err)
			}
			out := make(map[string]*PlayerState, len(inputs))
			for pid := range inputs {
				out[pid] = &PlayerState{PlayerID: pid, HP: 100, LastFrame: frame}
			}
			return out
		}),
	)

	r, err := svc.NewRoom("A", WithAutoStart(false))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svc.Join("A", "p1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := svc.Input("A", "p1", Input{}); err != nil {
		t.Fatalf("Input: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.step()
	}()
	waitOrFatal(t, done, "step() 未在期限内完成")

	if got := r.Frame(); got != 1 {
		t.Fatalf("帧应推进到 1，实际 %d", got)
	}
}

// 回调执行期间（房间处于**未加锁**窗口）别的方法必须能正常进出房间；
// 且回调返回的状态只合并到"仍在房间里的玩家"。
//
// Join 在回调执行期间不得阻塞（否则此处超时失败）。
// 合并语义：期间新进房的玩家保留其加入时的状态，不被回调返回值覆盖（他本来就没参与本帧）。
func TestInputApplierResultMergesWithConcurrentJoin(t *testing.T) {
	rec := &recorder{}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	svc := NewService(
		WithBroadcaster(rec.push),
		WithInputApplier(func(roomID string, frame int64, inputs map[string]Input) map[string]*PlayerState {
			once.Do(func() { close(entered) })
			<-release
			// 只返回本帧有输入的那位玩家（p1）。
			return map[string]*PlayerState{
				"p1": {PlayerID: "p1", HP: 200, LastFrame: frame},
			}
		}),
	)

	r, err := svc.NewRoom("A", WithAutoStart(false))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svc.Join("A", "p1"); err != nil {
		t.Fatalf("Join p1: %v", err)
	}
	if err := svc.Input("A", "p1", Input{}); err != nil {
		t.Fatalf("Input: %v", err)
	}

	stepDone := make(chan struct{})
	go func() {
		defer close(stepDone)
		r.step()
	}()

	waitOrFatal(t, entered, "回调未开始执行")
	// 回调执行中，房间锁必须是**放开的**：这次 Join 不许被阻塞。
	joinDone := make(chan struct{})
	go func() {
		defer close(joinDone)
		if err := svc.Join("A", "p2"); err != nil {
			t.Errorf("回调期间 Join 失败: %v", err)
		}
	}()
	waitOrFatal(t, joinDone, "回调期间 Join 被阻塞（房锁未放开）")

	close(release)
	waitOrFatal(t, stepDone, "step() 未在期限内完成")

	info := r.Info()
	if info.PlayerCount != 2 {
		t.Fatalf("回调期间进房的玩家必须保留，玩家数应为 2，实际 %d", info.PlayerCount)
	}
	r.mu.RLock()
	st, ok := r.players["p2"]
	r.mu.RUnlock()
	if !ok || st == nil {
		t.Fatal("期间进房的 p2 被整体替换语义丢掉了")
	}
	if p1 := r.players["p1"]; p1 == nil || p1.HP != 200 {
		t.Fatalf("p1 的状态应被回调结果覆盖为 HP=200，实际 %+v", p1)
	}
	if st.HP != 100 {
		t.Fatalf("期间进房的 p2 应保留其加入时的状态（HP=100），实际 HP=%d", st.HP)
	}
}

// 回调期间发生 ImportState（节点接管）时，本次回调结果必须**整体丢弃**：
// 否则会把刚导入的新状态覆盖成回调基于旧状态算出来的结果。
func TestInputApplierResultDroppedAfterImportState(t *testing.T) {
	rec := &recorder{}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	svc := NewService(
		WithBroadcaster(rec.push),
		WithInputApplier(func(roomID string, frame int64, inputs map[string]Input) map[string]*PlayerState {
			once.Do(func() { close(entered) })
			<-release
			return map[string]*PlayerState{
				"p1": {PlayerID: "p1", HP: 999, LastFrame: frame},
			}
		}),
	)

	r, err := svc.NewRoom("A", WithAutoStart(false))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svc.Join("A", "p1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := svc.Input("A", "p1", Input{}); err != nil {
		t.Fatalf("Input: %v", err)
	}

	stepDone := make(chan struct{})
	go func() {
		defer close(stepDone)
		r.step()
	}()
	waitOrFatal(t, entered, "回调未开始执行")

	// 回调期间接管：整体替换房间运行态（帧号 100、p1 的 HP 为 7）。
	if _, err := svc.ImportState(RoomState{
		RoomID:  "A",
		Frame:   100,
		Players: map[string]*PlayerState{"p1": {PlayerID: "p1", HP: 7, LastFrame: 100}},
		Presence: map[string]PresenceState{
			"p1": {Connected: true, LastSeenFrame: 100},
		},
		Running: false,
	}); err != nil {
		t.Fatalf("ImportState: %v", err)
	}

	close(release)
	waitOrFatal(t, stepDone, "step() 未在期限内完成")

	if got := r.Frame(); got != 100 {
		t.Fatalf("接管后的帧号不该被过期回调结果覆盖，应为 100，实际 %d", got)
	}
	// 注意这里读的是房间内部表：刻意确认"过期结果没有写进去"。
	if p1 := r.players["p1"]; p1 == nil || p1.HP != 7 {
		t.Fatalf("接管导入的 p1 状态不该被过期回调结果覆盖，实际 %+v", p1)
	}
}

// 回调返回房间里不存在的玩家：不许新增成员（成员只能经 Join 进入），并留痕。
func TestInputApplierExtraPlayersIgnored(t *testing.T) {
	rec := &recorder{}
	svc := NewService(
		WithBroadcaster(rec.push),
		WithInputApplier(func(_ string, frame int64, _ map[string]Input) map[string]*PlayerState {
			return map[string]*PlayerState{
				"p1":    {PlayerID: "p1", HP: 100, LastFrame: frame},
				"ghost": {PlayerID: "ghost", HP: 100, LastFrame: frame},
			}
		}),
	)
	r, err := svc.NewRoom("A", WithAutoStart(false))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svc.Join("A", "p1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := svc.Input("A", "p1", Input{}); err != nil {
		t.Fatalf("Input: %v", err)
	}
	r.step()

	if r.Info().PlayerCount != 1 {
		t.Fatalf("回调返回的房外玩家必须被忽略，玩家数应为 1，实际 %d", r.Info().PlayerCount)
	}
	if _, ok := r.players["ghost"]; ok {
		t.Fatal("房外玩家被写进了成员表（成员只能经 Join 进入）")
	}
}
