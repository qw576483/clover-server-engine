package frame

import (
	"sync"
	"testing"
)

// ============================================================================
// B1 复现与根因定位：帧同步房间"Broadcast 失败"
//
// 背景：历史上记录过「帧同步房间自测 1004401 的 Broadcast 失败」，
// 当时**没有测试载体**（本目录曾无任何 _test.go），所以只留了"待查 tick 循环与输入帧推进条件"。
//
// 本文件重建载体，把两个候选根因各自钉成**可复现的断言**（先跑测试、再看结论）：
//
//	根因 1：`PushMessageID` 默认为 0（`DefaultConfig()` 不设它），而 `broadcast()` 首行
//	        就是 `if pushMsgID == 0 { return }` ⇒ 每帧都在"广播"，却一条都发不出去（静默空转）。
//	        这与"Broadcast 失败"最契合：帧推进正常、日志无异常、客户端永远收不到帧推。
//
//	根因 2（已修，回归见场景 D）：wait-for-all 与「输入超时兜底」互相依赖 —— 历史上兜底判据是
//	        `frame - ps.LastFrame >= InputTimeoutTicks`；若房间内**没有任何人**提交输入，
//	        帧号恒为 0（下一次推进目标是 1），`1-0 >= 90` 永远不成立 ⇒ 自锁（房间卡住且无提示）。
//	        现判据改为 inputWaitTicks（连续未提交的 step 次数）：由 ticker 驱动、卡帧时仍增长，
//	        达到阈值即填 fallback 输入推进。
//
// 根因 1 是「配置必须显式给 PushMessageID」的语义（见 broadcast 注释，已登记不是待修 bug）；
// 根因 2 已按兜底语义修好，两者现象都叫"Broadcast 失败"，必须能被一眼分辨。
// ============================================================================

// pushRec 一条广播记录。
type pushRec struct {
	playerID string
	msgID    uint32
	push     FramePush
}

// recorder 线程安全的广播记录器（tickLoop 在别的 goroutine 里跑时也不会竞态）。
type recorder struct {
	mu   sync.Mutex
	recs []pushRec
}

func (rec *recorder) push(playerID string, msgID uint32, v any) error {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	fp, _ := v.(FramePush)
	rec.recs = append(rec.recs, pushRec{playerID: playerID, msgID: msgID, push: fp})
	return nil
}

func (rec *recorder) count() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return len(rec.recs)
}

func (rec *recorder) all() []pushRec {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	out := make([]pushRec, len(rec.recs))
	copy(out, rec.recs)
	return out
}

// newTestService 造一个可控房间服务：记录每次广播 + 注入确定性输入应用器。
// 注意**不设** PushMessageID —— 那正是根因 1 要验证的点。
func newTestService(rec *recorder) *Service {
	return NewService(
		WithBroadcaster(rec.push),
		WithInputApplier(func(roomID string, frame int64, inputs map[string]Input) map[string]*PlayerState {
			out := make(map[string]*PlayerState, len(inputs))
			for pid := range inputs {
				out[pid] = &PlayerState{PlayerID: pid, HP: 100, LastFrame: frame, LastAction: "noop"}
			}
			return out
		}),
	)
}

// 场景 A（根因 1）：默认配置不设 PushMessageID ⇒ **帧能推进，但广播 0 条**。
//
// 这条断言直接把"Broadcast 失败"翻译成了"配置少了消息号"。
// 注意：静默跳过是**已登记的设计**（见 room.go broadcast 的 ⚠️ 注释：0 = 不广播），
// 不是待修 bug —— 建房间时必须显式 frame.WithPushMessageID(...)。
func TestB1_BroadcastSilentWhenPushMessageIDUnset(t *testing.T) {
	rec := &recorder{}
	svc := newTestService(rec)

	// WithAutoStart(false)：不让 ticker 自己跑，测试手动 step()，结果才确定。
	r, err := svc.NewRoom("A", WithAutoStart(false))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svc.Join("A", "p1"); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// 每帧都提交输入 ⇒ 输入到齐 ⇒ 帧应当推进（推进本身没问题）
	for i := 0; i < 3; i++ {
		if err := svc.Input("A", "p1", Input{}); err != nil {
			t.Fatalf("Input #%d: %v", i, err)
		}
		r.step()
	}

	if got := r.Frame(); got != 3 {
		t.Fatalf("帧号 = %d, 期望 3（输入到齐就该推进）", got)
	}
	if n := rec.count(); n != 0 {
		var ids []uint32
		for _, x := range rec.all() {
			ids = append(ids, x.msgID)
		}
		t.Fatalf("PushMessageID 未配置时 broadcast 应静默跳过，实际广播 %d 条 msgID=%v", n, ids)
	}
	t.Log("根因 1 复现：帧推进正常，但广播 0 条 —— 缺 PushMessageID 就是「Broadcast 失败」的真相")
}

// 场景 B（修法）：显式 `WithPushMessageID(1004401)` ⇒ 每帧广播到每个成员。
//
// 1004401 就是 B1 记录里的那个自测消息号。
func TestB1_BroadcastFiresWithPushMessageID(t *testing.T) {
	rec := &recorder{}
	svc := newTestService(rec)

	r, err := svc.NewRoom("B", WithAutoStart(false), WithPushMessageID(1004401))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svc.Join("B", "p1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := svc.Input("B", "p1", Input{}); err != nil {
		t.Fatalf("Input: %v", err)
	}
	r.step()

	recs := rec.all()
	if len(recs) == 0 {
		t.Fatal("配了 PushMessageID 仍然没有广播")
	}
	for i, x := range recs {
		if x.msgID != 1004401 {
			t.Fatalf("第 %d 条广播 msgID = %d, 期望 1004401", i, x.msgID)
		}
		if x.playerID != "p1" {
			t.Fatalf("第 %d 条广播收件人 = %q, 期望 p1", i, x.playerID)
		}
		if x.push.Frame != 1 {
			t.Fatalf("第 %d 条广播 Frame = %d, 期望 1", i, x.push.Frame)
		}
		if x.push.Waiting {
			t.Fatalf("第 %d 条广播不应是等待态（输入已到齐）", i)
		}
	}
	t.Logf("修法验证：配好 PushMessageID 后每帧正常广播（本帧 %d 条）", len(recs))
}

// 场景 C：**等待态**的帧推同样依赖 PushMessageID。
//
// 漏了它，客户端永远看不到"还在等谁提交输入"，表现为"房间卡住且无任何提示"。
func TestB1_WaitingPushAlsoNeedsPushMessageID(t *testing.T) {
	rec := &recorder{}
	svc := newTestService(rec)

	r, err := svc.NewRoom("C", WithAutoStart(false), WithPushMessageID(1004401))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	for _, pid := range []string{"p1", "p2"} {
		if err := svc.Join("C", pid); err != nil {
			t.Fatalf("Join(%s): %v", pid, err)
		}
	}
	// 只让 p1 提交 ⇒ p2 缺失 ⇒ 走 waiting 分支
	if err := svc.Input("C", "p1", Input{}); err != nil {
		t.Fatalf("Input: %v", err)
	}
	r.step()

	if got := r.Frame(); got != 0 {
		t.Fatalf("输入不齐不应推进，帧号 = %d", got)
	}
	recs := rec.all()
	if len(recs) == 0 {
		t.Fatal("等待态没有广播：客户端将看不到「在等谁」")
	}
	last := recs[len(recs)-1]
	if !last.push.Waiting {
		t.Fatalf("最后一条广播应是等待态，实际 Waiting=%v", last.push.Waiting)
	}
	if len(last.push.Missing) != 1 || last.push.Missing[0] != "p2" {
		t.Fatalf("缺失输入列表 = %v, 期望 [p2]", last.push.Missing)
	}
	if last.push.WaitingReason != "waiting_input" {
		t.Fatalf("等待原因 = %q, 期望 waiting_input", last.push.WaitingReason)
	}
}

// 场景 D（根因 2 回归）：**单人从不提交输入也不再自锁**。
//
// 判据不再是帧号差（卡帧时恒为 0/1，阈值永不满足），而是 inputWaitTicks（连续未提交的 step 次数）：
// ①②分别钉住「阈值未满仍等待」与「达到阈值即兜底推进」，防止把"帧永远停在 0"重新当成应然行为。
func TestB1_InputTimeoutFallbackUnlocksFrames(t *testing.T) {
	// --- D1：默认阈值（90）→ 阈值未满仍等待，第 90 次 step 兜底生效 ---
	recLenient := &recorder{}
	svcLenient := newTestService(recLenient)
	r90, err := svcLenient.NewRoom("D90", WithAutoStart(false))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svcLenient.Join("D90", "p1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	for i := 0; i < 89; i++ {
		r90.step() // p1 从不提交输入
	}
	if got := r90.Frame(); got != 0 {
		t.Fatalf("阈值未满（89 < 90）不该推进，实际 frame=%d", got)
	}
	r90.step() // 第 90 次：连续未提交次数达到阈值 → 填 fallback 输入
	if got := r90.Frame(); got != 1 {
		t.Fatalf("第 90 次 step 应触发输入超时兜底并推进到 1，实际 frame=%d（帧自锁未修复）", got)
	}

	// --- D2：阈值 = 1 → 兜底立刻生效 ---
	recTight := &recorder{}
	svcTight := newTestService(recTight)
	r1, err := svcTight.NewRoom("D1", WithAutoStart(false), WithInputTimeoutTicks(1))
	if err != nil {
		t.Fatalf("NewRoom: %v", err)
	}
	if err := svcTight.Join("D1", "p1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	for i := 0; i < 5; i++ {
		r1.step()
	}
	if got := r1.Frame(); got != 5 {
		t.Fatalf("阈值=1 时应由超时兜底推进到 5，实际 frame=%d", got)
	}
	t.Log("根因 2 回归：阈值按 step 次数计，达到阈值即兜底推进，不再依赖帧号差")
}
