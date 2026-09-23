package event

import (
	"errors"
	"testing"

	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// TestReplyContractPushOnlyVsError 固化「handler 返回值 ⟺ 客户端能否收到回包」这条契约。
//
// # 为什么需要这条用例
//
// 客户端发的是 **Call**（等回包）。而框架的规则是（`logic.go:63`）：
//   - handler 返回**非 nil** ⇒ 框架强制回 `EErrorReply`（`logic.go:748-750`：`slot.force(l.errMsgID, encodeErrorReply(herr))`）；
//   - handler 返回 **nil 且未 `MarkReplied`** ⇒ **不回包**（fire-and-forget），客户端**必然超时**。
//
// 于是出现过一个真实缺陷（业务仓 clover-project-lol-howling-abyss 的「bug#5」）：
// 拒绝分支只调 `Game.Alert`（**主动推送**）而**没有** `return` 错误 ⇒
// 服务端确实"说了话"（弹窗推到了客户端），但 `Call` 等的那条**回包**永远不会来，
// 客户端把"服务端明确拒绝"误报成"链路超时"。
//
// # 本用例的两拍
//
//	拍(i)  只推送 + 返回 nil  ⇒ 断言**无回包**
//	拍(ii) 返回非 nil error   ⇒ 断言回包 `MsgID == errMsgID(EMsgError)` 且 body 可解为 `EErrorReply{Err}`
//
// 拍(i) 就是**修之前**的形状（只 Alert、return nil），拍(ii) 就是**修之后**的形状
// （`reject()` = 先 Alert 再 `return errors.New(文案)`）。
// ⇒ 同一个断言下，旧行为 FAIL / 新行为 PASS。
//
// # 拍(i) 与「只 Alert」的等价性（为什么 stub 可以不真的调 Alert）
//
// `Game.Alert`（`internal/app/facade.go:508`）→ `Game.Alert`（`internal/app/game.go:563`）
// → `Core.AlertToPlayer`（`internal/app/core.go:159`）**只**把告警 publish 到
// `notifyPub / notifySubject`（PUSH 通道）；它**不接收 `*Ctx`**、也**不触碰 `replySlot`**
// ⇒ 「主动推送」与「回包」在引擎里是**两条互不影响的通道**。
// 因此在**回包维度**上，「只 Alert + return nil」==「什么都不做 + return nil」。
// 本文件是 `package event` 的内层用例（要读未导出的 `replySlot` / `errMsgID`），
// ⛔ 不能 import `internal/app`（会形成 app→event 的导入环），所以拍(i) 用"返回 nil"来表示那个形状。
//
// ⚠️ 反向警戒（别把本用例当"业务侧可以直接抄的写法"）：拍(i) 是**反例**。
// 任何"客户端 `Call` 等回包"的消息，其拒绝分支都必须 `return` 一个非 nil error（或显式 `MarkReplied`）。
func TestReplyContractPushOnlyVsError(t *testing.T) {
	// ---- 号段 / 错误回包 opcode 的前置契约（独立于业务的可证伪点，改了就响）----
	if proto.InternalMsgMax != 10000 {
		t.Fatalf("InternalMsgMax=%d，应为 10000：业务号段契约被破坏，OnMsg 的拒绝边界会随之改变", proto.InternalMsgMax)
	}
	if proto.EMsgError != 0xFFFFFFFF {
		t.Fatalf("EMsgError=0x%X，应为 0xFFFFFFFF：错误回包的 opcode 被改动，客户端按号匹配的逻辑要同步改", proto.EMsgError)
	}

	l := New(Config{Headless: true})

	const (
		// ⚠️ 两个 stub 必须用**不同 msgID**：`OnMsg`（logic.go:337-）对
		// 「同 msgID + 同 handler 函数指针 + 同 priority」会**静默去重**，
		// 同号注册两次等于只注册了一次。
		msgPushOnly = proto.InternalMsgMax + 9001
		msgReject   = proto.InternalMsgMax + 9002

		rejectReason = "法力不足，无法施放"
	)

	pushOnlyCalls := 0
	// 拍(i)：fire-and-forget 形状（== 修之前的"只 Alert + return nil"）
	l.OnMsg(msgPushOnly, func(_ *Ctx) error {
		pushOnlyCalls++
		return nil
	})
	// 拍(ii)：拒绝形状（== 修之后 `reject()` 的形状：推送照发，同时 return error）
	l.OnMsg(msgReject, func(_ *Ctx) error {
		return errors.New(rejectReason)
	})

	req := func(msgID uint32, requestID uint32) *proto.GWLogicPacket {
		return &proto.GWLogicPacket{
			ConnID:    "c-bug5",
			Owner:     "acc-bug5",
			RequestID: requestID,
			MsgID:     msgID,
			Body:      []byte(`{}`),
		}
	}

	t.Run("拍(i)_只推送返回nil_必须无回包", func(t *testing.T) {
		got := l.Dispatch(req(msgPushOnly, 1))
		if pushOnlyCalls != 1 {
			t.Fatalf("拍(i) FAIL：handler 未被派发（pushOnlyCalls=%d want=1）", pushOnlyCalls)
		}
		if got != nil {
			t.Fatalf("拍(i) FAIL：未 MarkReplied 且返回 nil 的 handler 竟然产出了回包 "+
				"msgID=%d requestID=%d body=%s —— 说明框架行为变了，客户端超时语义要重新评估",
				got.MsgID, got.RequestID, string(got.Body))
		}
	})

	t.Run("拍(ii)_返回error_必须回EErrorReply", func(t *testing.T) {
		got := l.Dispatch(req(msgReject, 2))
		if got == nil {
			t.Fatalf("拍(ii) FAIL：handler 返回了 error，框架却没有回包 —— "+
				"客户端 Call 会一直等到超时（这正是 bug#5 的观测形态）")
		}
		if got.RequestID != 2 {
			t.Fatalf("拍(ii) FAIL：回包 RequestID=%d want=2（回包按 requestID 与请求配对，配错等于没回）", got.RequestID)
		}
		if got.MsgID != l.errMsgID {
			t.Fatalf("拍(ii) FAIL：回包 MsgID=%d want=errMsgID=%d", got.MsgID, l.errMsgID)
		}
		if got.MsgID != proto.EMsgError {
			t.Fatalf("拍(ii) FAIL：回包 MsgID=0x%X want=EMsgError(0x%X)", got.MsgID, proto.EMsgError)
		}
		var rep proto.EErrorReply
		if err := ujson.Unmarshal(got.Body, &rep); err != nil {
			t.Fatalf("拍(ii) FAIL：回包 body 不是 EErrorReply: err=%v body=%s", err, string(got.Body))
		}
		if rep.Err != rejectReason {
			t.Fatalf("拍(ii) FAIL：回包 Err=%q want=%q（拒绝原因必须原样带回，客户端靠它显示提示）", rep.Err, rejectReason)
		}
	})

	// 两拍对照表：**同一条断言**——「客户端到底有没有收到回包」（= `Dispatch` 返回值非 nil）。
	//
	//	旧实现形状（`orders.go` 修前：只 Alert + return nil） ⇒ 无回包 ⇒ 客户端 Call 超时 ⇒ FAIL
	//	新实现形状（`orders.go` 修后：reject() 先 Alert 再 return error） ⇒ 有回包 ⇒ PASS
	//
	// 与上面两个子用例的关系：拍(i)/拍(ii) 是把两侧的**期望分别钉死**（无回包 / 有回包且字段正确）；
	// 本表把它们**并到同一条断言下对拍**，直接读出 FAIL/PASS 两拍，避免"两次断言不同、不能相减"的争议。
	t.Run("两拍对照_同一断言_旧FAIL_新PASS", func(t *testing.T) {
		cases := []struct {
			name      string
			handler   Handler
			wantReply bool // 「客户端收到回包」的期望值
		}{
			{
				name:      "旧实现形状(只推送+return nil)",
				handler:   func(_ *Ctx) error { return nil },
				wantReply: false,
			},
			{
				name:      "新实现形状(return error)",
				handler:   func(_ *Ctx) error { return errors.New(rejectReason) },
				wantReply: true,
			},
		}
		for i, tc := range cases {
			// ⚠️ 每条用例必须用**不同 msgID**：OnMsg 对「同 msgID + 同 handler + 同 priority」去重。
			msgID := uint32(proto.InternalMsgMax + 9100 + i)
			l.OnMsg(msgID, tc.handler)
			got := l.Dispatch(req(msgID, uint32(100+i)))
			gotReply := got != nil
			verdict := "FAIL"
			if gotReply == tc.wantReply {
				verdict = "PASS"
			}
			// t.Logf 而非 t.Errorf：本表是**对照读数**，PASS/FAIL 由下面的一致性判定给出。
			t.Logf("[%s] 客户端收到回包=%v 期望=%v ⇒ %s", tc.name, gotReply, tc.wantReply, verdict)
			if gotReply != tc.wantReply {
				t.Fatalf("[%s] 旧/新形状与期望不符：客户端收到回包=%v 期望=%v", tc.name, gotReply, tc.wantReply)
			}
		}
	})
}
