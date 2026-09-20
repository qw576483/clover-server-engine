package event

import (
	"testing"

	"clover-server-engine/internal/shared/proto"
)

// 固化「收包路径不做框架级去重」这条边界。
//
// dispatch 只按 MsgID 找 handler（logic.go:643-649），requestID 仅用于回包配对；
// 因此同一条 C2S 帧被投递两次（客户端重发 / 网关双路 / 重连重放）时，handler 会被执行两次。
// 幂等责任在业务 handler，或由投递层承担（跨节点可靠投递有自己的 deduper）。
//
// 本用例同时是**契约守卫**：若将来在收包路径引入框架级去重，它会失败，
// 提醒同步更新 clover-doc 与业务侧的幂等约定。
func TestDispatchDoesNotDeduplicateSameFrame(t *testing.T) {
	l := New(Config{Headless: true})

	const msgID = proto.InternalMsgMax + 1 // 业务号段（引擎保留号会被 OnMsg panic 拒绝）
	calls := 0
	l.OnMsg(msgID, func(_ *Ctx) error {
		calls++
		return nil
	})

	pkt := &proto.GWLogicPacket{ConnID: "c-1", RequestID: 7, MsgID: msgID, Body: []byte("x")}
	l.Dispatch(pkt)
	l.Dispatch(pkt)

	if calls != 2 {
		t.Fatalf("同一帧投递两次，handler 执行了 %d 次；当前契约是不去重（应为 2 次）", calls)
	}
}
