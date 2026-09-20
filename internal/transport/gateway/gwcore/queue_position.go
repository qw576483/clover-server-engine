package gwcore

import (
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/pkg/foundation/logger"
	ujson "clover-server-engine/pkg/shared/json"
)

// 排队位置通知（EMsgQueuePosition）：把「您前面还有 N 人」真正送到客户端。
//
// 为什么必须由**网关自己发**，不能只留一个回调给业务：
// 排队中的连接**尚未建立会话**（既不在 sessionShards 里，也没有 crypto / upstream），
// 网关手上只有一个 connID 字符串。若只回调解一个 connID，业务即便拿到位置也**无连接可写**，
// 于是能力看着有、实际发不出去。所以下发职责归网关：它持有 raw 连接（isession.Session），
// 能直接编帧写回；业务侧回调（WithOnQueued）只保留为观测点（埋点 / 日志）。
//
// 帧形态：requestID=0（推送，无需回包）+ msgID=EMsgQueuePosition + JSON body。
// 与 EMsgUDPBindGrant 同族——都是网关直发的引擎 S2C 帧；此时会话尚未协商加密，
// 因此按明文发送（客户端在登录回包后才启用会话密钥，与 replyUnauthenticated 同款语义）。

// sendQueuePosition 向排队中的连接下发一条位置通知；返回是否发送成功。
//
// ahead = 前面还有多少人（0 = 已到队首），total = 队列总人数（含自己），ticket = 入队序号。
// 发送失败只留痕不中断排队：连接可能已断开（会走闭环移除），位置通知本就是尽力而为的体验信息。
func (g *Gateway) sendQueuePosition(pc *pendingConn, ahead, total int) bool {
	if pc == nil || pc.raw == nil {
		return false
	}
	connID := pc.raw.ConnID()
	body, err := ujson.Marshal(proto.EQueuePositionNotify{
		Ahead:  ahead,
		Total:  total,
		Ticket: pc.ticket,
	})
	if err != nil {
		logger.Errorf("gwcore: encode queue position for %s: %v", connID, err)
		return false
	}
	frame := proto.EncodeClientFrame(0, proto.EMsgQueuePosition, body)
	if err := pc.raw.Send(frame); err != nil {
		logger.Warnf("gwcore: send queue position to %s (ahead=%d): %v", connID, ahead, err)
		return false
	}
	metricSentFrame(len(frame))
	logger.Debugf("gwcore: queued conn %s notified ahead=%d total=%d ticket=%d", connID, ahead, total, pc.ticket)
	return true
}

// notifyQueued 位置变化时的统一出口：引擎下发 + 业务观测回调。
//
// 顺序固定为「先下发、后回调」：回调（若抛错）不该影响引擎自己的下发；
// 回调语义已从「由你下发」改为「给你观测」（见 WithOnQueued 注释）。
func (g *Gateway) notifyQueued(pc *pendingConn, ahead, total int) {
	if pc == nil || pc.raw == nil {
		return
	}
	// 下发成功后回写已通知位置（offer/requeueHead 置的 -1 → 实际位置）：
	// 不回写则 refreshPositions 判定「未通知」会把同一条位置再发一遍（队首连接必然命中）。
	// 失败时保持待通知状态，交给刷新循环按位置变化重试。
	if g.sendQueuePosition(pc, ahead, total) {
		g.queue.markNotified(pc, ahead)
	}
	if g.onQueued != nil {
		g.onQueued(pc.raw.ConnID(), ahead)
	}
}
