package gwcore

import (
	"encoding/json"
	"sync"
	"testing"

	"clover-server-engine/internal/shared/proto"
	isession "clover-server-engine/internal/transport/net/session"
)

// 本文件守住两件事：
//  1. 排队位置帧**真的发得出去**——留了钩子没人接线、或钩子只给 connID 拿不到连接，
//     都是"看着能用其实用不了"。测试直接断言客户端收到的字节。
//  2. 位置语义与刷新纪律——ahead 是「前面还有多少人」（0 = 队首），
//     位置没变不重发（否则排队期间会以刷新频率刷屏）。

// stubSession 测试替身：记录收到的下行帧，其余能力按最小实现。
type stubSession struct {
	connID string
	mu     sync.Mutex
	sent   [][]byte
	closed bool
}

func (s *stubSession) ConnID() string { return s.connID }

func (s *stubSession) Send(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, append([]byte(nil), data...))
	return nil
}

func (s *stubSession) SendUnreliable(data []byte) error { return s.Send(data) }

func (s *stubSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *stubSession) RemoteAddr() string { return "127.0.0.1:1" }

func (s *stubSession) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *stubSession) Capabilities() isession.ConnCapabilities {
	return isession.ConnCapabilities{Reliable: true}
}

func (s *stubSession) frames() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.sent...)
}

// lastQueuePosition 解出最近一条位置帧（并校验它确实是引擎约定的位置帧形态）。
func lastQueuePosition(t *testing.T, s *stubSession) proto.EQueuePositionNotify {
	t.Helper()
	frames := s.frames()
	if len(frames) == 0 {
		t.Fatalf("conn %s 未收到任何下行帧", s.connID)
	}
	requestID, msgID, body, err := proto.DecodeClientFrame(frames[len(frames)-1])
	if err != nil {
		t.Fatalf("conn %s 下行帧解码失败: %v", s.connID, err)
	}
	if requestID != 0 {
		t.Errorf("位置帧应为推送（requestID=0），实际 %d", requestID)
	}
	if msgID != proto.EMsgQueuePosition {
		t.Fatalf("下行帧应为 EMsgQueuePosition(%d)，实际 %d", proto.EMsgQueuePosition, msgID)
	}
	var n proto.EQueuePositionNotify
	if err := json.Unmarshal(body, &n); err != nil {
		t.Fatalf("位置帧 body 不是 EQueuePositionNotify: %v (body=%s)", err, body)
	}
	return n
}

// newTestGateway 构造只带等候队列的网关（位置通知走真实下发路径）。
func newTestGateway(cap int) *Gateway {
	g := &Gateway{}
	g.queue = newConnQueue(cap, 0, nil, g.notifyQueued)
	return g
}

// TestQueuePositionNotifiedOnEnqueue 入队即下发，且 ahead/total/ticket 语义正确：
// 第 1 人 ahead=0（队首）、第 2 人 ahead=1；编号单调递增只用于展示。
func TestQueuePositionNotifiedOnEnqueue(t *testing.T) {
	g := newTestGateway(4)

	first := &stubSession{connID: "c-1"}
	if !g.tryQueue("c-1", first, []byte("first-frame-1")) {
		t.Fatal("c-1 入队失败")
	}
	if got := lastQueuePosition(t, first); got.Ahead != 0 || got.Total != 1 || got.Ticket != 1 {
		t.Errorf("c-1 位置错误：ahead=%d total=%d ticket=%d，期望 0/1/1", got.Ahead, got.Total, got.Ticket)
	}

	second := &stubSession{connID: "c-2"}
	if !g.tryQueue("c-2", second, []byte("first-frame-2")) {
		t.Fatal("c-2 入队失败")
	}
	if got := lastQueuePosition(t, second); got.Ahead != 1 || got.Total != 2 || got.Ticket != 2 {
		t.Errorf("c-2 位置错误：ahead=%d total=%d ticket=%d，期望 1/2/2", got.Ahead, got.Total, got.Ticket)
	}

	// 首帧仍被缓冲（放行时重放），未被位置通知挤掉。
	if got := g.queue.len(); got != 2 {
		t.Errorf("队列长度 %d，期望 2", got)
	}
}

// TestQueuePositionRefreshOnlyOnChange 位置前进后续发；位置没变不重发。
func TestQueuePositionRefreshOnlyOnChange(t *testing.T) {
	g := newTestGateway(4)

	first := &stubSession{connID: "c-1"}
	second := &stubSession{connID: "c-2"}
	g.tryQueue("c-1", first, nil)
	g.tryQueue("c-2", second, nil)

	// 模拟放行循环放行队首（c-1），c-2 前进到队首。
	if pc := g.queue.dequeue(); pc == nil || pc.raw.ConnID() != "c-1" {
		t.Fatalf("dequeue 未取到队首 c-1")
	}
	if n := len(first.frames()); n != 1 {
		t.Errorf("已放行的 c-1 不该再收到位置帧，实际收到 %d 条", n)
	}

	g.queue.refreshPositions()
	if got := lastQueuePosition(t, second); got.Ahead != 0 || got.Total != 1 {
		t.Errorf("c-2 前进后位置错误：ahead=%d total=%d，期望 0/1", got.Ahead, got.Total)
	}
	if n := len(second.frames()); n != 2 {
		t.Fatalf("c-2 应收到 2 条位置帧（入队 + 前进），实际 %d 条", n)
	}

	// 位置未变：刷新不再下发。
	g.queue.refreshPositions()
	if n := len(second.frames()); n != 2 {
		t.Errorf("位置未变时不该重发，实际共 %d 条", n)
	}
}

// TestQueuePositionRequeueNotifiesHead 放行时仍满载被放回队首 → 位置回 0，
// 客户端不会停在放行前看到的旧数字上。
func TestQueuePositionRequeueNotifiesHead(t *testing.T) {
	g := newTestGateway(4)

	first := &stubSession{connID: "c-1"}
	g.tryQueue("c-1", first, nil)

	pc := g.queue.dequeue()
	if pc == nil {
		t.Fatal("dequeue 未取到 c-1")
	}
	// releasePending 在仍满载 / 上游不可用时走这条路（此处直接调队列方法模拟该分支）。
	g.queue.requeueHead(pc)
	g.queue.refreshPositions()

	if got := lastQueuePosition(t, first); got.Ahead != 0 || got.Total != 1 {
		t.Errorf("回队首后位置错误：ahead=%d total=%d，期望 0/1", got.Ahead, got.Total)
	}
}

// TestQueueNotEnabledNoNotify 未启用排队（队列为 nil）时：入队返回 false，绝不发帧。
func TestQueueNotEnabledNoNotify(t *testing.T) {
	g := &Gateway{} // 未构造 queue
	s := &stubSession{connID: "c-1"}
	if g.tryQueue("c-1", s, nil) {
		t.Fatal("未启用排队时 tryQueue 应返回 false")
	}
	if n := len(s.frames()); n != 0 {
		t.Errorf("未启用排队时不该下发位置帧，实际 %d 条", n)
	}
}
