package mmo

import (
	"encoding/json"
	"sync"
	"testing"

	idata "clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/shared/proto"
)

// 本文件是「视野进出事件不下发客户端」这一真实缺陷的复现用例。
//
// 缺陷：Scene 的 AOI 在实体进出视野时把 viewPush（含 Event enter/leave）发到 viewSubject，
// 但 viewSubject 的唯一消费者 EntitySync.onViewChange 只维护反向索引、**不向客户端推送**，
// 导致客户端 Game.Sync.OnEntityEnter/OnEntityLeave 永远不触发。
//
// 修复后本用例应通过：enter/leave 必须各产生一条 EPushDataSync(4003) 推给 watcher，
// body 外层 key 为事件名（客户端 WorldSync 只认这个形状），内层带 entity_id。

// fakePub 捕获发布到各 subject 的原始载荷，实现 idata.Publisher。
type fakePub struct {
	mu   sync.Mutex
	msgs map[string][][]byte
}

func newFakePub() *fakePub { return &fakePub{msgs: make(map[string][][]byte)} }

func (p *fakePub) Publish(subject string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	p.msgs[subject] = append(p.msgs[subject], cp)
	return nil
}

// take 取出并清空某 subject 上的载荷（模拟 NATS 送达给订阅方）。
func (p *fakePub) take(subject string) [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.msgs[subject]
	delete(p.msgs, subject)
	return out
}

// fakeSub 捕获订阅 handler，供测试手工投递（等价 NATS 送达）。
type fakeSub struct {
	mu       sync.Mutex
	handlers map[string]func(subject string, payload []byte)
}

func (s *fakeSub) Subscribe(subject string, handler func(subject string, payload []byte)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handlers == nil {
		s.handlers = make(map[string]func(string, []byte))
	}
	s.handlers[subject] = handler
	return nil
}

func (s *fakeSub) deliver(subject string, payload []byte) bool {
	s.mu.Lock()
	h := s.handlers[subject]
	s.mu.Unlock()
	if h == nil {
		return false
	}
	h(subject, payload)
	return true
}

// fakeAcc 只提供 NotifySubject（WireEntitySync 需要的最小能力面）。
type fakeAcc struct{ subject string }

func (a fakeAcc) NotifySubject() string { return a.subject }

// pushRecord 一条被推给玩家的记录（解码自 NotifyPush）。
type pushRecord struct {
	Subject string
	Target  string
	MsgID   uint32
	Mode    proto.DeliveryMode
	Body    []byte
}

// collectPushes 解码发往 clover.notify 的全部载荷。
func collectPushes(t *testing.T, pub *fakePub) []pushRecord {
	t.Helper()
	var out []pushRecord
	for _, raw := range pub.take(proto.NATSSubjectNotify) {
		np, err := proto.DecodeNotifyPush(raw)
		if err != nil {
			t.Fatalf("DecodeNotifyPush: %v", err)
		}
		out = append(out, pushRecord{
			Subject: proto.NATSSubjectNotify,
			Target:  np.Target,
			MsgID:   np.MsgID,
			Mode:    np.DeliveryMode,
			Body:    np.Body,
		})
	}
	return out
}

// findViewEvent 在推送里找一条「4003 且 body 外层 key 命中事件名、内层 entity_id 命中」的记录。
func findViewEvent(records []pushRecord, target string, event string, entityID uint64) *pushRecord {
	for i := range records {
		r := &records[i]
		if r.MsgID != proto.EPushDataSync || r.Target != target {
			continue
		}
		var body map[string]struct {
			EntityID uint64 `json:"entity_id"`
		}
		if err := json.Unmarshal(r.Body, &body); err != nil {
			continue
		}
		inner, ok := body[event]
		if !ok || inner.EntityID != entityID {
			continue
		}
		return r
	}
	return nil
}

func TestViewEnterLeavePushedToWatcher(t *testing.T) {
	const (
		viewSubject = "mmo.view"
		watcherID   = uint64(1001)
		targetID    = uint64(1002)
	)

	pub := newFakePub()
	sub := &fakeSub{}
	if err := WireEntitySync(viewSubject, fakeAcc{subject: "test.notify"}, sub, idata.Publisher(pub)); err != nil {
		t.Fatalf("WireEntitySync 失败: %v", err)
	}

	sm := NewSceneManager(WithPublisher(pub), WithViewSubject(viewSubject))
	scene := sm.CreateScene(1, "test-map")

	// 观察者进场景并开启视野。
	if err := scene.Enter(watcherID, Vec3{X: 0, Y: 0, Z: 0}); err != nil {
		t.Fatalf("watcher Enter 失败: %v", err)
	}
	scene.SetViewRadius(watcherID, 50)

	// 目标进入观察者视野范围 → 应产生 enter 视野事件。
	if err := scene.Enter(targetID, Vec3{X: 10, Y: 0, Z: 0}); err != nil {
		t.Fatalf("target Enter 失败: %v", err)
	}
	drainViewSubject(t, pub, sub, viewSubject)

	enters := collectPushes(t, pub)
	enterRec := findViewEvent(enters, "1001", "enter", targetID)
	if enterRec == nil {
		t.Fatalf("视野 enter 未下发到 watcher 1001：4003 推送共 %d 条，收到=%s", len(enters), dumpRecords(enters))
	}
	if enterRec.Mode != proto.DeliveryModeReliable {
		t.Errorf("enter 事件应为可靠传输，实际 DeliveryMode=%d", enterRec.Mode)
	}

	// 目标离开场景 → 应产生 leave 视野事件。
	scene.Leave(targetID)
	drainViewSubject(t, pub, sub, viewSubject)

	leaves := collectPushes(t, pub)
	leaveRec := findViewEvent(leaves, "1001", "leave", targetID)
	if leaveRec == nil {
		t.Fatalf("视野 leave 未下发到 watcher 1001：4003 推送共 %d 条，收到=%s", len(leaves), dumpRecords(leaves))
	}
	if leaveRec.Mode != proto.DeliveryModeReliable {
		t.Errorf("leave 事件应为可靠传输，实际 DeliveryMode=%d", leaveRec.Mode)
	}
}

// drainViewSubject 把 viewSubject 上的载荷逐条投递给订阅方（模拟 NATS 送达），
// 使 Scene 发布 → EntitySync 消费 → 推送玩家 这条链路在单进程内完整跑一遍。
func drainViewSubject(t *testing.T, pub *fakePub, sub *fakeSub, subject string) {
	t.Helper()
	for _, raw := range pub.take(subject) {
		if !sub.deliver(subject, raw) {
			t.Fatalf("viewSubject %q 无订阅者（WireEntitySync 未生效）", subject)
		}
	}
}

func dumpRecords(records []pushRecord) string {
	out, _ := json.Marshal(records)
	return string(out)
}
