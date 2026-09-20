package mmo

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	idata "clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/transport"
)

// ============================================================================
// 订阅反注册（生命周期对称性）回归
//
// 守的历史缺陷：WireEntitySync 订阅了 viewSubject 与 accessor 的 notify subject，
// 而两条订阅**没有对称的反注册出口** ——
//   - transport/nats.Client 只保存 []*nats.Subscription，不保存「句柄 ↔ subject」的对应
//     关系，上层根本无法只退自己那几条；
//   - mmo.Module.Stop 只停 SceneManager，订阅照旧被派发 ⇒ 模块已停、回调还在跑。
// 现在：Client 按 subject 记句柄并暴露 Unsubscribe；pubsub.Unsubscriber 是可选能力；
// EntitySync.Close 逐条退订且幂等。
// ============================================================================

// unsubCapableSub 既能订阅、也实现了 pubsub.Unsubscriber（记录退订过的 subject）。
type unsubCapableSub struct {
	mu          sync.Mutex
	handlers    map[string]bool
	unsubbed    []string
	unsubErr    error
	unsubCalled int
}

func newUnsubCapableSub() *unsubCapableSub {
	return &unsubCapableSub{handlers: make(map[string]bool)}
}

func (s *unsubCapableSub) Subscribe(subject string, _ func(subject string, payload []byte)) error {
	s.mu.Lock()
	s.handlers[subject] = true
	s.mu.Unlock()
	return nil
}

func (s *unsubCapableSub) Unsubscribe(subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsubCalled++
	if s.unsubErr != nil {
		return s.unsubErr
	}
	delete(s.handlers, subject)
	s.unsubbed = append(s.unsubbed, subject)
	return nil
}

func (s *unsubCapableSub) state() (subjects []string, unsubbed []string, calls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s := range s.handlers {
		subjects = append(subjects, s)
	}
	sort.Strings(subjects)
	unsubbed = append(unsubbed, s.unsubbed...)
	sort.Strings(unsubbed)
	return subjects, unsubbed, s.unsubCalled
}

// Close 必须把 wireEntitySync 注册的两条订阅都退掉，且幂等（重复 Close 不重复退订）。
func TestEntitySyncCloseUnsubscribesBothSubjects(t *testing.T) {
	const viewSubject = "test.view"
	const notifySubject = "test.notify"

	sub := newUnsubCapableSub()
	pub := newFakePub()

	es, err := WireEntitySyncHandle(viewSubject, fakeAcc{subject: notifySubject}, sub, idata.Publisher(pub))
	if err != nil {
		t.Fatalf("WireEntitySyncHandle 失败: %v", err)
	}
	subjects, _, _ := sub.state()
	if len(subjects) != 2 {
		t.Fatalf("应注册 2 条订阅，实际 %v", subjects)
	}

	es.Close()

	subjects, unsubbed, calls := sub.state()
	if len(subjects) != 0 {
		t.Fatalf("Close 后不该还有订阅，实际 %v", subjects)
	}
	if len(unsubbed) != 2 {
		t.Fatalf("应退订 2 条，实际 %v（调用 %d 次）", unsubbed, calls)
	}
	want := []string{notifySubject, viewSubject}
	sort.Strings(want)
	for i := range want {
		if unsubbed[i] != want[i] {
			t.Fatalf("退订集合应为 %v，实际 %v", want, unsubbed)
		}
	}

	// 幂等：重复 Close 不该再调 Unsubscribe。
	es.Close()
	if _, _, calls2 := sub.state(); calls2 != calls {
		t.Fatalf("Close 必须幂等，实际 Unsubscribe 调用次数从 %d 变成 %d", calls, calls2)
	}
}

// 第二条订阅失败时必须把第一条退掉：否则调用方拿到 error 就走人，
// 那条 viewSubject 订阅会永远悬挂（部分接线泄漏）。
func TestEntitySyncPartialSubscribeFailureUnsubscribesFirst(t *testing.T) {
	sub := &failSecondSub{}
	pub := newFakePub()

	if _, err := WireEntitySyncHandle("test.view", fakeAcc{subject: "test.notify"}, sub, idata.Publisher(pub)); err == nil {
		t.Fatal("第二条订阅失败时应返回 error")
	}
	if !sub.firstUnsubbed {
		t.Fatal("第二条订阅失败时必须把第一条订阅退掉，否则它永远悬挂")
	}
}

type failSecondSub struct {
	mu            sync.Mutex
	n             int
	firstUnsubbed bool
}

func (s *failSecondSub) Subscribe(_ string, _ func(subject string, payload []byte)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	if s.n >= 2 {
		return errors.New("subscribe boom")
	}
	return nil
}

func (s *failSecondSub) Unsubscribe(_ string) error {
	s.mu.Lock()
	s.firstUnsubbed = true
	s.mu.Unlock()
	return nil
}

// 订阅方不支持反注册时：不许 panic，也不许阻断（降级为告警，退化成旧行为）。
func TestEntitySyncCloseWithoutUnsubscriberIsNoop(t *testing.T) {
	sub := &fakeSub{}
	pub := newFakePub()

	es, err := WireEntitySyncHandle("test.view", fakeAcc{subject: "test.notify"}, sub, idata.Publisher(pub))
	if err != nil {
		t.Fatalf("WireEntitySyncHandle 失败: %v", err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("订阅方不支持反注册时 Close 不该 panic：%v", r)
		}
	}()
	es.Close()
	es.Close()
}

// 未开启接收端（未注入路由 / 节点 ID 为 0）时 Stop 不该有任何副作用，也不许 panic。
func TestSceneManagerStopWithoutReceiverIsNoop(t *testing.T) {
	sm := NewSceneManager()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Stop 不该 panic：%v", r)
		}
	}()
	sm.Stop()
	sm.Stop() // 幂等
	if sm.remoteSubject != "" {
		t.Fatalf("未接线时不该有订阅 subject，实际 %q", sm.remoteSubject)
	}
}

// fakeRoute 只满足 SceneRoute 的能力面（本用例不关心 KV 行为，只关心订阅生命周期）。
type fakeRoute struct{}

func (fakeRoute) RegisterScene(ctx context.Context, _, _ uint64) error { return nil }
func (fakeRoute) RefreshScene(ctx context.Context, _, _ uint64) error  { return nil }
func (fakeRoute) UnregisterScene(ctx context.Context, _ uint64) error  { return nil }
func (fakeRoute) LookupScene(ctx context.Context, _ uint64) (uint64, bool, error) {
	return 0, false, nil
}

// 开了接收端（注入了路由 + 订阅能力）时，Stop 必须退掉那条**定向** subject 的订阅，且幂等。
func TestSceneManagerStopUnsubscribesRemoteTransfer(t *testing.T) {
	const nodeID = 7
	sub := newUnsubCapableSub()
	sm := NewSceneManager(
		WithClusterRoute(fakeRoute{}, nodeID),
		WithRemoteTransferSubscriber(sub),
	)
	want := transport.RemoteTransferSubjectFor(nodeID)
	subjects, _, _ := sub.state()
	if len(subjects) != 1 || subjects[0] != want {
		t.Fatalf("开启接收端后应订阅 %q，实际 %v", want, subjects)
	}

	sm.Stop()

	subjects, unsubbed, _ := sub.state()
	if len(subjects) != 0 {
		t.Fatalf("Stop 后不该还有订阅，实际 %v", subjects)
	}
	if len(unsubbed) != 1 || unsubbed[0] != want {
		t.Fatalf("Stop 应退订 %q，实际 %v", want, unsubbed)
	}
	if sm.remoteSubject != "" {
		t.Fatalf("退订后应清空 remoteSubject，实际 %q", sm.remoteSubject)
	}

	// 幂等：重复 Stop 不该再退一次（nats.Client.Unsubscribe 对未订阅 subject 返回 nil）。
	_, _, calls := sub.state()
	sm.Stop()
	if _, _, calls2 := sub.state(); calls2 != calls {
		t.Fatalf("Stop 必须幂等，实际 Unsubscribe 调用次数从 %d 变成 %d", calls, calls2)
	}
}
