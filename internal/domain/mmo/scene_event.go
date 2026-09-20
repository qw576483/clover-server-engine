package mmo

import (
	"context"
	"errors"
)

// SceneEventHandler 场景事件处理器：处理 (场景, 事件名) 这一组合。
// s 为事件目标场景，eventType 为事件名（如 "boss.spawn"、"activity.start"），payload 为事件携带的任意数据。
// 返回 error 会向上透传给 SendEvent 的调用方。
type SceneEventHandler func(ctx context.Context, s *Scene, eventType string, payload any) error

// OnEvent 注册场景事件处理器（eventType → handler）。同一事件名重复注册会覆盖。
//
//	scene.OnEvent("boss.spawn", func(ctx context.Context, s *mmo.Scene, eventType string, payload any) error {
//	    boss := payload.(*BossData)
//	    ...
//	    return nil
//	})
func (s *Scene) OnEvent(eventType string, h SceneEventHandler) {
	if h == nil || eventType == "" {
		return
	}
	s.mu.Lock()
	if s.evtHandlers == nil {
		s.evtHandlers = make(map[string]SceneEventHandler)
	}
	s.evtHandlers[eventType] = h
	s.mu.Unlock()
}

// SendEvent 向场景发事件（同步派发通道：不串行，多个调用方的 handler 可能并发执行）。
// 未绑定事件处理器返回 ErrNoSceneEventHandler；事件类型为空返回 ErrEmptySceneEventType。
//
// 适合只读 / 纯通知类事件（如 "boss.spawn" 广播），零串行开销。
//
//	scene.SendEvent(ctx, "boss.spawn", &BossData{...})
func (s *Scene) SendEvent(ctx context.Context, eventType string, payload any) error {
	if eventType == "" {
		return ErrEmptySceneEventType
	}
	s.mu.RLock()
	h := s.evtHandlers[eventType]
	s.mu.RUnlock()
	if h == nil {
		return ErrNoSceneEventHandler
	}
	return h(ctx, s, eventType, payload)
}

// SendQueueEvent 向场景发事件（串行派发通道：同一场景上的事件严格互斥执行）。
//
// 与 SendEvent 的区别：
//   - SendEvent：并发通道，多个玩家同时发事件时 handler 并发跑，读写场景共享状态会竞态；
//   - SendQueueEvent：串行通道，后到者排队等先到者执行完，读-改-写场景共享状态是原子的。
//
// 典型用途：占领进度、资源点、活动状态等多玩家可同时修改的场景级状态。
// handler 内做阻塞 IO（DB / RPC）会阻塞本场景后续事件，请自行权衡。
//
// 注意：handler 内禁止再次对本场景调用 SendQueueEvent（会自我死锁）；嵌套投递请用 SendEvent。
//
//	scene.SendQueueEvent(ctx, "capture.tick", &CaptureData{...})
func (s *Scene) SendQueueEvent(ctx context.Context, eventType string, payload any) error {
	if eventType == "" {
		return ErrEmptySceneEventType
	}
	s.evtExecMu.Lock()
	defer s.evtExecMu.Unlock()
	return s.SendEvent(ctx, eventType, payload)
}

// Sync 在场景串行锁内执行 fn，与 SendQueueEvent 共用同一把锁。
//
// 为什么需要它：SendQueueEvent 只保证「玩家 handler 之间」串行，管不到 Tick——
// Scene.Tick 由业务定时器在另一个 goroutine 驱动（SceneManager.Run 并不驱动 Tick），
// 若 Tick 直接读写场景共享状态，会与 handler 并发互相覆盖。
// 用 Sync 包住 Tick 内的状态读写，Tick 就与所有 SendQueueEvent 的 handler 严格互斥，
// 等价于「单线程场景服」：场景内共享状态的读写全部串行。
//
//	beat.Add(mmo.TierFast, func(dt time.Duration) {
//	    scene.Sync(func() { progress -= dt.Seconds() * decayRate })
//	})
//
// 注意：fn 内禁止再次调用 Sync 或 SendQueueEvent（会自我死锁）。
func (s *Scene) Sync(fn func()) {
	if fn == nil {
		return
	}
	s.evtExecMu.Lock()
	defer s.evtExecMu.Unlock()
	fn()
}

// 错误定义。
var (
	// ErrNoSceneEventHandler 该事件名未绑定处理器。
	ErrNoSceneEventHandler = errors.New("scene: no handler bound for event")
	// ErrEmptySceneEventType 事件类型为空。
	ErrEmptySceneEventType = errors.New("scene: empty event type")
)
