// Package fsm 通用有限状态机（finite state machine）。

// 下沉为与业务无关、并发安全、零外部依赖的通用状态机原语。

// 设计要点：
// - 声明式：RegisterState 登记状态生命周期钩子，Transition 登记「事件 → 目标态」边（可带守卫/动作）。
// - 并发安全：读写均加锁；钩子在释放锁后调用，钩子内可安全地再次 Trigger（无死锁）。
// - 守卫（Guard）：Trigger 前校验，拒绝则返回 ErrGuard，便于「满足条件才转移」。
// - 后台 Tick：每帧调用 Tick(dt) 驱动当前态的 OnTick（AI / 计时类行为）。
// - Force(to) 绕过守卫直接置态（运维强制改态）；
// - States/Events/Transitions 自省，便于工具列出「某对象当前态 / 可触发事件 / 全转移表」。
// - 钩子通过 Context() 拿到宿主（如 *gobject.GameObject），状态变化可写回对象属性并自动同步。
// - HFSM 层次状态机（见 hfsm.go）：在 Machine 之上加父子层级，进入/退出时按层级链触发钩子。

// 典型用法：

// m := fsm.New(fsm.State("Idle"))
// m.RegisterState(fsm.State("Fighting"), fsm.StateDef{OnTick: func(m *fsm.Machine, dt time.Duration) { ... }})
// m.Transition(fsm.State("Idle"), fsm.State("Fighting"), "start",
//
//	fsm.WithGuard(func(m *fsm.Machine, from fsm.State, payload any) bool { return ready }))
//
// if err := m.Trigger("start", nil); err != nil { ... }
package fsm

import (
	"encoding/json"
	"errors"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"clover-server-engine/pkg/foundation/logger"
)

// runHook 统一带 recover 执行状态机回调：panic 不传播、不影响已提交的状态，
// 但必须记录日志——静默吞掉会让业务钩子的崩溃无迹可查。
// 日志必须带钩子名（action/exit/enter/change）与堆栈，否则无法按名字归因。
func runHook(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("fsm: hook %q panicked: %v\n%s", name, r, debug.Stack())
		}
	}()
	fn()
}

// State 是一个状态标识，建议用字符串常量（如 "Idle" / "Fighting"）。
type State string

// NoState 表示「无状态」（未初始化或已退出）。
const NoState State = ""

// TransitionHook 在每次成功转移（Trigger 或 Force）后回调。
// from/to 分别为转移前/后的状态；payload 为触发时携带的载荷。
type TransitionHook func(m *Machine, from, to State, payload any)

// StateDef 描述一个状态的生命周期钩子。任一钩子均可省略（nil）。
type StateDef struct {
	// OnEnter 进入该态时调用（cur 为转移前的旧态）。
	OnEnter func(m *Machine, from State, payload any)
	// OnExit 离开该态时调用（to 为转移后的新态）。
	OnExit func(m *Machine, to State, payload any)
	// OnTick 每帧 Tick(dt) 时、且当前处于该态时调用。
	OnTick func(m *Machine, dt time.Duration)
}

// TOption 定制一条转移边。
type TOption func(*transition)

type transition struct {
	to     State
	guard  func(m *Machine, from State, payload any) bool
	action func(m *Machine, payload any)
}

// WithGuard 增加转移的守卫：返回 false 时 Trigger 报 ErrGuard 且不转移。
func WithGuard(fn func(m *Machine, from State, payload any) bool) TOption {
	return func(t *transition) { t.guard = fn }
}

// WithAction 增加转移「发生瞬间」的副作用（在 OnExit 之前、状态提交之后执行）。
func WithAction(fn func(m *Machine, payload any)) TOption {
	return func(t *transition) { t.action = fn }
}

// TransitionInfo 自省用的一条转移记录。
type TransitionInfo struct {
	Event string
	From  State
	To    State
}

// Machine 并发安全的有限状态机。
type Machine struct {
	mu        sync.RWMutex
	current   State
	states    map[State]StateDef
	edges     map[State]map[string]transition // from -> event -> transition
	listeners []TransitionHook
	ctx       any // 宿主引用（如 *gobject.GameObject），供钩子使用
}

var (
	// ErrUnknownEvent 当前态没有该事件对应的转移边。
	ErrUnknownEvent = errors.New("fsm: no transition for event in current state")
	// ErrGuard 守卫拒绝本次转移。
	ErrGuard = errors.New("fsm: transition guard rejected")
	// ErrUnknownState Force 的目标态未登记（状态须先 RegisterState）。
	ErrUnknownState = errors.New("fsm: target state is not registered")
	// ErrRace 提交转移前当前态被其它 Trigger 抢先改变（并发冲突）。
	ErrRace = errors.New("fsm: current state changed before commit")
)

// New 构造状态机，初始处于 initial 态。initial 不要求预先 RegisterState——它只是起始值。
func New(initial State) *Machine {
	return &Machine{
		current: initial,
		states:  make(map[State]StateDef),
		edges:   make(map[State]map[string]transition),
	}
}

// SetContext 存入宿主引用，供钩子通过 Context() 取用（如把状态机挂到 GameObject 上）。
func (m *Machine) SetContext(ctx any) {
	m.mu.Lock()
	m.ctx = ctx
	m.mu.Unlock()
}

// Context 返回 SetContext 存入的宿主引用（并发安全读）。
func (m *Machine) Context() any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ctx
}

// RegisterState 登记一个状态及其生命周期钩子。可重复登记以覆盖钩子。
func (m *Machine) RegisterState(name State, def StateDef) {
	m.mu.Lock()
	m.states[name] = def
	m.mu.Unlock()
}

// Transition 声明「在 from 态收到 event，可转移到 to 态」。可带守卫/动作。
// 同一 (from, event) 后登记者覆盖前者。
func (m *Machine) Transition(from, to State, event string, opts ...TOption) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := transition{to: to}
	for _, o := range opts {
		o(&t)
	}
	if m.edges[from] == nil {
		m.edges[from] = make(map[string]transition)
	}
	m.edges[from][event] = t
}

// Current 返回当前态（并发安全读）。
func (m *Machine) Current() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// Can 报告在当前态触发 event 是否会通过守卫（不实际转移）。

// 注意：守卫在锁外执行（先取边与守卫、释放 RLock 再跑守卫），与 Trigger 一致；
// 否则守卫内若回读机器（如 m.Context()/m.Current()）会因 RWMutex 不可重入而死锁。
func (m *Machine) Can(event string) bool {
	m.mu.RLock()
	cur := m.current
	es := m.edges[cur]
	if es == nil {
		m.mu.RUnlock()
		return false
	}
	t, ok := es[event]
	if !ok {
		m.mu.RUnlock()
		return false
	}
	guard := t.guard
	m.mu.RUnlock()
	if guard != nil && !guard(m, cur, nil) {
		return false
	}
	return true
}

// Trigger 触发 event。流程：取边 → 跑守卫（锁外）→ 提交状态（锁内，带并发重检）→
// 锁外执行 action/OnExit/OnEnter → 通知监听者。缺口（未知事件/守卫拒绝/并发抢先）返回对应错误。
func (m *Machine) Trigger(event string, payload any) error {
	m.mu.RLock()
	cur := m.current
	es := m.edges[cur]
	var t transition
	var ok bool
	if es != nil {
		t, ok = es[event]
	}
	guard := t.guard
	m.mu.RUnlock()
	if !ok {
		return ErrUnknownEvent
	}
	if guard != nil && !guard(m, cur, payload) {
		return ErrGuard
	}

	m.mu.Lock()
	if m.current != cur { // 守卫执行期间被其它 Trigger 抢先改变当前态
		m.mu.Unlock()
		return ErrRace
	}
	to := t.to
	// 目标态必须已登记，语义与 Force 一致：Transition 若把 to 写错（未 RegisterState），
	// 转移过去将进入一个既无 OnEnter/OnExit 也无 OnTick 的死态且再也转不出来。
	if _, ok := m.states[to]; !ok {
		m.mu.Unlock()
		return ErrUnknownState
	}
	exit := m.states[cur].OnExit
	enter := m.states[to].OnEnter
	action := t.action
	m.current = to
	m.mu.Unlock()

	// recover 每个回调，防止 panic 传播击穿调用方；panic 记日志不静默。
	if action != nil {
		runHook("action", func() { action(m, payload) })
	}
	if exit != nil {
		runHook("exit", func() { exit(m, to, payload) })
	}
	if enter != nil {
		runHook("enter", func() { enter(m, cur, payload) })
	}
	m.notify(cur, to, payload)
	return nil
}

// Force 绕过守卫直接置为 to 态（GM/运维强制改态）。to 必须已 RegisterState。
// 仍会执行 OnExit/OnEnter/action(无，Force 无 action) 与监听通知，并把状态写回宿主（见 gobject 集成）。
// recover 每个回调防止 panic 传播。

// （当前态未登记的行为，明确约定，与 Trigger 完全一致）：
// - 若当前态 cur 未经 RegisterState（例如 New(initial) 的初始态本就允许不登记），则其 OnExit
// 为 nil，此处不调用 OnExit。这不是「漏掉」而是「本就没有可调的钩子」——语义正确且确定。
// - Force 仍会照常提交状态、执行目标态 OnEnter、触发 notify（from=未登记的 cur, to）。
// - 若需要为某状态挂 OnExit，请先 RegisterState 该状态；未登记即代表「无退出钩子」这一显式契约。
func (m *Machine) Force(to State, payload any) error {
	m.mu.Lock()
	if _, ok := m.states[to]; !ok {
		m.mu.Unlock()
		return ErrUnknownState
	}
	cur := m.current
	exit := m.states[cur].OnExit
	enter := m.states[to].OnEnter
	m.current = to
	m.mu.Unlock()

	if exit != nil {
		runHook("exit", func() { exit(m, to, payload) })
	}
	if enter != nil {
		runHook("enter", func() { enter(m, cur, payload) })
	}
	m.notify(cur, to, payload)
	return nil
}

// Tick 驱动当前态的 OnTick(dt)。无当前态钩子则为 no-op。通常每帧由宿主调用。
func (m *Machine) Tick(dt time.Duration) {
	m.mu.RLock()
	tick := m.states[m.current].OnTick
	m.mu.RUnlock()
	if tick != nil {
		tick(m, dt)
	}
}

// OnChange 注册转移监听（Trigger 与 Force 都触发）。可多次注册。
func (m *Machine) OnChange(fn TransitionHook) {
	m.mu.Lock()
	m.listeners = append(m.listeners, fn)
	m.mu.Unlock()
}

// States 返回全部已登记状态（升序）。
func (m *Machine) States() []State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]State, 0, len(m.states))
	for s := range m.states {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Events 返回全部已声明事件（去重、升序），便于工具枚举「可触发什么」。
func (m *Machine) Events() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	set := make(map[string]struct{})
	for _, es := range m.edges {
		for e := range es {
			set[e] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for e := range set {
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

// Transitions 返回全部转移边（自省）。
func (m *Machine) Transitions() []TransitionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TransitionInfo, 0)
	for from, es := range m.edges {
		for ev, t := range es {
			out = append(out, TransitionInfo{Event: ev, From: from, To: t.to})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		if out[i].Event != out[j].Event {
			return out[i].Event < out[j].Event
		}
		return out[i].To < out[j].To
	})
	return out
}

// notify 在锁外回调所有监听者（复制快照，避免持锁遍历）。
func (m *Machine) notify(from, to State, payload any) {
	m.mu.RLock()
	ls := make([]TransitionHook, len(m.listeners))
	copy(ls, m.listeners)
	m.mu.RUnlock()
	for _, fn := range ls {
		if fn != nil {
			// 与 action/exit/enter 一致，用 runHook 包 recover：
			// 单个监听者 panic 不应击穿调用方，也不应中断其余监听者的通知。
			h := fn
			runHook("change", func() { h(m, from, to, payload) })
		}
	}
}

// DumpData 状态机的可序列化快照（迁移协议用）。
// 只序列化当前状态名——edges/states/hooks 是代码，不可序列化，目标节点须重建。
type DumpData struct {
	Current State `json:"current"`
}

// Dump 导出当前状态为 JSON（迁移时用）。
func (m *Machine) Dump() []byte {
	m.mu.RLock()
	data := DumpData{Current: m.current}
	m.mu.RUnlock()
	b, _ := json.Marshal(data)
	return b
}

// Import 从迁移 JSON 恢复状态（目标节点调用）。
// 必须在目标节点重建好 FSM（RegisterState + Transition）之后调用。
// 使用 Force 绕过守卫直接置态，触发 OnExit/OnEnter 与监听通知。
func (m *Machine) Import(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var d DumpData
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	if d.Current == "" {
		return nil
	}
	return m.Force(d.Current, nil)
}
