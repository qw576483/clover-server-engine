// Package object 通用游戏对象内核：对象号（ObjectID）+ 对象管理器（Manager）+ 消息绑定与按号派发。

//  1. 对象号（ObjectID）：每个对象拥有稳定、带类型、可比较、可线化的唯一身份。
//     高 16 位为对象类型（player/monster/guild/...），低 48 位为实例序号。可直接作为
//     map 键，也可 MarshalUint64 后在网络中传输。

//  2. 消息绑定 + 按号发消息：Manager 维护全局对象注册表，并把「消息类型」绑定到「对象类型」上；
//     任意代码只要持有一个 ObjectID，即可 manager.Send(id, msg) 把消息精准投递给该对象，
//     由绑定的处理器（或对象自身的 OnMessage）执行。

// 本包让「按 ID 读改对象」变得自然、统一、可测试。
package object

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
)

// maxSeq 实例序号上限（低 48 位）。
const maxSeq = (uint64(1) << 48) - 1

// 标准对象类型常量。
const (
	TypePlayer uint16 = 1 // 玩家
	TypeScene  uint16 = 2 // 场景 / 地图区块
)

// ObjectID 对象号：稳定、带类型、可比较、可线化的对象身份。
// 可直接作为 map 键；MarshalUint64 编码为单个 uint64（高 16 位 type，低 48 位 seq）用于网络 / 存储线化。
type ObjectID struct {
	Type uint16 // 对象类型（player / monster / guild / server / ...）
	Seq  uint64 // 实例序号
}

// NewObjectID 构造对象号；seq 超出 48 位时按位截断，保证可安全线化。
func NewObjectID(typ uint16, seq uint64) ObjectID {
	return ObjectID{Type: typ, Seq: seq & maxSeq}
}

// IsZero 是否为零值（未赋值）。
func (id ObjectID) IsZero() bool { return id.Type == 0 && id.Seq == 0 }

// String 返回 "type:seq" 形式，便于日志 / 调试 / GM 命令行解析（「只有个 id 就行」）。
func (id ObjectID) String() string {
	return conv.FormatUint(uint64(id.Type)) + ":" + conv.FormatUint(id.Seq)
}

// ParseObjectID 解析 "type:seq" 字符串；缺省 type 时按 0 处理。
func ParseObjectID(s string) (ObjectID, error) {
	var ts, ss string
	if i := strings.IndexByte(s, ':'); i >= 0 {
		ts, ss = s[:i], s[i+1:]
	} else {
		ts, ss = "0", s // 缺省 type = 0
	}
	typ, err := strconv.ParseUint(ts, 10, 16)
	if err != nil {
		return ObjectID{}, errors.New("object: bad type " + strconv.Quote(ts))
	}
	seq, err := strconv.ParseUint(ss, 10, 64)
	if err != nil {
		return ObjectID{}, errors.New("object: bad seq " + strconv.Quote(ss))
	}
	if seq > maxSeq {
		// 超过 48 位的序号会被 NewObjectID 静默按位截断（如 2^48 → Seq=0），
		// 解析出一个与真实对象无关的号 → 对象号碰撞。此处必须显式报错。
		return ObjectID{}, errors.New("object: seq out of range (max 2^48-1): " + strconv.Quote(ss))
	}
	return NewObjectID(uint16(typ), seq), nil
}

// MarshalUint64 编码为单个 uint64（高 16 位 type，低 48 位 seq），用于网络 / 存储线化。
func (id ObjectID) MarshalUint64() uint64 {
	return uint64(id.Type)<<48 | (id.Seq & maxSeq)
}

// FromUint64 从 MarshalUint64 的结果还原对象号。
func FromUint64(v uint64) ObjectID {
	return ObjectID{Type: uint16(v >> 48), Seq: v & maxSeq}
}

// Message 可被 Manager 绑定与派发的消息。消息自述其类型（MsgType），处理器据此路由；
// 消息体可携带任意业务字段（参考 MMO 的「消息绑定」：同一个对象号可接收多种消息）。
type Message interface {
	MsgType() uint32
}

// Object 可被 Manager 管理的对象：至少暴露自己的 ObjectID。
type Object interface {
	ObjectID() ObjectID
}

// MessageReceiver 能直接处理消息的对象（消息绑定的另一种形式：对象自管）。
// 当 Manager 找不到「类型级绑定处理器」时，会退化为调用对象的 OnMessage。
type MessageReceiver interface {
	Object
	OnMessage(ctx context.Context, msg Message) error
}

// Handler 类型级消息处理器：处理 (对象类型, 消息类型) 这一组合。
// obj 为消息目标对象（已通过 ObjectID 查到），处理器自行类型断言后读写其字段。
// 返回 error 会向上透传给 Send 的调用方（GM 工具可据此判断是否成功）。
type Handler func(ctx context.Context, obj Object, msg Message) error

// EventHandler 对象事件处理器：处理 (对象类型, 事件名) 这一组合。
// obj 为事件目标对象，eventType 为事件名（如 "take_damage"），payload 为事件携带的任意数据。
// 返回 error 会向上透传给 SendEvent 的调用方。
type EventHandler func(ctx context.Context, obj Object, eventType string, payload any) error

// 错误定义。
var (
	// ErrObjectNotFound 目标对象未注册到 Manager。
	ErrObjectNotFound = errors.New("object: object not found")
	// ErrNoHandler 该 (对象类型, 消息类型) 未绑定处理器，且对象未实现 MessageReceiver。
	ErrNoHandler = errors.New("object: no handler bound for message")
	// ErrNoEventHandler 该 (对象类型, 事件名) 未绑定处理器。
	ErrNoEventHandler = errors.New("object: no handler bound for event")
)

// Manager 全局对象注册表 + 消息派发内核。并发安全。

// 对象以 ObjectID 注册，消息按 (对象类型, 消息类型) 绑定后按号派发。
// 一个进程通常持有一个 Manager 实例（可分层：game 服一个、scene 服一个……）。
type Manager struct {
	mu      sync.RWMutex
	objects map[ObjectID]Object
	// bindings[对象类型][消息类型] = 处理器
	bindings map[uint16]map[uint32]Handler
	// evtBindings[对象类型][事件名] = 事件处理器
	evtBindings map[uint16]map[string]EventHandler

	// evtQMutexes[对象号] = 该对象的串行锁，仅 SendQueueEvent 使用；Unregister 时标记回收。
	evtQMu      sync.Mutex
	evtQMutexes map[ObjectID]*eventLock
}

// eventLock 对象级串行锁 + 引用计数（refs = 已持有 + 等锁中的调用者数，dead = 对象已注销）。
// refs/dead 由 Manager.evtQMu 保护；dead 且 refs==0 时回收，避免长期累积。
type eventLock struct {
	mu   sync.Mutex
	refs int
	dead bool
}

// NewManager 构造空对象管理器。
func NewManager() *Manager {
	return &Manager{
		objects:     make(map[ObjectID]Object),
		bindings:    make(map[uint16]map[uint32]Handler),
		evtBindings: make(map[uint16]map[string]EventHandler),
		evtQMutexes: make(map[ObjectID]*eventLock),
	}
}

// Handle 绑定 (对象类型, 消息类型) → 处理器（「给各种消息绑定」）。同一组合重复绑定会覆盖。
// 通常在进程启动时、按架构注册一批消息处理器（玩家 / 军团 / 服务器各有各的 handler 集合）。
func (m *Manager) Handle(objType uint16, msgType uint32, h Handler) {
	if h == nil {
		return
	}
	m.mu.Lock()
	if m.bindings[objType] == nil {
		m.bindings[objType] = make(map[uint32]Handler)
	}
	m.bindings[objType][msgType] = h
	m.mu.Unlock()
}

// Register 注册对象到注册表（以其 ObjectID() 为键）。重复注册覆盖。
// 对象上线 / 加载时调用；下线 / 卸载时调用 Unregister。
func (m *Manager) Register(o Object) {
	if o == nil {
		return
	}
	m.mu.Lock()
	m.objects[o.ObjectID()] = o
	m.mu.Unlock()
}

// Unregister 注销对象（按对象号）。同时回收该对象的串行锁，避免长期运行下累积。
//
// 串行锁回收必须带引用计数：若仍有调用者持有/等待该锁（在途 SendQueueEvent），
// 直接删除会让「注销→再注册→新事件」拿到一把新锁，与旧锁并存 → 同一对象并发执行
// 读-改-写，原子性被破坏。此时只标死，等最后一个使用者释放后再真正删除。
func (m *Manager) Unregister(id ObjectID) {
	m.mu.Lock()
	delete(m.objects, id)
	m.mu.Unlock()
	m.evtQMu.Lock()
	if l, ok := m.evtQMutexes[id]; ok {
		if l.refs == 0 {
			delete(m.evtQMutexes, id)
		} else {
			l.dead = true
		}
	}
	m.evtQMu.Unlock()
}

// Get 按对象号取对象。GM 工具「跨环境读对象」即调用本方法。
func (m *Manager) Get(id ObjectID) (Object, bool) {
	m.mu.RLock()
	o, ok := m.objects[id]
	m.mu.RUnlock()
	return o, ok
}

// Send 给指定对象号发消息（「给对象号发消息」）。

// 派发顺序：
//  1. 优先调用 (对象类型, 消息类型) 绑定的类型处理器（Handle 注册）；
//  2. 若未绑定且对象实现了 MessageReceiver，则退化为 obj.OnMessage；
//  3. 都没有则返回 ErrNoHandler。

// 目标对象不存在返回 ErrObjectNotFound。

// manager.Send(ctx, playerID, &SetAttrMsg{Field: "gold", Value: 999})
func (m *Manager) Send(ctx context.Context, id ObjectID, msg Message) error {
	m.mu.RLock()
	o, ok := m.objects[id]
	var h Handler
	if ok {
		if m := m.bindings[id.Type]; m != nil {
			h = m[msg.MsgType()]
		}
	}
	m.mu.RUnlock()
	if !ok {
		return ErrObjectNotFound
	}
	if h != nil {
		return h(ctx, o, msg)
	}
	if r, ok := o.(MessageReceiver); ok {
		return r.OnMessage(ctx, msg)
	}
	return ErrNoHandler
}

// ForEach 遍历某类型的全部对象（typ=0 表示全部类型）。fn 返回 false 可提前中止。
// 遍历在快照上进行，fn 内可安全调用 Register/Unregister/Send。
func (m *Manager) ForEach(typ uint16, fn func(o Object) bool) {
	m.mu.RLock()
	objs := make([]Object, 0, len(m.objects))
	for id, o := range m.objects {
		if typ != 0 && id.Type != typ {
			continue
		}
		objs = append(objs, o)
	}
	m.mu.RUnlock()
	for _, o := range objs {
		if !fn(o) {
			return
		}
	}
}

// SendAll 给某类型的全部对象广播同一条消息（GM 批量改服务器 / 全服数据友好）。
// 返回首个遇到的错误（不中断其余对象）。
func (m *Manager) SendAll(ctx context.Context, typ uint16, msg Message) error {
	var firstErr error
	m.ForEach(typ, func(o Object) bool {
		if err := m.Send(ctx, o.ObjectID(), msg); err != nil && firstErr == nil {
			firstErr = err
		}
		return true
	})
	return firstErr
}

// OnEvent 绑定 (对象类型, 事件名) → 事件处理器（对象事件驱动）。
// 同一组合重复绑定会覆盖。事件名如 "take_damage"、"monster_die" 等。
func (m *Manager) OnEvent(objType uint16, eventType string, h EventHandler) {
	if h == nil || eventType == "" {
		return
	}
	m.mu.Lock()
	if m.evtBindings[objType] == nil {
		m.evtBindings[objType] = make(map[string]EventHandler)
	}
	m.evtBindings[objType][eventType] = h
	m.mu.Unlock()
}

// SendEvent 给指定对象号发事件（纯本地内存调用，零网络开销）。

// 派发顺序：
//  1. 查 (对象类型, 事件名) 绑定的处理器；
//  2. 未绑定则返回 ErrNoEventHandler。

// 目标对象不存在返回 ErrObjectNotFound。
func (m *Manager) SendEvent(ctx context.Context, id ObjectID, eventType string, payload any) error {
	m.mu.RLock()
	o, ok := m.objects[id]
	var h EventHandler
	if ok {
		if byType := m.evtBindings[id.Type]; byType != nil {
			h = byType[eventType]
		}
	}
	m.mu.RUnlock()
	if !ok {
		return ErrObjectNotFound
	}
	if h == nil {
		return ErrNoEventHandler
	}
	return h(ctx, o, eventType, payload)
}

// SendQueueEvent 串行通道：给指定对象号发事件，同一对象上的事件严格互斥执行
// （不同对象之间仍然并行）。
//
// 与 SendEvent 的区别：
//   - SendEvent：并发通道，多个玩家同时打同一个 Boss 时 handler 并发跑，
//     读-改-写 HP 会丢失更新（双杀奖励 / 血量错乱）；
//   - SendQueueEvent：串行通道，同一对象的事件排队执行，读-改-写是原子的。
//
// 典型用途：怪物掉血与死亡判定、掉落归属、对象级共享计数等。
// handler 内禁止再次对同一对象调用 SendQueueEvent（会自我死锁）；嵌套投递请用 SendEvent。
//
//	manager.SendQueueEvent(ctx, bossID, "take_damage", &Damage{Amount: 90})
func (m *Manager) SendQueueEvent(ctx context.Context, id ObjectID, eventType string, payload any) error {
	m.mu.RLock()
	o, ok := m.objects[id]
	var h EventHandler
	if ok {
		if byType := m.evtBindings[id.Type]; byType != nil {
			h = byType[eventType]
		}
	}
	m.mu.RUnlock()
	if !ok {
		return ErrObjectNotFound
	}
	if h == nil {
		return ErrNoEventHandler
	}
	l := m.acquireEventLock(id)
	defer m.releaseEventLock(id, l)
	if err := ctx.Err(); err != nil {
		return err
	}
	return h(ctx, o, eventType, payload)
}

// acquireEventLock 取到对象 id 对应的串行锁并加锁（惰性创建）。
// 返回的锁必须通过 releaseEventLock 释放。
func (m *Manager) acquireEventLock(id ObjectID) *eventLock {
	m.evtQMu.Lock()
	if m.evtQMutexes == nil {
		m.evtQMutexes = make(map[ObjectID]*eventLock)
	}
	l, ok := m.evtQMutexes[id]
	if !ok {
		l = &eventLock{}
		m.evtQMutexes[id] = l
	}
	l.refs++
	m.evtQMu.Unlock()
	l.mu.Lock()
	return l
}

// releaseEventLock 释放串行锁；若该锁已被注销且无人再用，则顺手回收。
func (m *Manager) releaseEventLock(id ObjectID, l *eventLock) {
	l.mu.Unlock()
	m.evtQMu.Lock()
	l.refs--
	if l.dead && l.refs == 0 {
		if cur, ok := m.evtQMutexes[id]; ok && cur == l {
			delete(m.evtQMutexes, id)
		}
	}
	m.evtQMu.Unlock()
}

// SendEventToAll 给某类型的全部对象广播同一个事件（纯本地，不走网络）。
// 返回首个遇到的错误（不中断其余对象）。
func (m *Manager) SendEventToAll(ctx context.Context, objType uint16, eventType string, payload any) error {
	var firstErr error
	m.ForEach(objType, func(o Object) bool {
		if err := m.SendEvent(ctx, o.ObjectID(), eventType, payload); err != nil && firstErr == nil {
			firstErr = err
		}
		return true
	})
	return firstErr
}

// Count 返回注册对象数（typ=0 表示全部类型）。
func (m *Manager) Count(typ uint16) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if typ == 0 {
		return len(m.objects)
	}
	n := 0
	for id := range m.objects {
		if id.Type == typ {
			n++
		}
	}
	return n
}

// BasicObject 可被业务对象内嵌的极简对象实现：仅持有 ObjectID。
// 业务对象内嵌本结构即可满足 Object 接口；若要自处理消息，在外部结构自行定义
// OnMessage 方法（会自然遮蔽本结构的方法——但本结构不提供 OnMessage，避免伪装成 Receiver）。
type BasicObject struct {
	id ObjectID
}

// NewBasicObject 构造持有指定对象号的基础对象。
func NewBasicObject(id ObjectID) BasicObject { return BasicObject{id: id} }

// ObjectID 返回该对象的对象号。
func (b BasicObject) ObjectID() ObjectID { return b.id }

// InternalManager 返回 Manager 本身（恒等转换）：object 域的**真身就在本包**，
// 不存在另一份 internal 版 Manager —— 这里没有可解包的包装层。
// 保留该函数是为了让 pkg 门面调用点与 InternalStore / InternalGameObject 等
// 「还原 internal 侧」的路径保持同一形态（将来若真形成包装层，调用点无需改动）。
func InternalManager(m *Manager) *Manager { return m }
