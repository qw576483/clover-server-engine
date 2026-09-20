// Package mmo 提供 MMO 场景（场景管理、视野同步、实体路由）的独立模块。
//
// 设计要点：
// - 引擎不对 MMO 自动初始化；业务通过 NewModule() 显式创建并自行持有生命周期。
// - 不依赖 internal/app 包，避免循环引用；依赖通过接口注入。
package mmo

import (
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	iaccessor "github.com/qw576483/clover-server-engine/internal/domain/data/accessor"
	"github.com/qw576483/clover-server-engine/internal/transport/event"
	engine "github.com/qw576483/clover-server-engine/internal/transport/event/engine"
	"github.com/qw576483/clover-server-engine/internal/transport/nats"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// Module 封装 MMO SceneManager 及其关联的同步管线。
// 业务通过 NewModule 创建，通过 SceneManager() 获取 SceneManager 进行场景操作，
// 通过 OnDisconnectAutoSceneLeave 订阅断线自动离场。
type Module struct {
	sm      *SceneManager
	store   *data.Store
	acc     *iaccessor.Accessor
	pub     Publisher
	sub     Subscriber
	subject string
	es      *EntitySync // 实体同步管线句柄（断线时清理其 watchers 反向索引）
}

// ModuleOption 创建 MMO 模块的可选配置。
type ModuleOption func(*moduleOptions)

type moduleOptions struct {
	route  SceneRoute
	nodeID uint64
}

// WithClusterTransfer 让本模块创建的 SceneManager 开启跨机对象迁移：
// 注入 scene→node 路由表与本节点数字 ID（通常取自 app.Game.SceneRoute / app.Game.NodeID）。
//
// 不注入时为单机模式：TransferRemote 返回 ErrNoRoute，行为与「没有这套能力」一致。
func WithClusterTransfer(route SceneRoute, nodeID uint64) ModuleOption {
	return func(o *moduleOptions) {
		o.route = route
		o.nodeID = nodeID
	}
}

// NewModule 创建 MMO 场景管理器并接线实体同步。
//
// 参数：
//   - nc: NATS 客户端，内部适配为 Publisher / Subscriber
//   - store: 数据仓库（落库 / 回血）
//   - acc: 实体同步通知器
//   - subject: 视野同步使用的 NATS subject（如 "mmo.view"）
//   - opts: 可选配置，如 WithClusterTransfer 开启跨机对象迁移
func NewModule(nc *nats.Client, store *data.Store, acc *iaccessor.Accessor, subject string, opts ...ModuleOption) (*Module, error) {
	mo := moduleOptions{}
	for _, o := range opts {
		o(&mo)
	}

	pub := natsPublisher{nc}
	sub := natsSub{nc}

	smOpts := []Option{
		WithStore(store),
		WithPublisher(pub),
		WithViewSubject(subject),
		WithEntityAccessor(acc),
	}
	// 跨机对象迁移：配了路由表 + 非零节点 ID 才开启；
	// 接收端复用本模块已有的 sub（同一个 NATS 客户端），无需调用方再传一次。
	if mo.route != nil && mo.nodeID != 0 {
		smOpts = append(smOpts,
			WithClusterRoute(mo.route, mo.nodeID),
			WithRemoteTransferSubscriber(sub),
		)
	}
	sm := NewSceneManager(smOpts...)
	es, err := wireEntitySync(subject, acc, sub, pub)
	if err != nil {
		sm.Stop()
		return nil, fmt.Errorf("mmo: WireEntitySync failed: %w", err)
	}

	return &Module{sm: sm, store: store, acc: acc, pub: pub, sub: sub, subject: subject, es: es}, nil
}

// SceneManager 返回底层的 MMO 场景管理器，用于创建/销毁场景、实体增删等。
func (m *Module) SceneManager() *SceneManager { return m.sm }

// Stop 停止 MMO 场景管理器并**反注册本模块注册的订阅**，释放资源。
//
// 之前只停场景管理器、不退订阅：视野同步（wireEntitySync 订阅了 viewSubject 与
// notify subject）的回调在模块停止后仍会被 NATS 派发，属于「订阅生命周期比持有者长」
// 的泄漏 —— 模块没了，回调还在改动已释放的状态。
// 订阅方不支持反注册时（只实现了 Subscriber）降级为一条 Warn。
// 本方法幂等：重复调用只退一次，第二次是空操作。
func (m *Module) Stop() {
	if m.sm != nil {
		m.sm.Stop()
	}
	if m.es != nil {
		m.es.Close()
	}
}

// OnDisconnectAutoSceneLeave 订阅引擎断线事件，断线时自动让实体离开场景。
//
// 参数：
//   - l: 引擎事件总线（event.Logic），通过 g.Logic() 获取
//   - resolver: 根据 owner 返回所在 Scene 和实体 objID；返回 nil/0 表示不处理
func (m *Module) OnDisconnectAutoSceneLeave(l *event.Logic, resolver func(owner string) (*Scene, uint64)) {
	if resolver == nil {
		return
	}
	l.OnEvent(engine.ConnDisconnectType, func(c *event.Ctx) error {
		ev, ok := c.Payload().(*engine.ConnDisconnectEvent)
		if !ok {
			logger.Warnf("mmo: disconnect event payload type %T unexpected, auto scene leave skipped", c.Payload())
			return nil
		}
		if ev.Owner == "" {
			logger.Warnf("mmo: disconnect event has empty owner, auto scene leave skipped")
			return nil
		}
		scene, objID := resolver(ev.Owner)
		if scene == nil || objID == 0 {
			return nil
		}
		scene.Leave(objID)
		// 断线兜底：leave 事件可能根本没发出来（连接已断 / 推送失败），
		// 此时 EntitySync 的「谁在观察谁」反向索引里该玩家的条目会永久残留。
		m.dropEntityWatcher(objID)
		return nil
	})
}

// dropEntityWatcher 清理某观察者残留在实体同步管线里的反向索引（幽灵视野兜底）。
func (m *Module) dropEntityWatcher(watcherID uint64) {
	if m == nil || m.es == nil || watcherID == 0 {
		return
	}
	m.es.dropWatcher(watcherID)
}

// NATS 适配器（与 app 包内同名结构等价，但私有，外部不可见）。
// 私有实现，避免 mmo → app 循环引用。

type natsPublisher struct{ c *nats.Client }

func (p natsPublisher) Publish(subject string, data []byte) error {
	return p.c.PublishRaw(subject, data)
}

type natsSub struct{ c *nats.Client }

func (s natsSub) Subscribe(subject string, handler func(subject string, payload []byte)) error {
	return s.c.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Subject, msg.Data)
	})
}

// Unsubscribe 让本适配器实现 pubsub.Unsubscriber：mmo 模块 Stop 时据此反注册订阅。
func (s natsSub) Unsubscribe(subject string) error { return s.c.Unsubscribe(subject) }
