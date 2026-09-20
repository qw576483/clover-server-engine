package room

import (
	"fmt"

	iframe "github.com/qw576483/clover-server-engine/internal/domain/room/frame"
	iproto "github.com/qw576483/clover-server-engine/internal/shared/proto"
	ievent "github.com/qw576483/clover-server-engine/internal/transport/event"
	proom "github.com/qw576483/clover-server-engine/pkg/domain/room"
	pframe "github.com/qw576483/clover-server-engine/pkg/domain/room/frame"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/transport/event"
)

// pkgfacade.go 实现 pkg/domain/room 的 P3 注册钩子：
// 把 internal 的具体实现注册为 pkg 侧的构造工厂，并适配 pkg 的接口。
// 依赖方向：internal → pkg（类型/接口真身在 pkg，实现在 internal）。

func init() {
	proom.RegisterModuleFactory(newModuleFacade)
	proom.RegisterMasterRegistryFactory(newMasterRegistryFacade)
	proom.RegisterMasterHandlersFactory(newMasterHandlersFacade)
	proom.RegisterMasterRegistryWrapper(func(inner any) proom.MasterRegistry {
		if r, ok := inner.(*MasterRegistry); ok {
			return r
		}
		return nil
	})
}

// ==================== Module ====================

// moduleFacade 把内部 *Module 适配为 pkg 的 Module 接口。
type moduleFacade struct{ inner *Module }

func (m *moduleFacade) Frame() pframe.Service {
	if m.inner == nil || m.inner.Frame == nil {
		return nil
	}
	return iframe.Facade(m.inner.Frame)
}

func (m *moduleFacade) EnsureRoom(roomID string) error {
	if m.inner == nil {
		logger.Warnf("room: EnsureRoom room=%s rejected: module not constructed", roomID)
		return fmt.Errorf("room: module 未构造")
	}
	return m.inner.EnsureRoom(roomID)
}

func (m *moduleFacade) JoinRoom(connID, roomID, playerID string) (string, bool, error) {
	if m.inner == nil {
		logger.Warnf("room: JoinRoom room=%s player=%s rejected: module not constructed", roomID, playerID)
		return "", false, fmt.Errorf("room: module 未构造")
	}
	return m.inner.JoinRoom(connID, roomID, playerID)
}

func (m *moduleFacade) LeaveRoom(roomID, playerID string) error {
	if m.inner == nil {
		logger.Warnf("room: LeaveRoom room=%s player=%s rejected: module not constructed", roomID, playerID)
		return fmt.Errorf("room: module 未构造")
	}
	return m.inner.LeaveRoom(roomID, playerID)
}

func (m *moduleFacade) DestroyRoom(roomID string) error {
	if m.inner == nil {
		logger.Warnf("room: DestroyRoom room=%s rejected: module not constructed", roomID)
		return fmt.Errorf("room: module 未构造")
	}
	return m.inner.DestroyRoom(roomID)
}

func (m *moduleFacade) Close() {
	if m.inner != nil {
		m.inner.Close()
	}
}

// newModuleFacade 构造内部 Module。
//
// ★ 不再逐字段拷贝：Config 的类型真身只有一份（在 pkg），internal 侧是类型别名
// （见 internal/domain/room/module.go），故配置可原样传入。
// FrameSvc 由门面接口还原为内部具体服务的动作也已收进 NewModule（unwrapFrameService），
// 与 Config 的装配放在一起 —— 这里只剩一层薄适配。
func newModuleFacade(cfg proom.Config) proom.Module {
	return &moduleFacade{inner: NewModule(cfg)}
}

// ==================== MasterRegistry ====================

func newMasterRegistryFacade() proom.MasterRegistry { return NewMasterRegistry() }

// ==================== MasterHandlers ====================

// masterHandlersFacade 把内部 *MasterHandlers 适配为 pkg 的 MasterHandlers 接口。
type masterHandlersFacade struct{ inner *MasterHandlers }

func (w *masterHandlersFacade) Register() { w.inner.Register() }

func newMasterHandlersFacade(mg proom.MasterHandlerGame) proom.MasterHandlers {
	return &masterHandlersFacade{inner: NewMasterHandlers(&masterHandlerGameBridge{pkg: mg})}
}

// masterHandlerGameBridge 把 pkg 的 MasterHandlerGame 适配为 internal 的 MasterHandlerGame。
type masterHandlerGameBridge struct{ pkg proom.MasterHandlerGame }

// masterInternalRegistrar 是 pkg 宿主的**可选能力**：注册引擎内部保留号（≤ proto.InternalMsgMax）。
//
// 为什么不把它加进 proom.MasterHandlerGame：那是已发布的业务可见接口，
// 给接口加方法 = 所有既有实现者编译失败。引擎内建房间协议需要这条路径，
// 普通业务宿主（自己实现 MasterHandlerGame 的测试替身等）不需要，故单独断言。
// pkg 侧实现者：internal/app.MasterGameFacade（见 facade.go 的编译期断言）。
type masterInternalRegistrar interface {
	InternalOnMsg(msgID uint32, handler event.Handler)
}

// wrapMasterCtx 把 internal 的 *ievent.Ctx 回调适配为 pkg 的 event.Handler。
// pkg 的 Ctx 是接口，internal *event.Ctx 实现它，故回调处做类型断言还原。
func wrapMasterCtx(msgID uint32, handler func(c *ievent.Ctx) error) event.Handler {
	return func(c event.Ctx) error {
		// 必须用 ok 形式断言：pkg.Ctx 是接口，非 *ievent.Ctx 的实现传进来时
		// 直接断言会 panic（而不是返回错误）。
		ic, ok := c.(*ievent.Ctx)
		if !ok {
			logger.Warnf("room: master handler msgID=%d got unexpected ctx type %T", msgID, c)
			return fmt.Errorf("room: 非预期的 ctx 类型 %T", c)
		}
		return handler(ic)
	}
}

func (b *masterHandlerGameBridge) OnMsg(msgID uint32, handler func(c *ievent.Ctx) error) {
	b.pkg.OnMsg(msgID, wrapMasterCtx(msgID, handler))
}

// InternalOnMsg 把内部号注册转发给宿主（保留号内部路径）。
// 宿主不支持时明确 panic：退回 OnMsg 只会撞上业务号守卫（6001 必被拒），
// 那条 panic 信息里看不到「是 room 在注册内建号」，排查成本高。
func (b *masterHandlerGameBridge) InternalOnMsg(msgID uint32, handler func(c *ievent.Ctx) error) {
	host, ok := b.pkg.(masterInternalRegistrar)
	if !ok {
		panic(fmt.Sprintf("room: master 宿主 %T 未实现 InternalOnMsg —— 引擎内建房间消息号 %d（≤%d）无法注册",
			b.pkg, msgID, iproto.InternalMsgMax))
	}
	host.InternalOnMsg(msgID, wrapMasterCtx(msgID, handler))
}

func (b *masterHandlerGameBridge) Reply(c *ievent.Ctx, v interface{}) { b.pkg.Reply(c, v) }

func (b *masterHandlerGameBridge) MasterRegistry() *MasterRegistry {
	if r := b.pkg.MasterRegistry(); r != nil {
		if mr, ok := r.(*MasterRegistry); ok {
			return mr
		}
	}
	return nil
}

// 编译期断言。
var (
	_ proom.Module         = (*moduleFacade)(nil)
	_ proom.MasterHandlers = (*masterHandlersFacade)(nil)
	// 内部号注册路径的实现必须同时提供 OnMsg / Reply / MasterRegistry（internal 门面接口）。
	_ MasterHandlerGame = (*masterHandlerGameBridge)(nil)
)
