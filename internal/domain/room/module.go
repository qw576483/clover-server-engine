// Package room 提供房间子系统：可插拔内核 + 所有权管理 + 跨服接管。
//
// Module 是「房间外壳」——它把三件事拼在一起：
//   - 谁管这个房间（owner 路由）
//   - 节点挂了怎么搬（跨服接管）
//   - 房间内部怎么同步（内核，可插拔）
//
// 内核由 Config 决定：
//   - Config.Kernel：业务自写内核（例如状态同步房间）
//   - Config.FrameCfg / FrameSvc / FrameSvcOpts：引擎内置帧同步内核
//
// 用法（帧同步）：
//
//	roomMod := room.NewModule(room.Config{
//	    // MasterCaller 是「CallMaster + SwitchUpstream」两方法的接口，必须整传 g
//	    MasterCaller: g,
//	    // g.PushToPlayer 带变参（opts ...proto.DeliveryMode），不能直接赋给 Pusher，需包一层
//	    Pusher: func(playerID string, msgID uint32, v any) error {
//	        return g.PushToPlayer(playerID, msgID, v)
//	    },
//	    NodeAddr: g.Addr(),
//	    FrameCfg: frame.DefaultConfig(),
//	})
//	roomMod.EnsureRoom(roomID)
//	roomMod.JoinRoom(c.ConnID(), roomID, c.PlayerID())
//
// 用法（业务自写内核，例如状态同步）：
//
//	roomMod := room.NewModule(room.Config{
//	    MasterCaller: g,
//	    Pusher: func(playerID string, msgID uint32, v any) error {
//	        return g.PushToPlayer(playerID, msgID, v)
//	    },
//	    NodeAddr: g.Addr(),
//	    Kernel:   myStateSyncKernel,
//	})
package room

import (
	"fmt"
	"reflect"

	iframe "github.com/qw576483/clover-server-engine/internal/domain/room/frame"
	proom "github.com/qw576483/clover-server-engine/pkg/domain/room"
	pframe "github.com/qw576483/clover-server-engine/pkg/domain/room/frame"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// isNilInterface 判断接口值是否为 nil 或「typed-nil」（非空接口里装了 nil 指针/映射/切片等）。
//
// 只写 `cfg.Kernel != nil` 时，业务传入 (*someKernel)(nil) 会被判为「已挂载」，
// 随后 m.Kernel.EnsureRoom(...) 调用 nil 接收者方法直接 panic。
func isNilInterface(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// Config 是 room.Module 的构造参数，业务层按需填充。
//
// ★ 类型真身**只有一份**：`pkg/domain/room.Config`。本行是**类型别名**（`type X = Y`），
// 不是又一份字段逐一对应的 struct —— 两侧编译期就是同一个类型，字段不可能漂移。
//
// 历史教训：这里曾复制一份同名 struct，靠 pkgfacade 的逐字段手工拷贝衔接，
// 任何一侧加字段而拷贝处漏改就**静默漂移**（不报错、值丢失）。别名从根上消除该风险。
//
// 与 pkg 侧唯一的形态差异是 FrameSvc：门面侧是接口 `frame.Service`，
// 内部装内核前需经 iframe.UnwrapService 还原成本引擎的具体服务（见 NewModule）。
type Config = proom.Config

// Module 是房间外壳：内核 + 所有权路由 + 跨服接管。
type Module struct {
	Kernel   proom.Kernel         // 可插拔内核（帧同步 / 业务自写）
	Frame    *iframe.Service      // 帧同步服务；仅在挂载帧同步内核时非 nil
	Owner    *OwnerClient         // 跨节点路由
	Takeover *RoomTakeoverManager // 跨服接管（nil=不启用）
	cfg      Config
}

// unwrapFrameService 把门面帧服务（`pkg/domain/room/frame.Service` 接口）还原为本引擎的具体服务。
//
// 返回 nil 的两种情形（调用方一律按「未提供 FrameSvc」处理，与历史行为一致）：
//   - 入参为 nil 或「非空接口里的 nil 值」（typed-nil）；
//   - 入参不是本引擎构造的实现（门面接口未绑定内部实现，无法还原）—— 此时告警留痕：
//     若静默丢弃，Module 会另建一个空 Service，业务预构造的服务及其全部选项/监听会静默消失。
func unwrapFrameService(svc pframe.Service) *iframe.Service {
	if svc == nil || isNilInterface(svc) {
		return nil
	}
	inner, ok := iframe.UnwrapService(svc)
	if !ok {
		logger.Warnf("room: config.FrameSvc (%T) is not an engine-built frame service; "+
			"the prebuilt service and its options are ignored", svc)
		return nil
	}
	return inner
}

// NewModule 构造房间外壳，并按 Config 装配内核。
func NewModule(cfg Config) *Module {
	mod := &Module{cfg: cfg}

	// 房间所有权客户端（依赖 MasterCaller）
	if cfg.MasterCaller != nil {
		mod.Owner = NewOwnerClient(cfg.MasterCaller)
	}

	// 内核：业务自写优先，其次引擎内置帧同步。
	kernelProvided := cfg.Kernel != nil && !isNilInterface(cfg.Kernel)
	if cfg.Kernel != nil && !kernelProvided {
		logger.Warnf("room: Config.Kernel 为非空接口里的 nil 值（typed-nil），按未提供处理")
	}
	// Config.FrameSvc 是门面接口（真身在 pkg），装内核前先还原为本引擎的具体服务。
	// 这一步原先在 pkgfacade 的逐字段拷贝里做，Config 改成别名后统一收在装配处。
	prebuiltFrame := unwrapFrameService(cfg.FrameSvc)
	switch {
	case kernelProvided:
		mod.Kernel = cfg.Kernel
		logger.Infof("room: module mounted business-provided kernel")
	case prebuiltFrame != nil:
		mod.Frame = prebuiltFrame
		mod.Kernel = newFrameKernel(mod.Frame)
		logger.Infof("room: module mounted frame kernel (prebuilt service)")
	case cfg.Pusher != nil || cfg.FrameCfg != nil || len(cfg.FrameSvcOpts) > 0:
		opts := make([]iframe.ServiceOption, 0, len(cfg.FrameSvcOpts)+4)
		if cfg.Pusher != nil {
			opts = append(opts, iframe.WithBroadcaster(iframe.Broadcaster(cfg.Pusher)))
		}
		// FrameCfg 真正作为「房间默认配置」注入：历史上它只被当成布尔开关，字段值被忽略。
		if cfg.FrameCfg != nil {
			opts = append(opts, iframe.WithDefaultRoomConfig(*cfg.FrameCfg))
		}
		// 接管钩子 & 所有权注册（仅帧同步内核需要；业务内核由外壳统一注册）。
		if cfg.MasterCaller != nil && mod.Owner != nil {
			opts = append(opts,
				iframe.WithTakeoverHook(func(roomID string) {
					if mod.Takeover == nil {
						return
					}
					// 激活失败必须留痕：静默丢弃会让「房间没搬过来」完全无从排查。
					if err := mod.Takeover.Activate(roomID); err != nil {
						logger.Warnf("room: takeover activate room=%s failed: %v", roomID, err)
					}
				}),
				iframe.WithOnRoomCreated(func(roomID string) {
					mod.Owner.RegisterRoom(roomID, cfg.NodeAddr)
				}),
				iframe.WithOnRoomDestroy(func(roomID string) {
					mod.Owner.UnregisterRoom(roomID, cfg.NodeAddr)
				}),
			)
		}
		opts = append(opts, cfg.FrameSvcOpts...)
		mod.Frame = iframe.NewService(opts...)
		mod.Kernel = newFrameKernel(mod.Frame)
		// 打真实的帧率值：FrameCfg 是「逐项覆盖 DefaultConfig()」，TargetFPS<=0 时取默认 30。
		fps := iframe.DefaultConfig().TargetFPS
		if cfg.FrameCfg != nil && cfg.FrameCfg.TargetFPS > 0 {
			fps = cfg.FrameCfg.TargetFPS
		}
		logger.Infof("room: module mounted frame kernel (fps=%d)", fps)
	default:
		logger.Warnf("room: NewModule 未挂载任何内核（Kernel / FrameCfg / FrameSvc / FrameSvcOpts 全空）；" +
			"EnsureRoom / JoinRoom 会直接返回错误")
	}

	// 跨服接管：与同步方式无关，挂了内核 + owner 即可用。
	if mod.Kernel != nil && mod.Owner != nil && cfg.MasterCaller != nil {
		mod.Takeover = NewRoomTakeoverManager(TakeoverDeps{
			Kernel:     func() proom.Kernel { return mod.Kernel },
			CallMaster: cfg.MasterCaller.CallMaster,
			Pusher:     cfg.Pusher,
			NodeAddr:   cfg.NodeAddr,
			Owner:      mod.Owner,
		})
	}
	return mod
}

// EnsureRoom 确保房间存在（按挂载的内核创建），并处理 owner 注册 + 接管激活。
func (m *Module) EnsureRoom(roomID string) error {
	if m == nil || m.Kernel == nil {
		logger.Warnf("room: EnsureRoom room=%s rejected: no kernel mounted", roomID)
		return fmt.Errorf("room: 未挂载房间内核")
	}
	if roomID == "" {
		logger.Warnf("room: EnsureRoom rejected: empty room id")
		return fmt.Errorf("room: 房间 ID 不能为空")
	}
	if err := m.Kernel.EnsureRoom(roomID); err != nil {
		logger.Warnf("room: EnsureRoom room=%s failed: %v", roomID, err)
		return err
	}
	// owner 注册：帧同步内核由 WithOnRoomCreated 回调完成，业务内核没有回调，
	// 这里统一兜底一次（RegisterRoom 幂等，重复调用无副作用）。
	if m.Owner != nil {
		m.Owner.RegisterRoom(roomID, m.cfg.NodeAddr)
	}
	if m.Takeover != nil {
		if err := m.Takeover.Activate(roomID); err != nil {
			logger.Warnf("room: takeover activate room=%s failed: %v", roomID, err)
		}
	}
	return nil
}

// JoinRoom 把玩家路由到房间 owner 节点并进房。
// 返回 (owner 节点地址, 是否发生了连接切换, error)。
func (m *Module) JoinRoom(connID, roomID, playerID string) (string, bool, error) {
	if m == nil || m.Kernel == nil {
		logger.Warnf("room: JoinRoom room=%s player=%s rejected: no kernel mounted", roomID, playerID)
		return "", false, fmt.Errorf("room: 未挂载房间内核")
	}
	owner, switched := m.cfg.NodeAddr, false
	if m.Owner != nil {
		var err error
		owner, switched, err = m.Owner.EnsureOwner(connID, roomID, m.cfg.NodeAddr)
		if err != nil {
			logger.Warnf("room: JoinRoom room=%s player=%s ensure owner failed: %v", roomID, playerID, err)
			return "", false, err
		}
		if switched {
			// 连接已切到 owner 节点，本进程不进房：后续由 owner 节点处理 JoinRoom。
			return owner, true, nil
		}
	}
	if m.Takeover != nil {
		if err := m.Takeover.Activate(roomID); err != nil {
			logger.Warnf("room: takeover activate room=%s failed: %v", roomID, err)
		}
	}
	if err := m.Kernel.Join(roomID, playerID); err != nil {
		logger.Warnf("room: JoinRoom room=%s player=%s failed: %v", roomID, playerID, err)
		return owner, false, err
	}
	return owner, false, nil
}

// LeaveRoom 玩家离房。
func (m *Module) LeaveRoom(roomID, playerID string) error {
	if m == nil || m.Kernel == nil {
		logger.Warnf("room: LeaveRoom room=%s player=%s rejected: no kernel mounted", roomID, playerID)
		return fmt.Errorf("room: 未挂载房间内核")
	}
	if err := m.Kernel.Leave(roomID, playerID); err != nil {
		logger.Warnf("room: LeaveRoom room=%s player=%s failed: %v", roomID, playerID, err)
		return err
	}
	return nil
}

// DestroyRoom 销毁房间，并注销 owner。
func (m *Module) DestroyRoom(roomID string) error {
	if m == nil || m.Kernel == nil {
		logger.Warnf("room: DestroyRoom room=%s rejected: no kernel mounted", roomID)
		return fmt.Errorf("room: 未挂载房间内核")
	}
	if err := m.Kernel.Destroy(roomID); err != nil {
		logger.Warnf("room: DestroyRoom room=%s failed: %v", roomID, err)
		return err
	}
	if m.Owner != nil {
		m.Owner.UnregisterRoom(roomID, m.cfg.NodeAddr)
	}
	return nil
}

// Close 关闭房间子系统（释放内核资源）。
func (m *Module) Close() {
	if m == nil || m.Kernel == nil {
		return
	}
	m.Kernel.Close()
}
