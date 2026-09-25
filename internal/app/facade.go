// facade.go 是应用启动层的**门面真身**：把 internal 的具体 *Game / *MasterGame / *LogGame /
// *AuthGame 包装成业务可用的形态（GameFacade 等），并把 `pkg/transport/event` 的
// Ctx / Handler 桥接到 internal 的具体类型。
//
// 为什么真身必须在 internal：`pkg/**` 只允许做门面（类型别名 / 变量转发 / 极薄适配），
// 见 `结构规则.md` §5.1；本文件里的包装、桥接与装配是行为实现体，故落在 internal。
// `pkg/app`（门面包，业务唯一可 import 的装配入口）只持有别名与变量转发。
//
// 命名：包装结构加 `Facade` 后缀（`GameFacade` / `MasterGameFacade` / `LogGameFacade` /
// `AuthGameFacade`），以避开本包既有的同名实现；接口 `AdminServerFacade` / `TimeEventFacade`
// 同理避开既有的 `AdminServer` / `TimeEvent`。业务侧可见名是**不带后缀**的那些
// （`type Game = internal.app.GameFacade` 之类）。
package app

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	masterclient "github.com/qw576483/clover-server-engine/internal/domain/master/client"
	"github.com/qw576483/clover-server-engine/internal/domain/mmo"
	ievent "github.com/qw576483/clover-server-engine/internal/transport/event"
	"github.com/qw576483/clover-server-engine/pkg/domain/data"
	"github.com/qw576483/clover-server-engine/pkg/domain/data/account"
	"github.com/qw576483/clover-server-engine/pkg/domain/data/order"
	"github.com/qw576483/clover-server-engine/pkg/domain/data/player"
	"github.com/qw576483/clover-server-engine/pkg/domain/master"
	"github.com/qw576483/clover-server-engine/pkg/domain/object"
	"github.com/qw576483/clover-server-engine/pkg/domain/room"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logbuf"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logstore"
	"github.com/qw576483/clover-server-engine/pkg/runtime/timer"
	"github.com/qw576483/clover-server-engine/pkg/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/transport/event"
)

// 编译期断言：*GameFacade 满足 master 侧的两个业务接口。
var (
	_ master.RankGame         = (*GameFacade)(nil)
	_ master.PlayerLookupGame = (*GameFacade)(nil)
)

// 编译期断言：internal 的 *masterclient.Client 满足门面接口 master.MasterClient。
var _ master.MasterClient = (*masterclient.Client)(nil)

// 编译期断言：*MasterGameFacade 满足 room.MasterHandlerGame 接口。
var _ room.MasterHandlerGame = (*MasterGameFacade)(nil)

// 编译期断言：*MasterGameFacade 具备「注册引擎内部保留号」的能力
// （internal/domain/room/pkgfacade.go 的 masterInternalRegistrar 按同一方法集做类型断言）。
// 钉在这里是为了让方法改名/删除在编译期就炸，而不是等 room 注册内建房间号时才 panic。
var _ interface {
	InternalOnMsg(msgID uint32, h event.Handler)
} = (*MasterGameFacade)(nil)

// GameFacade 游戏服（事件驱动）。业务只 OnMsg / OnEvent，框架负责派发与回包。
// 门面以包装结构实现（嵌入 internal *Game），把 Ctx / Handler 桥接到 pkg/transport/event 的接口形式。
// 业务侧可见名 = `pkg/app.Game`。
type GameFacade struct{ *Game }

// MasterGameFacade master 侧游戏逻辑入口。嵌入 Core，提供 OnMsg / OnEvent / OnHTTP / Bind / Reply。
// 门面以包装结构实现（嵌入 internal *MasterGame），把 Ctx / Handler 桥接到 pkg/transport/event 的接口形式。
// 业务侧可见名 = `pkg/app.MasterGame`。
type MasterGameFacade struct{ *MasterGame }

// LogGameFacade log 服入口。与 MasterGame 同构：headless Core + 自己的传输层（批量日志 TCP）。
// 业务 handler 签名与 Game / MasterGame 一致（func(c event.Ctx) error）。
// 业务侧可见名 = `pkg/app.LogGame`。
type LogGameFacade struct{ *LogGame }

// AuthGameFacade 账号服入口。与 MasterGame / LogGame 同构（headless Core），HTTP 由 Core 控制面提供。
// 账号专属逻辑（JWT / 撞库锁 / 渠道绑定）在引擎内部，业务主要用 OnHTTP 追加路由；
// 订单归属账号服，支付回调经 OnHTTP 挂到 /auth/pay/... 后用 OrderStore 建单 / 流转状态。
// 业务侧可见名 = `pkg/app.AuthGame`。
type AuthGameFacade struct{ *AuthGame }

// AdminServerFacade 内置 admin HTTP 控制面服务（方法承载型）。
//
// 由 internal 的 *AdminServer 直接满足（直断言），F12 停在接口即可看到完整方法集。
// 业务通常不需要直接持有它——在 app.Mount 回调里通过 g.OnAdminHTTP(pattern, h)
// 注册自定义探针端点（health、ready、metrics、debug 等），引擎在 runMounts 之后
// 统一挂载到 admin server（默认 127.0.0.1:8041），供运维 / 监控系统访问。
// 业务侧可见名 = `pkg/app.AdminServer`。
type AdminServerFacade interface {
	// Handle 注册一个 admin 路由（nil handler 或空 pattern 被忽略；重复注册后者被忽略）。
	Handle(pattern string, h http.Handler)
	// HandleFunc 是 Handle 的函数形式。
	HandleFunc(pattern string, fn func(http.ResponseWriter, *http.Request))
	// Routes 返回已注册路由快照（排序后）。
	Routes() []string
	// Addr 返回实际监听地址（Start 之后有效，:0 自动分配端口场景下尤为有用）。
	Addr() string
	// Start 开始监听（后台 goroutine 提供服务，不阻塞调用方）；nil 接收者返回 nil。
	Start() error
	// Stop 关闭服务，等待在途请求结束、超时后强制关闭；幂等。
	Stop(ctx context.Context)
}

// 编译期断言：internal 的 *AdminServer 满足门面接口 AdminServerFacade。
//
// 用 typed-nil 做断言而不是 NewAdminServer(...)：构造调用在**包初始化期**执行
// 有真实副作用（建 mux、挂 /deadletter 与 drain 路由等），且该实例永远不会被 Close。
// typed-nil 同样只做方法集检查，零副作用。
var _ AdminServerFacade = (*AdminServer)(nil)

// FromAdminServer 将 internal 的 *AdminServer 包装为门面 AdminServerFacade 接口；nil 返回 nil。
// 业务侧可见名 = `pkg/app.FromAdminServer`。
func FromAdminServer(s *AdminServer) AdminServerFacade {
	if s == nil {
		return nil
	}
	return s
}

// InternalAdminServer 将门面 AdminServerFacade 接口还原为 internal 的 *AdminServer；非底层则 ok=false。
// 业务侧可见名 = `pkg/app.InternalAdminServer`。
func InternalAdminServer(s AdminServerFacade) (*AdminServer, bool) {
	v, ok := s.(*AdminServer)
	return v, ok
}

// TimeEventFacade 共享定时器调度器，提供业务侧定时任务注册能力（方法承载型）。
//
// 业务经 g.Timer（*TimeEvent）或显式引用本接口调用 Every/After/ByTime/DailyAt/Cron
// 等定时注册、时区解析与定时器组管理。
// 定时器本体（*timer.Scheduler / *timer.Group）定义在 pkg/runtime/timer，F12 直达实现。
// 业务侧可见名 = `pkg/app.TimeEvent`。
type TimeEventFacade interface {
	// Scheduler 返回底层定时器调度器（pkg/runtime/timer 的 *Scheduler，可再按组分配）。引擎级共享实例。
	Scheduler() *timer.Scheduler
	// Close 关闭调度器，取消其下全部定时任务。
	Close()
	// Every 每 interval 执行一次任务，返回可取消的 timer.Timer。
	Every(name string, interval time.Duration, task timer.Task) timer.Timer
	// After 延迟 delay 后执行一次任务。
	After(name string, delay time.Duration, task timer.Task) timer.Timer
	// Location 返回当前生效的时区。
	Location() *time.Location
	// ParseDate 按当前时区解析日期字符串。
	ParseDate(value string) (time.Time, error)
	// Date 以本地时区构造 time.Time。
	Date(year int, month time.Month, day, hour, min, sec, nsec int) time.Time
	// ByTime 在指定时刻执行一次任务。
	ByTime(name string, target time.Time, task timer.Task) timer.Timer
	// DailyAt 每天固定时刻执行任务。
	DailyAt(name string, hour, minute, second int, task timer.Task) timer.Timer
	// Cron 以 cron 表达式规律执行任务。
	Cron(name, spec string, task timer.Task) (timer.Timer, error)
	// StopTimer 按名字取消一个定时任务；成功取消返回 true。
	StopTimer(name string) bool
	// TimerGroup 取得/创建名为 scope 的定时器组（pkg/runtime/timer 的 *Group，可按组批量取消）。
	TimerGroup(scope string) *timer.Group
	// StopTimerGroup 取消名为 scope 的定时器组下全部任务。
	StopTimerGroup(scope string)
}

// FromTimeEvent 将 *TimeEvent 以门面 TimeEventFacade 接口形式返回；nil 返回 nil。
// 业务侧可见名 = `pkg/app.FromTimeEvent`。
func FromTimeEvent(t *TimeEvent) TimeEventFacade {
	if t == nil {
		return nil
	}
	return t
}

// InternalTimeEvent 将门面 TimeEventFacade 接口还原为 *TimeEvent；非引擎实现则 ok=false。
// 业务侧可见名 = `pkg/app.InternalTimeEvent`。
func InternalTimeEvent(t TimeEventFacade) (*TimeEvent, bool) {
	if v, ok := t.(*TimeEvent); ok {
		return v, true
	}
	return nil, false
}

// RunFacade 从配置目录 / 文件加载配置并按 server_type 启动对应进程，阻塞到收到退出信号。
// bootstrap（可选）在逻辑服启动监听前被调用，用于给 GameFacade 绑定业务 handler。
// 业务通常在 init 中通过 Mount 声明挂载，无需再传 bootstrap。
// 业务侧可见名 = `pkg/app.Run`。
func RunFacade(configPath string, bootstrap ...func(g *GameFacade) error) error {
	return Run(configPath, wrapBootstraps(bootstrap)...)
}

// RunWithConfigFacade 使用已构造好的 *Config 启动（跳过配置加载），其余行为与 Run 一致。
// 调用方通过 LoadConfig 提前构造 cfg，即可注入 MasterBusiness 等回调。
// 业务侧可见名 = `pkg/app.RunWithConfig`。
func RunWithConfigFacade(cfg *Config, bootstrap ...func(g *GameFacade) error) error {
	return RunWithConfig(cfg, wrapBootstraps(bootstrap)...)
}

// RunGameFacade 从配置构造并启动游戏服，自动创建通用数据存储（含建表）。
// 接受 ctx 参数，调用方可传入可取消 context 以停止服务，
// 避免硬编码 context.Background() 导致 DB 建表不可达时阻塞不响应 SIGTERM。
// 业务侧可见名 = `pkg/app.RunGame`。
func RunGameFacade(ctx context.Context, cfg *Config) (*GameFacade, error) {
	g, err := RunGame(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &GameFacade{g}, nil
}

// NewMasterClient 创建一个到 master 的客户端连接。
// 业务侧自己管理生命周期，创建后可访问排行榜等 master 能力。
// 业务侧可见名 = `pkg/app.NewMasterClient`。
func NewMasterClient(addr string) (master.MasterClient, error) {
	return NewMasterClientWithToken(addr, "")
}

// NewMasterClientWithToken 与 NewMasterClient 相同，但额外带上 master 内部 RPC 的共享密钥
// （配置里的 master_token）。master 绑非回环地址时**必须**用它：服务端要求每条连接
// 首帧完成 MsgAuth 握手，否则请求一律被拒并断开（见 domain/master/server）。
// 业务侧可见名 = `pkg/app.NewMasterClientWithToken`。
func NewMasterClientWithToken(addr, token string) (master.MasterClient, error) {
	c, err := masterclient.NewClient(addr, masterclient.WithToken(token))
	if err != nil {
		// typed-nil 守卫：避免 (*Client)(nil) 被装箱成非空接口。
		return nil, err
	}
	return c, nil
}

// RegisterLogBackend 注册一个日志落盘后端，供配置 `log_backend` 按名字引用。
//
// 业务在 init 里调用即可接入自有后端（ClickHouse / 自建服务 / 转发到 NATS…），
// **无需改引擎代码**：
//
//	func init() {
//	    app.RegisterLogBackend("clickhouse", func(raw map[string]any) (logstore.Backend, error) {
//	        return newClickHouseBackend(raw) // raw = 配置里的 log_backend_config
//	    })
//	}
//
// 然后在 server.yaml 里：
//
//	log_backend: "clickhouse"
//	log_backend_config:
//	  dsn: "clickhouse://127.0.0.1:9000"
//	  table: "biz_log"
//
// 说明：
//   - 后端契约见 pkg/foundation/logstore.Backend；引擎只在启动时按名字取一次工厂；
//   - 内置 "mysql" 不走注册表（它读 data.mysql），配置留空即用它；
//   - 同名重复注册会覆盖，便于测试与热替换；
//   - 名字为空 / 工厂为 nil 直接 panic（装配期错误，fail-fast）。
func RegisterLogBackend(name string, f logstore.Factory) {
	logstore.Register(name, f)
}

// PublishTableLoadedFacade 广播 table.Loaded 事件（门面 GameFacade 版本）。
// 业务侧可见名 = `pkg/app.PublishTableLoaded`。
func PublishTableLoadedFacade(g *GameFacade, names []string) {
	if g == nil {
		return
	}
	PublishTableLoaded(g.Game, names)
}

// Mount 登记业务挂载函数——**四个角色共用这一个入口**（通常在业务包 init 中调用）。
//
// fn 必须是下列之一，且与 role 匹配；不匹配在 init 期（进程启动时）立刻 panic，
// 不会出现"挂错角色、消息永远到不了"的静默失败：
//
//	func(*GameFacade)        ← RoleGame
//	func(*MasterGameFacade)  ← RoleMaster
//	func(*LogGameFacade)     ← RoleLog
//	func(*AuthGameFacade)    ← RoleAuth
//
// 用法：
//
//	app.Mount(app.RoleGame,   func(g *app.Game)       { g.OnMsg(1000101, onXxx) })
//	app.Mount(app.RoleMaster, func(m *app.MasterGame) { m.OnMsg(3000101, onXxx) })
//	app.Mount(app.RoleLog,    func(l *app.LogGame)    { l.OnMsg(3000201, onXxx) })
//	app.Mount(app.RoleAuth,   func(a *app.AuthGame)   { a.OnHTTP("/x", onXxx) })
//
// 客户端只直连网关与账号服的 HTTP；master / log 的业务消息一律由 game 用
// Game.Call(role, msgID, req, resp) 转发——所以这四个 handler 的写法完全一致。
//
// 业务侧可见名 = `pkg/app.Mount`。
func Mount(role Role, fn any) {
	switch role {
	case RoleGame:
		f, ok := fn.(func(*GameFacade))
		if !ok {
			panic(mountSigErr(role, "func(*app.Game)", fn))
		}
		RegisterMountSlot(role, func(host any) {
			h, ok := host.(*Game)
			if !ok {
				panic(fmt.Sprintf("app: Mount(%s): host 类型不符，期望 *Game，实际 %T", role, host))
			}
			f(&GameFacade{h})
		})
	case RoleMaster:
		f, ok := fn.(func(*MasterGameFacade))
		if !ok {
			panic(mountSigErr(role, "func(*app.MasterGame)", fn))
		}
		RegisterMountSlot(role, func(host any) {
			h, ok := host.(*MasterGame)
			if !ok {
				panic(fmt.Sprintf("app: Mount(%s): host 类型不符，期望 *MasterGame，实际 %T", role, host))
			}
			f(&MasterGameFacade{h})
		})
	case RoleLog:
		f, ok := fn.(func(*LogGameFacade))
		if !ok {
			panic(mountSigErr(role, "func(*app.LogGame)", fn))
		}
		RegisterMountSlot(role, func(host any) {
			h, ok := host.(*LogGame)
			if !ok {
				panic(fmt.Sprintf("app: Mount(%s): host 类型不符，期望 *LogGame，实际 %T", role, host))
			}
			f(&LogGameFacade{h})
		})
	case RoleAuth:
		f, ok := fn.(func(*AuthGameFacade))
		if !ok {
			panic(mountSigErr(role, "func(*app.AuthGame)", fn))
		}
		RegisterMountSlot(role, func(host any) {
			h, ok := host.(*AuthGame)
			if !ok {
				panic(fmt.Sprintf("app: Mount(%s): host 类型不符，期望 *AuthGame，实际 %T", role, host))
			}
			f(&AuthGameFacade{h})
		})
	default:
		panic(fmt.Sprintf("app: Mount: 未知角色 %q", role))
	}
}

// mountSigErr 组签名不匹配的 panic 文案（启动期就暴露，不留给运行期）。
func mountSigErr(role Role, want string, fn any) string {
	got := "<nil>"
	if fn != nil {
		got = reflect.TypeOf(fn).String()
	}
	return fmt.Sprintf("app: Mount(%s, fn)：fn 必须是 %s，实际是 %s", role, want, got)
}

// Call 向指定角色的服务发送一条同步业务请求并阻塞等待响应——game 唯一的转发入口。
//
// 客户端不直连 master / log（账号服除外，它另有 HTTP）：这些角色的业务消息
// 统一由 game 收到 C2S 后用本方法转发过去，与目标角色的 app.Mount(role, ...) 配对。
//
//	g.Call(app.RoleLog, 3000201, req, &resp)
func (g *GameFacade) Call(role Role, msgID uint32, req, resp any) error {
	switch role {
	case RoleMaster:
		return g.CallMaster(msgID, req, resp)
	case RoleLog:
		return g.CallLog(msgID, req, resp)
	case RoleAuth:
		return g.CallAuth(msgID, req, resp)
	default:
		return fmt.Errorf("app: Call: 角色 %q 不支持转发（game 自身无转发需求）", role)
	}
}

// pkg/transport/event 接口桥接
//
// GameFacade 与 MasterGameFacade 是嵌入 internal 具体实现的包装结构，仅在此处
// 对携带 event.Ctx / event.Handler 的方法做「门面接口 ↔ internal 具体类型」的转换，
// 其余方法（Data / Timer / CallMaster 等）经嵌入自动提升，无需逐一转发。

// PlayerStore 返回角色存储的业务接口（pkg/domain/data/player.Store）。
// AccountStore 返回账号存储的业务接口（pkg/domain/data/account.Store）。
// ChannelStore 返回账号渠道绑定的业务接口（pkg/domain/data/account.ChannelStore）。
// OrderStore 返回订单存储的业务接口（pkg/domain/data/order.Store）。
// 覆写嵌入的 internal 方法，使其返回 pkg 门面接口而非 internal 具体类型——
// 业务经 F12 即可看到完整方法集与注释，且无需 import internal 包。
func (g *GameFacade) PlayerStore() player.Store {
	s := g.Game.PlayerStore()
	if s == nil {
		// 无 DB 时 internal 返回 typed-nil 具体指针；直接转接口会得到
		// 「非 nil 接口含 nil 指针」，业务判空失败、调用即 panic。这里转成真正的 nil 接口。
		return nil
	}
	return s
}

func (g *GameFacade) AccountStore() account.Store {
	s := g.Game.AccountStore()
	if s == nil {
		return nil
	}
	return s
}

func (g *GameFacade) ChannelStore() account.ChannelStore {
	s := g.Game.ChannelStore()
	if s == nil {
		return nil
	}
	return s
}

func (g *GameFacade) OrderStore() order.Store {
	s := g.Game.OrderStore()
	if s == nil {
		return nil
	}
	return s
}

// AddLog 写入一条业务日志（必填字段 + option 选填），攒积后批量上报 log 服。
// 未初始化 log 服时静默丢弃，不阻塞业务主流程。
// 覆写嵌入的 internal 方法，使签名的 opts 使用 pkg/foundation/logbuf.Option 门面，
// 业务经 F12 停留在 pkg 侧，无需 import internal 包。
func (g *GameFacade) AddLog(ownerType, ownerID, typ, info string, opts ...logbuf.Option) {
	g.Game.AddLog(ownerType, ownerID, typ, info, opts...)
}

// wrapBootstraps 把业务侧 *GameFacade 包装为 internal 的 *Game 传给引擎启动流程。
func wrapBootstraps(in []func(g *GameFacade) error) []func(g *Game) error {
	if len(in) == 0 {
		return nil
	}
	out := make([]func(g *Game) error, 0, len(in))
	for _, b := range in {
		fn := b
		out = append(out, func(g *Game) error { return fn(&GameFacade{g}) })
	}
	return out
}

// unwrapEventCtx 把门面 event.Ctx 接口还原为 internal 具体 *ievent.Ctx。
// 传入实现 must 是 *ievent.Ctx（门面接口仅此一份实现，见 event 包编译期断言）；
// 传入其他实现属调用方编码错误，立即 panic 以便在边界暴露问题而非静默失败。
func unwrapEventCtx(c event.Ctx) *ievent.Ctx {
	ic, ok := c.(*ievent.Ctx)
	if !ok {
		panic("app: reply ctx 不是 engine 派发的 *event.Ctx，禁止手动构造 Ctx")
	}
	return ic
}

// bridgeHandler 把 pkg 的 event.Handler（func(event.Ctx) error）转成 internal 的 event.Handler。
// nil handler 直接 panic：包装闭包永远非 nil，注册 nil 会让 internal 侧视为有效 handler，
// 直到派发期调用 nil 函数才 panic —— 届时已远离注册现场，无法定位是谁注册的。
func bridgeHandler(h event.Handler) ievent.Handler {
	if h == nil {
		panic("app: nil event handler registered (OnMsg/OnEvent)")
	}
	return func(ic *ievent.Ctx) error { return h(ic) }
}

// OnMsg 注册消息 handler（handler 签名 func(c event.Ctx) error）。
func (g *GameFacade) OnMsg(msgID uint32, h event.Handler, priority ...int) {
	var p []int
	if len(priority) > 0 {
		p = priority
	}
	g.Game.OnMsg(msgID, bridgeHandler(h), p...)
}

// OnEvent 注册领域事件 handler（handler 签名 func(c event.Ctx) error）。
func (g *GameFacade) OnEvent(typ string, h event.Handler) { g.Game.OnEvent(typ, bridgeHandler(h)) }

// OnBeforeDispatch 注册业务侧「消息级横切钩子」（handler 签名 func(c event.Ctx) error）。
//
// 执行时机：引擎内置横切逻辑（停机检查 / 灰度下线迁移 / 连接 KV 与身份恢复 / session token
// 续期 / 跨服玩家注册）之后、进入业务 handler 之前；任一钩子返回 error 即拒绝本次派发。
// 返回 *proto.BizError 时错误回包会带上 Code（如 403），客户端据码分支，无需匹配文案。
//
// 与 OnMsg / OnEvent 同款桥接：把 pkg 的 event.Ctx 接口适配为 internal 的具体 *Ctx，
// 使业务可按 func(c event.Ctx) error 注册，无需引用 internal 类型。
func (g *GameFacade) OnBeforeDispatch(fn func(c event.Ctx) error) {
	if fn == nil {
		panic("app: OnBeforeDispatch: nil handler")
	}
	g.Game.OnBeforeDispatch(func(ic *ievent.Ctx) error { return fn(ic) })
}

// OnHTTP 注册一条「HTTP 事件」路由（无状态请求-响应，无会话 / 玩家上下文）。
// pattern 支持 ":name" 捕获段（如 "/admin/player/:pid" → handler 内 c.Param("pid")）。
// 未配置 Logic.HTTPListen 时 HTTP 控制面不启动，注册不生效也不报错。
func (g *GameFacade) OnHTTP(pattern string, h event.HTTPHandler) {
	if h == nil {
		panic("app: OnHTTP: nil handler")
	}
	g.Game.OnHTTP(pattern, func(ic *ievent.HTTPCtx) error { return h(ic) })
}

// EmitEvent 主动派发一条领域事件给本节点 OnEvent 订阅者。
//
// parent 为发起方上下文：传业务 handler 内的 event.Ctx 时，事件 handler 内
// c.PlayerID() / c.Account() / c.ConnID() 完整可用；传 nil（定时器 / 后台触发）
// 时构造最小 Ctx，这些标识为空。
func (g *GameFacade) EmitEvent(typ string, parent event.Ctx, payload any) error {
	if parent == nil {
		return g.Game.EmitEvent(typ, nil, payload)
	}
	return g.Game.EmitEvent(typ, unwrapEventCtx(parent), payload)
}

// Reply 以结构体 JSON 编码回包给客户端。同一 Ctx 仅首次生效。
func (g *GameFacade) Reply(c event.Ctx, v any) { g.Game.Reply(unwrapEventCtx(c), v) }

// ReplyRaw 以原始字节回包（需自定义编码时，按 requestID 配对）。
func (g *GameFacade) ReplyRaw(c event.Ctx, body []byte) { g.Game.ReplyRaw(unwrapEventCtx(c), body) }

// Alert 自适应弹窗：根据 ctx 自动选择路由 ID（PlayerID 优先，未登录时回退到 Account）。
func (g *GameFacade) Alert(c event.Ctx, a *proto.EAlertNotify) error {
	return g.Game.Alert(unwrapEventCtx(c), a)
}

// ============ 主动推送（Player / Scene / All） ============
// 三个 JSON 入口是业务主动下发消息的公开第一选择；Raw 版本供自定义编码使用。
// 注意：业务侧（如 20fps 状态同步）也可直接 g.Reply 轮询，本组 API 用于真正的服务端主动推送。

// PushToPlayer 向指定玩家推送一条业务消息（对象按 JSON 编码，默认可靠送达）。
func (g *GameFacade) PushToPlayer(playerID string, msgID uint32, v any, opts ...proto.DeliveryMode) error {
	return g.Game.PushToPlayerJSON(playerID, msgID, v, opts...)
}

// PushToScene 向一个场景广播业务消息（对象按 JSON 编码，默认可靠送达）。
func (g *GameFacade) PushToScene(scene proto.ESceneBroadcaster, msgID uint32, v any, opts ...proto.DeliveryMode) error {
	return g.Game.PushToSceneJSON(scene, msgID, v, opts...)
}

// PushToAll 向全部在线玩家广播业务消息（对象按 JSON 编码，默认可靠送达）。
func (g *GameFacade) PushToAll(msgID uint32, v any, opts ...proto.DeliveryMode) error {
	return g.Game.PushToAllJSON(msgID, v, opts...)
}

// PushToPlayerRaw 向指定玩家推送原始字节（自定义编码时使用）。
func (g *GameFacade) PushToPlayerRaw(playerID string, msgID uint32, body []byte, opts ...proto.DeliveryMode) error {
	return g.Game.PushToPlayer(playerID, msgID, body, opts...)
}

// PushToSceneRaw 向场景广播原始字节（自定义编码时使用）。
func (g *GameFacade) PushToSceneRaw(scene proto.ESceneBroadcaster, msgID uint32, body []byte, opts ...proto.DeliveryMode) error {
	return g.Game.PushToScene(scene, msgID, body, opts...)
}

// PushToAllRaw 向全部在线玩家广播原始字节（自定义编码时使用）。
func (g *GameFacade) PushToAllRaw(msgID uint32, body []byte, opts ...proto.DeliveryMode) error {
	return g.Game.PushToAll(msgID, body, opts...)
}

// PushPlayerFullSync 触发一次玩家全量数据同步推送（EPushPlayerFullSync=4001）。
//
// ★ 引擎**不会**自动调用它，这是刻意的分层，不是漏接线：
// 全量同步的语义是"客户端可以开始玩游戏了"，而"什么时候算开始了"是业务语义
// （有的游戏要选服 / 选角 / 过新手引导）。引擎无从判断，硬猜只会推早或推重。
// 业务应在以下时机显式调用：
//   - 登录成功、且该账号**已有角色**时；
//   - 或业务自己创建角色成功之后。
//
// 不调的后果是**完全静默**的：客户端 `Game.Net.Session.PlayerID` 永远为空、
// `Game.Sync.OnFullSync` 永不触发、断线恢复凭证 session_token 永不下发。
//
// 注：本方法在 internal 侧已实现，经匿名内嵌本来也能隐式调到；这里显式声明是为了
// 让它成为**正式能力面**（有签名、有文档、能被检索到），而不是"碰巧能调到"。
func (g *GameFacade) PushPlayerFullSync(playerID, account string) {
	g.Game.PushPlayerFullSync(playerID, account)
}

// SendEventToPlayer 向指定玩家投递领域事件，自动跨节点寻址。
func (g *GameFacade) SendEventToPlayer(c event.Ctx, playerID, typ string, payload any) error {
	return g.Game.SendEventToPlayer(unwrapEventCtx(c), playerID, typ, payload)
}

// SendQueueEventToPlayer 向指定玩家投递领域事件，保证同一玩家跨节点事件串行 FIFO。
func (g *GameFacade) SendQueueEventToPlayer(c event.Ctx, playerID, typ string, payload any) error {
	return g.Game.SendQueueEventToPlayer(unwrapEventCtx(c), playerID, typ, payload)
}

// ============ GObject 事件 ============

// ObjectManager 返回 Game 内置的对象管理器。
func (g *GameFacade) ObjectManager() *object.Manager { return g.Game.ObjectManager() }

// ============ MMO 跨机对象迁移（切场景）============
//
// 把下面三样注入 mmo.NewSceneManager 即可开启跨机迁移：
//
//	sm := mmo.NewSceneManager(
//	    mmo.WithStore(store), mmo.WithPublisher(pub),
//	    mmo.WithClusterRoute(g.SceneRoute(), g.NodeID()),
//	    mmo.WithRemoteTransferSubscriber(g.SceneSubscriber()),
//	)
//
// 缺失时三者分别为 nil / 0 / nil（未配 node_id、或未接 NATS），迁移自动不启用，
// TransferRemote 返回 ErrNoRoute —— 单机部署无需关心。

// NodeID 返回本节点在集群内的数字标识（跨机对象迁移的路由键）；未配置时为 0。
func (g *GameFacade) NodeID() uint64 { return g.Game.NodeID() }

// SceneRoute 返回 scene→node 路由表，供 mmo.WithClusterRoute 注入。
func (g *GameFacade) SceneRoute() mmo.SceneRoute { return g.Game.SceneRoute() }

// SceneSubscriber 返回订阅能力，供 mmo.WithRemoteTransferSubscriber 注入，
// 用于接收跨机迁移指令。
func (g *GameFacade) SceneSubscriber() mmo.Subscriber { return g.Game.SceneSubscriber() }

// SendEventToGObject 向指定游戏对象投递事件（纯本地内存调用，零网络开销）。
func (g *GameFacade) SendEventToGObject(ctx context.Context, id object.ObjectID, eventType string, payload any) error {
	return g.Game.SendEventToGObject(ctx, id, eventType, payload)
}

// OnGObjectEvent 绑定 (对象类型, 事件名) → 事件处理器。
func (g *GameFacade) OnGObjectEvent(objType uint16, eventType string, h object.EventHandler) {
	g.Game.OnGObjectEvent(objType, eventType, h)
}

// SendQueueEventToGObject 串行通道：向指定游戏对象投递事件，同一对象上的事件互斥执行。
// 需要原子读-改-写对象状态（怪物掉血、掉落归属、共享计数）时用这个。
func (g *GameFacade) SendQueueEventToGObject(ctx context.Context, id object.ObjectID, eventType string, payload any) error {
	return g.Game.SendQueueEventToGObject(ctx, id, eventType, payload)
}

// LoadStruct 可修改加载/自动落库（业务 handler 内使用）。
func (g *GameFacade) LoadStruct(c event.Ctx, schema data.StructSchema, id string, v any) error {
	return g.Game.LoadStruct(unwrapEventCtx(c), schema, id, v)
}

// LoadRecord 记录加载（业务 handler 内使用）。
func (g *GameFacade) LoadRecord(c event.Ctx, schema data.RecordSchema, id string) (data.Record, error) {
	return g.Game.LoadRecord(unwrapEventCtx(c), schema, id)
}

// RegisterEngineHandler 注册引擎级消息处理器。供外部模块（masterRank 等）使用。
// handler 签名适配 pkg/domain/master.RankGame 接口。
func (g *GameFacade) RegisterEngineHandler(msgID uint32, handler func(ctx context.Context, msgID uint32, data []byte) ([]byte, error)) {
	g.Game.RegisterEngineHandler(msgID, handler)
}

// MasterClient 返回跨服 bus 的 master TCP 客户端。供外部模块使用。
func (g *GameFacade) MasterClient() master.MasterClient {
	return g.Game.MasterClient()
}

// OnMsg 注册 Master TCP 消息 handler（handler 签名 func(c event.Ctx) error）。
func (mg *MasterGameFacade) OnMsg(msgID uint32, h event.Handler) {
	mg.MasterGame.OnMsg(msgID, bridgeHandler(h))
}

// InternalOnMsg 注册**引擎内部保留号**（≤ pkg/shared/proto.InternalMsgMax）的 Master handler。
//
// 供引擎内部模块使用：`room.NewMasterHandlers(mg).Register()` 要注册的 4 条
// master↔game 房间协议号（EMasterRoomRegister=6001..TakeoverClaim=6004）是引擎内建的，
// 经 OnMsg 会被业务号守卫拒绝（`app: business message id must be > 10000; got 6001`）。
//
// ⛔ 这不是给业务开口子：业务仍只能用 OnMsg 注册 >10000 的号（守卫未放宽）。
// 调用方是 internal/domain/room 的 master handler 门面（pkgfacade.go 的 masterInternalRegistrar）。
func (mg *MasterGameFacade) InternalOnMsg(msgID uint32, h event.Handler) {
	mg.MasterGame.InternalOnMsg(msgID, bridgeHandler(h))
}

// OnEvent 注册 Master 侧领域事件 handler。
func (mg *MasterGameFacade) OnEvent(typ string, h event.Handler) {
	mg.MasterGame.OnEvent(typ, bridgeHandler(h))
}

// OnHTTP 注册 Master 侧 HTTP 路由（handler 签名 func(c event.HTTPCtx) error）。
func (mg *MasterGameFacade) OnHTTP(pattern string, h event.HTTPHandler) {
	if h == nil {
		panic("app: MasterGame.OnHTTP: nil handler")
	}
	mg.MasterGame.OnHTTP(pattern, func(ic *ievent.HTTPCtx) error { return h(ic) })
}

// EmitEvent 主动派发一条领域事件给本节点 OnEvent 订阅者；parent 为 nil 时构造最小 Ctx。
func (mg *MasterGameFacade) EmitEvent(typ string, parent event.Ctx, payload any) error {
	if parent == nil {
		return mg.MasterGame.EmitEvent(typ, nil, payload)
	}
	return mg.MasterGame.EmitEvent(typ, unwrapEventCtx(parent), payload)
}

// Reply 以结构体 JSON 编码回包。同一 Ctx 仅首次生效。
func (mg *MasterGameFacade) Reply(c event.Ctx, v any) { mg.MasterGame.Reply(unwrapEventCtx(c), v) }

// ReplyRaw 以原始字节回包（需自定义编码时，按 requestID 配对）。
func (mg *MasterGameFacade) ReplyRaw(c event.Ctx, body []byte) {
	mg.MasterGame.ReplyRaw(unwrapEventCtx(c), body)
}

// LoadStruct 可修改加载/自动落库（Master handler 内使用）。
func (mg *MasterGameFacade) LoadStruct(c event.Ctx, schema data.StructSchema, id string, v any) error {
	return mg.MasterGame.LoadStruct(unwrapEventCtx(c), schema, id, v)
}

// LoadRecord 记录加载（Master handler 内使用）。
func (mg *MasterGameFacade) LoadRecord(c event.Ctx, schema data.RecordSchema, id string) (data.Record, error) {
	return mg.MasterGame.LoadRecord(unwrapEventCtx(c), schema, id)
}

// MasterRegistry 返回房间 owner 注册表（pkg 门面接口）。
// 具体包装由 internal/domain/room 注册的实现完成，pkg 侧不出现 internal 类型。
func (mg *MasterGameFacade) MasterRegistry() room.MasterRegistry {
	return room.WrapMasterRegistry(mg.MasterGame.MasterRegistry())
}

// —— LogGameFacade 门面桥接（签名与 Game / MasterGame 完全一致）——

// OnMsg 注册 log 服业务消息 handler（handler 签名 func(c event.Ctx) error）。
func (lg *LogGameFacade) OnMsg(msgID uint32, h event.Handler) {
	lg.LogGame.OnMsg(msgID, bridgeHandler(h))
}

// OnEvent 注册 log 服领域事件 handler。
func (lg *LogGameFacade) OnEvent(typ string, h event.Handler) {
	lg.LogGame.OnEvent(typ, bridgeHandler(h))
}

// OnHTTP 注册 log 服 HTTP 路由（handler 签名 func(c event.HTTPCtx) error）。
func (lg *LogGameFacade) OnHTTP(pattern string, h event.HTTPHandler) {
	if h == nil {
		panic("app: LogGame.OnHTTP: nil handler")
	}
	lg.LogGame.OnHTTP(pattern, func(ic *ievent.HTTPCtx) error { return h(ic) })
}

// Reply 以结构体 JSON 编码回包。同一 Ctx 仅首次生效。
func (lg *LogGameFacade) Reply(c event.Ctx, v any) { lg.LogGame.Reply(unwrapEventCtx(c), v) }

// ReplyRaw 以原始字节回包（需自定义编码时，按 requestID 配对）。
func (lg *LogGameFacade) ReplyRaw(c event.Ctx, body []byte) {
	lg.LogGame.ReplyRaw(unwrapEventCtx(c), body)
}

// —— AuthGameFacade 门面桥接 ——

// OrderStore 返回账号服持有的订单存储（pkg/domain/data/order.Store）。
// 订单归属账号服：渠道支付回调经 OnHTTP 挂到 /auth/pay/... 后，用它建单 /
// 流转状态（Create / MarkPaid / MarkDone）。发货仍由 game 执行。
// 账号服未接可用 MySQL 数据层时为 nil（此时账号服本身也起不来）。
func (ag *AuthGameFacade) OrderStore() order.Store {
	s := ag.AuthGame.OrderStore()
	if s == nil {
		// 账号服未接可用 MySQL 数据层时为 typed-nil：同上，转成真正的 nil 接口。
		return nil
	}
	return s
}

// OnEvent 注册账号服领域事件 handler。
func (ag *AuthGameFacade) OnEvent(typ string, h event.Handler) {
	ag.AuthGame.OnEvent(typ, bridgeHandler(h))
}

// OnHTTP 注册账号服 HTTP 路由（handler 签名 func(c event.HTTPCtx) error）。
func (ag *AuthGameFacade) OnHTTP(pattern string, h event.HTTPHandler) {
	if h == nil {
		panic("app: AuthGame.OnHTTP: nil handler")
	}
	ag.AuthGame.OnHTTP(pattern, func(ic *ievent.HTTPCtx) error { return h(ic) })
}

// OnMsg 注册账号服业务消息 handler（handler 签名 func(c event.Ctx) error）。
// 入站通道为 auth.rpc_listen，game 侧用 Game.CallAuth 调用；登录校验仍走 HTTP /auth/verify。
func (ag *AuthGameFacade) OnMsg(msgID uint32, h event.Handler) {
	ag.AuthGame.OnMsg(msgID, bridgeHandler(h))
}

// Reply 以结构体 JSON 编码回包。同一 Ctx 仅首次生效。
func (ag *AuthGameFacade) Reply(c event.Ctx, v any) { ag.AuthGame.Reply(unwrapEventCtx(c), v) }

// ReplyRaw 以原始字节回包（需自定义编码时，按 requestID 配对）。
func (ag *AuthGameFacade) ReplyRaw(c event.Ctx, body []byte) {
	ag.AuthGame.ReplyRaw(unwrapEventCtx(c), body)
}
