// Package app 是 clover 通用「应用启动层」的公开门面。
//
// 业务项目只调用 app.Run(configPath) 即可按配置启动
// 网关 + 逻辑服并阻塞到退出信号，无需再写 server.go / game.go。
//
// 本包是**门面包**（见 `结构规则.md` §5.1），也是业务唯一能 import 的装配入口：
// 业务模块是独立 Go module，受 Go `internal` 规则限制进不去 `internal/`
// —— 所以扩展点（Mount / RegisterChannelVerifier / RegisterLogBackend …）必须在这里可见，
// 而它们的**真身全部在 `internal/app/facade.go`**（包装结构、桥接、装配）。
//
// 本包只允许三类东西：**类型别名**（`type X = internal.X`）、**常量转发**
// （`const K = internal.K`）、**声明转发**（`var F = internal.F`）。不含实现体。
// F12 停在 `app.Game` / `app.Run` 上即跳进 internal 看完整实现与注释。
package app

import (
	iapp "github.com/qw576483/clover-server-engine/internal/app"
)

// ServerTypeGame 游戏服（game 服）server_type：承载玩法与玩家状态。
const ServerTypeGame = iapp.ServerTypeGame

// ServerTypeGateway 网关 server_type。
const ServerTypeGateway = iapp.ServerTypeGateway

// ServerTypeAll 网关 + 游戏服同进程一体启动。
const ServerTypeAll = iapp.ServerTypeAll

// EngineVersion 当前 clover-server-engine 版本号（引擎自身常量，业务只读引用）。
const EngineVersion = iapp.EngineVersion

// TableLoadedType 表加载完成事件类型（业务手动 emit 时使用）。
const TableLoadedType = iapp.TableLoadedType

// 类型透传：配置与内核。
type (
	// Config clover 应用整体配置（网关 / 逻辑服共享同一结构，按需取用）。
	// 字段：ServerType 进程角色（game|gateway|all|master|log|auth）；Tags 业务标签；Gateway 网关接入配置；Logic 逻辑服配置；NATS NATS 消息队列；Data 通用数据存储；Etcd 服务发现；NATSSubject 下行推送 subject；Log 日志配置；Misc 杂项配置（时区等）；Admin 内置 admin HTTP 控制面；Auth 账号服与登录链路配置（HTTP 默认要求 TLS，明文须显式 insecure_plaintext=true）；MasterHealth master 节点健康探测；MasterSessionToken session token 存储后端；Reliable 跨节点可靠投递配置；MasterListenAddr / MasterHTTPListenAddr / LogListenAddr / LogHTTPListenAddr master 与 log 服监听配置；MasterAddr / LogAddr 对应连接地址；MasterToken master 内部 RPC 共享密钥（MasterListenAddr 绑非回环时必填）；MasterGame 暴露 Master 端游戏内核。
	Config = iapp.Config
	// GatewayConfig 网关进程配置（别名到 internal 的定义，字段与服务端一处定义）。
	// 字段：ListenWS 客户端 WebSocket 接入地址；WSPath WebSocket 升级路径；WSAllowAllOrigins 允许跨域 WS（测试工具用）；ListenTCP 客户端 TCP 接入地址；ListenUDP UDP 接入地址（QUIC + 裸 UDP 共享）；EnableWT 是否启用 WebTransport；TLSCert/TLSKey TLS 证书/私钥路径；WTCert/WTKey WebTransport 专用证书/私钥路径；WTPin 是否启用证书固定（空自动生成）；MaxConns 连接总数上限；QueueCap 等候队列容量；QueueReleasePerSec 排队每秒放行数；QueueTimeout 排队最长时间；ReconnectGrace 重连宽限；DisconnectGrace 断线宽限；MaxFrameSize 上行客户端帧最大长度；AuthDisabled 关闭登录门禁；AuthExemptMsgIDs 免登录消息号白名单；TCPTLSDisabled true=TCP 接入不加密；MaxConnsPerSec 每秒新建连接数上限（0=不限）。各连接/限流字段 0 值表示「不启用 / 立即触发」。
	//
	// 用别名而非独立 struct：独立 struct 会与 internal 的定义漂移（缺字段时业务按
	// 门面类型根本设不了登录门禁）。
	GatewayConfig = iapp.GatewayConfig
	// LogicConfig 逻辑服配置（别名到 internal 的定义）。
	// 字段：ListenAddr TCP 监听地址（网关拨号此地址）；HTTPListen HTTP 控制面监听地址（空=不启用）；Heartbeat 网关↔逻辑服心跳间隔（默认 30s）；FrameTimeout 单帧处理上限（默认 30s）；ReconnectGrace 重连宽限（默认 30s）。
	LogicConfig = iapp.LogicConfig
	// MiscConfig 杂项配置（时区等，别名到 internal 的定义）。
	// 字段：Timezone 时区（如 "Asia/Shanghai"），空=系统本地时区。
	MiscConfig = iapp.MiscConfig
	// Core 是 Game 和 MasterGame 的共享内核，内嵌 *event.Logic（自动提升 OnMsg / OnEvent / OnHTTP）；Timer *TimeEvent 共享定时器调度器。
	Core = iapp.Core
	// Role 进程角色：挂载时用它声明"这段逻辑挂在哪个角色上"。
	Role = iapp.Role

	// Game 游戏服（事件驱动）。业务只 OnMsg / OnEvent，框架负责派发与回包。
	// 门面以包装结构实现（嵌入 internal *Game），把 Ctx / Handler 桥接到 pkg/transport/event 的接口形式。
	Game = iapp.GameFacade
	// MasterGame master 侧游戏逻辑入口。嵌入 Core，提供 OnMsg / OnEvent / OnHTTP / Bind / Reply。
	MasterGame = iapp.MasterGameFacade
	// LogGame log 服入口。与 MasterGame 同构：headless Core + 自己的传输层（批量日志 TCP）。
	// 业务 handler 签名与 Game / MasterGame 一致（func(c event.Ctx) error）。
	LogGame = iapp.LogGameFacade
	// AuthGame 账号服入口。与 MasterGame / LogGame 同构（headless Core），HTTP 由 Core 控制面提供。
	// 账号专属逻辑（JWT / 撞库锁 / 渠道绑定）在引擎内部，业务主要用 OnHTTP 追加路由；
	// 订单归属账号服，支付回调经 OnHTTP 挂到 /auth/pay/... 后用 OrderStore 建单 / 流转状态。
	AuthGame = iapp.AuthGameFacade

	// AdminServer 内置 admin HTTP 控制面服务（方法承载型，由 internal 的 *AdminServer 直接满足）。
	// 业务通常不需要直接持有它——在 app.Mount 回调里通过 g.OnAdminHTTP(pattern, h)
	// 注册自定义探针端点（health、ready、metrics、debug 等），引擎在 runMounts 之后
	// 统一挂载到 admin server（默认 127.0.0.1:8041），供运维 / 监控系统访问。
	AdminServer = iapp.AdminServerFacade
	// TimeEvent 共享定时器调度器，提供业务侧定时任务注册能力（方法承载型）。
	// 业务经 g.Timer（*TimeEvent）或显式引用本接口调用 Every/After/ByTime/DailyAt/Cron
	// 等定时注册、时区解析与定时器组管理。
	// 定时器本体（*timer.Scheduler / *timer.Group）定义在 pkg/runtime/timer，F12 直达实现。
	TimeEvent = iapp.TimeEventFacade
)

// —— 灰度下线（滚动重启热更）与监控表：以类型别名透出 ——
//
// 用别名而非包装结构：internal 的定义即业务可见契约。别名让业务能**显式命名**这些类型
// （否则签名里的 internal 类型只能靠类型推断使用，无法声明变量 / 传参），
// 同时 g.Drain / g.DrainStatus / g.MonitorStore 直接可用。
type (
	// DrainMode 存量连接的处理方式（DrainGrace / DrainMigrate / DrainHybrid）。
	DrainMode = iapp.DrainMode
	// DrainOptions 一次灰度下线的参数；零值回落到引擎默认。
	DrainOptions = iapp.DrainOptions
	// DrainStatus 一次灰度下线的实时状态。
	DrainStatus = iapp.DrainStatus
	// MonitorStore 监控表存取接口（导出 / 导入本节点监控项，重连恢复用）。
	MonitorStore = iapp.MonitorStore
)

// 进程角色常量。
const (
	// RoleGame 逻辑服：唯一接收客户端（经网关）的角色。
	RoleGame = iapp.RoleGame
	// RoleMaster 协调服：只收 game 转发（CallMaster），不接客户端。
	RoleMaster = iapp.RoleMaster
	// RoleLog 日志服：只收 game 转发（CallLog），不接客户端。
	RoleLog = iapp.RoleLog
	// RoleAuth 账号服：HTTP 对客户端（/auth/*）+ 收 game 转发（CallAuth）。
	RoleAuth = iapp.RoleAuth
)

// 灰度下线模式常量（与 internal 同值）。
const (
	// DrainGrace 只等玩家自然退出，宽限期到后分批强踢。
	DrainGrace = iapp.DrainGrace
	// DrainMigrate 逐连接迁移到新进程，客户端无感。
	DrainMigrate = iapp.DrainMigrate
	// DrainHybrid 先迁移，宽限期到后对剩余连接强踢（滚动重启推荐）。
	DrainHybrid = iapp.DrainHybrid
)

// 配置加载。
var (
	// DefaultConfig 返回开发基线配置。
	DefaultConfig = iapp.DefaultConfig
	// LoadFromFile 从单个 YAML/JSON/TOML 配置文件加载配置。
	LoadFromFile = iapp.LoadFromFile
	// LoadFromDir 从配置目录加载配置（按文件名排序合并）。
	LoadFromDir = iapp.LoadFromDir
	// LoadConfig 加载服务配置（自动识别目录/单文件），供业务层在 Run 前注入
	// MasterBusiness 等回调用到。需配合 RunWithConfig 使用。
	LoadConfig = iapp.LoadConfig
)

// 启动与装配。
var (
	// Run 从配置目录 / 文件加载配置并按 server_type 启动对应进程，阻塞到收到退出信号。
	// bootstrap（可选）在逻辑服启动监听前被调用，用于给 *Game 绑定业务 handler。
	// 业务通常在 init 中通过 app.Mount 声明挂载，无需再传 bootstrap。
	Run = iapp.RunFacade
	// RunWithConfig 使用已构造好的 *Config 启动（跳过配置加载），其余行为与 Run 一致。
	// 调用方通过 LoadConfig 提前构造 cfg，即可注入 MasterBusiness 等回调。
	RunWithConfig = iapp.RunWithConfigFacade
	// RunGame 从配置构造并启动游戏服，自动创建通用数据存储（含建表）。
	// 接受 ctx 参数，调用方可传入可取消 context 以停止服务，
	// 避免硬编码 context.Background() 导致 DB 建表不可达时阻塞不响应 SIGTERM。
	RunGame = iapp.RunGameFacade
	// Mount 登记业务挂载函数——**四个角色共用这一个入口**（通常在业务包 init 中调用）。
	//
	// fn 必须是下列之一，且与 role 匹配；不匹配在 init 期（进程启动时）立刻 panic，
	// 不会出现"挂错角色、消息永远到不了"的静默失败：
	//
	//	func(*Game)        ← RoleGame
	//	func(*MasterGame)  ← RoleMaster
	//	func(*LogGame)     ← RoleLog
	//	func(*AuthGame)    ← RoleAuth
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
	Mount = iapp.Mount
	// NewMasterClient 创建一个到 master 的客户端连接。
	// 业务侧自己管理生命周期，创建后可访问排行榜等 master 能力。
	// ⚠️ 不带 token：仅适用于 master 绑回环地址的部署；master 绑非回环时用
	// NewMasterClientWithToken（服务端要求首帧 MsgAuth 握手）。
	NewMasterClient = iapp.NewMasterClient
	// NewMasterClientWithToken 同上，但携带 master 内部 RPC 的共享密钥（配置里的 master_token）。
	NewMasterClientWithToken = iapp.NewMasterClientWithToken
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
	// 说明：
	//   - 后端契约见 pkg/foundation/logstore.Backend；引擎只在启动时按名字取一次工厂；
	//   - 内置 "mysql" 不走注册表（它读 data.mysql），配置留空即用它；
	//   - 同名重复注册会覆盖，便于测试与热替换；
	//   - 名字为空 / 工厂为 nil 直接 panic（装配期错误，fail-fast）。
	RegisterLogBackend = iapp.RegisterLogBackend
	// PublishTableLoaded 广播 table.Loaded 事件。
	PublishTableLoaded = iapp.PublishTableLoadedFacade
)

// internal ↔ 门面转换。
var (
	// FromAdminServer 将 internal 的 *AdminServer 包装为门面 AdminServer 接口；nil 返回 nil。
	FromAdminServer = iapp.FromAdminServer
	// InternalAdminServer 将门面 AdminServer 接口还原为 internal 的 *AdminServer；非底层则 ok=false。
	InternalAdminServer = iapp.InternalAdminServer
	// FromTimeEvent 将 *TimeEvent 以门面 TimeEvent 接口形式返回；nil 返回 nil。
	FromTimeEvent = iapp.FromTimeEvent
	// InternalTimeEvent 将门面 TimeEvent 接口还原为 *TimeEvent；非引擎实现则 ok=false。
	InternalTimeEvent = iapp.InternalTimeEvent
)
