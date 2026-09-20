// Package app 是 clover 的「通用应用启动层」。
//
// 设计目标（用户强约束）：业务项目只负责「按配置启动一个
// 网关 + 一个逻辑服 + 数据存储，并阻塞到退出信号」，所有接线（配置加载、角色分发、
// 网关装配、逻辑服装配、登录鉴权注册、玩家档案注入、etcd 服务发现）都由引擎统一完成，
// 业务项目不应再写 server.go / game.go 这类文件。
//
// 用法（业务项目极薄）：
//
//	app.Run("configs/all") // 解析 configs/all 下 *.yaml，按 server_type 启动并阻塞
//
// 运行时依赖（均为 base 内部包，业务无需感知）：
//   - pkg/transport/event  事件驱动逻辑服内核（On / OnEvent / 派发 / 回包）
//   - internal/transport/net/auth     登录身份层（账号鉴权完全封装在底层，game 不感知）
//   - internal/transport/gateway/gwcore  网关内核（WS / TCP 接入 + 上行转发 + NATS 下行桥接）
//   - pkg/domain/data         通用三元键存储层 + 账号/玩家/订单结构体（统一出口）
//   - internal/app/svc.go   进程启动骨架（server_type 分发 + 信号阻塞）
//
// 扩展点：业务要加玩法，传 bootstrap 回调（在 Run 前给 *Game 绑 On / OnEvent），
// 或直接 import 本包后用 NewGame / RunGame 自行装配（更底层）。
package app

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"github.com/qw576483/clover-server-engine/internal/transport/gateway/gwcore"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// safeBootstrap 与 safeMount 对称：bootstrap 回调 panic 时转为 error 返回，
// 避免业务装配阶段的 panic 直接击穿进程（无 recover）。
func safeBootstrap(fn func(*Game) error, g *Game) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// 保留堆栈：与 safeMount 对称，便于定位是哪个 bootstrap 回调、哪一行 panic。
			err = fmt.Errorf("app: bootstrap panic: %v\n%s", r, debug.Stack())
		}
	}()
	return fn(g)
}

// loggerReady 标记日志是否已初始化，避免重复 Init。
// 使用 atomic.Bool 保证 Run / RunWithConfig 并发入口的可见性。
var loggerReady atomic.Bool

// EngineVersion 当前 clover-server-engine 版本号，供启动汇总报告展示。
// 单一来源在引擎自身，业务无需（也不应）注入。
const EngineVersion = "pre-v0.0.1"

// Run 从配置目录 / 文件加载配置并按 server_type 启动对应进程，阻塞到收到 SIGINT/SIGTERM。
// 配置文件中的 server_type 字段为 "game" / "gateway" / "all" / "master"。
//
// bootstrap（可选）在逻辑服启动监听前被调用，用于给 *Game 绑定业务 handler
// （On / OnEvent）。不传则仅启动「登录 + 网关转发」的纯框架，不含任何业务。
func Run(configPath string, bootstrap ...func(g *Game) error) error {
	if !loggerReady.Load() {
		_ = logger.Init(nil)
		loggerReady.Store(true)
	}
	defer logger.Sync()

	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	// 用配置文件中的 log 节点重新初始化（覆盖启动时的默认值）。
	// dir 模式按天落盘时，以 server_type 作为日志文件名后缀，使不同角色日志独立成文件。
	if cfg.Log.Service == "" {
		cfg.Log.Service = cfg.ServerType
	}
	_ = logger.Init(&cfg.Log)
	return runApp(cfg, bootstrap...)
}

// RunWithConfig 使用已构造好的 *Config 启动（跳过配置加载），其余行为与 Run 一致。
// 调用方通过 LoadConfig 提前构造 cfg，即可注入 MasterBusiness 等回调。
func RunWithConfig(cfg *Config, bootstrap ...func(g *Game) error) error {
	if !loggerReady.Load() {
		_ = logger.Init(nil)
		loggerReady.Store(true)
	}
	defer logger.Sync()
	if cfg.Log.Service == "" {
		cfg.Log.Service = cfg.ServerType
	}
	_ = logger.Init(&cfg.Log)
	return runApp(cfg, bootstrap...)
}

// runApp 是 Run / RunWithConfig 的公共执行体。
func runApp(cfg *Config, bootstrap ...func(g *Game) error) error {
	// 内置 admin 控制面：不论 server_type 是什么角色都统一拉起，
	// 让 health / ready / metrics / log level 等运维接口有一个固定入口。
	// 配置 admin.disable=true 时 NewAdminServer 返回 nil，后续调用均为空操作。
	// 注意：这里只构造、不 Start——业务在 RegisterMount 里通过 g.OnAdminHTTP
	// 注册的自定义探针路由，需在 runMounts 之后统一挂载再 Start（见下方闭包内）。
	//
	// 构造错误（未配 admin.token 却要绑非回环）**必须中止启动**，不能降级成 Warn：
	// 那是把「无鉴权的高危控制面」照原样开在网上，属配置错误而非运行时故障。
	adminSrv, err := NewAdminServer(cfg.Admin)
	if err != nil {
		return fmt.Errorf("app: %w", err)
	}
	defer adminSrv.Stop(context.Background())
	setCurrentAdminServer(adminSrv)
	defer setCurrentAdminServer(nil)

	// 进程级看门狗（周期巡检 + 告警）：与 admin 控制面同思路，不论 server_type 是什么角色
	// 都统一拉起；业务在 RegisterMount / bootstrap 里用 watchdog.Default() 注册自己的规则。
	// 必须在 runProcess 之前 Install：各角色的装配与业务 mount 都发生在其后。
	wd := installWatchdog(adminSrv)
	defer wd.Stop()

	// 纯 master / log / auth 角色在这里直接 Start admin：runProcess 里没有任何分支会为
	// 它们启动（game 分支在挂载完业务探针后 Start，gateway 分支仅作兜底补 Start），
	// 不启动则这些角色的 /ping、/metrics、/watchdog、/admin/* 全部不可达——
	// 与上面「不论 server_type 是什么角色都统一拉起」的承诺相悖。
	// all 模式必须跳过：它由 Logic 分支在 runMounts / mountAdminRoutes 之后统一 Start，
	// 这里提前绑定会让业务经 g.OnAdminHTTP 注册的路由因「Start 后注册被忽略」而丢失。
	switch cfg.ServerType {
	case ServerTypeMaster, ServerTypeLog, ServerTypeAuth:
		if err := adminSrv.Start(); err != nil {
			// admin 是旁路能力，起不来不应阻断主服务，仅告警（与其他角色分支同款处理）。
			logger.Warnf("app: admin http server start failed: %v (continuing without admin)", err)
		}
	}

	// 网关实例指针：供 onReady 汇总报告读取实际监听状态（含 WT/QUIC 降级原因）。
	var gwInst *gwcore.Gateway
	return runProcess(cfg.ServerType, ProcessStarters{
		Logic: func() (func(), error) {
			g, err := RunGame(context.Background(), cfg)
			if err != nil {
				return nil, err
			}
			currentGame.Store(g)
			// 统一挂载：base 按优先级执行所有已注册的业务挂载函数。
			if err := RunMounts(RoleGame, g); err != nil {
				g.Stop()
				return nil, err
			}
			// 业务在 mount 里通过 g.OnAdminHTTP 注册的探针路由，此刻统一挂到 admin server。
			mountAdminRoutes(adminSrv, g)
			if err := adminSrv.Start(); err != nil {
				// admin 是旁路能力，起不来不应阻断主服务，仅告警。
				logger.Warnf("app: admin http server start failed: %v (continuing without admin)", err)
			}
			// 执行全部 bootstrap，并与 runMounts/safeMount 对称地 recover，防止业务 panic 击穿进程。
			for i, b := range bootstrap {
				if b == nil {
					continue
				}
				if err := safeBootstrap(b, g); err != nil {
					g.Stop()
					return nil, fmt.Errorf("app: bootstrap[%d]: %w", i, err)
				}
			}
			return g.Stop, nil
		},
		Gateway: func() (func(), error) {
			gw, err := runGateway(cfg)
			if err != nil {
				return nil, err
			}
			gwInst = gw
			currentGateway.Store(gw)
			// 纯 gateway 模式下 admin server 尚未启动（只有 game 分支会 Start），
			// 这里补一次，使 /admin/gateway/upstream、/admin/shutdown 等端点可达。
			// all 模式下 admin 已启动，Addr() 非空则跳过（重复 Start 会报 already started）。
			if adminSrv.Addr() == "" {
				if err := adminSrv.Start(); err != nil {
					logger.Warnf("app: admin http server start failed in gateway: %v (continuing without admin)", err)
				}
			}
			// all 模式：game 可通过 g.KickConn() 主动踢下线
			if g := currentGame.Load(); g != nil {
				g.SetKicker(gw.Kick)
			} else if cfg.ServerType == "all" {
				logger.Warnf("app: gateway started before game ready, KickConn disabled")
			}
			return func() {
				currentGateway.Store(nil)
				gw.Stop()
			}, nil
		},
		Master: func() (func(), error) {
			return runMaster(cfg)
		},
		Log: func() (func(), error) {
			return runLog(cfg)
		},
		Auth: func() (func(), error) {
			return runAuth(cfg)
		},
	},
		func() {
			printStartupReport(cfg, gwInst)
		},
	)
}
