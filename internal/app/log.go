package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"clover-server-engine/internal/domain/log/server"
	"clover-server-engine/internal/domain/log/state"
	"clover-server-engine/internal/shared/config"
	"clover-server-engine/internal/transport/event"
	"clover-server-engine/internal/transport/tcpmsg"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/foundation/logstore"
	"clover-server-engine/pkg/shared/timeutil"
)

// LogGame log 服内核：headless *Core（消息派发 / 事件 / 定时器 / 数据）+ 日志专属逻辑（落盘）。
//
// 与 MasterGame 完全同构——非 game 角色统一走「headless Core + 自己的传输层」：
//   - 业务 handler 一律 event.Handler（func(c *event.Ctx) error），与 Game / MasterGame 一致；
//   - Core 用 Headless 模式，不启逻辑服 TCP 监听；HTTP 由 Core 控制面提供（/ping /health /ready /stats）；
//   - 批量日志上报仍是裸 TCP（tcpmsg.Server），由 logTCPBridge 桥进 Core 派发管线。
type LogGame struct {
	*Core

	st       state.LogService
	tcpAddr  string
	httpAddr string
	// backend 实际生效的落盘后端名（buildLogBackend 的返回值），
	// 供 /stats 如实上报——不能再硬编码。
	backend string

	routesMu sync.RWMutex
	routes   []string

	// bridge 把批量日志的裸 TCP 通道桥进 Core 派发管线。
	bridge tcpMsgBridge

	stopOnce sync.Once
}

// newLogGame 创建 LogGame 内核（headless Logic + 落盘后端）。
// backend 为 buildLogBackend 解析出的实际后端名，供 /stats 上报。
func newLogGame(cfg *Config, st state.LogService, tcpAddr, httpAddr, backend string) *LogGame {
	syncReg := event.NewSyncRegistry()

	core := event.New(event.Config{
		Headless:   true,
		HTTPListen: httpAddr,
	},
		event.WithStore(nil),
		event.WithSyncDownlink(nil, "", syncReg),
		// log 服 HTTP 只暴露自身探针；未显式配置鉴权时默认拒绝其余路径（控制面语义）。
		event.WithHTTPAuth(func(r *http.Request) bool {
			switch r.URL.Path {
			case "/ping", "/health", "/ready", "/stats":
				return true
			}
			return false
		}),
	)

	lg := &LogGame{
		Core:     newCore(cfg, nil, syncReg),
		st:       st,
		tcpAddr:  tcpAddr,
		httpAddr: httpAddr,
		backend:  backend,
	}
	lg.Core.Logic = core
	lg.bridge.attach(core)
	timeutil.Init(cfg.Misc.Timezone)
	lg.Core.Timer = NewTimeEvent(timeutil.Location())
	lg.Core.nodeID = "log"

	lg.OnHTTP("/ping", lg.handlePing)
	lg.OnHTTP("/health", lg.handleHealth)
	lg.OnHTTP("/ready", lg.handleReady)
	lg.OnHTTP("/stats", lg.handleStats)
	return lg
}

// OnMsg 注册 log 服业务消息 handler（签名与 Game / MasterGame 一致：func(c event.Ctx) error）。
//
// 内部走共用桥接（tcpMsgBridge）：TCP 帧 → GWLogicPacket → Logic.Dispatch → handler(c)。
func (lg *LogGame) OnMsg(msgID uint32, h event.Handler) { lg.bridge.OnMsg(msgID, h) }

// setServer 绑定 TCP 服务端，并把之前缓存的 handler 全部补注册。
func (lg *LogGame) setServer(srv *tcpmsg.Server) { lg.bridge.setServer(srv) }

// OnEvent 注册 log 服领域事件 handler。
func (lg *LogGame) OnEvent(typ string, h event.Handler) { lg.Logic.OnEvent(typ, h) }

// OnHTTP 注册 log 服 HTTP 路由（Core 控制面）。
func (lg *LogGame) OnHTTP(pattern string, h event.HTTPHandler) {
	lg.Logic.OnHTTP(pattern, h)
	lg.routesMu.Lock()
	lg.routes = append(lg.routes, pattern)
	lg.routesMu.Unlock()
}

// Reply / ReplyRaw 继承自嵌入的 *Core（见 core.go），四个角色共用一份实现。

// Start 启动 Core（headless：不启逻辑服 TCP 监听，只启 HTTP 控制面 + 定时器 + 派发内核）。
func (lg *LogGame) Start() error { return lg.Logic.Start() }

// Stop 停止 log 服：TCP → Core → 落盘后端。重复调用只生效一次。
func (lg *LogGame) Stop() {
	lg.stopOnce.Do(func() {
		lg.bridge.stop()
		lg.Logic.Stop()
		if lg.st != nil {
			_ = lg.st.Close()
		}
		lg.closeCoreBackends()
	})
}

// —— HTTP 探针 ——

func (lg *LogGame) handlePing(c *event.HTTPCtx) error {
	lg.routesMu.RLock()
	routes := make([]string, len(lg.routes))
	copy(routes, lg.routes)
	lg.routesMu.RUnlock()
	return c.Reply(map[string]any{"ok": true, "service": "log", "routes": routes})
}

func (lg *LogGame) handleHealth(c *event.HTTPCtx) error {
	return c.Reply(map[string]any{"status": "ok"})
}

func (lg *LogGame) handleReady(c *event.HTTPCtx) error {
	return c.Reply(map[string]any{"status": "ready"})
}

func (lg *LogGame) handleStats(c *event.HTTPCtx) error {
	return c.Reply(map[string]any{
		"backend": lg.backend,
		"tcp":     lg.tcpAddr,
		"http":    lg.httpAddr,
	})
}

// runLog 启动 log 服，返回 stop 函数。
//
// 启动顺序：落盘后端（按 log_backend 选）→ LogGame 内核（headless Core）→ TCP Server → 业务挂载 → 监听。
//
// 落盘后端**可配置**：默认内置 MySQL（biz_log 表）；配置 log_backend 可切到业务注册的自定义
// 后端（ClickHouse / 自建服务 / NATS 转发…），契约见 pkg/foundation/logstore.Backend。
// **换后端只改配置，不动引擎代码** —— 这是后端可插拔的意义所在。
func runLog(cfg *Config) (func(), error) {
	// 1. 落盘后端：按 log_backend 配置选（默认 mysql；配置错误即启动失败，不静默降级）
	st, backendName, err := buildLogBackend(cfg)
	if err != nil {
		return nil, err
	}
	logger.Infof("log: disk backend=%s", backendName)

	listenAddr := config.DefString(cfg.LogListenAddr, defaultLogListenAddr)
	httpAddr := config.DefString(cfg.LogHTTPListenAddr, defaultLogHTTPListenAddr)

	// 2. LogGame 内核（headless Core）
	lg := newLogGame(cfg, st, listenAddr, httpAddr, backendName)

	// 3. 批量日志 TCP 服务端（先创建不监听，等业务 handler 注册完毕再 Listen）
	srv := server.Create(listenAddr, st)
	lg.setServer(srv)

	// 4. 业务挂载（与 master 对称：监听前统一执行）
	cfg.LogGame = lg
	if err := RunMounts(RoleLog, lg); err != nil {
		lg.Stop()
		return nil, err
	}

	// 5. 启动 Core（headless：HTTP 控制面 + 定时器 + 派发内核）
	if err := lg.Start(); err != nil {
		lg.Stop()
		return nil, err
	}
	server.Listen(srv)
	logger.Infof("log: TCP serving on %s", listenAddr)

	// 服务发现：注册本实例地址，使 game 侧能发现多实例——同步 RPC（CallLog）
	// 与业务日志管道（AddLog，经 logbuf 按片轮询分发）都消费这个前缀。
	// 未配 etcd 时空操作（退化为配置里的 log_addr 静态直连）。
	ec, closeEtcd := newEtcdClientForRole(cfg, roleLog)
	unregister, err := registerService(context.Background(), ec, roleLog, listenAddr)
	if err != nil {
		closeEtcd()
		lg.Stop()
		return nil, err
	}

	stop := func() {
		unregister()
		lg.Stop()
		closeEtcd()
	}
	return stop, nil
}

// buildLogBackend 按 log_backend 配置构造落盘后端，返回后端实例与其名字。
//
// 空配置 → 内置 mysql（读 data.mysql）；其它名字 → pkg/foundation/logstore 注册表。
// 名字未知、工厂报错或工厂返回 nil 都视为启动失败：日志落盘是必须能力，
// 不做「静默退化到丢弃」——那等于悄悄丢数据。
func buildLogBackend(cfg *Config) (state.LogService, string, error) {
	name := cfg.LogBackend
	if name == "" {
		name = logstore.DefaultBackend
	}

	if name == logstore.DefaultBackend {
		if len(cfg.LogBackendConfig) > 0 {
			// 内置 mysql 后端读 data.mysql（见 Config.LogBackendConfig 注释）：
			// 配了 log_backend_config 却仍走内置后端时必须留痕，否则参数被静默丢弃，
			// 运维会以为配置已生效。
			logger.Warnf("log: log_backend_config is ignored by the built-in %q backend (it reads data.mysql); "+
				"remove it or set log_backend to a custom registered backend", name)
		}
		if cfg.Data.MySQL.Host == "" {
			return nil, "", errors.New("log: data.mysql.host is required for the built-in mysql backend (file backend has been removed)")
		}
		ms, err := state.NewMySQL(cfg.Data.MySQL, "biz_log")
		if err != nil {
			return nil, "", fmt.Errorf("log: create mysql backend: %w", err)
		}
		return ms, name, nil
	}

	f, ok := logstore.Get(name)
	if !ok {
		return nil, "", fmt.Errorf("log: unknown log_backend %q; available: %s", name, logstore.Describe())
	}
	b, err := f(cfg.LogBackendConfig)
	if err != nil {
		return nil, "", fmt.Errorf("log: create %q backend: %w", name, err)
	}
	if b == nil {
		return nil, "", fmt.Errorf("log: backend %q factory returned nil", name)
	}
	return b, name, nil
}
