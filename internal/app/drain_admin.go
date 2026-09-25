// 灰度下线 / 滚动重启的运维端点（挂在 admin HTTP 控制面）。
//
// 端点一览：
//
//	POST /admin/drain         启动灰度下线（body 见 drainRequest）
//	GET  /admin/drain/status  查询编排状态
//	POST /admin/drain/cancel  取消编排（回滚）
//	POST /admin/gateway/upstream?addr=host:port   切换网关默认上游（新连接导向新版本进程）
//	GET  /admin/gateway/upstream                  查询当前上游
//	POST /admin/shutdown      程序化优雅退出（Windows 无 SIGTERM，灰度重启收尾用）
//
// 鉴权与监听：
//   - 默认只监听 127.0.0.1（见 AdminConfig.DefaultListenAddr）；
//   - 配置 admin.token 后，本组端点（/admin/*）与 /deadletter、/log/level、/debug/pprof 一律
//     要求带同一令牌（X-Admin-Token 或 Authorization: Bearer），见 AdminServer.guard；
//   - **未配置 token 时 Normalize 要求监听地址必须是回环**（AdminConfig.Normalize），
//     非回环 + 无 token 会在构造期报错、启动被拒，所以「无鉴权」只可能发生在回环上。
//     生产若要绑内网，必须同时配 admin.token。
package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/qw576483/clover-server-engine/internal/foundation/ophttp"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// drainRequest POST /admin/drain 的请求体。全部字段可选，缺省回落默认值。
// 时长字段用 Go duration 字符串（"5m" / "90s"），与 yaml 配置风格一致。
type drainRequest struct {
	Mode         string `json:"mode"`          // grace | migrate | hybrid，默认 hybrid
	Target       string `json:"target"`        // 新进程逻辑服地址；migrate/hybrid 必填
	Grace        string `json:"grace"`         // 迁移 / 等待自然退出的总时长，默认 5m
	HardTimeout  string `json:"hard_timeout"`  // 强踢阶段硬上限，默认 2m
	MigrateBatch int    `json:"migrate_batch"` // 每 tick 迁移条数，默认 128
	KickBatch    int    `json:"kick_batch"`    // 强踢每批条数，默认 32
	StopAfter    *bool  `json:"stop_after"`    // 归零后自动停机，默认 false
}

const (
	// maxDrainBodyBytes /admin/drain 请求体上限（防超大 body 打满内存）。
	maxDrainBodyBytes = 1 << 20
	// maxDrainBatch 单批迁移 / 强踢条数上限。normalize 只对 <=0 回落默认、不设上限，
	// 传入极大值会走到 drain 内的巨量切片分配。
	maxDrainBatch = 1 << 16
)

// validateHostPort 校验 host:port 形态（非空 host + 数字端口 1..65535）。
// 用于 admin 端点里会被「网关拨号」消费的地址参数（drain target / gateway upstream）。
func validateHostPort(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		return errors.New("host required")
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid port %q", port)
	}
	if p <= 0 || p > 65535 {
		return fmt.Errorf("port out of range: %d", p)
	}
	return nil
}

// InstallDrainRoutes 把灰度下线相关端点挂到 admin server。
// 必须在 admin server Start 之前调用（ServeMux 运行中注册不是并发安全的）。
// 懒加载 currentGame / currentGateway：端点被调用时才取实时实例，
// 避免「构造期实例尚未创建」与「运行中改 mux」两种情况（与 deadLetterDispatcher 同款写法）。
func InstallDrainRoutes(as *AdminServer) {
	if as == nil {
		return
	}
	as.Handle("/admin/drain", drainDispatcher())
	as.Handle("/admin/drain/status", drainDispatcher())
	as.Handle("/admin/drain/cancel", drainDispatcher())
	as.Handle("/admin/gateway/upstream", gatewayUpstreamHandler())
	as.Handle("/admin/shutdown", shutdownHandler())
}

// drainDispatcher 灰度下线端点分发（按精确路径路由）。
//
// 所有拒绝路径都留日志（含 405 / 404）：控制面被「谁在什么时候调、为什么被拒」问到时，
// 只有日志能回答；静默 4xx 只会让人以为端点没被调到。
func drainDispatcher() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g := currentGame.Load()
		if g == nil {
			// 纯 gateway / master / log 进程没有 Game 实例，drain 不适用。
			logger.Warnf("admin: %s %s rejected: no game instance in this process (from %s)", r.Method, r.URL.Path, r.RemoteAddr)
			ophttp.Error(w, http.StatusServiceUnavailable, "no game instance in this process")
			return
		}
		switch r.URL.Path {
		case "/admin/drain":
			handleDrainStart(g, w, r)
		case "/admin/drain/status":
			ophttp.JSON(w, http.StatusOK, g.DrainStatus())
		case "/admin/drain/cancel":
			if r.Method != http.MethodPost {
				logger.Warnf("admin: %s %s rejected: POST required (from %s)", r.Method, r.URL.Path, r.RemoteAddr)
				ophttp.Error(w, http.StatusMethodNotAllowed, "POST required")
				return
			}
			g.CancelDrain()
			ophttp.JSON(w, http.StatusOK, map[string]any{"ok": true})
		default:
			logger.Warnf("admin: %s %s not found (from %s)", r.Method, r.URL.Path, r.RemoteAddr)
			http.NotFound(w, r)
		}
	})
}

func handleDrainStart(g *Game, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.Warnf("admin: %s %s rejected: POST required (from %s)", r.Method, r.URL.Path, r.RemoteAddr)
		ophttp.Error(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	// body 与 query 二选一：body 便于脚本传结构化参数，query 便于 curl 手敲。
	// 限制请求体大小并显式处理解码错误：非法 JSON 静默丢弃会让
	// grace / hard_timeout 悄悄回落到默认值，且不留任何痕迹（空 body 属正常）。
	var req drainRequest
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxDrainBodyBytes)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			logger.Warnf("admin: drain start rejected: invalid body (from %s): %v", r.RemoteAddr, err)
			ophttp.Error(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return
		}
	}
	if req.Mode == "" {
		req.Mode = r.URL.Query().Get("mode")
	}
	if req.Target == "" {
		req.Target = r.URL.Query().Get("target")
	}
	// mode 白名单（空 = 回落默认 hybrid）：非法串会被 Drain 直接当 migrate/hybrid
	// 进入迁移流程，而不是报错。
	switch DrainMode(req.Mode) {
	case "", DrainGrace, DrainMigrate, DrainHybrid:
	default:
		logger.Warnf("admin: drain start rejected: invalid mode %q (from %s)", req.Mode, r.RemoteAddr)
		ophttp.Error(w, http.StatusBadRequest, "invalid mode: "+req.Mode+" (want grace | migrate | hybrid)")
		return
	}
	// batch 上限（<=0 回落默认值，由 normalize 负责）。
	if req.MigrateBatch > maxDrainBatch || req.KickBatch > maxDrainBatch {
		logger.Warnf("admin: drain start rejected: batch too large (migrate=%d kick=%d max=%d, from %s)",
			req.MigrateBatch, req.KickBatch, maxDrainBatch, r.RemoteAddr)
		ophttp.Error(w, http.StatusBadRequest, fmt.Sprintf("batch too large (max %d)", maxDrainBatch))
		return
	}
	// target 是网关随后要拨号的新上游地址：非空时必须为合法 host:port。
	if req.Target != "" {
		if err := validateHostPort(req.Target); err != nil {
			logger.Warnf("admin: drain start rejected: invalid target %q (from %s): %v", req.Target, r.RemoteAddr, err)
			ophttp.Error(w, http.StatusBadRequest, "invalid target: "+err.Error())
			return
		}
	}

	opts := DrainOptions{
		Mode:         DrainMode(req.Mode),
		Target:       req.Target,
		MigrateBatch: req.MigrateBatch,
		KickBatch:    req.KickBatch,
		StopAfter:    req.StopAfter != nil && *req.StopAfter,
	}
	var err error
	if opts.Grace, err = parseDuration(req.Grace, 0); err != nil {
		logger.Warnf("admin: drain start rejected: invalid grace %q (from %s): %v", req.Grace, r.RemoteAddr, err)
		ophttp.Error(w, http.StatusBadRequest, "invalid grace: "+err.Error())
		return
	}
	if opts.HardTimeout, err = parseDuration(req.HardTimeout, 0); err != nil {
		logger.Warnf("admin: drain start rejected: invalid hard_timeout %q (from %s): %v", req.HardTimeout, r.RemoteAddr, err)
		ophttp.Error(w, http.StatusBadRequest, "invalid hard_timeout: "+err.Error())
		return
	}
	if err := g.Drain(opts); err != nil {
		logger.Warnf("admin: drain start rejected by Drain() (mode=%s target=%s from %s): %v",
			opts.Mode, opts.Target, r.RemoteAddr, err)
		ophttp.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	ophttp.JSON(w, http.StatusOK, g.DrainStatus())
}

// parseDuration 解析时长字符串；空串返回 def（由 DrainOptions.normalize 兜底）。
func parseDuration(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	return time.ParseDuration(s)
}

// gatewayUpstreamHandler 网关默认上游的读写端点。仅影响此后新建的会话。
func gatewayUpstreamHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gw := currentGateway.Load()
		if gw == nil {
			logger.Warnf("admin: %s %s rejected: no gateway instance in this process (from %s)", r.Method, r.URL.Path, r.RemoteAddr)
			ophttp.Error(w, http.StatusServiceUnavailable, "no gateway instance in this process")
			return
		}
		switch r.Method {
		case http.MethodGet:
			ophttp.JSON(w, http.StatusOK, map[string]any{"upstream": gw.Upstream()})
		case http.MethodPost:
			addr := r.URL.Query().Get("addr")
			if addr == "" {
				logger.Warnf("admin: gateway upstream switch rejected: addr required (from %s)", r.RemoteAddr)
				ophttp.Error(w, http.StatusBadRequest, "addr required (query param)")
				return
			}
			// 拒绝畸形地址：该值会让新建连接的上游被网关拨号，
			// 只校验非空等于把任意垃圾串静默落地。
			if err := validateHostPort(addr); err != nil {
				logger.Warnf("admin: gateway upstream switch rejected: invalid addr %q (from %s): %v", addr, r.RemoteAddr, err)
				ophttp.Error(w, http.StatusBadRequest, "invalid addr: "+err.Error())
				return
			}
			gw.SetUpstream(addr)
			logger.Warnf("admin: gateway upstream switched to %s (from %s)", addr, r.RemoteAddr)
			ophttp.JSON(w, http.StatusOK, map[string]any{"ok": true, "upstream": gw.Upstream()})
		default:
			logger.Warnf("admin: %s %s rejected: GET or POST required (from %s)", r.Method, r.URL.Path, r.RemoteAddr)
			ophttp.Error(w, http.StatusMethodNotAllowed, "GET or POST required")
		}
	})
}

// shutdownHandler 程序化优雅退出。等价于向进程发送 SIGTERM，
// 用于 Windows（无信号）与灰度重启收尾（旧进程归零后自我了断）。
func shutdownHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			logger.Warnf("admin: %s %s rejected: POST required (from %s)", r.Method, r.URL.Path, r.RemoteAddr)
			ophttp.Error(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		logger.Warnf("admin: shutdown requested from %s", r.RemoteAddr)
		ophttp.JSON(w, http.StatusOK, map[string]any{"ok": true, "shutting_down": true})
		// 先冲刷回包再触发退出，避免调用方拿到 connection reset 而非响应体。
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// 异步：让本 handler 正常返回，shutdown 由 waitSignal 侧统一执行 stop。
		go requestShutdown()
	})
}
