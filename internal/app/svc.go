// 进程启动骨架，供各业务框架共用：
// - runProcess：按 serverType 启动对应进程（game / gateway / all / master / log / auth），
// 并阻塞到收到退出信号；
// - waitSignal：阻塞直到收到 SIGINT/SIGTERM 后执行 stop。
package app

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// ProcessStarters 各角色进程的启动函数集合，由调用方按自己的支持范围填充。
//
// 用结构体而非一长串位置参数：角色会持续增加（见 runProcess 注释中的 cross / room 等），
// 位置参数每加一个都要改所有调用点；结构体只需补一个字段，且字段为 nil 天然表达「本调用
// 不支持该角色」，由 doRunProcess 报出可读错误。
//
// 每个字段须（后台）启动对应进程并返回 stop 函数（须幂等，会被 safeStop 包裹调用）。
type ProcessStarters struct {
	Logic   func() (stop func(), err error) // 游戏服：承载玩法与玩家状态
	Gateway func() (stop func(), err error) // 网关：客户端接入与转发
	Master  func() (stop func(), err error) // 顶层协调服：节点注册与摘除 / 玩家定位 / 排行榜 / SessionToken
	Log     func() (stop func(), err error) // 日志服：接收 game 批量上报的业务日志并落盘
	Auth    func() (stop func(), err error) // 账号服：注册 / 登录 / 签发 token，只暴露 HTTP
}

// runProcess 按 server_type 启动对应进程，并阻塞到收到 SIGINT/SIGTERM。
// onReady 在全部进程启动成功后、进入阻塞等待前回调一次（供调用方打印启动汇总报告），可为 nil。
//
// server_type 语义（服务器分类）：
//   - "game"：游戏服，承载玩法与玩家状态；
//   - "gateway"：网关，负责客户端接入与转发；
//   - "all"：网关 + 游戏服 + master + log + 账号服同进程一体启动（单进程全功能，master 必须启动成功）；
//   - "master"：顶层协调服，负责节点注册与摘除、玩家定位、排行榜、SessionToken 与死节点探测；
//   - "log"：日志服，接收 game 批量上报的业务日志并落盘；
//   - "auth"：账号服，注册 / 登录 / 签发 token，只暴露 HTTP（不接游戏长连接）。
//
// 未来架构还将出现 "cross"（跨数据服）、"room"（房间服）等，
// 各框架在调用 runProcess 前自行归一化 server_type 到上述核心角色即可。
func runProcess(serverType string, s ProcessStarters, onReady func()) error {
	stop, err := doRunProcess(serverType, s)
	if err != nil {
		return err
	}
	if onReady != nil {
		onReady()
	}
	waitSignal(stop)
	return nil
}

func doRunProcess(serverType string, s ProcessStarters) (func(), error) {
	var start func() (func(), error)
	switch serverType {
	case ServerTypeGame:
		start = s.Logic
	case ServerTypeGateway:
		start = s.Gateway
	case ServerTypeMaster:
		start = s.Master
	case ServerTypeLog:
		start = s.Log
	case ServerTypeAuth:
		// 账号服独立部署：只起 HTTP 监听，不含网关 / 逻辑服 / master / log。
		// 它是游戏服登录的**必经依赖**（游戏服调 /auth/verify 换 owner），
		// 与游戏服同进程（server_type=all）时由 all 分支一并拉起。
		start = s.Auth
	case ServerTypeAll:
		// 网关 + 游戏服 + master + log + 账号服 同进程一体启动。
		// master 必须最先：game 的 setupNATSBackends 会通过 TCP 连接 master，未就绪则全局能力永久降级；
		// log 其次：game 的 logbuf 会连接 log 服做批量上报；
		// auth 第三：登录依赖账号服 /auth/verify，同进程启动保证「起了服务器就一定起了账号服」。
		if s.Logic == nil || s.Gateway == nil || s.Master == nil || s.Log == nil || s.Auth == nil {
			return nil, errors.New("svc: Logic, Gateway, Master, Log and Auth starters are all required for all")
		}
		start = func() (func(), error) {
			ms, err := s.Master()
			if err != nil {
				return nil, fmt.Errorf("svc: master start failed: %w", err)
			}
			lsv, err := s.Log()
			if err != nil {
				ms()
				return nil, fmt.Errorf("svc: log start failed: %w", err)
			}
			as, err := s.Auth()
			if err != nil {
				lsv()
				ms()
				return nil, fmt.Errorf("svc: auth start failed: %w", err)
			}
			ls, err := s.Logic()
			if err != nil {
				as()
				lsv()
				ms()
				return nil, err
			}
			gs, err := s.Gateway()
			if err != nil {
				ls()
				as()
				lsv()
				ms()
				return nil, err
			}
			return func() {
				gs()
				ls()
				as()
				lsv()
				ms()
			}, nil
		}
	default:
		return nil, errors.New("svc: unknown server_type '" + serverType + "' (want game, gateway, all, master, log or auth)")
	}
	if start == nil {
		return nil, errors.New("svc: start function required for " + serverType)
	}
	return start()
}

// shutdownCh 程序化关机信号通道。
//
// 存在理由：Windows 没有 SIGTERM，运维无法用信号触发优雅退出；
// 灰度重启（drain 归零后）也需要由进程内部主动结束自己。
// requestShutdown 关闭该通道，等价于收到一次退出信号。
var shutdownCh = make(chan struct{})

// requestShutdown 请求进程优雅退出（幂等）。与收到 SIGINT/SIGTERM 等价。
func requestShutdown() {
	select {
	case <-shutdownCh:
		// 已关闭，幂等返回。
	default:
		close(shutdownCh)
	}
}

// waitSignal 阻塞直到收到 SIGINT/SIGTERM 或程序化关机请求，随后调用 stop（stop 可为 nil）。
func waitSignal(stop func()) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)
	doWaitSignal(stop, sig)
}

// doWaitSignal 等待退出触发：进程信号或程序化关机请求（shutdownCh）任一先到即返回。
func doWaitSignal(stop func(), sigCh <-chan os.Signal) {
	select {
	case <-sigCh:
	case <-shutdownCh:
		logger.Infof("svc: shutdown requested via control plane")
	}
	logger.Infof("svc: shutting down")
	if stop != nil {
		safeStop(stop)
	}
}

// stopTimeout 是 safeStop 中执行 stop 的最大允许时长；超时后直接退出以防组件卡死。
const stopTimeout = 30 * time.Second

// safeStop 执行 stop 并 recover panic + 超时保护，避免关停期某处崩溃击穿主 goroutine
// 或某组件 Stop 调用卡死导致进程永不退出。
//
// 超时之后能做什么：Go 杀不掉协程，卡住的 stop() 只能原地留着（进程紧接着就退出，
// 泄漏随进程一起消失；真正要治的是那个卡住的 Stop 本身）。所以超时分支的职责是
// **留下能定位的证据** —— 打印全部协程栈指出卡在哪一行，而不是只说一句「超时了」。
func safeStop(stop func()) {
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// 打全部协程栈（与超时分支同一证据标准）：关停期 panic 需要能直接
				// 定位到是哪个组件的哪一行，而不是只剩一句「panic 了」。
				logger.Errorf("svc: panic during shutdown stop: %v\n%s", r, allGoroutineStacks())
			}
			close(done)
		}()
		stop()
	}()
	t := time.NewTimer(stopTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		logger.Errorf("svc: shutdown stop timed out after %v, forcing exit; goroutine dump:\n%s",
			stopTimeout, allGoroutineStacks())
	}
}

// allGoroutineStacks 抓取全部协程栈（关停超时时用于定位卡死的 Stop）。
func allGoroutineStacks() []byte {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, len(buf)*2)
	}
}
