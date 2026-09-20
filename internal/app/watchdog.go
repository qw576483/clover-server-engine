package app

import (
	"net/http"
	"runtime"
	"time"

	"clover-server-engine/internal/foundation/ophttp"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/foundation/metrics"
	"clover-server-engine/pkg/runtime/watchdog"
)

// 内置巡检规则：进程 CPU 饱和。
//
// 为什么引擎要内置这一条：进程被 CPU 打满是「不需要业务知识就能判定」的通用异常，
// 且在没有接 Prometheus 的部署里（离线 / 私有化）永远不会有人发现。
// 其余规则（业务积压、队列深度、自定义阈值）应由模块 / 业务注册，引擎不替它们定阈值。
const (
	// watchdogRuleProcessCPU 规则名。固定常量：它同时是日志前缀、指标 label 值与告警冷却键，
	// **不允许**拼入 player_id / conn_id 等动态值。
	watchdogRuleProcessCPU = "process_cpu_saturated"
	// watchdogCPUWarnRatio 触发阈值：该值 = 进程 CPU 使用率 ÷ 可用核数（GOMAXPROCS）。
	watchdogCPUWarnRatio = 0.9
	// watchdogCPUConsecutive 连续命中轮数。巡检间隔默认 10s，即「持续约 30s 才算饱和」，
	// 用来滤掉编译 / GC / 冷启动这类瞬时毛刺。
	watchdogCPUConsecutive = 3
	// watchdogCPUCooldown 该规则的告警冷却：饱和是持续状态，没必要每轮都叫人。
	watchdogCPUCooldown = 10 * time.Minute
)

// installWatchdog 创建并启动进程级看门狗，返回实例供调用方在退出时 Stop（Stop 幂等）。
//
// 时序约定：**必须先 Install 再让业务注册规则**。runApp 在本函数返回后才进入各角色的
// 装配 / mount 回调，所以业务在 RegisterMount / bootstrap 里通过 watchdog.Default()
// 注册的规则一定落在同一个实例上。
//
// adminSrv 可为 nil（配置 admin.disable=true 时）：AdminServer 的方法对 nil 接收者是空操作。
func installWatchdog(adminSrv *AdminServer) *watchdog.Watcher {
	w := watchdog.New(watchdog.Options{})
	if err := w.Register(watchdog.Rule{
		Name:           watchdogRuleProcessCPU,
		Cooldown:       watchdogCPUCooldown,
		MinConsecutive: watchdogCPUConsecutive,
		Check:          cpuSaturationCheck(),
	}); err != nil {
		// 内置规则注册失败属装配期错误（重名 / 参数非法），必须留痕但不阻断启动：
		// 看门狗是旁路能力，缺一条规则不该让整个进程起不来。
		logger.Errorf("app: register builtin watchdog rule %s failed: %v", watchdogRuleProcessCPU, err)
	}
	watchdog.Install(w)
	w.Start()

	// 状态快照挂到 admin 控制面（只读）：运维可 curl /watchdog 看「每条规则现在什么状态」，
	// 与 /metrics 的分工是「瞬时状态 vs 时间序列」。
	adminSrv.Handle("/watchdog", http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		ophttp.JSON(rw, http.StatusOK, map[string]any{
			"rules": w.Snapshot(),
			"count": w.Rules(),
			// 非零 = 有告警因出口阻塞被丢弃（运维要处理的是出口，不是看门狗）。
			"dropped": w.Dropped(),
		})
	}))
	return w
}

// cpuSaturationCheck 构造「进程 CPU 饱和」巡检函数。
//
// 数据来源是 metrics.ProcessCPUSeconds（进程累计 CPU 秒）：
//   - 平台不支持采集时恒返回正常，并在首次调用时留一条 Warn 日志——
//     规则静默失效比报假警更糟，所以「不生效」这件事必须留痕；
//   - 首次调用只建立基准（累计量必须两次采样才能算速率），之后按「本次 - 上次 ÷ 墙钟间隔」算使用率。
//
// 线程安全：同一个 Check 不会被并发调用（Watcher 用 running 标志跳过未跑完的轮次），
// 因此闭包内的采样基准无需加锁。
func cpuSaturationCheck() watchdog.Check {
	cores := float64(runtime.GOMAXPROCS(0))
	var (
		prevSec     float64
		prevAt      time.Time
		primed      bool
		warnedNoSup bool
	)
	return func() watchdog.Result {
		sec, ok := metrics.ProcessCPUSeconds()
		if !ok {
			if !warnedNoSup {
				warnedNoSup = true
				logger.Warnf("app: watchdog rule %s is inactive: process CPU sampling unsupported on this platform", watchdogRuleProcessCPU)
			}
			return watchdog.OK
		}
		now := time.Now()
		if !primed {
			prevSec, prevAt, primed = sec, now, true
			return watchdog.OK
		}
		elapsed := now.Sub(prevAt).Seconds()
		delta := sec - prevSec
		prevSec, prevAt = sec, now
		if elapsed <= 0 || delta < 0 {
			// 时钟退化 / 计数器回退：不编造数据，本轮按正常处理。
			return watchdog.OK
		}
		ratio := delta / elapsed
		used := ratio / cores
		if used >= watchdogCPUWarnRatio {
			return watchdog.Critical("进程 CPU 使用率 %.0f%%（%.2f/%.0f 核）", used*100, ratio, cores)
		}
		return watchdog.OK
	}
}
