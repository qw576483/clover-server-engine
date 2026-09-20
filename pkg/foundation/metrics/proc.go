package metrics

import (
	"sync"
	"time"
)

// 进程 CPU 采样（平台无关部分）。
//
// 平台实现分离在三个文件里，本文件只承载「与平台无关的推导」：
//
//	proc_unix.go    getrusage(RUSAGE_SELF)      —— Linux / macOS / BSD
//	proc_windows.go GetProcessTimes             —— Windows
//	proc_other.go   不支持                      —— 其余平台（恒返回 ok=false）
//
// 为什么不用 gopsutil 之类的库：本包的设计目标是**零第三方依赖**（见 metrics.go 包说明），
// 且进程 CPU 只需要一次系统调用，引入一个采集框架得不偿失。

// ProcessCPUSeconds 返回进程自启动以来累计消耗的 CPU 时间（用户态 + 内核态，单位秒）。
//
// 只读、无分配、不 STW：Linux/macOS 走 getrusage(2)（约 1µs），Windows 走 GetProcessTimes。
// ok=false 表示当前平台不支持采集，此时 /metrics **不导出** process_cpu_* 两项
// （宁可不导出，也不导出恒为 0 的假数据让看板误判）。
//
// 调用时机：/metrics 抓取路径（CollectRuntimeTo 内部已调用）、低频巡检（看门狗规则）。
// **不要放进游戏主循环或每帧逻辑**。
//
// 由于返回的是「累计量」，想算使用率需要自己保存上一次的 (秒, 时刻) 做差——
// 需要「相对上一采样点」的比率时，可直接使用本包导出的 process_cpu_ratio 指标
// （它由 CollectRuntimeTo 维护采样差）。
func ProcessCPUSeconds() (float64, bool) { return processCPUSeconds() }

// cpuSampler 计算进程 CPU 使用率所需的两次采样差。
//
// ratio = ΔCPU秒 / Δ墙钟秒：1.0 = 正好跑满一个核，>1.0 = 多核并行（4 核满载约 4.0）。
//
// 只在采集路径（/metrics 抓取）使用，用一把小锁保护「上一次采样」——
// 抓取可能被多个 Prometheus 并发触发，且同一次抓取里 runtime 指标与 CPU 指标必须一致。
// 不在热路径上，锁竞争可忽略（与 Registry 的写锁一样，只在采集瞬间发生）。
type cpuSampler struct {
	mu     sync.Mutex
	sec    float64
	at     time.Time
	primed bool
}

// ratio 用本次采样值推进状态，并返回相对上一次采样的 CPU 使用率。
// 首次采样（没有基准）返回 ok=false —— 不编造数据，等下一次抓取。
func (s *cpuSampler) ratio(sec float64, now time.Time) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prevSec, prevAt, primed := s.sec, s.at, s.primed
	s.sec, s.at, s.primed = sec, now, true
	if !primed {
		return 0, false
	}
	elapsed := now.Sub(prevAt).Seconds()
	if elapsed <= 0 {
		// 同一时刻的重复抓取（或测试注入的退化时钟）：无法求差。
		return 0, false
	}
	r := (sec - prevSec) / elapsed
	if r < 0 {
		// 计数器回退（理论上不会发生）：回落到 0，避免 Prometheus 侧出现负值被误读成"负负载"。
		r = 0
	}
	return r, true
}

// procSampler 是 process_cpu_ratio 的采样状态（进程级单例）。
var procSampler cpuSampler
