package metrics

import (
	"runtime"
	"time"
)

// 运行时指标名，命名遵循 Prometheus 官方 go_* 约定，
// 现成的 Grafana Go Runtime 看板可直接复用。
const (
	MetricGoGoroutines   = "go_goroutines"                    // 当前 goroutine 数
	MetricGoThreads      = "go_threads"                       // 当前 OS 线程数
	MetricGoInfo         = "go_info"                          // Go 版本信息（恒为 1，版本在 label 上）
	MetricGoGCCount      = "go_gc_cycles_total"               // 累计 GC 次数
	MetricGoGCPauseTotal = "go_gc_pause_seconds_total"        // 累计 GC STW 暂停时长
	MetricGoGCCPUFrac    = "go_gc_cpu_fraction"               // GC 占用 CPU 比例
	MetricGoHeapAlloc    = "go_memstats_heap_alloc_bytes"     // 当前堆上存活对象字节数
	MetricGoHeapSys      = "go_memstats_heap_sys_bytes"       // 向 OS 申请的堆字节数
	MetricGoHeapInuse    = "go_memstats_heap_inuse_bytes"     // 使用中的 span 字节数
	MetricGoHeapIdle     = "go_memstats_heap_idle_bytes"      // 空闲 span 字节数
	MetricGoHeapObjects  = "go_memstats_heap_objects"         // 堆上存活对象数
	MetricGoStackInuse   = "go_memstats_stack_inuse_bytes"    // goroutine 栈占用字节数
	MetricGoSys          = "go_memstats_sys_bytes"            // 进程向 OS 申请的总字节数
	MetricGoAllocTotal   = "go_memstats_alloc_bytes_total"    // 累计分配字节数
	MetricGoMallocs      = "go_memstats_mallocs_total"        // 累计分配对象数
	MetricGoFrees        = "go_memstats_frees_total"          // 累计释放对象数
	MetricGoNextGC       = "go_memstats_next_gc_bytes"        // 下次 GC 的堆目标字节数
	MetricGoLastGC       = "go_memstats_last_gc_time_seconds" // 上次 GC 的 Unix 时间戳
	MetricProcStartTime  = "process_start_time_seconds"       // 进程启动 Unix 时间戳
	MetricProcUptime     = "process_uptime_seconds"           // 进程已运行秒数
	MetricProcCPUSeconds = "process_cpu_seconds_total"        // 进程累计消耗的 CPU 秒数（用户态 + 内核态）
	MetricProcCPURatio   = "process_cpu_ratio"                // 进程 CPU 使用率（1.0 = 跑满一个核）
)

// startTime 进程启动时间，包初始化时锁定，用于计算 uptime。
var startTime = time.Now()

// CollectRuntime 采集 Go 运行时指标到默认注册表。
//
// 由 /metrics handler 在每次抓取前自动调用；若业务需要定期落盘或推送，
// 也可自行用 timer 周期调用。
func CollectRuntime() { CollectRuntimeTo(DefaultRegistry) }

// CollectRuntimeTo 采集 Go 运行时指标到指定注册表。
//
// 【性能注意】runtime.ReadMemStats 会 **STW（stop-the-world）**，耗时随堆大小增长
// （大堆可达数百微秒）。因此本函数只应在 /metrics 抓取路径（通常 15s ~ 60s 一次）
// 或低频定时任务里调用，**绝不要放进游戏主循环或每帧逻辑**。
//
// 所有指标以 Gauge 形式写入（包括语义上单调的累计量）：Counter 只能 Add，
// 而 ReadMemStats 给出的是绝对值，用 Gauge.Set 直接覆盖才不会重复累加。
// Prometheus 侧 counter 与 gauge 的存储完全一致，rate() 对 gauge 同样可用，
// 唯一差异是 # TYPE 行 —— 相比累加错误，这个代价可以接受。
func CollectRuntimeTo(r *Registry) {
	if r == nil {
		return
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	set := func(name string, v float64, labels ...string) {
		r.Gauge(name, labels...).Set(v)
	}

	// 调度器
	set(MetricGoGoroutines, float64(runtime.NumGoroutine()))
	threads, _ := runtime.ThreadCreateProfile(nil)
	set(MetricGoThreads, float64(threads))
	set(MetricGoInfo, 1, "version", runtime.Version())

	// GC
	set(MetricGoGCCount, float64(ms.NumGC))
	set(MetricGoGCPauseTotal, float64(ms.PauseTotalNs)/float64(time.Second))
	set(MetricGoGCCPUFrac, ms.GCCPUFraction)
	set(MetricGoNextGC, float64(ms.NextGC))
	set(MetricGoLastGC, float64(ms.LastGC)/float64(time.Second))

	// 堆内存
	set(MetricGoHeapAlloc, float64(ms.HeapAlloc))
	set(MetricGoHeapSys, float64(ms.HeapSys))
	set(MetricGoHeapInuse, float64(ms.HeapInuse))
	set(MetricGoHeapIdle, float64(ms.HeapIdle))
	set(MetricGoHeapObjects, float64(ms.HeapObjects))
	set(MetricGoStackInuse, float64(ms.StackInuse))
	set(MetricGoSys, float64(ms.Sys))
	set(MetricGoAllocTotal, float64(ms.TotalAlloc))
	set(MetricGoMallocs, float64(ms.Mallocs))
	set(MetricGoFrees, float64(ms.Frees))

	// 进程
	now := time.Now()
	set(MetricProcStartTime, float64(startTime.UnixNano())/float64(time.Second))
	set(MetricProcUptime, now.Sub(startTime).Seconds())

	// 进程 CPU：平台不支持时**不导出**这两项（见 proc_other.go），而不是导出恒为 0 的假曲线。
	// process_cpu_seconds_total 是累计量、process_cpu_ratio 是相对上一次抓取的使用率
	// （1.0 = 一个核跑满，4 核满载约 4.0 ⇒ 换算成百分比要除以 GOMAXPROCS）。
	// 采集成本是一次 getrusage(2) / GetProcessTimes，远低于同路径上的 ReadMemStats。
	if cpuSec, ok := ProcessCPUSeconds(); ok {
		set(MetricProcCPUSeconds, cpuSec)
		if ratio, ok := procSampler.ratio(cpuSec, now); ok {
			set(MetricProcCPURatio, ratio)
		}
	}

	r.SetHelp(MetricGoGoroutines, "Number of goroutines that currently exist.")
	r.SetHelp(MetricGoThreads, "Number of OS threads created.")
	r.SetHelp(MetricGoInfo, "Information about the Go environment.")
	r.SetHelp(MetricGoGCCount, "Total number of completed GC cycles.")
	r.SetHelp(MetricGoGCPauseTotal, "Total GC stop-the-world pause duration in seconds.")
	r.SetHelp(MetricGoGCCPUFrac, "Fraction of CPU time used by GC since process start.")
	r.SetHelp(MetricGoHeapAlloc, "Number of heap bytes allocated and still in use.")
	r.SetHelp(MetricGoHeapSys, "Number of heap bytes obtained from the system.")
	r.SetHelp(MetricGoHeapInuse, "Number of heap bytes in in-use spans.")
	r.SetHelp(MetricGoHeapIdle, "Number of heap bytes waiting to be used.")
	r.SetHelp(MetricGoHeapObjects, "Number of allocated objects on the heap.")
	r.SetHelp(MetricGoStackInuse, "Number of bytes in use by the stack allocator.")
	r.SetHelp(MetricGoSys, "Number of bytes obtained from the system.")
	r.SetHelp(MetricGoAllocTotal, "Total number of bytes allocated, even if freed.")
	r.SetHelp(MetricGoMallocs, "Total number of heap objects allocated.")
	r.SetHelp(MetricGoFrees, "Total number of heap objects freed.")
	r.SetHelp(MetricGoNextGC, "Number of heap bytes when next GC will take place.")
	r.SetHelp(MetricGoLastGC, "Unix timestamp of the last GC.")
	r.SetHelp(MetricProcStartTime, "Unix timestamp of process start time.")
	r.SetHelp(MetricProcUptime, "Seconds since process start.")
	r.SetHelp(MetricProcCPUSeconds, "Total user and system CPU time spent in seconds.")
	r.SetHelp(MetricProcCPURatio, "Process CPU usage since the previous scrape (1.0 = one saturated core).")
}
