package metrics

import (
	"net/http"
)

// DefaultRegistry 全局默认注册表。业务无需自行构造 Registry，
// 直接用包级快捷函数（CounterOf / GaugeOf / HistogramOf）即可。
//
// 需要指标隔离的场景（如单元测试、多租户导出）才自行 NewRegistry。
var DefaultRegistry = NewRegistry()

// ContentType 是 Prometheus 文本导出的 Content-Type，
// 与官方 client_golang 的 text/plain; version=0.0.4 保持一致。
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// CounterOf 从默认注册表取 Counter。
func CounterOf(name string, labels ...string) *Counter {
	return DefaultRegistry.Counter(name, labels...)
}

// GaugeOf 从默认注册表取 Gauge。
func GaugeOf(name string, labels ...string) *Gauge {
	return DefaultRegistry.Gauge(name, labels...)
}

// HistogramOf 从默认注册表取 Histogram。buckets 为 nil 时使用 DefBuckets。
func HistogramOf(name string, buckets []float64, labels ...string) *Histogram {
	return DefaultRegistry.Histogram(name, buckets, labels...)
}

// SetHelp 为默认注册表中的指标名设置 # HELP 描述。
func SetHelp(name, help string) { DefaultRegistry.SetHelp(name, help) }

// Handler 返回默认注册表的 /metrics HTTP handler。
//
//	mux.Handle("/metrics", metrics.Handler())
func Handler() http.Handler { return HandlerFor(DefaultRegistry) }

// HandlerFor 返回指定注册表的 /metrics HTTP handler。
//
// 实现细节：每次抓取都会先执行 CollectRuntime 刷新运行时指标，
// 保证 goroutine 数 / 堆内存等瞬时值是抓取时刻的真实值，而非某个后台采样点的陈旧值。
func HandlerFor(r *Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		CollectRuntimeTo(r)
		w.Header().Set("Content-Type", ContentType)
		// 写入过程中出错（客户端断开）无处可返回，只能忽略——
		// header 已发出，此时再 WriteHeader(500) 会触发 superfluous 警告。
		_ = r.WriteText(w)
	})
}
