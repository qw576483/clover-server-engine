// Package metrics 提供 Prometheus 兼容的指标采集与文本导出能力。

// 设计目标：**零第三方依赖**（纯标准库自研）、**高并发低开销**（读写路径全部走 sync/atomic，
// 不加锁）、**Prometheus exposition format 严格兼容**（可被 Prometheus / VictoriaMetrics
// 直接抓取）。

// 提供 Counter / Gauge / Histogram 三种标准指标类型，按 Prometheus 文本格式经 /metrics 端点导出。

// 三种指标类型：

//	Counter   单调递增计数器（请求数、错误数），只能 Inc / Add(正数)
//	Gauge     可增可减的瞬时值（在线人数、队列长度、内存占用）
//	Histogram 固定 bucket 的分布统计（请求耗时），导出 _bucket / _sum / _count

// 典型用法：

// metrics.CounterOf("clover_msg_total", "msg_id", "10001").Inc()
// metrics.GaugeOf("clover_online_players").Set(1234)
// metrics.HistogramOf("clover_handler_seconds", nil, "handler", "login").Observe(0.031)
// http.Handle("/metrics", metrics.Handler())
package metrics

import (
	"math"
	"sort"
	"strings"
	"sync/atomic"
)

// DefBuckets 是默认的 Histogram 分桶边界（单位：秒），覆盖 5ms ~ 10s。
// 与 Prometheus 官方 client_golang 的 DefBuckets 保持一致，便于现成看板复用。
var DefBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// 单调递增计数器，并发安全且完全无锁（单个 atomic.Uint64 承载 float64 位模式）。
// 之所以用 uint64 存 float64 的位模式而非直接用整数：Prometheus 的 counter 语义允许
// 浮点增量（如累计流量字节的加权值），统一成 float64 可避免后续再引入第二套类型。
type Counter struct {
	bits atomic.Uint64 // math.Float64bits(当前值)
}

// 计数器加一。
func (c *Counter) Inc() { c.Add(1) }

// Add 计数器增加 delta。delta 为负时**静默忽略**——counter 语义上单调递增，
// 允许下降会让 Prometheus 的 rate() 误判为计数器重置，产生毛刺。
func (c *Counter) Add(delta float64) {
	if delta < 0 || math.IsNaN(delta) {
		return
	}
	for {
		old := c.bits.Load()
		next := math.Float64frombits(old) + delta
		if c.bits.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

// Value 返回当前累计值。
func (c *Counter) Value() float64 { return math.Float64frombits(c.bits.Load()) }

// Gauge 可增可减的瞬时值，并发安全且完全无锁。
type Gauge struct {
	bits atomic.Uint64
}

// Set 直接设置为 v。
func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }

// Inc 加一。
func (g *Gauge) Inc() { g.Add(1) }

// Dec 减一。
func (g *Gauge) Dec() { g.Add(-1) }

// Add 增加 delta（可为负）。
func (g *Gauge) Add(delta float64) {
	if math.IsNaN(delta) {
		return
	}
	for {
		old := g.bits.Load()
		next := math.Float64frombits(old) + delta
		if g.bits.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

// Sub 减少 delta。
func (g *Gauge) Sub(delta float64) { g.Add(-delta) }

// Value 返回当前值。
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// Histogram 固定 bucket 的分布统计，并发安全且完全无锁。

// 内部结构：
//   - upper：升序的 bucket 上界（不含 +Inf，+Inf 由 count 承担）；
//   - counts：与 upper 一一对应的**非累积**桶计数，导出时才滚动累加成累积计数；
//   - sum / count：观测值总和与总次数。

// 为什么 counts 存非累积值：Observe 只需对命中的那一个桶做一次 atomic.Add，
// 若直接存累积值则每次要更新 O(n) 个桶，高频路径开销随桶数线性放大。
type Histogram struct {
	upper  []float64
	counts []atomic.Uint64 // 非累积计数，counts[i] 对应 (upper[i-1], upper[i]]
	sumBit atomic.Uint64   // math.Float64bits(sum)
	count  atomic.Uint64   // 总观测次数（等价 +Inf 桶）
}

// newHistogram 按给定上界构造 Histogram。buckets 为空时使用 DefBuckets。
// 内部会拷贝并排序去重，调用方后续修改切片不影响已构造实例。
func newHistogram(buckets []float64) *Histogram {
	src := buckets
	if len(src) == 0 {
		src = DefBuckets
	}
	up := make([]float64, 0, len(src))
	for _, b := range src {
		if math.IsNaN(b) || math.IsInf(b, 1) {
			continue // +Inf 由 count 承担，NaN 无意义，均剔除
		}
		up = append(up, b)
	}
	sort.Float64s(up)
	// 去重：重复上界会导出两条 le 相同的样本，Prometheus 抓取时报重复样本错误。
	dedup := up[:0]
	for i, b := range up {
		if i == 0 || b != up[i-1] {
			dedup = append(dedup, b)
		}
	}
	up = dedup
	return &Histogram{upper: up, counts: make([]atomic.Uint64, len(up))}
}

// Observe 记录一次观测值。
func (h *Histogram) Observe(v float64) {
	if math.IsNaN(v) {
		return
	}
	// 二分定位第一个 upper[i] >= v 的桶；未命中任何桶说明落在 +Inf，只累加 count。
	i := sort.SearchFloat64s(h.upper, v)
	if i < len(h.counts) {
		h.counts[i].Add(1)
	}
	h.count.Add(1)
	for {
		old := h.sumBit.Load()
		next := math.Float64frombits(old) + v
		if h.sumBit.CompareAndSwap(old, math.Float64bits(next)) {
			break
		}
	}
}

// Sum 返回所有观测值之和。
func (h *Histogram) Sum() float64 { return math.Float64frombits(h.sumBit.Load()) }

// Count 返回观测次数。
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Buckets 返回 bucket 上界与对应的**累积**计数（Prometheus le 语义）。
// 返回的是快照拷贝，调用方可安全持有。
func (h *Histogram) Buckets() (upper []float64, cumulative []uint64) {
	upper = make([]float64, len(h.upper))
	copy(upper, h.upper)
	cumulative = make([]uint64, len(h.counts))
	var acc uint64
	for i := range h.counts {
		acc += h.counts[i].Load()
		cumulative[i] = acc
	}
	return upper, cumulative
}

// 指标标识
// metricKind 指标类型枚举，用于导出 # TYPE 行。
type metricKind uint8

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

func (k metricKind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	default:
		return "histogram"
	}
}

// entry 是注册表中的一条指标实例（同名 + 同 label 值组合唯一）。
type entry struct {
	name   string
	kind   metricKind
	labels []string // 扁平化的 k1,v1,k2,v2...，已按 key 排序
	help   string   //lint:ignore U1000 Prometheus HELP metadata placeholder

	counter   *Counter
	gauge     *Gauge
	histogram *Histogram
}

// normalizeLabels 把 k1,v1,k2,v2... 规整为按 key 升序排列的扁平切片，
// 保证 ("a","1","b","2") 与 ("b","2","a","1") 命中同一实例，避免同一逻辑指标
// 因传参顺序不同而被拆成两条时间序列。
// 长度为奇数时丢弃最后一个残缺的 key（防御性处理，不 panic）。
func normalizeLabels(labels []string) []string {
	n := len(labels) / 2 * 2
	if n == 0 {
		return nil
	}
	type kv struct{ k, v string }
	pairs := make([]kv, 0, n/2)
	for i := 0; i < n; i += 2 {
		pairs = append(pairs, kv{labels[i], labels[i+1]})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	out := make([]string, 0, n)
	for _, p := range pairs {
		out = append(out, p.k, p.v)
	}
	return out
}

// entryKey 生成注册表 map 的查找键。用 \x00 / \x01 这两个不可能出现在
// Prometheus 合法标识符里的控制字符做分隔，杜绝 "a_b{}" 与 "a{b=...}" 撞键。
func entryKey(name string, kind metricKind, labels []string) string {
	var sb strings.Builder
	sb.Grow(len(name) + 8 + len(labels)*8)
	sb.WriteString(name)
	sb.WriteByte(0x01)
	sb.WriteByte(byte('0' + kind))
	for _, s := range labels {
		sb.WriteByte(0x00)
		sb.WriteString(s)
	}
	return sb.String()
}
