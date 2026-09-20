package metrics

import (
	"bufio"
	"io"
	"math"
	"sort"
	"strings"
	"sync"

	"clover-server-engine/pkg/shared/conv"
)

// Registry 指标注册表，负责指标实例的**去重复用**与**文本格式导出**。

// 并发模型：一把 RWMutex 保护 map。读路径（已存在的指标）走 RLock 快速返回，
// 只有首次创建才升级为写锁。由于游戏服的指标名是有限集合，写锁竞争只发生在启动初期，
// 稳态下等价于纯读锁，配合指标本体的无锁 atomic，整体开销可忽略。

// 使用建议：**指标名与 label 值必须是有限集合**。把 playerID / connID / traceID 这类
// 高基数值拼进 label 会造成时间序列爆炸（注册表无界增长且 Prometheus 侧 OOM）。
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
	helps   map[string]string // name -> help，同名指标共享一条 # HELP
}

// NewRegistry 创建一个空的指标注册表。
func NewRegistry() *Registry {
	return &Registry{
		entries: make(map[string]*entry),
		helps:   make(map[string]string),
	}
}

// SetHelp 为指标名设置 # HELP 描述文本。同名指标（不同 label 组合）共享同一条描述。
// 未设置时导出一条兜底的 "<name> metric" 说明，保证 exposition format 完整。
func (r *Registry) SetHelp(name, help string) {
	r.mu.Lock()
	r.helps[name] = help
	r.mu.Unlock()
}

// Counter 返回 name + labels 对应的 Counter，不存在则创建。
// labels 形如 "k1","v1","k2","v2"；顺序无关（内部按 key 排序归一）。
func (r *Registry) Counter(name string, labels ...string) *Counter {
	e := r.getOrCreate(name, kindCounter, labels, nil)
	return e.counter
}

// Gauge 返回 name + labels 对应的 Gauge，不存在则创建。
func (r *Registry) Gauge(name string, labels ...string) *Gauge {
	e := r.getOrCreate(name, kindGauge, labels, nil)
	return e.gauge
}

// Histogram 返回 name + labels 对应的 Histogram，不存在则创建。
// buckets 为 nil / 空时使用 DefBuckets；**同名指标的 buckets 以首次创建者为准**，
// 后续传入不同 buckets 不会重建（Prometheus 要求同名指标 bucket 布局一致）。
func (r *Registry) Histogram(name string, buckets []float64, labels ...string) *Histogram {
	e := r.getOrCreate(name, kindHistogram, labels, buckets)
	return e.histogram
}

// getOrCreate 是三个构造入口的公共实现：先读锁探测，未命中再写锁双检创建。
func (r *Registry) getOrCreate(name string, kind metricKind, labels []string, buckets []float64) *entry {
	norm := normalizeLabels(labels)
	key := entryKey(name, kind, norm)

	r.mu.RLock()
	e := r.entries[key]
	r.mu.RUnlock()
	if e != nil {
		return e
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// 双重检查：读锁释放到写锁获取之间可能已被其它 goroutine 创建。
	if e = r.entries[key]; e != nil {
		return e
	}
	e = &entry{name: name, kind: kind, labels: norm}
	switch kind {
	case kindCounter:
		e.counter = &Counter{}
	case kindGauge:
		e.gauge = &Gauge{}
	default:
		e.histogram = newHistogram(buckets)
	}
	r.entries[key] = e
	return e
}

// Unregister 移除 name + labels 对应的指标实例。
// 用于生命周期结束的动态维度（如某个 zone 下线后清理其指标），避免注册表无界增长。
func (r *Registry) Unregister(name string, labels ...string) {
	norm := normalizeLabels(labels)
	r.mu.Lock()
	for _, k := range []metricKind{kindCounter, kindGauge, kindHistogram} {
		delete(r.entries, entryKey(name, k, norm))
	}
	r.mu.Unlock()
}

// Reset 清空注册表中的全部指标。主要用于测试隔离，生产不应调用
// （清空后 Prometheus 侧会看到计数器重置毛刺）。
func (r *Registry) Reset() {
	r.mu.Lock()
	r.entries = make(map[string]*entry)
	r.helps = make(map[string]string)
	r.mu.Unlock()
}

// WriteText 按 Prometheus exposition format (text/plain; version=0.0.4) 写出全部指标。

// 输出规则：
//   - 同名指标聚合在一起，先输出一条 # HELP + 一条 # TYPE，再输出各 label 组合的样本；
//   - 指标名按字典序排序，同名内按 label 串排序 —— 保证输出**完全确定**，便于测试断言与 diff；
//   - Histogram 输出 <name>_bucket{le="..."}（累积计数，含 le="+Inf"）、<name>_sum、<name>_count。
func (r *Registry) WriteText(w io.Writer) error {
	// 先在锁内取出 entry 快照，导出与格式化在锁外做，避免长时间持锁阻塞埋点。
	r.mu.RLock()
	list := make([]*entry, 0, len(r.entries))
	for _, e := range r.entries {
		list = append(list, e)
	}
	helps := make(map[string]string, len(r.helps))
	for k, v := range r.helps {
		helps[k] = v
	}
	r.mu.RUnlock()

	sort.Slice(list, func(i, j int) bool {
		if list[i].name != list[j].name {
			return list[i].name < list[j].name
		}
		if list[i].kind != list[j].kind {
			return list[i].kind < list[j].kind
		}
		return strings.Join(list[i].labels, "\x00") < strings.Join(list[j].labels, "\x00")
	})

	bw := bufio.NewWriter(w)
	lastName := ""
	for _, e := range list {
		if e.name != lastName {
			help := helps[e.name]
			if help == "" {
				help = e.name + " metric"
			}
			_, _ = bw.WriteString("# HELP ")
			_, _ = bw.WriteString(e.name)
			_ = bw.WriteByte(' ')
			_, _ = bw.WriteString(escapeHelp(help))
			_ = bw.WriteByte('\n')
			_, _ = bw.WriteString("# TYPE ")
			_, _ = bw.WriteString(e.name)
			_ = bw.WriteByte(' ')
			_, _ = bw.WriteString(e.kind.String())
			_ = bw.WriteByte('\n')
			lastName = e.name
		}
		writeEntry(bw, e)
	}
	return bw.Flush()
}

// writeEntry 输出单条 entry 的全部样本行。
func writeEntry(bw *bufio.Writer, e *entry) {
	switch e.kind {
	case kindCounter:
		writeSample(bw, e.name, e.labels, "", "", e.counter.Value())
	case kindGauge:
		writeSample(bw, e.name, e.labels, "", "", e.gauge.Value())
	default:
		upper, cum := e.histogram.Buckets()
		for i, ub := range upper {
			writeSample(bw, e.name+"_bucket", e.labels, "le", formatFloat(ub), float64(cum[i]))
		}
		// +Inf 桶恒等于总观测数，Prometheus 要求必须存在。
		writeSample(bw, e.name+"_bucket", e.labels, "le", "+Inf", float64(e.histogram.Count()))
		writeSample(bw, e.name+"_sum", e.labels, "", "", e.histogram.Sum())
		writeSample(bw, e.name+"_count", e.labels, "", "", float64(e.histogram.Count()))
	}
}

// writeSample 输出一行样本：name{k="v",...} value
// extraKey / extraVal 非空时追加为最后一个 label（用于 histogram 的 le）。
func writeSample(bw *bufio.Writer, name string, labels []string, extraKey, extraVal string, val float64) {
	_, _ = bw.WriteString(name)
	if len(labels) > 0 || extraKey != "" {
		_ = bw.WriteByte('{')
		first := true
		for i := 0; i+1 < len(labels); i += 2 {
			if !first {
				_ = bw.WriteByte(',')
			}
			first = false
			_, _ = bw.WriteString(labels[i])
			_, _ = bw.WriteString(`="`)
			_, _ = bw.WriteString(escapeLabelValue(labels[i+1]))
			_ = bw.WriteByte('"')
		}
		if extraKey != "" {
			if !first {
				_ = bw.WriteByte(',')
			}
			_, _ = bw.WriteString(extraKey)
			_, _ = bw.WriteString(`="`)
			_, _ = bw.WriteString(escapeLabelValue(extraVal))
			_ = bw.WriteByte('"')
		}
		_ = bw.WriteByte('}')
	}
	_ = bw.WriteByte(' ')
	_, _ = bw.WriteString(formatFloat(val))
	_ = bw.WriteByte('\n')
}

// formatFloat 按 Prometheus 约定格式化浮点值。
// 整数值输出为不带小数点的整数形式（1 而非 1e+00），可读性与官方客户端一致；
// Inf / NaN 输出为 Prometheus 认可的字面量 +Inf / -Inf / NaN。
func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return conv.FormatInt(int64(v))
	}
	return conv.FormatFloat(v)
}

// escapeLabelValue 按 exposition format 转义 label 值中的 \ " 与换行。
func escapeLabelValue(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for _, c := range s {
		switch c {
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteRune(c)
		}
	}
	return sb.String()
}

// escapeHelp 按 exposition format 转义 HELP 文本中的 \ 与换行（HELP 中的 " 无需转义）。
func escapeHelp(s string) string {
	if !strings.ContainsAny(s, "\\\n") {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for _, c := range s {
		switch c {
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteRune(c)
		}
	}
	return sb.String()
}
