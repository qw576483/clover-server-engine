package metrics

import "time"

// 指标命名规范（新增埋点必须遵守）

// 统一格式：

//	clover_<模块>_<指标>_<单位>

//	clover_     固定前缀，与机器上其它进程（node_exporter 等）的指标隔离
//	<模块>      引擎模块名，取值见下方 Module* 常量，禁止自造
//	<指标>      被测对象，小写蛇形，用名词或「名词_动词过去分词」，如 connections / messages_dropped
//	<单位>      基础单位后缀，见下方规则；无量纲的计数省略单位、但必须以 _total 结尾

// 单位后缀规则（Prometheus 官方约定，务必用**基础单位**而非毫秒/KB）：

//	_seconds   时间（永远用秒，哪怕实际测的是微秒，除以 1e9 转成秒）
//	_bytes     字节
//	_total     单调递增计数（Counter 专用，必须有）
//	_ratio     0~1 的比例
//	（Gauge 的瞬时数量无后缀，如 clover_gateway_connections）

// 正确示例：

//	clover_gateway_connections                      Gauge      当前连接数
//	clover_gateway_messages_total                   Counter    累计收发消息数
//	clover_gateway_message_bytes                    Histogram  消息体大小分布
//	clover_event_publish_duration_seconds           Histogram  事件发布耗时
//	clover_rpc_requests_total                       Counter    RPC 请求数
//	clover_rpc_request_duration_seconds             Histogram  RPC 耗时
//	clover_db_query_duration_seconds                Histogram  DB 查询耗时

// 反例（禁止）：

//	gateway_conn_num              缺 clover_ 前缀、缩写、无单位语义
//	clover_rpc_latency_ms         用了毫秒，应为 _duration_seconds
//	clover_event_publish          Counter 缺 _total

// # label 使用规范

//	1. **严禁高基数 label**：player_id / conn_id / trace_id / order_id / 具体错误文本
//	   一律不得作为 label。每个不同的 label 值都会在 Prometheus 里生成一条独立时间序列，
//	   十万在线玩家 = 十万条序列 = 抓取端 OOM，同时本进程的 Registry 也会无界增长。
//	   需要按玩家排查请走日志 + trace_id，不要走指标。
//	2. label 值必须来自**有限枚举**：msg_id、handler 名、错误分类（timeout / refused / decode）、
//	   节点角色等。经验阈值：单个指标的 label 组合数 <= 1000。
//	3. label key 用小写蛇形，跨模块保持一致（都叫 code 就别有的地方叫 err_code）。

// 常用 label key 见下方 Label* 常量，优先复用，不要自造同义词。

// 指标名前缀。
const Namespace = "clover"

// 模块名常量。新增模块请在此登记，避免各处拼写漂移（gw / gateway / gate 混用）。
const (
	ModuleGateway = "gateway" // 网关：连接、收发包
	ModuleNet     = "net"     // 网络层：TCP 读写、编解码
	ModuleEvent   = "event"   // 事件总线：发布/订阅/跨节点投递
	ModuleRPC     = "rpc"     // RPC 调用
	ModuleDB      = "db"      // MySQL 等关系库
	ModuleCache   = "cache"   // Redis 等缓存
	ModuleMaster  = "master"  // Master 节点：注册、心跳、状态同步
	ModuleGame    = "game"    // 游戏逻辑：handler 执行、定时器
	ModuleMMO     = "mmo"     // MMO：AOI、战斗、寻路
	ModuleRuntime = "runtime" // 运行时自身

	// ModuleWatchdog 看门狗（周期巡检与告警，见 pkg/runtime/watchdog）。
	// 单独登记而不是借用 ModuleRuntime：运维按模块名查指标时，
	// 「巡检规则命中」与「运行时自身指标」是两件事，混在一起会看不出告警来自哪里。
	ModuleWatchdog = "watchdog"
)

// 常用 label key。**务必复用**，不要自造同义词。
const (
	LabelModule  = "module"  // 模块名
	LabelNode    = "node"    // 节点 ID
	LabelRole    = "role"    // 节点角色
	LabelMsgID   = "msg_id"  // 消息号（有限枚举，安全）
	LabelHandler = "handler" // handler 名
	LabelMethod  = "method"  // RPC 方法名
	LabelStatus  = "status"  // 结果状态：ok / error
	LabelCode    = "code"    // 错误码（有限枚举）
	LabelReason  = "reason"  // 错误分类：timeout / refused / decode（**不是**错误原文）
	LabelDir     = "dir"     // 方向：in / out
	LabelKind    = "kind"    // 子类型
	LabelRule    = "rule"    // 巡检规则名（看门狗）：必须是规则注册时的固定名，禁止拼入玩家/连接等动态值
	LabelLevel   = "level"   // 严重级别（warning / critical / recovered 等有限枚举）
)

// 状态与方向的标准取值，避免出现 "OK" / "success" / "succeed" 三种写法。
const (
	StatusOK    = "ok"
	StatusError = "error"
	DirIn       = "in"
	DirOut      = "out"
)

// 常用 Histogram 分桶。

// DurationBuckets 是耗时分布的默认分桶（秒），等价 DefBuckets，覆盖 5ms ~ 10s。
var DurationBuckets = DefBuckets

// FastDurationBuckets 面向**进程内高频短耗时**（handler 执行、锁等待、编解码），
// 覆盖 50μs ~ 1s。游戏帧逻辑常在百微秒量级，用 DefBuckets 会全部挤在第一个桶里，
// P99 完全看不出来。
var FastDurationBuckets = []float64{
	0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
}

// SizeBuckets 面向消息/包体大小（字节），覆盖 64B ~ 1MB。
var SizeBuckets = []float64{
	64, 256, 1024, 4096, 16384, 65536, 262144, 1048576,
}

// Name 按命名规范拼出完整指标名：clover_<module>_<parts...>。

// metrics.Name(metrics.ModuleGateway, "connections")            // clover_gateway_connections
// metrics.Name(metrics.ModuleEvent, "publish_duration_seconds") // clover_event_publish_duration_seconds
func Name(module string, parts ...string) string {
	n := Namespace
	if module != "" {
		n += "_" + module
	}
	for _, p := range parts {
		if p != "" {
			n += "_" + p
		}
	}
	return n
}

// 各模块通用埋点辅助构造器

// 下面这组构造器把「命名规范 + 分桶选择 + label 约定」固化下来，
// 各模块直接调用即可，无需自己拼字符串，从源头杜绝命名漂移。

// ModuleMetrics 是某个模块的埋点句柄，封装了该模块下最常用的四类指标。

// 用法（在模块的包级变量里建一次，之后复用）：

//	var mm = metrics.ForModule(metrics.ModuleEvent)

// mm.Count("publish_total", metrics.LabelStatus, metrics.StatusOK)
// defer mm.Timer("publish_duration_seconds")()
type ModuleMetrics struct {
	reg    *Registry
	module string
}

// ForModule 基于默认注册表创建模块埋点句柄。
func ForModule(module string) *ModuleMetrics { return ForModuleWith(DefaultRegistry, module) }

// ForModuleWith 基于指定注册表创建模块埋点句柄（测试隔离时用）。
func ForModuleWith(r *Registry, module string) *ModuleMetrics {
	if r == nil {
		r = DefaultRegistry
	}
	return &ModuleMetrics{reg: r, module: module}
}

// Registry 返回底层注册表。
func (m *ModuleMetrics) Registry() *Registry { return m.reg }

// Counter 返回 clover_<module>_<name> 计数器。name 应以 _total 结尾。
func (m *ModuleMetrics) Counter(name string, labels ...string) *Counter {
	return m.reg.Counter(Name(m.module, name), labels...)
}

// Count 是 Counter(...).Inc() 的快捷写法。
func (m *ModuleMetrics) Count(name string, labels ...string) {
	m.reg.Counter(Name(m.module, name), labels...).Inc()
}

// Add 是 Counter(...).Add(v) 的快捷写法，用于按量累加（字节数、批量条数）。
// v 为负会被底层 Counter 忽略（计数器单调不减）。
func (m *ModuleMetrics) Add(name string, v float64, labels ...string) {
	m.reg.Counter(Name(m.module, name), labels...).Add(v)
}

// Gauge 返回 clover_<module>_<name> 瞬时值。
func (m *ModuleMetrics) Gauge(name string, labels ...string) *Gauge {
	return m.reg.Gauge(Name(m.module, name), labels...)
}

// Histogram 返回 clover_<module>_<name> 分布统计，buckets 为 nil 时用 DefBuckets。
func (m *ModuleMetrics) Histogram(name string, buckets []float64, labels ...string) *Histogram {
	return m.reg.Histogram(Name(m.module, name), buckets, labels...)
}

// Duration 返回耗时分布（DurationBuckets）。name 应以 _duration_seconds 结尾。
func (m *ModuleMetrics) Duration(name string, labels ...string) *Histogram {
	return m.reg.Histogram(Name(m.module, name), DurationBuckets, labels...)
}

// FastDuration 返回短耗时分布（FastDurationBuckets），用于进程内高频路径。
func (m *ModuleMetrics) FastDuration(name string, labels ...string) *Histogram {
	return m.reg.Histogram(Name(m.module, name), FastDurationBuckets, labels...)
}

// Size 返回大小分布（SizeBuckets）。name 应以 _bytes 结尾。
func (m *ModuleMetrics) Size(name string, labels ...string) *Histogram {
	return m.reg.Histogram(Name(m.module, name), SizeBuckets, labels...)
}

// Timer 开始计时，返回一个「停止并记录」的闭包。**耗时自动换算成秒**，符合命名规范。

// defer mm.Timer("handle_duration_seconds", metrics.LabelHandler, "login")()
func (m *ModuleMetrics) Timer(name string, labels ...string) func() {
	h := m.Duration(name, labels...)
	start := time.Now()
	return func() { h.Observe(time.Since(start).Seconds()) }
}

// FastTimer 同 Timer，但用 FastDurationBuckets，适合进程内高频短耗时路径。
func (m *ModuleMetrics) FastTimer(name string, labels ...string) func() {
	h := m.FastDuration(name, labels...)
	start := time.Now()
	return func() { h.Observe(time.Since(start).Seconds()) }
}

// Observe 记录一次带结果状态的操作：同时累加 <name>_total 与 <name>_duration_seconds。
// 这是最常用的「一次调用两个指标」组合写法，err 非 nil 时 status=error。

//	start := time.Now()
//	err := doRPC()
//	mm.Observe("request", start, err, metrics.LabelMethod, "GetPlayer")

// 产出：

// clover_rpc_request_total{method="GetPlayer",status="ok"}
// clover_rpc_request_duration_seconds{method="GetPlayer",status="ok"}
func (m *ModuleMetrics) Observe(name string, start time.Time, err error, labels ...string) {
	status := StatusOK
	if err != nil {
		status = StatusError
	}
	// 复制一份再追加：直接 append 变参切片会就地写入调用方数组，
	// 调用方复用该切片时会读到被污染的 label。
	full := make([]string, 0, len(labels)+2)
	full = append(full, labels...)
	full = append(full, LabelStatus, status)

	m.reg.Counter(Name(m.module, name+"_total"), full...).Inc()
	m.reg.Histogram(Name(m.module, name+"_duration_seconds"), DurationBuckets, full...).
		Observe(time.Since(start).Seconds())
}
