// Package watchdog 进程级「周期巡检 + 告警」看门狗。
//
// 定位：把「自己发现异常并叫人」这件事收敛成一个可复用的运行时原语。
// 它**不采集数据、不定义阈值、不存历史**，只负责四件事：
//
//  1. 按规则周期调用「只读检查函数」（Check）；
//  2. 命中后按冷却窗口抑制重复告警，并按级别记日志 + 打指标；
//  3. 把告警投给 Sink（出口）—— 默认**不接任何外部通道**：告警一律落引擎日志
//     （logAlert，零依赖零网络、Sink 全坏也丢不掉），webhook / IM / 邮件等真实通道
//     由业务实现 Sink 注入，**换出口不改引擎代码**（与 app.RegisterLogBackend 同一思路）；
//     投递走「单 worker + 有界队列 + 满即丢」，外部通道抖动拖不住巡检线程；
//  4. 提供 Snapshot() 供运维面查看「每条规则现在是什么状态」。
//
// # 为什么不落库
//
// 指标经 /metrics 暴露给 Prometheus，历史留存由 TSDB 负责；
// 本包**不做任何持久化**：把指标写进 MySQL/文件会让进程变成监控系统的一部分，
// 且必然与游戏数据抢 IO。需要离线留存时由部署侧拉取 / 转发。
//
// # 硬约束（照抄 master/failover 探测器的成熟形状）
//
//  1. **Check 必须只读、快速、不阻塞。** 每条规则在独立 goroutine 里跑，卡住只影响自己：
//     下一轮 due 时发现仍在跑 → 跳过并计 skipped（不会堆成 goroutine 洪水），
//     但长久卡住等于该规则失效，所以禁止在 Check 里做重活 / 阻塞 IO。
//  2. **禁止持锁遍历 / 全表分配快照。** 历史教训：drain 持锁调 ListConnIDs 全表分配，
//     万级连接时阻塞派发（见 `服务器待做.md`）。Check 里只取「只读快照」再在锁外计算。
//  3. **规则名是有限集合。** 它直接作为指标 label 值与冷却键，
//     禁止把 player_id / conn_id / 时间戳拼进规则名（否则时间序列爆炸）。
//  4. **Sink 必须快速返回。** 引擎已用有界队列保护，但仍要求实现方不阻塞：
//     丢包会计入 clover_watchdog_alerts_dropped_total，运维据此知道「告警没送出去」。
//
// # 用法
//
// 引擎侧（internal/app）已按进程统一拉起并 Install 默认实例，业务只需注册规则：
//
//	watchdog.Default().RegisterFunc("my_rule", func() watchdog.Result {
//	    if backlogTooLarge() {
//	        return watchdog.Critical("待处理积压 %d 条", n)
//	    }
//	    return watchdog.OK
//	})
//
// 需要接入真实告警通道时替换出口（在 bootstrap / mount 里）：
//
//	watchdog.Install(watchdog.New(watchdog.Options{
//	    Sink: watchdog.SinkFunc(func(a watchdog.Alert) { myWebhook(a) }),
//	}))
package watchdog

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/pkg/foundation/logger"
)

// 默认值与上限。
const (
	// DefaultInterval 巡检 tick 粒度，也是规则未指定 Interval 时的缺省值。
	DefaultInterval = 10 * time.Second
	// DefaultCooldown 同一规则的告警冷却：冷却期内不重复告警（但状态继续更新）。
	DefaultCooldown = 5 * time.Minute
	// DefaultQueueSize 告警投递队列容量：满了丢弃并计数，绝不阻塞巡检线程。
	DefaultQueueSize = 64
	// skipLogInterval 同一规则「跳过」日志的最小间隔（防刷屏）。
	skipLogInterval = time.Minute
)

// Level 告警级别。
type Level string

const (
	// LevelOK 巡检正常。
	LevelOK Level = "ok"
	// LevelWarning 需要关注，但尚未影响可用性。
	LevelWarning Level = "warning"
	// LevelCritical 已经影响可用性 / 有数据丢失风险，需人工介入。
	LevelCritical Level = "critical"
	// LevelRecovered 从告警恢复到正常（只出现在投给 Sink 的 Alert 上，不作为 Check 返回值）。
	LevelRecovered Level = "recovered"
)

// known 判断是否是 Check 允许返回的级别（recovered 只能由看门狗自己产生）。
func (l Level) known() bool {
	switch l {
	case LevelOK, LevelWarning, LevelCritical:
		return true
	default:
		return false
	}
}

// Result 一次巡检的结论。
type Result struct {
	// Level 结论级别，取 LevelOK / LevelWarning / LevelCritical。
	Level Level
	// Message 面向运维的一句人话（会进日志、指标 label 之外的告警体、/watchdog 快照）。
	// 正常时留空即可。
	Message string
}

// OK 表示本轮巡检正常。
//
// 用变量而不是函数：正常是绝大多数轮次的结果，写成 `return watchdog.OK` 比
// `return watchdog.Result{}` 更自解释，也避免调用方漏填 Level 造成「零值即 normal」的歧义。
var OK = Result{Level: LevelOK}

// Warn 构造一个 warning 结论。
func Warn(format string, a ...any) Result {
	return Result{Level: LevelWarning, Message: fmt.Sprintf(format, a...)}
}

// Critical 构造一个 critical 结论。
func Critical(format string, a ...any) Result {
	return Result{Level: LevelCritical, Message: fmt.Sprintf(format, a...)}
}

// Check 巡检函数：返回 OK 表示正常，返回 Warn / Critical 表示命中。
//
// 必须是只读、快速、不阻塞的（见包说明的硬约束 1、2）。
// 同一个 Check 不会被并发调用（上一轮没跑完时下一轮直接跳过）。
type Check func() Result

// Rule 一条巡检规则。
type Rule struct {
	// Name 规则名，必须非空且唯一。它同时是：日志前缀、指标 label 值、告警冷却键。
	// 禁止包含玩家 / 连接等动态值。
	Name string
	// Interval 该规则的巡检周期；0 表示用 Options.Interval。
	Interval time.Duration
	// Cooldown 告警冷却；0 表示用 Options.Cooldown。
	Cooldown time.Duration
	// MinConsecutive 连续命中多少次才告警（去抖动）；0 视为 1（命中即告警）。
	// 典型用法：CPU / 延迟这类抖动指标设 3，避免瞬时毛刺就叫人。
	MinConsecutive int
	// Check 巡检函数。
	Check Check
}

// Options 看门狗配置。零值即默认（Interval=10s / Cooldown=5m / QueueSize=64 / noopSink）。
type Options struct {
	// Interval 巡检 tick 粒度，也是规则 Interval 的缺省值。
	Interval time.Duration
	// Cooldown 规则 Cooldown 的缺省值。
	Cooldown time.Duration
	// QueueSize 告警投递队列容量（<=0 取 DefaultQueueSize）。
	QueueSize int
	// Sink 告警出口；nil 表示用内置 noopSink（空实现，日志留痕由 Watcher 统一负责）。
	Sink Sink
}

// normalize 把零值 / 非法值回落到默认。
func (o Options) normalize() Options {
	if o.Interval <= 0 {
		o.Interval = DefaultInterval
	}
	if o.Cooldown <= 0 {
		o.Cooldown = DefaultCooldown
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	if o.Sink == nil {
		o.Sink = noopSink{}
	}
	return o
}

// RuleStatus 规则的运行状态快照（供运维面展示，不参与调度）。
type RuleStatus struct {
	Name           string    `json:"name"`
	Interval       string    `json:"interval"`
	Cooldown       string    `json:"cooldown"`
	MinConsecutive int       `json:"min_consecutive"`
	Running        bool      `json:"running"`
	Firing         bool      `json:"firing"`
	Consecutive    int       `json:"consecutive"`
	LastLevel      Level     `json:"last_level"`
	LastMessage    string    `json:"last_message,omitempty"`
	LastRun        time.Time `json:"last_run,omitzero"`
	LastAlertAt    time.Time `json:"last_alert_at,omitzero"`
}

// ruleState 规则 + 运行期状态。
//
// 除 Check 之外的所有字段都在 Watcher.mu 保护下读写（Check 在锁外执行）。
type ruleState struct {
	rule           Rule
	interval       time.Duration
	cooldown       time.Duration
	minConsecutive int

	nextAt time.Time

	running     bool
	firing      bool
	consecutive int
	lastRun     time.Time
	lastLevel   Level
	lastMessage string
	lastAlertAt time.Time
}

// status 生成快照（调用方持锁）。
func (rs *ruleState) status() RuleStatus {
	return RuleStatus{
		Name:           rs.rule.Name,
		Interval:       rs.interval.String(),
		Cooldown:       rs.cooldown.String(),
		MinConsecutive: rs.minConsecutive,
		Running:        rs.running,
		Firing:         rs.firing,
		Consecutive:    rs.consecutive,
		LastLevel:      rs.lastLevel,
		LastMessage:    rs.lastMessage,
		LastRun:        rs.lastRun,
		LastAlertAt:    rs.lastAlertAt,
	}
}

// Watcher 看门狗实例。并发安全；Start / Stop 幂等。
type Watcher struct {
	opts Options
	sink Sink

	mu    sync.Mutex
	rules []*ruleState

	started atomic.Bool
	// dropped 因投递队列满被丢弃的告警数（同时计入 metrics，这里保留一份便于运维面直接看）。
	dropped atomic.Uint64

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	loopDone  chan struct{}
	sinkCh    chan Alert
	sinkDone  chan struct{}

	// now 时钟注入点（测试用）；默认 time.Now。
	now func() time.Time
	// throttle 日志降频器（跳过 / 丢弃两类日志共用）。
	throttle throttledLog
}

// New 创建看门狗。零值 Options 即默认配置。
func New(opts Options) *Watcher {
	o := opts.normalize()
	return &Watcher{
		opts:     o,
		sink:     o.Sink,
		stopCh:   make(chan struct{}),
		loopDone: make(chan struct{}),
		sinkCh:   make(chan Alert, o.QueueSize),
		sinkDone: make(chan struct{}),
		now:      time.Now,
		throttle: newThrottledLog(skipLogInterval),
	}
}

// Register 注册一条巡检规则。规则名为空 / 检查函数为空 / 重名都会返回错误并留痕。
func (w *Watcher) Register(r Rule) error {
	if w == nil {
		return errors.New("watchdog: watcher 未初始化")
	}
	name := strings.TrimSpace(r.Name)
	if name == "" {
		logger.Errorf("watchdog: register rejected: empty rule name")
		return errors.New("watchdog: 规则名不能为空")
	}
	if r.Check == nil {
		logger.Errorf("watchdog: register rejected: rule %s has no check function", name)
		return fmt.Errorf("watchdog: 规则 %q 未提供检查函数", name)
	}
	rs := &ruleState{
		rule:           r,
		interval:       r.Interval,
		cooldown:       r.Cooldown,
		minConsecutive: r.MinConsecutive,
	}
	if rs.interval <= 0 {
		rs.interval = w.opts.Interval
	}
	if rs.cooldown <= 0 {
		rs.cooldown = w.opts.Cooldown
	}
	if rs.minConsecutive <= 0 {
		rs.minConsecutive = 1
	}
	rs.rule.Name = name

	w.mu.Lock()
	for _, e := range w.rules {
		if e.rule.Name == name {
			w.mu.Unlock()
			logger.Errorf("watchdog: register rejected: duplicate rule %s", name)
			return fmt.Errorf("watchdog: 规则 %q 已注册", name)
		}
	}
	// 首次执行推后一个周期：注册往往发生在启动装配期，立刻打一轮会制造启动噪声。
	rs.nextAt = w.now().Add(rs.interval)
	w.rules = append(w.rules, rs)
	n := len(w.rules)
	w.mu.Unlock()

	metricRules(n)
	logger.Infof("watchdog: rule registered: %s (interval=%s cooldown=%s min_consecutive=%d)",
		name, rs.interval, rs.cooldown, rs.minConsecutive)
	return nil
}

// RegisterFunc 是 Register 的简化形式（用全局默认周期与冷却，命中即告警）。
func (w *Watcher) RegisterFunc(name string, check Check) error {
	return w.Register(Rule{Name: name, Check: check})
}

// Dropped 返回因投递队列满被丢弃的告警数。
//
// 非零意味着「有告警没送出去」——出口通道（webhook / IM）比告警产生速度慢，
// 需要人工确认，它不是「正常背压」。
func (w *Watcher) Dropped() uint64 {
	if w == nil {
		return 0
	}
	return w.dropped.Load()
}

// Rules 返回已注册规则数。
func (w *Watcher) Rules() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.rules)
}

// Snapshot 返回全部规则的状态快照（按规则名排序，便于稳定展示与 diff）。
func (w *Watcher) Snapshot() []RuleStatus {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	out := make([]RuleStatus, 0, len(w.rules))
	for _, rs := range w.rules {
		out = append(out, rs.status())
	}
	w.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Start 启动巡检循环与告警投递 worker（幂等，不会重复起协程）。
func (w *Watcher) Start() {
	if w == nil {
		return
	}
	w.startOnce.Do(func() {
		w.started.Store(true)
		go w.loop()
		go w.sinkLoop()
		logger.Infof("watchdog: started (interval=%s rules=%d queue=%d)", w.opts.Interval, w.Rules(), w.opts.QueueSize)
	})
}

// Stop 停止看门狗：先停巡检，再把队列里已排队的告警投完，最后返回（幂等）。
// 未 Start 时直接返回；必须在 Start 之后调用（否则无协程可退）。
func (w *Watcher) Stop() {
	if w == nil {
		return
	}
	// started 判定必须在 stopOnce **之外**：放进 Do 体内时，一次「未 Start 就 Stop」
	// 会把 once 消耗掉，之后再 Start，Stop 不再执行 close(stopCh)，
	// 巡检与 sink 协程永远退不出（goroutine 泄漏）。
	if !w.started.Load() {
		return
	}
	w.stopOnce.Do(func() {
		close(w.stopCh)
		<-w.loopDone
		<-w.sinkDone
		logger.Infof("watchdog: stopped")
	})
}

// loop 巡检主循环：只按 tick 判定「哪些规则到点了」，不在这里执行 Check
// （Check 可能慢，放在独立 goroutine 里，慢规则不拖累其它规则）。
func (w *Watcher) loop() {
	defer close(w.loopDone)
	t := time.NewTicker(w.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-t.C:
			w.runDue(w.now())
		}
	}
}

// runDue 执行本轮到点的规则（导出为同包方法，便于测试直接驱动，不等真实定时器）。
func (w *Watcher) runDue(now time.Time) {
	var jobs []*ruleState
	var skipped []string

	w.mu.Lock()
	for _, rs := range w.rules {
		if now.Before(rs.nextAt) {
			continue
		}
		rs.nextAt = now.Add(rs.interval)
		if rs.running {
			// 上一轮还没跑完：跳过本轮（不再起 goroutine，避免慢检查堆成 goroutine 洪水）。
			skipped = append(skipped, rs.rule.Name)
			continue
		}
		rs.running = true
		jobs = append(jobs, rs)
	}
	w.mu.Unlock()

	for _, name := range skipped {
		metricCheck(name, statusSkipped)
		if w.throttle.allow(name, now) {
			logger.Warnf("watchdog: rule %s skipped this round: previous check still running", name)
		}
	}
	for _, rs := range jobs {
		go w.runCheck(rs, now)
	}
}

// runCheck 执行一次 Check 并落状态。
func (w *Watcher) runCheck(rs *ruleState, at time.Time) {
	start := w.now()
	res, panicked := safeCheck(rs.rule.Check)
	metricCheckDuration(rs.rule.Name, w.now().Sub(start))

	if panicked != nil {
		// Check 自己 panic 属引擎 / 业务 bug：按 critical 报出来（有冷却，不会刷屏），
		// 并计 panic ——「静默失灵的巡检规则」比没有规则更危险。
		metricCheck(rs.rule.Name, statusPanic)
		logger.Errorf("watchdog: rule %s check panicked: %v", rs.rule.Name, panicked)
		res = Critical("巡检函数 panic: %v", panicked)
	} else if res.Level.known() {
		if res.Level == LevelOK {
			metricCheck(rs.rule.Name, statusOK)
		} else {
			metricCheck(rs.rule.Name, statusFiring)
		}
	} else {
		metricCheck(rs.rule.Name, statusPanic)
		logger.Errorf("watchdog: rule %s returned invalid level %q", rs.rule.Name, res.Level)
		res = Critical("巡检函数返回非法级别 %q", res.Level)
	}

	w.applyResult(rs, res, at)
}

// applyResult 更新规则状态，并在需要时产生一条告警。
func (w *Watcher) applyResult(rs *ruleState, res Result, at time.Time) {
	var alert *Alert

	w.mu.Lock()
	rs.running = false
	rs.lastRun = at
	prevLevel := rs.lastLevel
	prevMsg := rs.lastMessage
	rs.lastLevel = res.Level
	rs.lastMessage = res.Message

	switch {
	case res.Level == LevelOK:
		rs.consecutive = 0
		// 只在「确实从告警态恢复」时发恢复通知：正常状态的轮次保持安静。
		if rs.firing {
			rs.firing = false
			msg := "已恢复正常"
			if prevMsg != "" {
				msg = fmt.Sprintf("已恢复正常（此前 %s：%s）", prevLevel, prevMsg)
			}
			rs.lastAlertAt = at
			alert = &Alert{Rule: rs.rule.Name, Level: LevelRecovered, Message: msg, At: at}
		}
	default:
		rs.firing = true
		rs.consecutive++
		// 连续命中 + 冷却双重门限：前者防毛刺，后者防轰炸。
		if rs.consecutive >= rs.minConsecutive && at.Sub(rs.lastAlertAt) >= rs.cooldown {
			rs.lastAlertAt = at
			alert = &Alert{
				Rule:        rs.rule.Name,
				Level:       res.Level,
				Message:     res.Message,
				At:          at,
				Consecutive: rs.consecutive,
			}
		}
	}
	w.mu.Unlock()

	if alert == nil {
		return
	}
	// 指标记在「决定告警」这一步：投递不出去的会额外计入 alerts_dropped_total，
	// 两者对照即可看出「要不要告警」与「有没有送出去」。
	metricAlert(alert.Rule, alert.Level)
	w.logAlert(*alert)
	w.dispatch(*alert)
}

// logAlert 按级别把告警写进引擎日志（日志是默认出口的兜底：即使 Sink 全坏，日志里也有）。
func (w *Watcher) logAlert(a Alert) {
	switch a.Level {
	case LevelCritical:
		logger.Errorf("watchdog: ALERT [%s] %s (consecutive=%d)", a.Rule, a.Message, a.Consecutive)
	case LevelRecovered:
		logger.Infof("watchdog: RECOVERED [%s] %s", a.Rule, a.Message)
	default:
		logger.Warnf("watchdog: ALERT [%s] %s (consecutive=%d)", a.Rule, a.Message, a.Consecutive)
	}
}

// safeCheck 执行 Check 并捕获 panic。
func safeCheck(fn Check) (res Result, panicked any) {
	defer func() {
		if r := recover(); r != nil {
			panicked = r
		}
	}()
	return fn(), nil
}

// 进程级默认实例。
var (
	defaultMu sync.Mutex
	defaultW  *Watcher
)

// Default 返回进程级默认看门狗（懒创建，默认配置）。
// 业务模块用它注册规则即可，无需自己持有实例。
func Default() *Watcher {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultW == nil {
		defaultW = New(Options{})
	}
	return defaultW
}

// Install 用给定实例替换进程级默认实例（引擎装配期调用，业务经 Default() 拿到它）。
//
// 必须先 Install 再注册规则：引擎在 runApp 里先 Install 再进入各角色的装配回调，
// 因此业务在 RegisterMount / bootstrap 里注册的规则一定落在同一个实例上。
// 若替换时旧实例上已有规则，说明装配顺序反了，这里显式报错留痕。
func Install(w *Watcher) {
	if w == nil {
		return
	}
	defaultMu.Lock()
	old := defaultW
	defaultW = w
	defaultMu.Unlock()
	if old != nil && old.Rules() > 0 {
		logger.Errorf("watchdog: install replaced an instance with %d registered rules; those rules are dropped (register after install)", old.Rules())
	}
}

// throttledLog 同一 key 的日志最小间隔（防刷屏）。
//
// 高频路径上的失败必须留痕、又不能每秒几十条，所以统一「降频」而不是「静默」。
type throttledLog struct {
	mu   sync.Mutex
	min  time.Duration
	last map[string]time.Time
}

func newThrottledLog(min time.Duration) throttledLog {
	return throttledLog{min: min, last: make(map[string]time.Time)}
}

// allow 判断 key 的日志现在是否可以打（并推进计时）。
func (t *throttledLog) allow(key string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if last, ok := t.last[key]; ok && now.Sub(last) < t.min {
		return false
	}
	t.last[key] = now
	return true
}
