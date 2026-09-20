package watchdog

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingSink 记录收到的告警。
type recordingSink struct {
	mu     sync.Mutex
	alerts []Alert
}

func (s *recordingSink) Notify(a Alert) {
	s.mu.Lock()
	s.alerts = append(s.alerts, a)
	s.mu.Unlock()
}

func (s *recordingSink) snapshot() []Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Alert(nil), s.alerts...)
}

// takeWithout 返回全部告警中「规则名 != rule」的部分，并把整体记录替换为该部分
// （即从记录中移除命中的哨兵）。found 报告是否出现过哨兵。
func (s *recordingSink) takeWithout(rule string) (out []Alert, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out = make([]Alert, 0, len(s.alerts))
	for _, a := range s.alerts {
		if a.Rule == rule {
			found = true
			continue
		}
		out = append(out, a)
	}
	if found {
		s.alerts = out
	}
	return out, found
}

// fakeClock 可注入时钟：让「连续轮次 / 冷却窗口」的断言不依赖真实时间。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

// newTestWatcher 构造一个「不自动跑巡检循环、但会消费告警队列」的看门狗。
// 巡检由测试直接驱动（runDue / runCheck），保证结果确定。
func newTestWatcher(t *testing.T, opts Options) (*Watcher, *recordingSink, *fakeClock) {
	t.Helper()
	sink := &recordingSink{}
	opts.Sink = sink
	w := New(opts)
	clock := &fakeClock{t: time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)}
	w.now = clock.now

	go w.sinkLoop()
	t.Cleanup(func() {
		select {
		case <-w.stopCh:
		default:
			close(w.stopCh)
		}
		<-w.sinkDone
	})
	return w, sink, clock
}

// ruleOf 取出已注册规则的状态（同包测试直接访问内部状态，避免为测试加导出 API）。
func ruleOf(t *testing.T, w *Watcher, name string) *ruleState {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, rs := range w.rules {
		if rs.rule.Name == name {
			return rs
		}
	}
	t.Fatalf("规则 %s 未注册", name)
	return nil
}

// waitAlerts 等投递 worker 收满 n 条告警（投递本身是异步的）。
func waitAlerts(t *testing.T, s *recordingSink, n int) []Alert {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := s.snapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 %d 条告警超时，实际收到 %d 条", n, len(got))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// drainAlerts 把告警投递队列排空到「当前时刻」的确定状态，返回已投递告警（不含哨兵）。
//
// 断言「此后不应再有告警」时用它替代固定 time.Sleep：它入队一条哨兵并等它被投递，
// 队列 FIFO 且由单 worker 串行消费，故任何在此前入队的（误）告警都必然已投递。
// 固定 sleep 的负向断言在慢机/高负载下会因「投递耗时 > sleep」而假绿。
func drainAlerts(t *testing.T, w *Watcher, s *recordingSink) []Alert {
	t.Helper()
	const sentinel = "__drain_sentinel__"
	w.dispatch(Alert{Rule: sentinel})
	deadline := time.Now().Add(3 * time.Second)
	for {
		// 哨兵到达后从 sink 记录里剔除（仅计数用），不污染后续断言。
		if got, found := s.takeWithout(sentinel); found {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待哨兵告警投递超时")
		}
		time.Sleep(time.Millisecond)
	}
}

// waitFor 轮询等待条件成立。
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待条件超时：%s", desc)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWatcher_FireCooldownRecovery(t *testing.T) {
	w, sink, clock := newTestWatcher(t, Options{Cooldown: time.Minute})

	cur := LevelWarning
	if err := w.RegisterFunc("r1", func() Result {
		return Result{Level: cur, Message: "boom"}
	}); err != nil {
		t.Fatalf("注册规则失败: %v", err)
	}
	rs := ruleOf(t, w, "r1")

	// 首次命中：立即告警（无历史告警，冷却不拦第一次）。
	base := clock.now()
	w.runCheck(rs, clock.now())
	got := waitAlerts(t, sink, 1)
	if got[0].Rule != "r1" || got[0].Level != LevelWarning {
		t.Fatalf("首条告警不符：%+v", got[0])
	}

	// 冷却期内继续命中：状态更新，但不重复告警。
	clock.set(base.Add(30 * time.Second))
	w.runCheck(rs, clock.now())
	if n := len(drainAlerts(t, w, sink)); n != 1 {
		t.Fatalf("冷却期内不应重复告警，实际 %d 条", n)
	}

	// 冷却过后仍命中：再次告警，并带上连续命中轮数。
	clock.set(base.Add(61 * time.Second))
	w.runCheck(rs, clock.now())
	got = waitAlerts(t, sink, 2)
	if got[1].Consecutive != 3 {
		t.Fatalf("连续命中次数应为 3，实际 %d", got[1].Consecutive)
	}

	// 恢复正常：投一条 recovered；此后的正常轮次必须保持安静。
	cur = LevelOK
	clock.set(base.Add(62 * time.Second))
	w.runCheck(rs, clock.now())
	got = waitAlerts(t, sink, 3)
	if got[2].Level != LevelRecovered {
		t.Fatalf("恢复通知级别应为 recovered，实际 %s", got[2].Level)
	}
	clock.set(base.Add(63 * time.Second))
	w.runCheck(rs, clock.now())
	if n := len(drainAlerts(t, w, sink)); n != 3 {
		t.Fatalf("恢复后正常轮次不应再告警，实际 %d 条", n)
	}
}

func TestWatcher_MinConsecutiveDebounces(t *testing.T) {
	w, sink, clock := newTestWatcher(t, Options{})
	if err := w.Register(Rule{
		Name:           "debounce",
		MinConsecutive: 3,
		Check:          func() Result { return Warn("jitter") },
	}); err != nil {
		t.Fatalf("注册规则失败: %v", err)
	}
	rs := ruleOf(t, w, "debounce")

	w.runCheck(rs, clock.now())
	w.runCheck(rs, clock.now().Add(time.Second))
	if n := len(drainAlerts(t, w, sink)); n != 0 {
		t.Fatalf("未达连续门限不应告警，实际 %d 条", n)
	}

	w.runCheck(rs, clock.now().Add(time.Second))
	got := waitAlerts(t, sink, 1)
	if got[0].Consecutive != 3 {
		t.Fatalf("达到门限时应带连续次数 3，实际 %d", got[0].Consecutive)
	}
}

func TestWatcher_CheckPanicBecomesCritical(t *testing.T) {
	w, sink, clock := newTestWatcher(t, Options{})
	if err := w.RegisterFunc("boom", func() Result { panic("kaboom") }); err != nil {
		t.Fatalf("注册规则失败: %v", err)
	}
	rs := ruleOf(t, w, "boom")

	w.runCheck(rs, clock.now())
	got := waitAlerts(t, sink, 1)
	if got[0].Level != LevelCritical || !strings.Contains(got[0].Message, "panic") {
		t.Fatalf("巡检 panic 应按 critical 报出，实际 %+v", got[0])
	}
}

func TestWatcher_InvalidLevelBecomesCritical(t *testing.T) {
	w, sink, clock := newTestWatcher(t, Options{})
	if err := w.RegisterFunc("bad-level", func() Result { return Result{Level: "nonsense"} }); err != nil {
		t.Fatalf("注册规则失败: %v", err)
	}
	rs := ruleOf(t, w, "bad-level")

	w.runCheck(rs, clock.now())
	got := waitAlerts(t, sink, 1)
	if got[0].Level != LevelCritical {
		t.Fatalf("非法级别应按 critical 处理，实际 %s", got[0].Level)
	}
}

func TestWatcher_SkipsRuleStillRunning(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})

	w, _, clock := newTestWatcher(t, Options{Interval: time.Second})
	if err := w.RegisterFunc("slow", func() Result {
		calls.Add(1)
		<-release
		return OK
	}); err != nil {
		t.Fatalf("注册规则失败: %v", err)
	}

	base := clock.now()
	// 注册时首轮被推后一个周期，这里推进时钟让它到点。
	clock.set(base.Add(2 * time.Second))
	w.runDue(clock.now())
	waitFor(t, "第一条规则开始执行", func() bool { return calls.Load() == 1 })

	// 上一轮仍在跑：本轮跳过，不再起 goroutine（慢检查不该堆成 goroutine 洪水）。
	clock.set(base.Add(3 * time.Second))
	w.runDue(clock.now())
	if got := calls.Load(); got != 1 {
		t.Fatalf("上一轮未结束时不应重复执行，实际 %d 次", got)
	}
	snap := w.Snapshot()
	if len(snap) != 1 || !snap[0].Running {
		t.Fatalf("快照应显示规则正在运行：%+v", snap)
	}

	close(release)
	waitFor(t, "规则执行结束", func() bool {
		s := w.Snapshot()
		return len(s) == 1 && !s[0].Running && s[0].LastLevel == LevelOK
	})
}

func TestWatcher_QueueFullDropsAlerts(t *testing.T) {
	// 不启动消费 worker：队列容量 1，投 3 条 ⇒ 丢 2 条。
	w := New(Options{QueueSize: 1, Sink: SinkFunc(func(Alert) {})})
	for i := 0; i < 3; i++ {
		w.dispatch(Alert{Rule: "r", Level: LevelWarning})
	}
	if got := w.Dropped(); got != 2 {
		t.Fatalf("队列容量 1 投入 3 条应丢弃 2 条，实际 %d 条", got)
	}
}

func TestWatcher_StopFlushesQueuedAlerts(t *testing.T) {
	sink := &recordingSink{}
	w := New(Options{Sink: sink, QueueSize: 8})
	w.Start()
	w.dispatch(Alert{Rule: "r", Level: LevelCritical, Message: "x"})
	w.dispatch(Alert{Rule: "r", Level: LevelWarning, Message: "y"})
	w.Stop()

	if got := len(sink.snapshot()); got != 2 {
		t.Fatalf("Stop 应把队列里的告警投完再退出，实际投出 %d 条", got)
	}
	// Stop 幂等。
	w.Stop()
}

func TestWatcher_SinkPanicDoesNotKillWatcher(t *testing.T) {
	var delivered atomic.Int64
	sink := SinkFunc(func(Alert) { delivered.Add(1) })
	w := New(Options{QueueSize: 4, Sink: SinkFunc(func(a Alert) {
		if a.Message == "boom" {
			panic("sink 崩了")
		}
		sink.Notify(a)
	})})
	w.Start()
	w.dispatch(Alert{Rule: "r", Level: LevelWarning, Message: "boom"})
	w.dispatch(Alert{Rule: "r", Level: LevelWarning, Message: "ok"})
	w.Stop()

	if got := delivered.Load(); got != 1 {
		t.Fatalf("Sink panic 后仍应继续投递后续告警，实际成功 %d 条", got)
	}
}

func TestWatcher_RegisterRejectsBadRules(t *testing.T) {
	w := New(Options{})

	if err := w.Register(Rule{Check: func() Result { return OK }}); err == nil {
		t.Fatal("空规则名应被拒绝")
	}
	if err := w.Register(Rule{Name: "no-check"}); err == nil {
		t.Fatal("缺少检查函数应被拒绝")
	}
	if err := w.RegisterFunc("dup", func() Result { return OK }); err != nil {
		t.Fatalf("首次注册应成功: %v", err)
	}
	if err := w.RegisterFunc("dup", func() Result { return OK }); err == nil {
		t.Fatal("重名规则应被拒绝")
	}
	if got := w.Rules(); got != 1 {
		t.Fatalf("已注册规则数应为 1，实际 %d", got)
	}
}

func TestWatcher_SnapshotDefaults(t *testing.T) {
	w, _, _ := newTestWatcher(t, Options{})
	if err := w.RegisterFunc("r", func() Result { return OK }); err != nil {
		t.Fatalf("注册规则失败: %v", err)
	}
	snap := w.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("快照应有 1 条规则，实际 %d", len(snap))
	}
	if snap[0].Interval != DefaultInterval.String() {
		t.Fatalf("规则未指定周期时应回落到默认 %s，实际 %s", DefaultInterval, snap[0].Interval)
	}
	if snap[0].Cooldown != DefaultCooldown.String() {
		t.Fatalf("规则未指定冷却时应回落到默认 %s，实际 %s", DefaultCooldown, snap[0].Cooldown)
	}
	if snap[0].MinConsecutive != 1 {
		t.Fatalf("未指定连续门限时应为 1，实际 %d", snap[0].MinConsecutive)
	}
	if snap[0].LastLevel != "" {
		t.Fatalf("尚未巡检时不应有结论，实际 %q", snap[0].LastLevel)
	}
}
