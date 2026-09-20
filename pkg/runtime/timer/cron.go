package timer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/safe"
)

// cronSchedule 解析后的 crontab 表达式（5 字段：分 时 日 月 周）。
type cronSchedule struct {
	minute []bool // [0..59]
	hour   []bool // [0..23]
	dom    []bool // [1..31]
	month  []bool // [1..12]
	dow    []bool // [0..6]，0=周日
	domAll bool   // 日字段为 *
	dowAll bool   // 周字段为 *
}

// parseCron 解析 5 字段 crontab 表达式："分 时 日 月 周"。
// 字段支持 *、a、a-b、a,b,c、*/n、a-b/n。
func parseCron(spec string) (*cronSchedule, error) {
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: need 5 fields, got %d in %q", len(fields), spec)
	}
	min, _, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("cron minute: %w", err)
	}
	hr, _, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("cron hour: %w", err)
	}
	dom, domAll, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("cron dom: %w", err)
	}
	mon, _, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("cron month: %w", err)
	}
	// 周字段兼容标准 Vixie cron：接受 0-7，其中 0 与 7 均表示周日。
	dow, dowAll, err := parseField(fields[4], 0, 7)
	if err != nil {
		return nil, fmt.Errorf("cron dow: %w", err)
	}
	dow = foldSundayAlias(dow)
	return &cronSchedule{
		minute: min, hour: hr, dom: dom, month: mon, dow: dow,
		domAll: domAll, dowAll: dowAll,
	}, nil
}

// foldSundayAlias 把周字段中的 7（周日别名）折叠到 0，并裁剪回 [0..6] 位图，
// 以便 Next 用 time.Weekday()（0=周日..6=周六）直接索引。
func foldSundayAlias(dow []bool) []bool {
	if len(dow) > 7 {
		if dow[7] {
			dow[0] = true
		}
		dow = dow[:7]
	}
	return dow
}

// parseField 解析单个字段，返回允许值位图与是否全量（*）。
func parseField(spec string, min, max int) ([]bool, bool, error) {
	if spec == "" {
		return nil, false, fmt.Errorf("empty field")
	}
	if spec == "*" {
		allowed := make([]bool, max+1)
		for i := min; i <= max; i++ {
			allowed[i] = true
		}
		return allowed, true, nil
	}
	allowed := make([]bool, max+1)
	for _, token := range strings.Split(spec, ",") {
		lo, hi, step, err := parseRange(token, min, max)
		if err != nil {
			return nil, false, err
		}
		for i := lo; i <= hi; i += step {
			allowed[i] = true
		}
	}
	return allowed, false, nil
}

// parseRange 解析 "*/n"、"a"、"a-b"、"a-b/n" 为 [lo,hi] 与步长 step。
func parseRange(token string, min, max int) (lo, hi, step int, err error) {
	rangePart := token
	step = 1
	if idx := strings.Index(token, "/"); idx >= 0 {
		rangePart = token[:idx]
		n, e := strconv.Atoi(token[idx+1:])
		if e != nil {
			return 0, 0, 0, fmt.Errorf("bad step in %q", token)
		}
		if n <= 0 {
			return 0, 0, 0, fmt.Errorf("step must be >0 in %q", token)
		}
		step = n
	}
	switch {
	case rangePart == "*" || rangePart == "":
		lo, hi = min, max
	case strings.Contains(rangePart, "-"):
		parts := strings.SplitN(rangePart, "-", 2)
		a, e1 := strconv.Atoi(parts[0])
		b, e2 := strconv.Atoi(parts[1])
		if e1 != nil || e2 != nil {
			return 0, 0, 0, fmt.Errorf("bad range %q", token)
		}
		lo, hi = a, b
	default:
		v, e := strconv.Atoi(rangePart)
		if e != nil {
			return 0, 0, 0, fmt.Errorf("bad value %q", token)
		}
		lo, hi = v, v
	}
	if lo < min || hi > max || lo > hi {
		return 0, 0, 0, fmt.Errorf("out of range [%d,%d] in %q", min, max, token)
	}
	return lo, hi, step, nil
}

// Next 计算 >= after 的下一个匹配时间（分钟粒度）。找不到返回零值。
//
// 起点取 after 所在的整分钟本身：若该分钟已晚于 after 不可能（截断只可能 <= after），
// 因此只有「after 恰好落在整分钟」时才直接返回该分钟，满足文档承诺的 `>= after` 语义
// （旧实现无条件 +1 分钟，会把恰好匹配的 after 跳过，使「下次运行时间」整体偏后一个周期）。
func (c *cronSchedule) Next(after time.Time) time.Time {
	loc := after.Location()
	t := after.Truncate(time.Minute)
	if t.Before(after) {
		t = t.Add(time.Minute)
	}
	for i := 0; i < 5000; i++ {
		if !c.month[t.Month()] {
			y, m, _ := t.Date()
			nm, ny := m+1, y
			if nm > 12 {
				nm, ny = 1, y+1
			}
			t = time.Date(ny, nm, 1, 0, 0, 0, 0, loc)
			continue
		}
		domOK := c.dom[t.Day()]
		dowOK := c.dow[int(t.Weekday())]
		var dayOK bool
		switch {
		case c.domAll && c.dowAll:
			dayOK = true
		case c.domAll:
			dayOK = dowOK
		case c.dowAll:
			dayOK = domOK
		default:
			dayOK = domOK || dowOK
		}
		if !dayOK {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1)
			continue
		}
		if !c.hour[t.Hour()] {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc).Add(time.Hour)
			continue
		}
		if !c.minute[t.Minute()] {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{}
}

// cronTimer 周期性 crontab 任务的句柄：内部用一次性任务链自我重排。
type cronTimer struct {
	s       *Scheduler
	id      int64
	name    string
	scope   string
	mu      sync.Mutex
	cur     Timer
	stopped bool
}

func (c *cronTimer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	if c.cur != nil {
		c.cur.Stop()
		c.cur = nil
	}
	// 回收 CronName 写入的 taskReg/cronSpec 条目：内部是一次性匿名 After 任务，
	// 其 stop(id) 因 name=="" 不会清理本 cron 的注册项，不显式回收即永久残留闭包。
	c.s.unregisterCronReg(c.scope, c.name, c.id)
}

func (c *cronTimer) Active() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.stopped && c.cur != nil && c.cur.Active()
}

func (c *cronTimer) Name() string { return c.name }

// NextCron 计算从 from 开始的下一个匹配 cron 表达式 spec 的时间（分钟粒度）。
// 供调度类组件（如 job.Manager）预估「下次运行时间」使用；解析失败返回 error 与零值。
func NextCron(spec string, from time.Time) (time.Time, error) {
	sched, err := parseCron(spec)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(from), nil
}

// newCronTimer 构造一个 crontab 周期任务（5 字段：分 时 日 月 周，最小粒度分钟）。
// 解析失败返回 error；内部用一次性任务链自我重排，每次触发后自动排定下一次。
func (s *Scheduler) newCronTimer(spec string, task Task) (*cronTimer, error) {
	sched, err := parseCron(spec)
	if err != nil {
		return nil, err
	}
	ct := &cronTimer{s: s}
	var arm func()
	arm = func() {
		ct.mu.Lock()
		if ct.stopped {
			ct.mu.Unlock()
			return
		}
		ct.mu.Unlock()

		next := sched.Next(ct.s.now())
		if next.IsZero() {
			// 表达式在可预见范围内永不匹配（如 "0 0 31 2 *"）：停止重排。
			// 否则 time.Until(零值) 为巨大负数被钳为 0，任务会「立即触发→重排→
			// 再立即触发」无限风暴。
			logger.Warnf("timer.cron: spec has no matching time, stop rearming")
			return
		}
		d := time.Until(next)
		if d < 0 {
			d = 0
		}
		h := s.After(d, func() {
			safe.GoSafe(task)
			arm() // 排定下一次
		})

		// 检测 scheduler 是否已 Close：closed 的 After 返回空 timerHandle（Active 为 false）。
		// 不检测会导致 ct.cur 指向死 handle，cron 静默失效且无日志。
		if !h.Active() {
			logger.Warnf("timer.cron: scheduler closed, cron arm stopping (spec=%q)", spec)
			ct.mu.Lock()
			ct.stopped = true
			ct.mu.Unlock()
			h.Stop()
			return
		}

		ct.mu.Lock()
		if ct.stopped {
			ct.mu.Unlock()
			h.Stop()
			return
		}
		ct.cur = h
		ct.mu.Unlock()
	}
	arm()
	return ct, nil
}

// Cron 注册类 crontab 周期任务（匿名）。解析失败返回 error；成功返回可取消的 Timer。
func (s *Scheduler) Cron(spec string, task Task) (Timer, error) {
	return s.newCronTimer(spec, task)
}
