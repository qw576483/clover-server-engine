package app

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/event"
	apptypes "github.com/qw576483/clover-server-engine/pkg/app/types"
	"github.com/qw576483/clover-server-engine/pkg/runtime/timer"
	"github.com/qw576483/clover-server-engine/pkg/shared/timeutil"
)

// Role 进程角色：业务挂载时用它声明"这段逻辑挂在哪个角色上"。
//
// 四个角色是四个独立进程（`server_type` 决定），各自的宿主类型不同：
//
//	RoleGame   → *Game        逻辑服：唯一接收客户端（经网关）的角色
//	RoleMaster → *MasterGame  协调服：只收 game 转发，不接客户端
//	RoleLog    → *LogGame     日志服：只收 game 转发，不接客户端
//	RoleAuth   → *AuthGame    账号服：HTTP 对客户端 + 收 game 转发
type Role string

const (
	RoleGame   Role = "game"
	RoleMaster Role = "master"
	RoleLog    Role = "log"
	RoleAuth   Role = "auth"
)

// mountFn 是内部统一的挂载函数形态：host 由启动方按角色传入对应宿主实例。
type mountFn func(host any)

// mounts 角色 → 已登记的业务挂载函数，按登记顺序执行。
// 登记入口只有一个（RegisterMountSlot），角色不同不会引入第二套规则。
// mountsMu 保护 mounts：登记可能发生在包 init（业务 var 初始化）等任何时机，
// 而 RunMounts 可能在启动后才读——两者并发即 map 竞态。
var (
	mountsMu sync.Mutex
	mounts   = map[Role][]mountFn{}
)

// RegisterMountSlot 登记指定角色的业务挂载函数（内部实现，业务不直接调用）。
//
// 由 pkg/app.Mount 转发进来：业务用 `app.Mount(app.RoleXxx, func(host){...})` 声明，
// fn 的签名在那一层就校验过（不匹配启动期 panic）。
func RegisterMountSlot(role Role, fn func(host any)) {
	mountsMu.Lock()
	mounts[role] = append(mounts[role], fn)
	mountsMu.Unlock()
}

// RunMounts 按登记顺序执行某角色的全部业务挂载函数。
// host 必须是该角色对应的宿主实例（RoleGame→*Game，其余同理）。
func RunMounts(role Role, host any) error {
	// 先在锁内取快照再执行：挂载函数内部可能再调 RegisterMountSlot，
	// 持锁执行会自锁。
	mountsMu.Lock()
	fns := append([]mountFn(nil), mounts[role]...)
	mountsMu.Unlock()
	for _, fn := range fns {
		if err := safeMount(role, fn, host); err != nil {
			return err
		}
	}
	return nil
}

func safeMount(role Role, fn mountFn, host any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// 保留堆栈，便于定位是哪个业务挂载函数、哪一行 panic。
			// role 打角色名、host 只打类型：早期版本此处把 *Game 整体 %v 出来，
			// 既缺角色定位，又把整个宿主结构（含内部字段）刷进日志。
			err = fmt.Errorf("app: mount panic (role=%s host=%T): %v\n%s", role, host, r, debug.Stack())
		}
	}()
	fn(host)
	return nil
}

// TableLoadedType 表加载完成事件类型（业务手动 emit 时使用此常量，保持引擎事件名一致）。
const TableLoadedType = "table.Loaded"

// TableLoadedEvent 表加载完成事件载荷（业务手动 emit 时使用）。
type TableLoadedEvent = apptypes.TableLoadedEvent

// TableLoader 表加载接口。业务实现后自行调用 LoadAll，引擎不自动加载。
type TableLoader = apptypes.TableLoader

// PublishTableLoaded 辅助：广播 table.Loaded 事件。
func PublishTableLoaded(g *Game, names []string) {
	if g.Logic == nil {
		return
	}
	b := g.Logic.Bus()
	if b == nil {
		return
	}
	b.Publish(event.NewEvent(TableLoadedType, TableLoadedEvent{
		Count: len(names),
		Names: names,
	}))
}

// TimeEvent 封装共享定时器调度器，提供业务侧定时任务注册能力。
// 生命周期随 Game 启停统一托管。
//
// 注意：本调度器**未注入 PersistBackend**（见 ensure），因此经 Scheduler() 调用
// PersistScope / RestoreScope / ClearPersist 会返回 timer.ErrNoBackend；
// 且调度器本身是进程内的，不跨重启保留任务。
// 「到点必须发生」的事应把绝对 deadline 写进业务数据，在加载时重建（A 档范式）。
type TimeEvent struct {
	// mu 保护 sched 的惰性创建：`NewScheduler` 会**立即**起一条调度 goroutine，
	// 并发首次调用（Scheduler / Every / After / Cron …）若各建一个，先建的会被覆盖，
	// 其上注册的任务永不触发、那条 goroutine 也再没人关闭。
	mu    sync.Mutex
	sched *timer.Scheduler
	loc   *time.Location
}

// NewTimeEvent 创建定时任务集合并设置时区。loc 为 nil 时使用系统本地时区。
func NewTimeEvent(loc *time.Location) *TimeEvent {
	if loc == nil {
		loc = time.Local
	}
	return &TimeEvent{loc: loc}
}

// scheduler 返回底层调度器，首次调用时惰性创建（并发安全）。
func (t *TimeEvent) scheduler() *timer.Scheduler {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sched == nil {
		// 刻意不传 WithPersistence：引擎侧共享调度器不做持久化（见类型注释）。
		t.sched = timer.NewScheduler(timer.WithLocation(t.loc))
	}
	return t.sched
}

// Scheduler 返回底层调度器（供 job.Manager 等组件注入时使用）。
func (t *TimeEvent) Scheduler() *timer.Scheduler { return t.scheduler() }

// Close 关闭调度器，停止所有定时任务。
func (t *TimeEvent) Close() {
	t.mu.Lock()
	s := t.sched
	t.mu.Unlock()
	if s != nil {
		// 在锁外 Close：它会等调度 goroutine 退出，持 t.mu 做这件事会连带阻塞其它注册调用。
		s.Close()
	}
}

// Every 注册具名周期定时任务。同名再注册=重启语义。
func (t *TimeEvent) Every(name string, interval time.Duration, task timer.Task) timer.Timer {
	return t.scheduler().EveryName(name, "", interval, task)
}

// After 注册具名一次性延迟任务。同名再注册=覆盖旧的。
func (t *TimeEvent) After(name string, delay time.Duration, task timer.Task) timer.Timer {
	return t.scheduler().AfterName(name, "", delay, task)
}

// Location 返回引擎统一时区。
func (t *TimeEvent) Location() *time.Location {
	return t.loc
}

// ParseDate 将日期字符串解析为引擎时区的 time.Time，支持格式：
//
//	"2026-07-31"、"2026-07-31 14:30:00"、"2026-07-31T14:30:00"
func (t *TimeEvent) ParseDate(value string) (time.Time, error) {
	return timeutil.ParseDate(value)
}

// Date 使用引擎时区构造 time.Time，等价于 time.Date(y,m,d,h,min,sec,nsec, t.loc)。
// 供 ByTime 搭配使用：g.Timer.ByTime(name, g.Timer.Date(2026,7,31,14,30,0,0), task)
func (t *TimeEvent) Date(year int, month time.Month, day, hour, min, sec, nsec int) time.Time {
	return time.Date(year, month, day, hour, min, sec, nsec, t.loc)
}

// ByTime 在指定时刻执行一次性任务。target 用 g.Timer.Date() 构造，时区由引擎统一托管。
// 时刻已过则立即执行。
func (t *TimeEvent) ByTime(name string, target time.Time, task timer.Task) timer.Timer {
	delay := time.Until(target)
	if delay <= 0 {
		task()
		return nil
	}
	return t.After(name, delay, task)
}

// DailyAt 每天在指定时分秒时刻执行一次性任务（已过则次日）。
// 时区使用引擎统一配置，业务无需关心。
func (t *TimeEvent) DailyAt(name string, hour, minute, second int, task timer.Task) timer.Timer {
	now := time.Now().In(t.loc)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, 0, t.loc)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return t.After(name, time.Until(target), task)
}

// Cron 注册具名 crontab 定时任务。同名再注册=覆盖旧的。
func (t *TimeEvent) Cron(name, spec string, task timer.Task) (timer.Timer, error) {
	return t.scheduler().CronName(name, "", spec, task)
}

// StopTimer 按名字关闭具名定时任务。返回是否找到该名字。
func (t *TimeEvent) StopTimer(name string) bool {
	return t.scheduler().StopNamed(name)
}

// TimerGroup 获取对象（玩家/场景等）上的定时任务组。
func (t *TimeEvent) TimerGroup(scope string) *timer.Group {
	return t.scheduler().Group(scope)
}

// StopTimerGroup 关闭某一作用域下的全部定时任务。
//
// 引擎在玩家断线时会以**连接级 owner**（登录回执 owner 字段 / AccountID，不是角色 ID）
// 调用本方法（见 Game.OnDisconnect）。因此：
//   - 「要随掉线清理」的任务，scope 必须等于该 owner；
//   - 「离线也要继续推进」的任务，scope 要故意避开 owner（加前缀即可）。
func (t *TimeEvent) StopTimerGroup(scope string) {
	t.scheduler().StopScope(scope)
}
