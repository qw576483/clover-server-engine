// Package timeutil 提供时间相关工具函数，是引擎的统一时间中枢。

// 所有业务代码通过 timeutil 获取当前时间，确保时区一致：

//	timeutil.Init("Asia/Shanghai")  // 引擎启动时调用一次
//	now := timeutil.NowTime()       // 之后所有地方用 timeutil 取时间

// 并发安全性：Init 使用 atomic.Pointer 实现无锁读写，可安全并发调用。
package timeutil

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// 全局时区（atomic 无锁读写）
// loc 存储引擎统一时区，Init 时写入，之后只读。
// 使用 atomic.Pointer[time.Location] 避免加锁，读路径零开销。
var loc atomic.Pointer[time.Location]

func init() {
	// 默认使用系统本地时区，确保 Init 未调用时也能正常工作。
	loc.Store(time.Local)
}

// Init 初始化引擎统一时区。应在 app 启动时调用一次。

// name 为空时回退到系统本地时区；无效时区名 fallback UTC 并打印警告。
func Init(name string) {
	loc.Store(ParseTimezone(name))
}

// Location 返回引擎统一时区（Init 前返回系统本地时区）。
func Location() *time.Location {
	return loc.Load()
}

// ParseTimezone 解析时区字符串为 *time.Location。
// name 为空返回系统本地时区；无效时区名 fallback UTC 并打印警告。
func ParseTimezone(name string) *time.Location {
	if name == "" {
		return time.Local
	}
	l, err := time.LoadLocation(name)
	if err != nil {
		// fallback UTC + 日志告警，让服务至少能启动，而不是 panic。
		// 走 logger 而不是标准库 log：后者只到 stdout/stderr，不进日志系统，线上无痕。
		logger.Warnf("timeutil: invalid timezone %q, fallback to UTC: %v", name, err)
		return time.UTC
	}
	return l
}

// 时间戳（UTC epoch，时区无关）
// NowMS 返回毫秒级 Unix 时间戳，用于消息耗时统计、日志埋点、协议时间字段。
func NowMS() int64 {
	return time.Now().UnixMilli()
}

// NowSec 返回秒级 Unix 时间戳。
func NowSec() int64 {
	return time.Now().Unix()
}

// 带时区的时间函数
// NowTime 返回当前时刻（转为引擎统一时区）。
func NowTime() time.Time {
	return time.Now().In(Location())
}

// TodayZero 返回今天零点（引擎时区）。
func TodayZero() time.Time {
	now := NowTime()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, Location())
}

// TodayElapsed 返回今天已过去的秒数（从零点起算）。
func TodayElapsed() int {
	return int(time.Since(TodayZero()).Seconds())
}

// Layout 引擎统一的 DATETIME 文本格式（与 MySQL DATETIME 列兼容）。
//
// 单独成常量是因为此前各处直接内联 `"2006-01-02 15:04:05"`：一旦有人写成
// `time.Now().Format(...)` 就可能带上**系统本地时区**，与引擎时区（misc.timezone）
// 不一致——同一条 SQL 写入的时间列会随部署机器变化。
const Layout = "2006-01-02 15:04:05"

// NowStr 返回格式化的当前时间字符串 "2006-01-02 15:04:05"（引擎时区）。
func NowStr() string {
	return NowTime().Format(Layout)
}

// StrAfter 返回「引擎时区的当前时间 + d」的 DATETIME 文本（如 token 过期时间）。
func StrAfter(d time.Duration) string {
	return NowTime().Add(d).Format(Layout)
}

// Date 使用引擎时区构造 time.Time，等价于 time.Date(y,m,d,h,min,sec,nsec, Location())。
func Date(year int, month time.Month, day, hour, min, sec, nsec int) time.Time {
	return time.Date(year, month, day, hour, min, sec, nsec, Location())
}

// ParseDate 将日期字符串解析为引擎时区的 time.Time，支持格式：

// "2006-01-02"、"2006-01-02 15:04:05"、"2006-01-02T15:04:05"
func ParseDate(value string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	loc := Location()
	for _, layout := range layouts {
		if tm, err := time.ParseInLocation(layout, value, loc); err == nil {
			return tm, nil
		}
	}
	return time.Time{}, fmt.Errorf("timeutil: cannot parse date %q", value)
}

// IsNewDay 判断给定时刻是否属于新的一天（跨零点后的首次调用）。
// 参考时间 ref 通常为上次检查时的时间。
func IsNewDay(ref time.Time) bool {
	now := NowTime()
	return now.YearDay() != ref.YearDay() || now.Year() != ref.Year()
}
