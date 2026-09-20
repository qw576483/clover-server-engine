//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package metrics

import "syscall"

// processCPUSeconds 用 getrusage(RUSAGE_SELF) 取进程累计 CPU 时间。
//
// utime + stime 分别是用户态与内核态时间；一次系统调用、无分配、不 STW，
// 所以挂在 /metrics 抓取路径上完全无压力（同路径上的 runtime.ReadMemStats 才是大头）。
//
// 注意：Timeval 的字段类型在各平台并不一致（Linux 上是 int64，Darwin/BSD 上是 int32），
// 这里不做类型断言，统一用 float64(...) 转换 + 微秒折算，避免为每个平台写一份。
func processCPUSeconds() (float64, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		// 取不到就明确返回「不支持」而不是 0：让调用方决定是否导出该指标。
		return 0, false
	}
	sec := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6 +
		float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return sec, true
}
