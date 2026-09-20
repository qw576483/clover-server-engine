//go:build !windows && !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package metrics

// processCPUSeconds 在未适配的平台上恒返回「不支持」。
//
// 这里刻意不返回 0：调用方（CollectRuntimeTo）会据此**不导出** process_cpu_* 两项，
// 而不是导出一个恒为 0 的假 CPU 曲线让运维误判"进程很闲"。
func processCPUSeconds() (float64, bool) { return 0, false }
