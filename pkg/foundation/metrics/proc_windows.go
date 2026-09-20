// #nosec G103 -- unsafe.Pointer 指向本地 uint64 结构（恰为 FILETIME 大小），仅作 GetProcessTimes 的输出缓冲，不可构造越界/悬垂访问。

//go:build windows

package metrics

import (
	"syscall"
	"unsafe"
)

// procGetProcessTimes 惰性加载 kernel32 的 GetProcessTimes，避免在非 Windows 逻辑路径上付出 DLL 加载成本。
var procGetProcessTimes = syscall.NewLazyDLL("kernel32.dll").NewProc("GetProcessTimes")

// processCPUSeconds 用 GetProcessTimes 取进程累计 CPU 时间。
//
// FILETIME 以 100ns 为单位：内核态 + 用户态相加后 /1e7 即秒。
// 用 syscall 而非 golang.org/x/sys/windows，是为了守住本包「零第三方依赖」的约束
// （x/sys 目前只是间接依赖，不该为一个 API 把它变成直接依赖）。
func processCPUSeconds() (float64, bool) {
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, false
	}
	// GetProcessTimes(hProcess, lpCreationTime, lpExitTime, lpKernelTime, lpUserTime)
	// 四个出参都是 8 字节 FILETIME，用 uint64 承载即可（这里只关心后两个）。
	var creation, exit, kernel, user uint64
	r1, _, _ := procGetProcessTimes.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&creation)),
		uintptr(unsafe.Pointer(&exit)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	)
	if r1 == 0 {
		// 调用失败同样返回「不支持」，调用方据此不导出该指标。
		return 0, false
	}
	return float64(kernel+user) / 1e7, true
}
