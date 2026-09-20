// Package safe 提供 goroutine panic 安全兜底原语。
package safe

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
)

// errWriterMu 保护 errWriter 的读写并发安全。
var errWriterMu sync.RWMutex
var errWriter io.Writer = os.Stderr

// SetErrWriter 设置兜底日志输出目标（并发安全，建议启动阶段调用，避免运行时频繁切换）。
// 传入 nil 会被忽略：兜底路径必须始终有一个可写的目标，否则 recover 时反而会二次 panic。
func SetErrWriter(w io.Writer) {
	if w == nil {
		return
	}
	errWriterMu.Lock()
	errWriter = w
	errWriterMu.Unlock()
}

func getErrWriter() io.Writer {
	errWriterMu.RLock()
	w := errWriter
	errWriterMu.RUnlock()
	return w
}

// SafeRun 安全执行函数，内部自带 recover。
// fn 中的 panic 不会向上传播，仅打印堆栈到 ErrWriter，用于异步回调兜底。
// 注意：SafeRun 只能兜住普通 panic，runtime 级别致命错误（如 map 并发写
// 触发的 fatal error）不在 recover 覆盖范围内，Go 运行时会直接崩溃。
func SafeRun(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			reportPanic(r, debug.Stack())
		}
	}()
	fn()
}

// reportPanic 输出 panic 信息与堆栈。
// 兜底路径自身不允许再 panic：输出目标缺失或 Write 失败时静默放弃。
func reportPanic(r any, stack []byte) {
	defer func() {
		_ = recover()
	}()
	w := getErrWriter()
	if w == nil {
		w = os.Stderr
	}
	// 兜底路径不允许再失败：写不出去（目标已关闭 / 磁盘满）也只能放弃——
	// 报错本身会再次 panic，那才是真正的「兜底把进程带崩」。
	_, _ = fmt.Fprintf(w, "[util] SafeRun recovered panic: %v\n%s\n", r, stack)
}

// GoSafe 安全启动 goroutine，内部通过 SafeRun 兜底，
// 单个协程崩溃不会拖垮整个服务。
func GoSafe(fn func()) {
	go SafeRun(fn)
}

// ErrWriter 返回当前兜底日志输出目标（只读访问器）。
// 设置输出目标请用 SetErrWriter。
func ErrWriter() io.Writer { return getErrWriter() }
