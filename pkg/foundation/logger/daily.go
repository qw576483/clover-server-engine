// #nosec G304 -- 日志目录/文件路径来自 logger 配置（运维输入），非客户端可控。

package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// dailyWriter 按天滚动的日志写入器。
// 文件路径：{baseDir}/{YYYY-MM-DD}-{filename}.log
// 跨天时自动切换新文件，旧文件句柄关闭。
type dailyWriter struct {
	baseDir  string // 日志根目录，如 "./logs"
	filename string // 文件名后缀，如 "all"

	mu      sync.Mutex
	current string   // 当前打开的日期 YYYY-MM-DD
	file    *os.File // 当前文件句柄
}

// newDailyWriter 创建按天滚动写入器。
// baseDir 为日志根目录；filename 为文件名后缀（不含日期和扩展名）。
func newDailyWriter(baseDir, filename string) *dailyWriter {
	if filename == "" {
		filename = "app"
	}
	return &dailyWriter{baseDir: baseDir, filename: filename}
}

// Write 实现 io.Writer，按当前日期写入对应文件。
func (w *dailyWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().Format("2006-01-02")
	if w.current != now || w.file == nil {
		if err := w.rotate(now); err != nil {
			return 0, err
		}
	}

	return w.file.Write(p)
}

// reportDailyErr 输出日志子系统自身的失败（刷盘/关闭旧句柄）。
//
// 这里**不能**走 logger.Error*：本文件就是 logger 的落地实现，
// 经由 logger 上报会递归回 dailyWriter.Write → rotate，直接栈溢出。
// 落 stderr 是日志链路断裂时唯一还能留痕的出口。
func reportDailyErr(op, day string, err error) {
	fmt.Fprintf(os.Stderr, "[logger] daily rotate %s failed (day=%s): %v\n", op, day, err)
}

// rotate 切换到指定日期的日志文件。
func (w *dailyWriter) rotate(day string) error {
	if w.file != nil {
		// 切换前必须先刷盘：失败即丢掉当天的最后若干条日志，必须留痕而不是 `_ =` 吞掉。
		if err := w.file.Sync(); err != nil {
			reportDailyErr("sync", w.current, err)
		}
		if err := w.file.Close(); err != nil {
			reportDailyErr("close", w.current, err)
		}
		w.file = nil
	}

	// 0o750（属主 rwx + 同组 r-x，others 无权限）：日志目录里可能含玩家标识 / 订单号等
	// 业务数据，others 可列目录等于可读全部历史日志（gosec G301 建议 ≤ 0750）。
	// 只有本进程写日志，收窄权限不影响任何读者。
	if err := os.MkdirAll(w.baseDir, 0o750); err != nil {
		return fmt.Errorf("daily logger: mkdir %s: %w", w.baseDir, err)
	}

	path := filepath.Join(w.baseDir, day+"-"+w.filename+".log")
	// 0o600（属主读写，组/others 无权限）：日志文件内容同上，不该对同机其他用户可读
	//（gosec G302 建议 ≤ 0600）。需要多人排查时用日志采集器以属主身份读取。
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("daily logger: open %s: %w", path, err)
	}

	w.current = day
	w.file = f
	return nil
}

// Sync 刷盘。
func (w *dailyWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

// Close 关闭当前文件句柄。
func (w *dailyWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			reportDailyErr("sync", w.current, err)
		}
		err := w.file.Close()
		if err != nil {
			reportDailyErr("close", w.current, err)
		}
		w.file = nil
		w.current = ""
		return err
	}
	return nil
}
