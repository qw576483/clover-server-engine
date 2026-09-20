package logger

import (
	"os"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var (
	// globalZap / globalSugar 使用 atomic.Pointer 存储，避免 InitZap / CloseFile 与
	// 高频日志调用（safeLogger/safeSugar）之间的并发读写 data race。
	globalZap      atomic.Pointer[zap.Logger]
	globalSugar    atomic.Pointer[zap.SugaredLogger]
	globalDaily    *dailyWriter // 按天滚动写入器（dir 模式），供 CloseFile 释放句柄
	mu             sync.RWMutex // 读写锁，保护 globalDaily / globalTraceKey
	globalTraceKey = "trace_id" // 链路追踪字段名，固定为 trace_id

	// 预分配 nop 实例，InitZap 前调用 LogXxx 返回 NopLogger，避免 nil panic
	nopLogger = zap.NewNop()
	nopSugar  = nopLogger.Sugar()
)

// 内部工具
// parseLevel 将配置字符串转为 zap 日志级别，未知级别默认 info
func parseLevel(s string) zapcore.Level {
	if lv, ok := levelMap[s]; ok {
		return lv
	}
	return zapcore.InfoLevel
}

// prefixEncoder 包装 console encoder，把「service 固定字段」从行尾 JSON 挪到行首前缀。

// 关键机制：zap.Fields 注入的固定字段不会走 EncodeEntry 的 fields 参数，而是经
// core.With → addFields → enc.AddString(key, val) 直接累加进 encoder 内部。因此必须
// 在 AddString 这一层拦截：当 key 为 service 时把值存为行首前缀，而不转交底层 encoder，
// 从而让 service 不再作为尾部 JSON 字段输出。其余字段照常透传。
type prefixEncoder struct {
	zapcore.Encoder
	serviceKey string // 需要提升为前缀的字段名
	prefix     string // 捕获到的 service 值（经 AddString 拦截存入）
}

func newPrefixEncoder(enc zapcore.Encoder, serviceKey string) *prefixEncoder {
	return &prefixEncoder{Encoder: enc, serviceKey: serviceKey}
}

// AddString 拦截：service 字段的值不再写入底层 encoder，而是存为行首前缀。
func (e *prefixEncoder) AddString(key, val string) {
	if key == e.serviceKey {
		e.prefix = val
		return
	}
	e.Encoder.AddString(key, val)
}

// EncodeEntry 重写：编码前把捕获到的 service 前缀拼进消息头部。
func (e *prefixEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	if e.prefix != "" {
		ent.Message = "【" + e.prefix + "】" + ent.Message
	}
	return e.Encoder.EncodeEntry(ent, fields)
}

// Clone 复制自身（ioCore.With 会先 clone 再 addFields，必须把前缀状态一并带过去）。
func (e *prefixEncoder) Clone() zapcore.Encoder {
	clone := *e
	clone.Encoder = e.Encoder.Clone()
	return &clone
}

// InitZap 初始化全局 zap 日志实例
// 根据配置构建 encoder（JSON/Console）、writer（文件+控制台）、core，
// 注入 service/env/version 固定字段，赋值全局单例
func InitZap(cfg InitConfig) error {
	mu.Lock()
	defer mu.Unlock()

	// 用全局 AtomicLevel 承载级别（cfg.Level 作为初始值），
	// 使运行时可经 SetLevel / LevelHandler / WatchLevel 热切换而无需重建 logger。
	// 注意：走 parseLevel（未知值静默降级 info）。
	lvl := levelHolder()
	lvl.SetLevel(parseLevel(cfg.Level))
	level := *lvl

	ec := zap.NewProductionEncoderConfig()
	ec.TimeKey = "time"
	ec.EncodeTime = zapcore.ISO8601TimeEncoder
	ec.EncodeLevel = zapcore.CapitalLevelEncoder
	ec.EncodeCaller = zapcore.ShortCallerEncoder

	var encoder zapcore.Encoder
	if cfg.Format == "json" {
		encoder = zapcore.NewJSONEncoder(ec)
	} else {
		// console 模式：把 service 固定字段提升为行首 【service】 前缀，而非行尾 JSON。
		encoder = newPrefixEncoder(zapcore.NewConsoleEncoder(ec), "service")
	}

	// 重复 InitZap（运行时重载）时先关闭旧文件写入器，
	// 否则旧句柄泄漏（Windows 上旧日志文件被永久占用无法删除）。
	if globalDaily != nil {
		_ = globalDaily.Close()
		globalDaily = nil
	}
	var writers []zapcore.WriteSyncer
	// 仅支持 dir 模式：配置了日志根目录则按天落盘（{dir}/{YYYY-MM-DD}-{service}.log）。
	if cfg.Dir != "" {
		globalDaily = newDailyWriter(cfg.Dir, cfg.Service)
		writers = append(writers, zapcore.AddSync(globalDaily))
	}
	if cfg.Stdout || len(writers) == 0 {
		writers = append(writers, zapcore.AddSync(os.Stdout))
	}
	ws := zapcore.NewMultiWriteSyncer(writers...)

	core := zapcore.NewCore(encoder, ws, level)

	var fields []zap.Field
	if cfg.Service != "" {
		fields = append(fields, zap.String("service", cfg.Service))
	}
	if cfg.Env != "" {
		fields = append(fields, zap.String("env", cfg.Env))
	}

	// AddCallerSkip(2)：调用链 业务 → logger.Infof/Debugf(facade.go) → LogInfof/LogDebugf(zap.go)
	// → safeSugar().Infof → zap。skip=0 落在 LogInfof 行、skip=1 落在 facade 行，
	// skip=2 才落到业务调用方。若代码直接调 LogInfof（少一层），caller 会多跳一级。
	globalZap.Store(zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2), zap.Fields(fields...)))
	globalSugar.Store(globalZap.Load().Sugar())

	return nil
}

// GetZap 返回全局 *zap.Logger 原始实例
// 若未初始化则返回 NopLogger（不输出任何日志）
func GetZap() *zap.Logger {
	if l := globalZap.Load(); l != nil {
		return l
	}
	return zap.NewNop()
}

// SyncZap 刷新日志缓冲区，确保落盘
// 应在程序退出前调用，如 defer logger.Sync()
func SyncZap() {
	if l := globalZap.Load(); l != nil {
		_ = l.Sync()
	}
}

// CloseFile 关闭日志文件句柄，释放文件锁
// Windows 平台写文件后必须显式关闭，否则文件被占用无法删除
// 调用后全局日志实例置 nil（等效于未初始化状态），主要用于测试环境清理
// 生产环境一般不需要调用（进程退出自动释放），如确需运行时重载配置请先 CloseFile 再 InitZap

// （设计约定，安全性说明）：CloseFile 后 globalZap/globalSugar 为 nil，此后 LogXxx/LogXxxf
// 经 safeLogger()/safeSugar() 一律降级为 nopLogger/nopSugar（静默丢弃日志），这是预期行为而非故障——
// 通过 atomic.Pointer 读写 + 预分配 nop 实例兜底，保证与并发日志调用之间既无 data race 也不会 nil panic。
// 因此本函数只应在「测试清理」或「运行时重载（随后立即 InitZap）」两种场景使用；否则会静默关闭日志输出。
func CloseFile() {
	if l := globalZap.Load(); l != nil {
		_ = l.Sync()
	}
	mu.Lock()
	if globalDaily != nil {
		_ = globalDaily.Close()
		globalDaily = nil
	}
	globalTraceKey = "trace_id"
	mu.Unlock()
	// 置 nil 与并发日志调用通过 atomic 保证无 data race。
	globalZap.Store(nil)
	globalSugar.Store(nil)
}

// GetTraceKey 返回当前链路追踪字段名
func GetTraceKey() string {
	mu.RLock()
	defer mu.RUnlock()
	return globalTraceKey
}

// 底层便利方法（供 facade.go 的 Info/Infof 等同包别名转发调用）
// safeLogger 返回当前全局 zap 实例，未初始化时返回 NopLogger
// 通过 atomic.Pointer 无锁安全读取，与 InitZap / CloseFile 的写入无 data race。
func safeLogger() *zap.Logger {
	if l := globalZap.Load(); l != nil {
		return l
	}
	return nopLogger
}

// safeSugar 返回当前全局 SugaredLogger，未初始化时返回 nop Sugar
func safeSugar() *zap.SugaredLogger {
	if s := globalSugar.Load(); s != nil {
		return s
	}
	return nopSugar
}

func LogDebug(msg string, fields ...zap.Field) { safeLogger().Debug(msg, fields...) }
func LogInfo(msg string, fields ...zap.Field)  { safeLogger().Info(msg, fields...) }
func LogWarn(msg string, fields ...zap.Field)  { safeLogger().Warn(msg, fields...) }
func LogError(msg string, fields ...zap.Field) { safeLogger().Error(msg, fields...) }
func LogPanic(msg string, fields ...zap.Field) { safeLogger().Panic(msg, fields...) }
func LogFatal(msg string, fields ...zap.Field) { safeLogger().Fatal(msg, fields...); os.Exit(1) }

func LogDebugf(template string, args ...any) { safeSugar().Debugf(template, args...) }
func LogInfof(template string, args ...any)  { safeSugar().Infof(template, args...) }
func LogWarnf(template string, args ...any)  { safeSugar().Warnf(template, args...) }
func LogErrorf(template string, args ...any) { safeSugar().Errorf(template, args...) }
func LogPanicf(template string, args ...any) { safeSugar().Panicf(template, args...) }
func LogFatalf(template string, args ...any) { safeSugar().Fatalf(template, args...); os.Exit(1) }
