package logger

import (
	"context"

	"go.uber.org/zap"
)

// 以下为 LogXxx / LogXxxf 的同包薄转发别名（Info → LogInfo 等）。
// 两者同属 package logger：LogXxx 是底层实现，Info 等是对外调用名。

func Debug(msg string, fields ...zap.Field) { LogDebug(msg, fields...) }
func Info(msg string, fields ...zap.Field)  { LogInfo(msg, fields...) }
func Warn(msg string, fields ...zap.Field)  { LogWarn(msg, fields...) }
func Error(msg string, fields ...zap.Field) { LogError(msg, fields...) }
func Panic(msg string, fields ...zap.Field) { LogPanic(msg, fields...) }
func Fatal(msg string, fields ...zap.Field) { LogFatal(msg, fields...) }

func Debugf(template string, args ...any) { LogDebugf(template, args...) }
func Infof(template string, args ...any)  { LogInfof(template, args...) }
func Warnf(template string, args ...any)  { LogWarnf(template, args...) }
func Errorf(template string, args ...any) { LogErrorf(template, args...) }
func Panicf(template string, args ...any) { LogPanicf(template, args...) }
func Fatalf(template string, args ...any) { LogFatalf(template, args...) }

// Config 日志配置（带 yaml/mapstructure tag，供 viper 解析）；字段与 InitConfig 一致，
// Init 负责逐字段拷贝为 InitConfig 后调用 InitZap。
type Config struct {
	Level   string `yaml:"level" mapstructure:"level"`     // 日志级别: debug/info/warn/error/panic/fatal，默认info
	Format  string `yaml:"format" mapstructure:"format"`   // 输出格式: json（生产）/ console（开发）
	Dir     string `yaml:"dir" mapstructure:"dir"`         // 日志根目录；非空时按 {dir}/{YYYY-MM-DD}-{service}.log 按天落盘
	Stdout  bool   `yaml:"stdout" mapstructure:"stdout"`   // 是否同步输出到控制台，默认true
	Service string `yaml:"service" mapstructure:"service"` // 服务名称，如 clover-gateway；dir 模式作为文件名后缀
	Env     string `yaml:"env" mapstructure:"env"`         // 运行环境: dev/test/prod
}

// DefaultConfig 返回生产可用的默认日志配置。
// 默认 Format 采用 json（生产结构化日志），开发环境如需可读格式请显式传 Format:"console"。
func DefaultConfig() *Config {
	return &Config{Level: "info", Format: "json", Stdout: true}
}

// Init 初始化全局日志系统（内部等价 pkg 的 Init；nil 时使用默认配置）。
func Init(cfg *Config) error {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return InitZap(InitConfig{
		Level:   cfg.Level,
		Format:  cfg.Format,
		Dir:     cfg.Dir,
		Stdout:  cfg.Stdout,
		Service: cfg.Service,
		Env:     cfg.Env,
	})
}

// Get 返回全局 *zap.Logger 实例（内部等价 GetZap）。
func Get() *zap.Logger { return GetZap() }

// Sync 刷新日志缓冲区（内部等价 SyncZap）。
func Sync() { SyncZap() }

// DefaultTraceKey 默认链路追踪字段名。
const DefaultTraceKey = "trace_id"

// WithTrace 注入链路追踪字段。
func WithTrace(id string) zap.Field {
	return zap.String(GetTraceKey(), id)
}

// Field 通用自定义字段构造器。
func Field(key string, val any) zap.Field {
	return zap.Any(key, val)
}

// Context 链路日志 =
type ctxLoggerKey struct{}

// CtxWithFields 将 zap fields 绑定到 context。
func CtxWithFields(ctx context.Context, fields ...zap.Field) context.Context {
	return context.WithValue(ctx, ctxLoggerKey{}, fields)
}

// FieldsFromCtx 从 context 提取已绑定的 zap fields。
// ctx 为 nil 时返回 nil：CtxXxx 系列允许传 nil ctx（与同包 ctx.go 的 LogXxxCtx 口径一致），
// 直接 ctx.Value 会 panic。
func FieldsFromCtx(ctx context.Context) []zap.Field {
	if ctx == nil {
		return nil
	}
	if fields, ok := ctx.Value(ctxLoggerKey{}).([]zap.Field); ok {
		return fields
	}
	return nil
}

// mergeCtxFields 合并 ctx 内字段与调用方字段。
// 必须新建切片：直接 append(FieldsFromCtx(ctx), ...) 时，若 ctx 里存的切片尚有
// 富余容量（业务用 slice... 展开传入），并发的两次 CtxXxx 会向同一底层数组写入，
// 造成字段互相覆盖与 data race。
func mergeCtxFields(ctx context.Context, fields []zap.Field) []zap.Field {
	base := FieldsFromCtx(ctx)
	all := make([]zap.Field, 0, len(base)+len(fields))
	all = append(all, base...)
	return append(all, fields...)
}

// CtxDebug context 链路调试日志。
func CtxDebug(ctx context.Context, msg string, fields ...zap.Field) {
	Get().Debug(msg, mergeCtxFields(ctx, fields)...)
}

// CtxInfo context 链路信息日志。
func CtxInfo(ctx context.Context, msg string, fields ...zap.Field) {
	Get().Info(msg, mergeCtxFields(ctx, fields)...)
}

// CtxWarn context 链路警告日志。
func CtxWarn(ctx context.Context, msg string, fields ...zap.Field) {
	Get().Warn(msg, mergeCtxFields(ctx, fields)...)
}

// CtxError context 链路错误日志。
func CtxError(ctx context.Context, msg string, fields ...zap.Field) {
	Get().Error(msg, mergeCtxFields(ctx, fields)...)
}
