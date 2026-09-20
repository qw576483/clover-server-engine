package logger

import "go.uber.org/zap/zapcore"

// InitConfig 底层初始化配置（由 facade.go 的 Init 从 Config 逐字段拷贝构造）。
type InitConfig struct {
	Level   string // 日志级别: debug/info/warn/error/panic/fatal
	Format  string // 输出格式: json/console
	Dir     string // 日志根目录；非空时按 {dir}/{YYYY-MM-DD}-{service}.log 按天落盘
	Stdout  bool   // 是否同时输出到控制台
	Service string // 服务名称，注入为固定字段；dir 模式同时作为日志文件名后缀
	Env     string // 运行环境: dev/test/prod
}

// levelMap 字符串级别到 zapcore.Level 的映射
var levelMap = map[string]zapcore.Level{
	"debug": zapcore.DebugLevel,
	"info":  zapcore.InfoLevel,
	"warn":  zapcore.WarnLevel,
	"error": zapcore.ErrorLevel,
	"panic": zapcore.PanicLevel,
	"fatal": zapcore.FatalLevel,
}
