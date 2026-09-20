package config

import (
	"sync"

	"github.com/spf13/viper"
)

// SourceType 配置来源枚举类型
type SourceType int

const (
	SourceLocal SourceType = 1 // 本地文件配置源（当前引擎唯一在用的配置源）
)

// SourceInfo 配置源元信息
// 存储本地文件的路径等描述信息
type SourceInfo struct {
	filePath string // 本地配置文件路径，仅本地文件模式生效
}

// Loader 配置加载器底层实现结构体
// 封装viper实例、读写锁、配置源标识、监听状态，保证并发安全
type Loader struct {
	mu         sync.RWMutex // 读写互斥锁，保护viper并发读写
	v          *viper.Viper // viper底层配置实例
	source     SourceType   // 当前配置源类型
	sourceInfo SourceInfo   // 当前配置源详情信息
	watched    bool         // 是否已开启变更监听，防止重复注册
}

// LoaderInterface 配置加载器对外抽象接口
// 上层业务仅依赖该接口，完全屏蔽底层Loader、viper实现细节
type LoaderInterface interface {
	// Load 读取配置并反序列化到传入的结构体指针target
	Load(target any) error
	// Watch 开启配置变更监听
	// 1. 回调在 fsnotify 独立 goroutine 中触发，不持有任何锁；
	// 2. 回调内可直接调用 GetXxx 读取配置（方法内部自带锁）；
	// 3. 回调内如需 Load 重载配置，必须新开 goroutine 执行，避免阻塞文件监听。
	Watch(callback WatchCallback) error

	// Close 释放配置加载器资源，重置监听状态
	Close() error

	// GetSource 获取当前配置源类型
	GetSource() SourceType
	// GetSourceFilePath 获取本地配置文件路径，非本地源返回错误
	GetSourceFilePath() (string, error)

	// GetString 根据key读取string类型配置
	GetString(key string) string
	// GetInt 根据key读取int类型配置
	GetInt(key string) int
	// GetBool 根据key读取bool类型配置
	GetBool(key string) bool
	// GetFloat64 根据key读取float64类型配置
	GetFloat64(key string) float64
	// GetStringSlice 根据key读取字符串切片配置
	GetStringSlice(key string) []string
	// GetStringMap 根据key读取字符串map，内部深拷贝避免外部篡改内部数据
	GetStringMap(key string) map[string]any
}

// WatchCallback 配置变更回调函数
// 配置内容发生修改时自动触发执行
type WatchCallback func()
