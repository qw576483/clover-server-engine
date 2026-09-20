package config

import (
	"errors"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// 初始化句柄
func NewFileLoader(filePath string) LoaderInterface {
	info := SourceInfo{
		filePath: filePath,
	}
	return newLoaderInner(SourceLocal, info)
}

func newLoaderInner(source SourceType, sourceInfo SourceInfo) *Loader {
	l := &Loader{
		mu:         sync.RWMutex{},
		source:     source,
		sourceInfo: sourceInfo,
		v:          viper.New(),
	}

	if source == SourceLocal {
		l.v.SetConfigFile(sourceInfo.filePath)
	}

	return l
}

// 加载配置
//
// 读入 viper 与解码 target 必须在**同一把写锁**内完成：中间放开锁，
// 并发的热加载（Watch 的 OnConfigChange）会趁机把 viper 换成另一份配置，
// 解码结果就成了「新文件 + 旧内存」的混合体（此前的实现正是这样）。
func (l *Loader) Load(target any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.v.ReadInConfig(); err != nil {
		return err
	}
	return l.decode(target)
}

// decode 把当前 viper 状态解码到 target。调用方**必须**已持有 l.mu。
//
// 追加 TextUnmarshallerHookFunc：让实现了 encoding.TextUnmarshaler 的具名类型
// （如 data.StorageTier）可以在 YAML 里直接写字符串（tier: "TierRedisMySQL"）。
// 不加这个 hook 时，底层为整型的具名类型会被强行按数字解析并报
// `cannot parse 'tier' as 'data.StorageTier'` 导致整份配置加载失败。
//
// 前两个 hook 是 viper 的默认值，显式列出是因为一旦传了 DecodeHook 选项，
// viper 的默认 hook 就会被整体覆盖，必须自己补回来。
func (l *Loader) decode(target any) error {
	return l.v.Unmarshal(target, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		mapstructure.TextUnmarshallerHookFunc(),
	)))
}

func (l *Loader) Watch(callback WatchCallback) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if callback == nil {
		return errors.New("watch callback cannot be nil")
	}
	if l.watched {
		return errors.New("config watch already enabled")
	}

	l.v.WatchConfig()
	l.v.OnConfigChange(func(_ fsnotify.Event) {
		// 不持有锁，callback 内部调 GetXxx 自带锁
		// 如需 Load 重载，必须新开 goroutine，避免阻塞文件监听
		callback()
	})

	l.watched = true
	return nil
}

// Close 释放配置加载器资源并重置监听状态。
// 注意：viper 文件监听无法主动停止，本方法只把「已监听」状态复位（幂等）。
func (l *Loader) Close() error {
	l.mu.Lock()
	l.watched = false
	l.mu.Unlock()
	return nil
}

// 获得配置源信息
func (l *Loader) GetSource() SourceType {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.source
}

func (l *Loader) GetSourceFilePath() (string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.source != SourceLocal {
		return "", errors.New("current config source is not local file")
	}
	return l.sourceInfo.filePath, nil
}

// 获得配置内容
func (l *Loader) GetString(key string) string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.v.GetString(key)
}

func (l *Loader) GetInt(key string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.v.GetInt(key)
}

func (l *Loader) GetBool(key string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.v.GetBool(key)
}

func (l *Loader) GetFloat64(key string) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.v.GetFloat64(key)
}

func (l *Loader) GetStringSlice(key string) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.v.GetStringSlice(key)
}

func (l *Loader) GetStringMap(key string) map[string]any {
	l.mu.RLock()
	defer l.mu.RUnlock()
	raw := l.v.GetStringMap(key)
	return deepCopyStringMap(raw)
}

// deepCopyStringMap 递归深拷贝 map[string]any（含嵌套 map/slice），
// 避免外部持有并返回的副本仍与内部 viper 数据共享底层引用而被意外篡改。
func deepCopyStringMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyAny(v)
	}
	return out
}

// deepCopyAny 递归深拷贝任意值（仅对 map[string]any 与 []any 做深拷贝，其余按值返回）。
func deepCopyAny(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return deepCopyStringMap(x)
	case []any:
		cp := make([]any, len(x))
		for i, e := range x {
			cp[i] = deepCopyAny(e)
		}
		return cp
	default:
		return v
	}
}
