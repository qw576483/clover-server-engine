// Package logstore 定义日志落盘后端的真身与注册表。
//
// 为什么需要它：log 服的后端必须能由**业务方通过配置**切换，而不是让业务方 fork 引擎改代码。
// 业务方有两种用法：
//
//  1. 用引擎内置后端：配置 `log_backend: "mysql"`（默认，零配置）；
//  2. 接入自有后端：实现 Backend 接口，在 init 里 app.RegisterLogBackend("my-xxx", factory)
//     注册，然后配置 `log_backend: "my-xxx"` + `log_backend_config: {...}`。
//
// 引擎只在装配期（runLog）按名字取一次工厂，运行期不再感知具体实现。
package logstore

import (
	"fmt"
	"sort"
	"sync"

	"clover-server-engine/pkg/foundation/logbuf"
)

// DefaultBackend 未配置 log_backend 时使用的内置后端名。
const DefaultBackend = "mysql"

// Backend 日志落盘后端：接收批量日志并落盘。
//
// 实现者只需关心「怎么把一批 LogEntry 存下去」，不必处理攒批、分片、多实例分发——
// 那些在 logbuf 与 log 服侧已经做完。
type Backend interface {
	// WriteBatch 写入一批日志，返回实际写入条数。
	WriteBatch(source string, entries []logbuf.LogEntry) (int, error)
	// Close 关闭后端并刷新未落盘的数据。
	Close() error
}

// Factory 按配置构造一个后端实例。
//
// raw 是配置里 `log_backend_config` 子节点的原始内容（可为 nil）——
// 引擎不解释它，原样交给工厂，参数结构由后端自己定义。
type Factory func(raw map[string]any) (Backend, error)

var (
	mu       sync.RWMutex
	backends = map[string]Factory{}
)

// Register 注册一个后端工厂，供配置里的 `log_backend` 按名字引用。
//
// 同名重复注册会**覆盖**（便于测试，也允许业务热替换自己的实现）。
// name 为空或 f 为 nil 属装配期错误，直接 panic —— fail-fast，不留到运行时才发现。
func Register(name string, f Factory) {
	if name == "" {
		panic("logstore: Register: empty backend name")
	}
	if f == nil {
		panic("logstore: Register: nil factory for backend " + name)
	}
	mu.Lock()
	backends[name] = f
	mu.Unlock()
}

// Get 按名字取工厂。
func Get(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := backends[name]
	return f, ok
}

// Names 返回已注册的自定义后端名（升序）。内置后端不在其中。
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(backends))
	for n := range backends {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Describe 返回「可用后端」描述，用于配置写错时的报错信息。
func Describe() string {
	names := Names()
	if len(names) == 0 {
		return fmt.Sprintf("%q (built-in)", DefaultBackend)
	}
	return fmt.Sprintf("%q (built-in), registered: %v", DefaultBackend, names)
}
