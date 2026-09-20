// package globalstore 提供「集群全局 KV」这一引擎级原语：跨进程/跨服共享的配置、标志位、
// 全局计数与在线状态。
//
// 强一致可选后端（etcd 生产 / 内存开发测试）、命名空间前缀隔离、TTL 过期、
// 原子 CAS / Incr、Watch 变更订阅（配置热加载、在线状态联动）、JSON 存取。
// 用 Go 接口 + 内存/etcd 双后端，并发安全、可纯内存单测。
//
// 设计定位：全局配置旗标 / 维护公告 / 跨服在线计数 / 特性开关的统一基础设施，
// 工具可直接复用。
package globalstore

import (
	"context"
	"errors"
	"time"

	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// ErrKeyNotFound 表示请求的 key 不存在。
var ErrKeyNotFound = errors.New("gstore: key not found")

// ErrClosed 表示 Store/Backend 已关闭。
var ErrClosed = errors.New("gstore: store closed")

// ErrNotInteger 表示 Incr 的目标值不是合法整数。
var ErrNotInteger = errors.New("gstore: value is not an integer")

// ErrIncrConflict 表示 etcd 后端 Incr 在重试上限内仍未能提交（竞争过烈）。
var ErrIncrConflict = errors.New("gstore: incr conflict after retries")

// EventType 变更事件类型。
type EventType int

const (
	// EventPut 写入（含新增与更新）。
	EventPut EventType = iota
	// EventDelete 删除。
	EventDelete
	// EventResync 镜像重建信号：底层事件流出现过「事件段被压缩、永久取不回来」的情形
	// （etcd 压缩重置），紧随其后会补发一份**完整快照** —— 本前缀下现存的每个键各一条 EventPut。
	// 调用方收到本事件后应丢弃按增量维护的本地镜像、按后续事件重建；快照里没出现的键视为已不存在。
	EventResync
)

// Event 键值变更事件（Watch 回调参数）。
// Key 为相对 Store 命名空间的逻辑键（已剥除命名空间前缀）。
type Event struct {
	Type  EventType
	Key   string
	Value string
}

// Backend 底层 KV 存储抽象；Store 在其上叠加命名空间前缀。
// 任何后端（内存 / etcd / 其他）只需实现本接口即可接入。
type Backend interface {
	// Get 读取 key；不存在返回 ErrKeyNotFound。
	Get(ctx context.Context, key string) (string, error)
	// Put 写入 key，ttl>0 表示写入后经过该时长自动过期（ttl<=0 表示不过期）。
	Put(ctx context.Context, key, value string, ttl time.Duration) error
	// Delete 删除 key；key 不存在视为成功（幂等）。
	Delete(ctx context.Context, key string) error
	// Exists 判断 key 是否存在。
	Exists(ctx context.Context, key string) (bool, error)
	// CAS 比较并交换：old 为期望当前值；old 为空串表示「仅当 key 不存在时设置」
	// （put-if-absent）。成功交换返回 true。
	CAS(ctx context.Context, key, old, new string) (bool, error)
	// Incr 原子增减计数；key 不存在视为 0；返回新值；当前值非整数返回 ErrNotInteger。
	Incr(ctx context.Context, key string, delta int64) (int64, error)
	// List 列出 prefix 下全部键值；返回的 key 为带此前缀的完整键；无匹配返回空 map。
	List(ctx context.Context, prefix string) (map[string]string, error)
	// BatchPut 原子批量写入：要么整批生效，要么整批不生效。空 map 视为成功。
	BatchPut(ctx context.Context, kv map[string]string) error
	// Watch 监听 prefix 下所有 key 的变更；事件 Key 为带此前缀的完整键。
	Watch(ctx context.Context, prefix string, cb func(Event)) error
	// Close 释放底层资源。
	Close() error
}

// PutOption 定制 Put 行为。
type PutOption func(*putOpts)

type putOpts struct{ ttl time.Duration }

// WithTTL 设置写入键的过期时长（>0）。
func WithTTL(d time.Duration) PutOption { return func(o *putOpts) { o.ttl = d } }

// Store 带命名空间前缀的 KV 存储门面，对业务暴露干净的读写 API。
// 命名空间前缀自动作用于所有键，避免不同业务互相覆盖，也便于按前缀 Watch。
type Store struct {
	b      Backend
	prefix string
}

// NewStore 用给定 Backend 构造带前缀的 Store。prefix 为空时退化为 "/"（根命名空间）。
func NewStore(b Backend, prefix string) *Store {
	if prefix == "" {
		prefix = "/"
	}
	return &Store{b: b, prefix: prefix}
}

func (s *Store) full(key string) string { return s.prefix + key }

// Get 读取逻辑键；不存在返回 ErrKeyNotFound。
func (s *Store) Get(ctx context.Context, key string) (string, error) {
	return s.b.Get(ctx, s.full(key))
}

// GetBytes 读取逻辑键为字节切片。
func (s *Store) GetBytes(ctx context.Context, key string) ([]byte, error) {
	v, err := s.b.Get(ctx, s.full(key))
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

// Exists 判断逻辑键是否存在。
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	return s.b.Exists(ctx, s.full(key))
}

// Put 写入逻辑键。
func (s *Store) Put(ctx context.Context, key, value string, opts ...PutOption) error {
	o := putOpts{}
	for _, fn := range opts {
		fn(&o)
	}
	return s.b.Put(ctx, s.full(key), value, o.ttl)
}

// PutJSON 把任意可序列化值以 JSON 写入逻辑键。
func (s *Store) PutJSON(ctx context.Context, key string, v any, opts ...PutOption) error {
	data, err := ujson.Marshal(v)
	if err != nil {
		return err
	}
	return s.Put(ctx, key, string(data), opts...)
}

// GetJSON 把逻辑键的 JSON 值反序列化到 v。
func (s *Store) GetJSON(ctx context.Context, key string, v any) error {
	data, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	return ujson.Unmarshal([]byte(data), v)
}

// Delete 删除逻辑键。
func (s *Store) Delete(ctx context.Context, key string) error {
	return s.b.Delete(ctx, s.full(key))
}

// CAS 比较并交换逻辑键。
func (s *Store) CAS(ctx context.Context, key, old, new string) (bool, error) {
	return s.b.CAS(ctx, s.full(key), old, new)
}

// Incr 原子增减逻辑键计数，返回新值。
func (s *Store) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return s.b.Incr(ctx, s.full(key), delta)
}

// Watch 监听前缀（逻辑键）下所有变更；事件 Key 已剥除命名空间前缀，为逻辑键。
// 返回后变更通过 cb 异步推送；ctx 取消即停止该订阅。
func (s *Store) Watch(ctx context.Context, prefix string, cb func(Event)) error {
	return s.b.Watch(ctx, s.full(prefix), func(ev Event) {
		if s.prefix != "" && len(ev.Key) >= len(s.prefix) && ev.Key[:len(s.prefix)] == s.prefix {
			ev.Key = ev.Key[len(s.prefix):]
		}
		cb(ev)
	})
}

// List 列出逻辑前缀下全部键值；返回键为逻辑键，与 Watch 事件 Key 一致，
// 可直接用于 Get / Delete / CAS；无匹配返回空 map。
func (s *Store) List(ctx context.Context, prefix string) (map[string]string, error) {
	raw, err := s.b.List(ctx, s.full(prefix))
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		out[s.strip(k)] = v
	}
	return out, nil
}

// BatchPut 原子批量写入逻辑键：要么整批生效，要么整批不生效。
func (s *Store) BatchPut(ctx context.Context, kv map[string]string) error {
	if len(kv) == 0 {
		return nil
	}
	full := make(map[string]string, len(kv))
	for k, v := range kv {
		full[s.full(k)] = v
	}
	return s.b.BatchPut(ctx, full)
}

// strip 剥除命名空间前缀，把完整键还原为逻辑键。
// 判据用 >=（与 Watch 的剥离口径一致）：键恰好等于命名空间前缀时，
// List 也剥成空串，而不再原样返回带前缀的键——否则同一份数据在 List 与 Watch
// 两条路径下键的形状不一致。
func (s *Store) strip(fullKey string) string {
	if s.prefix != "" && len(fullKey) >= len(s.prefix) && fullKey[:len(s.prefix)] == s.prefix {
		return fullKey[len(s.prefix):]
	}
	return fullKey
}

// Close 释放底层资源。
func (s *Store) Close() error { return s.b.Close() }
