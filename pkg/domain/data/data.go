// Package data 数据层领域门面：对外提供数据存储抽象（Store / Record）、
// Schema 定义与类型注册、以及工厂函数入口。
//
// 结构方向（见 结构规则.md §五）：**接口与类型真身在本包**；底层实现（Store / Config 真身）
// 留在 internal/domain/data，由其 init() 经下方 Register*Factory 注册进来
//（P3 注册钩子，database/sql 式），因此本包不 import internal。

// 业务使用本包的方式只有一条路径：
//  1. 用 StructSchema / RecordSchema 声明自己的数据表（Type / OwnerType / Visibility / Tier）；
//  2. 在 init() 中调 RegisterTypeBySchema 注册；
//  3. 读写走 app.Game 的 LoadStruct / LoadRecord（自动落库 + 广播），不直接操作 Store。

// Store 及其配套（Key / Config / NewStore / ErrNotFound）保留，是因为
// 引擎装配链路与测试代码需要直接构造 data.Store，属于公开构造入口而非业务日常 API。
package data

import (
	"context"
	"errors"

	obj "github.com/qw576483/clover-server-engine/pkg/domain/object"
)

// 数据存储抽象（Store 接口）
// Store 数据存储抽象接口。业务层只依赖此接口，不感知底层 Redis / MySQL 实现。
// 由 internal 的 *data.Store 满足。
// 按「最小暴露」原则仅暴露业务实际可达的读写方法：Save / Load / SaveJSON / LoadJSON / Delete。
// 生命周期与连接管理（CreateTable / Close / Tier / Table）属引擎装配细节，不在门面暴露。
type Store interface {
	// Save 写入一条数据。data 为原始字节（二进制或 JSON 原文均可）。
	Save(ctx context.Context, key Key, data []byte) error
	// Load 读取一条数据，返回原始字节；不存在返回 ErrNotFound。
	Load(ctx context.Context, key Key) ([]byte, error)
	// SaveJSON 将 v 序列化为 JSON 后写入。
	SaveJSON(ctx context.Context, key Key, v any) error
	// LoadJSON 读取并反序列化 JSON 到 v；不存在返回 ErrNotFound。
	LoadJSON(ctx context.Context, key Key, v any) error
	// Delete 删除一条数据。
	Delete(ctx context.Context, key Key) error
}

// Config data 模块配置的不透明句柄：业务经 NewStore / MemoryConfig 等工厂函数使用，
// 不直接构造也不访问其字段（字段含引擎内部的 Redis/MySQL 配置类型）。
// 具体真身由引擎装配层创建，本包只负责原样透传。
type Config any

var (
	newStoreFn      func(Config) (Store, error)
	memoryConfigFn  func() Config
	defaultConfigFn func() Config
	mmoConfigFn     func() Config
	redisConfigFn   func() Config
)

// RegisterStoreFactory 注册底层 NewStore 实现（由 internal/domain/data init() 调用）。
// 注册 nil 视为装配错误：与「未注册」无法区分，且会把问题推迟到调用期。
func RegisterStoreFactory(fn func(Config) (Store, error)) {
	if fn == nil {
		panic("data: RegisterStoreFactory: nil factory")
	}
	newStoreFn = fn
}

// RegisterMemoryConfig 注册 MemoryConfig 实现（nil 视为装配错误）。
func RegisterMemoryConfig(fn func() Config) {
	if fn == nil {
		panic("data: RegisterMemoryConfig: nil factory")
	}
	memoryConfigFn = fn
}

// RegisterDefaultConfig 注册 DefaultConfig 实现（nil 视为装配错误）。
func RegisterDefaultConfig(fn func() Config) {
	if fn == nil {
		panic("data: RegisterDefaultConfig: nil factory")
	}
	defaultConfigFn = fn
}

// RegisterMMOConfig 注册 MMOConfig 实现（nil 视为装配错误）。
func RegisterMMOConfig(fn func() Config) {
	if fn == nil {
		panic("data: RegisterMMOConfig: nil factory")
	}
	mmoConfigFn = fn
}

// RegisterRedisConfig 注册 RedisConfig 实现（nil 视为装配错误）。
func RegisterRedisConfig(fn func() Config) {
	if fn == nil {
		panic("data: RegisterRedisConfig: nil factory")
	}
	redisConfigFn = fn
}

// errFactoryNotRegistered 装配缺口：工厂未注册。
var errFactoryNotRegistered = errors.New("data: factory not registered (internal/domain/data not linked?)")

// NewStore 根据配置创建数据存储实例。
// internal 未注册（未链接注册侧）时返回明确错误，而不是对 nil 函数指针调用 panic。
func NewStore(cfg Config) (Store, error) {
	if newStoreFn == nil {
		return nil, errFactoryNotRegistered
	}
	return newStoreFn(cfg)
}

// MemoryConfig 返回纯内存存储配置（适合本地开发/测试）。
func MemoryConfig() Config { return mustConfigFactory(memoryConfigFn, "MemoryConfig") }

// DefaultConfig 返回默认的 Redis + MySQL 存储配置。
func DefaultConfig() Config { return mustConfigFactory(defaultConfigFn, "DefaultConfig") }

// MMOConfig 返回 MMO 快照存储配置。
func MMOConfig() Config { return mustConfigFactory(mmoConfigFn, "MMOConfig") }

// RedisConfig 返回纯 Redis 存储配置。
func RedisConfig() Config { return mustConfigFactory(redisConfigFn, "RedisConfig") }

// mustConfigFactory 调用配置工厂；未注册时 panic 并给出准确原因
// （这几个 Getter 无 error 返回位，与 room.NewModule 的显式 panic 口径一致）。
func mustConfigFactory(fn func() Config, name string) Config {
	if fn == nil {
		panic("data: " + name + " factory not registered (internal/domain/data not linked?)")
	}
	return fn()
}

// Record：强类型行列表
// Record 强类型表接口：列名+列类型定义 schema，单元格 (row,col) 强类型读写，支持增量脏行推送。
type Record interface {
	// ColCount 返回列数。
	ColCount() int
	// ColName 返回第 col 列的列名（越界返回空串）。
	ColName(col int) string
	// ColType 返回第 col 列的列类型。
	ColType(col int) obj.Type
	// RowCount 返回当前行数。
	RowCount() int
	// MaxRows 返回建议容量上限（0 表示不限）。
	MaxRows() int
	// SetMaxRows 设置建议容量上限。
	SetMaxRows(n int)
	// AddColumn 追加一列（扩展 schema），现有行用该类型零值补齐，并标记结构性变动。
	AddColumn(name string, typ obj.Type)
	// AddRow 追加一行全零值，返回行索引；超过 MaxRows（>0）时返回 -1。
	AddRow() int
	// AddRowValues 追加一行给定值，返回行索引；长度不符补零/截断，超过上限返回 -1。
	AddRowValues(vals []obj.Value) int
	// DeleteRow 删除第 row 行（行级增量同步），返回是否成功。
	DeleteRow(row int) bool
	// Clear 清空所有行。
	Clear()
	// SetCell 写单元格 (row,col)；值真正变化才标记该行脏。
	SetCell(row, col int, v obj.Value)
	// SetNotifier 注册表变动即时回调（行级自动同步底座），传 nil 注销回调。
	SetNotifier(fn func(row, col int, v obj.Value, full bool))
	// GetCell 读单元格 (row,col)，越界返回零值。
	GetCell(row, col int) obj.Value
	// Row 返回第 row 行的值拷贝（越界返回 nil）。
	Row(row int) []obj.Value
	// ColIndex 根据列名返回列索引，未找到返回 -1。
	ColIndex(name string) int
	// Find 查找指定列中值与 v 相等的第一行，返回行索引；未找到返回 -1。
	Find(col any, v obj.Value) int
	// MarkClean 清除脏标记（落库 / 广播后调用）。
	MarkClean()
	// RowDirty 第 row 行自上次 MarkClean 是否变动。
	RowDirty(row int) bool
	// DirtyRows 返回变动行索引集合（升序）。
	DirtyRows() []int
	// FullDirty 是否发生结构性变动（需整表重同步）。
	FullDirty() bool
	// CommitDiff 生成增量同步字节流，第二个返回值表示是否整表重推；成功序列化后消费脏标记。
	CommitDiff() ([]byte, bool)
	// Changes 返回变动行快照、删除行索引、是否清空、是否整表重同步（schema 变更）。
	Changes() (map[int][]obj.Value, []int, bool, bool)
}

// 发布接口（event / objstore / mmo 的下行同步选项依赖它）
// Publisher 发布接口（解耦具体消息总线，便于测试用内存实现）。
type Publisher interface {
	Publish(subject string, data []byte) error
}
