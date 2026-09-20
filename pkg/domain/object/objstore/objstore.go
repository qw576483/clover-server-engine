// Package objstore 通用游戏对象仓储的公开门面。
//
// 业务 / 框架层统一从本包引用对象仓储能力：
//
//	import "clover-server-engine/pkg/domain/object/objstore"
//	repo := objstore.NewRepository(store,
//	    objstore.WithGenerator(idgen.NewStoreGenerator(store, 0, 0)),
//	    objstore.WithManager(manager),
//	    objstore.WithNotifier(pub, "player.sync", 100, 200),
//	    objstore.WithAutoSync(),
//	)
//	g, _ := repo.Load(ctx, object.ParseObjectID("1:1001")) // 凭一个 id 读出玩家
//	g.Props().SetInt("gold", 999)                          // 改
//	repo.Save(ctx, g)                                      // 落库 + 自动同步（若开了 autosync）
//
// 常用对象类型常量复用 object 包（pkg/domain/object.TypePlayer 等）。
//
// 本包是**门面包**（见 `结构规则.md` §5.1）：真身全部在
// `internal/domain/object/objstore/facade.go`（`RepositoryFacade` 接口 + 包装实现 + 工厂），
// 这里只有**类型别名**与**变量转发**，不含实现体。
//
// Repository 在业务侧以接口形式暴露（接口 = 具体类型的方法集 + 协变 wrapper），
// 底层是 internal 的 *Repository；其返回的 *gobject.GameObject 被协变包装为
// 门面 gobject.GameObject 接口。如需还原 internal 侧，用 FromRepository / InternalRepository。
package objstore

import (
	igobject "clover-server-engine/internal/domain/object/gobject"
	iobjstore "clover-server-engine/internal/domain/object/objstore"
)

// Option 仓储构造选项：接收门面层 Repository 接口，按需注入可选组件。
type Option = iobjstore.OptionFacade

// ApplyResult 单对象批量操作的执行结果。
type ApplyResult = iobjstore.ApplyResult

// Repository 通用游戏对象仓储：组合造号、对象内核、持久化、消息派发与自动同步。
// F12 停在接口上即跳进 `internal/domain/object/objstore/facade.go` 看完整方法集与注释。
type Repository = iobjstore.RepositoryFacade

// GameObject 统一游戏对象（活体）：线上对象加载后常驻内存，业务直接读写 Props/Records。
type GameObject = igobject.GameObjectFacade

// 工厂与 internal ↔ 门面转换。
var (
	// NewRepository 构造对象仓储（必填 store + 可选组件）。
	// store 为门面 data.Store 接口，内部会还原为具体 *data.Store 交给引擎实现。
	NewRepository = iobjstore.NewRepositoryFacade
	// FromRepository 将 internal 的 *Repository 包装为门面 Repository 接口。
	FromRepository = iobjstore.FromRepositoryFacade
	// InternalRepository 将门面 Repository 接口还原为 internal 的 *Repository；非底层则 ok=false。
	InternalRepository = iobjstore.InternalRepositoryFacade
)

// 构造选项。
var (
	// WithManager 注入全局对象注册表（启用消息派发 + 在线查找）。
	WithManager = iobjstore.WithManagerFacade
	// WithGenerator 注入对象号生成器（启用 Create 造号）。g 为门面 idgen.Generator。
	// nil（含 Generator.WithNode 非法参数返回 nil 的链路）直接拒收并留痕。
	WithGenerator = iobjstore.WithGeneratorFacade
	// WithNotifier 注入 data-event 发布器（启用写即自动同步）。
	WithNotifier = iobjstore.WithNotifierFacade
	// WithAutoSync 开启「写即自动同步」（须在 WithNotifier 之后）。
	WithAutoSync = iobjstore.WithAutoSyncFacade
)

// 错误透传。
var (
	ErrNoGenerator     = iobjstore.ErrNoGenerator
	ErrNoManager       = iobjstore.ErrNoManager
	ErrVersionConflict = igobject.ErrVersionConflict
)
