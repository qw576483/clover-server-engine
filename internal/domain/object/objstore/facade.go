// facade.go 是通用游戏对象仓储的**门面真身**：把 internal 的具体 *Repository
// 协变包装成业务可用的接口（`RepositoryFacade`，业务侧可见名 `objstore.Repository`），
// 并把其返回的 *gobject.GameObject 一并协变包装为门面 GameObject 接口。
//
// 为什么真身必须在 internal：`pkg/**` 只允许做门面（类型别名 / 变量转发 / 极薄适配），
// 见 `结构规则.md` §5.1；本文件里的包装与工厂是行为实现体，故落在 internal。
// `pkg/domain/object/objstore`（门面包）只持有别名与变量转发。
//
// 命名：接口 / 选项 / 工厂加 `Facade` 后缀，以避开本包既有的同名 struct 与同名选项
// （`Repository` / `Option` / `NewRepository` / `WithManager` …）。
package objstore

import (
	"context"
	"fmt"

	"clover-server-engine/internal/domain/data"
	object "clover-server-engine/internal/domain/object"
	igobject "clover-server-engine/internal/domain/object/gobject"
	pkgdata "clover-server-engine/pkg/domain/data"
	"clover-server-engine/pkg/domain/object/idgen"
	"clover-server-engine/pkg/foundation/logger"
)

// OptionFacade 仓储构造选项：接收门面层 RepositoryFacade 接口，按需注入可选组件。
// 业务侧可见名 = `pkg/domain/object/objstore.Option`。
type OptionFacade func(RepositoryFacade)

// RepositoryFacade 通用游戏对象仓储：组合造号、对象内核、持久化、消息派发与自动同步。
// 业务侧可见名 = `pkg/domain/object/objstore.Repository`。
type RepositoryFacade interface {
	// Create 分配一个新对象号并返回绑定仓库 store 的空对象（不自动注册/落库）。
	Create(ctx context.Context, typ uint16) (igobject.GameObjectFacade, error)
	// Load 从 store 按对象号载入对象（props + 全部表）；缺失对象返回空对象不报错。
	Load(ctx context.Context, id object.ObjectID) (igobject.GameObjectFacade, error)
	// Save 落库对象并 bump 乐观版本（强制覆盖，不做并发检查）。
	Save(ctx context.Context, g igobject.GameObjectFacade) error
	// SaveVersioned 乐观并发写回：版本不符返回 ErrVersionConflict 且不写库。
	SaveVersioned(ctx context.Context, g igobject.GameObjectFacade, expected int64) error
	// Delete 彻底删除对象数据并从 Manager 注销（若已注册）。
	Delete(ctx context.Context, id object.ObjectID) error
	// Exists 判断对象数据是否存在。
	Exists(ctx context.Context, id object.ObjectID) (bool, error)
	// Register 把对象注册到 Manager（上线），使其可被 Send 按号派发消息。
	Register(g igobject.GameObjectFacade) error
	// Unregister 从 Manager 注销对象（下线）。
	Unregister(id object.ObjectID) error
	// Get 取已注册（在线）对象；未注册或仓库无 Manager 返回 (nil, false)。
	Get(id object.ObjectID) (igobject.GameObjectFacade, bool)
	// Handle 绑定 (对象类型, 消息类型) → 处理器（转发到 Manager）。
	Handle(objType uint16, msgType uint32, h object.Handler) error
	// Send 给指定对象号发消息（目标须已 Register）。
	Send(ctx context.Context, id object.ObjectID, msg object.Message) error
	// BulkGet 批量加载一组对象，返回 id → 对象的映射。
	BulkGet(ctx context.Context, ids []object.ObjectID) (map[object.ObjectID]igobject.GameObjectFacade, error)
	// BulkApply 批量改：逐个 检查→载入→应用 fn→落库，逐对象返回结果（互不影响）。
	BulkApply(ctx context.Context, ids []object.ObjectID, fn func(g igobject.GameObjectFacade) error) ([]ApplyResult, error)
	// BulkDelete 批量删除一组对象（幂等），返回实际被删除的 id 列表。
	BulkDelete(ctx context.Context, ids []object.ObjectID) ([]object.ObjectID, error)
}

// repositoryFacade 门面 RepositoryFacade 接口的实现：包装 internal 的 *Repository
// 并做 covariant wrapper（具体类型 → 门面接口）。
type repositoryFacade struct{ inner *Repository }

// Create 分配一个新对象号并返回绑定仓库 store 的空对象（不自动注册/落库）。
func (r *repositoryFacade) Create(ctx context.Context, typ uint16) (igobject.GameObjectFacade, error) {
	g, err := r.inner.Create(ctx, typ)
	if err != nil {
		return nil, err
	}
	return igobject.FromGameObjectFacade(g), nil
}

// Load 从 store 按对象号载入对象（props + 全部表）；缺失对象返回空对象不报错。
func (r *repositoryFacade) Load(ctx context.Context, id object.ObjectID) (igobject.GameObjectFacade, error) {
	g, err := r.inner.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	return igobject.FromGameObjectFacade(g), nil
}

// Save 落库对象并 bump 乐观版本（强制覆盖，不做并发检查）。
func (r *repositoryFacade) Save(ctx context.Context, g igobject.GameObjectFacade) error {
	ig, ok := igobject.InternalGameObjectFacade(g)
	if !ok || ig == nil {
		// 非引擎实现（或 typed-nil）：内部会对 nil 对象解引用 panic，此处显式报错。
		logger.Warnf("objstore.Save: object is not an engine GameObject impl (%T)", g)
		return fmt.Errorf("objstore: Save: unsupported GameObject implementation (%T)", g)
	}
	return r.inner.Save(ctx, ig)
}

// SaveVersioned 乐观并发写回：版本不符返回 ErrVersionConflict 且不写库。
func (r *repositoryFacade) SaveVersioned(ctx context.Context, g igobject.GameObjectFacade, expected int64) error {
	ig, ok := igobject.InternalGameObjectFacade(g)
	if !ok || ig == nil {
		logger.Warnf("objstore.SaveVersioned: object is not an engine GameObject impl (%T)", g)
		return fmt.Errorf("objstore: SaveVersioned: unsupported GameObject implementation (%T)", g)
	}
	return r.inner.SaveVersioned(ctx, ig, expected)
}

// Delete 彻底删除对象数据并从 Manager 注销（若已注册）。
func (r *repositoryFacade) Delete(ctx context.Context, id object.ObjectID) error {
	return r.inner.Delete(ctx, id)
}

// Exists 判断对象数据是否存在。
func (r *repositoryFacade) Exists(ctx context.Context, id object.ObjectID) (bool, error) {
	return r.inner.Exists(ctx, id)
}

// Register 把对象注册到 Manager（上线），使其可被 Send 按号派发消息。
func (r *repositoryFacade) Register(g igobject.GameObjectFacade) error {
	ig, ok := igobject.InternalGameObjectFacade(g)
	if !ok || ig == nil {
		logger.Warnf("objstore.Register: object is not an engine GameObject impl (%T)", g)
		return fmt.Errorf("objstore: Register: unsupported GameObject implementation (%T)", g)
	}
	return r.inner.Register(ig)
}

// Unregister 从 Manager 注销对象（下线）。
func (r *repositoryFacade) Unregister(id object.ObjectID) error { return r.inner.Unregister(id) }

// Get 取已注册（在线）对象；未注册或仓库无 Manager 返回 (nil, false)。
func (r *repositoryFacade) Get(id object.ObjectID) (igobject.GameObjectFacade, bool) {
	g, ok := r.inner.Get(id)
	if !ok {
		return nil, false
	}
	return igobject.FromGameObjectFacade(g), true
}

// Handle 绑定 (对象类型, 消息类型) → 处理器（转发到 Manager）。
func (r *repositoryFacade) Handle(objType uint16, msgType uint32, h object.Handler) error {
	return r.inner.Handle(objType, msgType, h)
}

// Send 给指定对象号发消息（目标须已 Register）。
func (r *repositoryFacade) Send(ctx context.Context, id object.ObjectID, msg object.Message) error {
	return r.inner.Send(ctx, id, msg)
}

// BulkGet 批量加载一组对象，返回 id → 对象的映射。
func (r *repositoryFacade) BulkGet(ctx context.Context, ids []object.ObjectID) (map[object.ObjectID]igobject.GameObjectFacade, error) {
	m, err := r.inner.BulkGet(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[object.ObjectID]igobject.GameObjectFacade, len(m))
	for k, v := range m {
		out[k] = igobject.FromGameObjectFacade(v)
	}
	return out, nil
}

// BulkApply 批量改：逐个 检查→载入→应用 fn→落库，逐对象返回结果（互不影响）。
func (r *repositoryFacade) BulkApply(ctx context.Context, ids []object.ObjectID, fn func(g igobject.GameObjectFacade) error) ([]ApplyResult, error) {
	return r.inner.BulkApply(ctx, ids, func(g *igobject.GameObject) error {
		return fn(igobject.FromGameObjectFacade(g))
	})
}

// BulkDelete 批量删除一组对象（幂等），返回实际被删除的 id 列表。
func (r *repositoryFacade) BulkDelete(ctx context.Context, ids []object.ObjectID) ([]object.ObjectID, error) {
	return r.inner.BulkDelete(ctx, ids)
}

// fromRepositoryFacade 从 internal 的具体 *Repository 构造门面接口（空指针返回 nil 接口）。
func fromRepositoryFacade(r *Repository) RepositoryFacade {
	if r == nil {
		return nil
	}
	return &repositoryFacade{inner: r}
}

// 编译期断言：internal 的 *Repository 满足其协变后的方法集（返回类型差异由 wrapper 承接，故不断言直接满足）。
var _ RepositoryFacade = (*repositoryFacade)(nil)

// FromRepositoryFacade 将 internal 的 *Repository 包装为门面 RepositoryFacade 接口。
// 业务侧可见名 = `pkg/domain/object/objstore.FromRepository`。
func FromRepositoryFacade(r *Repository) RepositoryFacade { return fromRepositoryFacade(r) }

// InternalRepositoryFacade 将门面 RepositoryFacade 接口还原为 internal 的 *Repository；非底层则 ok=false。
// 业务侧可见名 = `pkg/domain/object/objstore.InternalRepository`。
func InternalRepositoryFacade(r RepositoryFacade) (*Repository, bool) {
	if v, ok := r.(*repositoryFacade); ok {
		return v.inner, true
	}
	return nil, false
}

// NewRepositoryFacade 构造对象仓储（必填 store + 可选组件），并以门面接口返回。
// store 为门面 data.Store 接口，内部会还原为具体 *data.Store 交给引擎实现。
// 业务侧可见名 = `pkg/domain/object/objstore.NewRepository`。
func NewRepositoryFacade(store pkgdata.Store, opts ...OptionFacade) RepositoryFacade {
	is, ok := data.InternalStore(store)
	if !ok || is == nil {
		// 与 gobject.New 同口径：非 internal 底层（含装箱 nil）不构造半成品，
		// 否则后续调用会在 nil 内部仓储上 panic。
		logger.Warnf("objstore.NewRepository: store is not an engine data.Store impl (%T); repository not created", store)
		return nil
	}
	iOpts := make([]Option, len(opts))
	for i, o := range opts {
		fn := o
		iOpts[i] = func(r *Repository) {
			fn(fromRepositoryFacade(r))
		}
	}
	return fromRepositoryFacade(NewRepository(is, iOpts...))
}

// WithManagerFacade 注入全局对象注册表（启用消息派发 + 在线查找）。
// 业务侧可见名 = `pkg/domain/object/objstore.WithManager`。
func WithManagerFacade(m *object.Manager) OptionFacade {
	// object 域的真身就在 pkg/domain/object（此处经 bridge.go 重导出，两边是同一个类型），
	// 不存在「另一份 internal 版 Manager」，所以无需解包 —— m 直接可用。
	return func(r RepositoryFacade) {
		ri, ok := InternalRepositoryFacade(r)
		if ok {
			WithManager(m)(ri)
		}
	}
}

// WithGeneratorFacade 注入对象号生成器（启用 Create 造号）。
// g 为门面 idgen.Generator（底层即 internal 具体类型）。
// nil（含 Generator.WithNode 非法参数返回 nil 的链路）直接拒收并留痕：
// 静默注入 nil 会让 Create 在后续才以 ErrNoGenerator 失败，脱离出错现场。
// 业务侧可见名 = `pkg/domain/object/objstore.WithGenerator`。
func WithGeneratorFacade(g *idgen.Generator) OptionFacade {
	if g == nil {
		logger.Warnf("objstore.WithGenerator: nil generator ignored (Create will return ErrNoGenerator)")
		return func(RepositoryFacade) {}
	}
	return func(r RepositoryFacade) {
		ri, ok := InternalRepositoryFacade(r)
		if ok {
			WithGenerator(g)(ri)
		}
	}
}

// WithNotifierFacade 注入 data-event 发布器（启用写即自动同步）。
// 业务侧可见名 = `pkg/domain/object/objstore.WithNotifier`。
func WithNotifierFacade(pub pkgdata.Publisher, subject string, propMsg, recMsg uint32) OptionFacade {
	return func(r RepositoryFacade) {
		ri, ok := InternalRepositoryFacade(r)
		if ok {
			WithNotifier(pub, subject, propMsg, recMsg)(ri)
		}
	}
}

// WithAutoSyncFacade 开启「写即自动同步」（须在 WithNotifier 之后）。
// 业务侧可见名 = `pkg/domain/object/objstore.WithAutoSync`。
func WithAutoSyncFacade() OptionFacade {
	return func(r RepositoryFacade) {
		ri, ok := InternalRepositoryFacade(r)
		if ok {
			WithAutoSync()(ri)
		}
	}
}
