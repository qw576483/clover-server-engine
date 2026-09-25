// Package objstore 通用游戏对象仓储：把 base 已有对象内核能力「合体」为一句话读改任意对象的门面。

// 本包 Repository 经 idgen 统一造号（稳定可寻址的 object.ObjectID）+ data.Store 持久化，
// 只要持有一个 ObjectID，即可 Load(id) → 改 Props/Record → Save(id) 完成读改。
// Repository 可挂接 object.Manager（消息按号派发）、注入 data.Publisher（写即自动同步），
// 对象内的 Bag / Record 已是标准化、行级变动可追踪的数据格式，与客户端共用编码。

// 本包是「统一游戏对象内核（gobject.GameObject）」之上的仓储层：它负责对象的
// 造号（idgen）/ 加载（data.Store）/ 注册（object.Manager）/ 同步（data.Publisher）四项职责，
// 让各架构用同一套 API 操作对象，无需各自重复造轮子。
package objstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/internal/domain/object/gobject"
	"github.com/qw576483/clover-server-engine/pkg/domain/object/idgen"
)

// 错误定义。
var (
	// ErrNoGenerator 仓库未配置 idgen.Generator 时调用 Create 报错。
	ErrNoGenerator = errors.New("objstore: repository has no id generator (Create requires WithGenerator)")
	// ErrNoManager 仓库未配置 object.Manager 时调用 Register/Send/Get/Handle 报错。
	ErrNoManager = errors.New("objstore: repository has no manager (Register/Send/Get/Handle require WithManager)")
)

// Repository 通用游戏对象仓储：组合 idgen(造号) + gobject(对象内核) + data.Store(持久化)
// + object.Manager(消息派发) + data.Publisher(写即自动同步) 为一句极简 API。

// 一个进程通常持有一个 Repository 实例（共享同一个 data.Store）
// 都从它读改对象。所有组件均可选：不配 idgen 则只能 Load 已有对象；不配 Manager 则只能离线读改；
// 不配 Publisher 则改动仅落库不广播。
type Repository struct {
	store    *data.Store
	manager  *object.Manager
	gen      *idgen.Generator
	pub      data.Publisher
	subject  string
	propMsg  uint32
	recMsg   uint32
	autoSync bool
}

// Option 仓库可选配置。
type Option func(*Repository)

// WithManager 注入全局对象注册表（启用「给对象号发消息」+ 在线查找）。
func WithManager(m *object.Manager) Option { return func(r *Repository) { r.manager = m } }

// WithGenerator 注入对象号生成器（启用 Create 造号）。
func WithGenerator(g *idgen.Generator) Option { return func(r *Repository) { r.gen = g } }

// WithNotifier 注入 data-event 发布器，使对象的变动经它自动广播（写即自动同步的前置条件）。
// propMsg / recMsg 分别为 props 与 slice 的广播消息号。
func WithNotifier(pub data.Publisher, subject string, propMsg, recMsg uint32) Option {
	return func(r *Repository) {
		r.pub = pub
		r.subject = subject
		r.propMsg = propMsg
		r.recMsg = recMsg
	}
}

// WithAutoSync 开启「写即自动同步」：对象任意 Props/Record 变动立即推送增量，无需显式 Save。
// 须在 WithNotifier 之后（无发布器则无效）。
func WithAutoSync() Option { return func(r *Repository) { r.autoSync = true } }

// NewRepository 用必填的 data.Store 构造仓储，并按需注入可选组件。
func NewRepository(store *data.Store, opts ...Option) *Repository {
	r := &Repository{store: store}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Store 返回仓储底层的 data.Store（只读访问，便于上层工具按需直接读写，
// 如 txn 包做事务回滚时把快照写回）。
func (r *Repository) Store() *data.Store { return r.store }

// wireSync 给一个新构造的 GameObject 接上同步（若仓库配置了 publisher）。
func (r *Repository) wireSync(g *gobject.GameObject) {
	if r.pub == nil {
		return
	}
	g.SetNotifier(r.pub, r.subject, r.propMsg, r.recMsg)
	if r.autoSync {
		g.EnableAutoSync()
	}
}

// Create 分配一个新对象号（须经 WithGenerator），返回绑定到仓库 store 的空 GameObject。
// 不自动注册到 Manager（对象需「上线」时再 Register），也不自动落库（设置字段后调用 Save）。
func (r *Repository) Create(ctx context.Context, typ uint16) (*gobject.GameObject, error) {
	if r.gen == nil {
		return nil, ErrNoGenerator
	}
	id, err := r.gen.Next(typ)
	if err != nil {
		return nil, err
	}
	g := gobject.New(r.store, id)
	r.wireSync(g)
	return g, nil
}

// Load 从 store 按 ObjectID 载入对象（props + 所有预定义表 + 子对象）。若仓库配了 notifier，
// 载入后自动接上「写即自动同步」。不自动注册到 Manager（离线读改友好：凭一个 id 即可读出/改）。
// 注意：缺失的对象返回空 GameObject（不报错），可用 Exists 判断是否真存在。
func (r *Repository) Load(ctx context.Context, id object.ObjectID) (*gobject.GameObject, error) {
	g := gobject.New(r.store, id)
	r.wireSync(g)
	if err := g.Load(ctx); err != nil {
		return nil, err
	}
	return g, nil
}

// Save 落库（props + 所有表 + 子对象）；若已接 notifier，会自动广播变动部分
// （字段级 patch / 行级 rows，呼应 MMO SyncRecChange）。

// Save 是「强制覆盖」：始终落库并把对象版本 +1，
// 不做并发检查。需要安全写回（避免覆盖在线玩家/其他 GM 在你编辑期间的改动）时，
// 请用 SaveVersioned 带上读取时的版本号。
func (r *Repository) Save(ctx context.Context, g *gobject.GameObject) error {
	return g.Save(ctx)
}

// SaveVersioned 乐观并发写回：
// 带上 Load 时记住的版本号 expected，若持久化版本已不等（对象被他人改动），
// 立即返回 gobject.ErrVersionConflict 且不写库；否则等价于 Save（落库 + 版本 +1）。

// 典型用法：

// g, _ := repo.Load(ctx, id)
// ver := g.Version()            // 记住读取时的版本
// g.Props().SetInt("gold", 999) // 用户编辑
//
//	if err := repo.SaveVersioned(ctx, g, ver); errors.Is(err, gobject.ErrVersionConflict) {
//	    // 数据已被在线玩家改动，提示用户重新拉取再改
//	}
func (r *Repository) SaveVersioned(ctx context.Context, g *gobject.GameObject, expected int64) error {
	return g.SaveIfVersion(ctx, expected)
}

// Delete 删除对象全部数据（props / 表 / 子对象，递归）并从其 Manager 注销（若已注册）。
func (r *Repository) Delete(ctx context.Context, id object.ObjectID) error {
	g := gobject.New(r.store, id)
	if err := g.Delete(ctx); err != nil {
		return err
	}
	if r.manager != nil {
		r.manager.Unregister(id)
	}
	return nil
}

// Exists 判断对象数据是否存在（查 props 键）。
func (r *Repository) Exists(ctx context.Context, id object.ObjectID) (bool, error) {
	key := data.Key{Owner: data.OwnerObject, ID: id.String(), Type: "props"}
	_, err := r.store.Load(ctx, key)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Register 把对象注册到 Manager（「上线」），使其可被 Send 按号派发消息。
func (r *Repository) Register(g *gobject.GameObject) error {
	if r.manager == nil {
		return ErrNoManager
	}
	r.manager.Register(g)
	return nil
}

// Unregister 从 Manager 注销对象（「下线」）。
func (r *Repository) Unregister(id object.ObjectID) error {
	if r.manager == nil {
		return ErrNoManager
	}
	r.manager.Unregister(id)
	return nil
}

// Get 取已注册（在线）对象；未注册或仓库无 Manager 返回 (nil, false)。
func (r *Repository) Get(id object.ObjectID) (*gobject.GameObject, bool) {
	if r.manager == nil {
		return nil, false
	}
	o, ok := r.manager.Get(id)
	if !ok {
		return nil, false
	}
	g, ok := o.(*gobject.GameObject)
	return g, ok
}

// Handle 绑定 (对象类型, 消息类型) → 处理器（转发到 Manager，启用「给各种消息绑定」）。
// 未配置 Manager 时返回 ErrNoManager（与 Register/Send 一致），调用方能据此发现绑定未生效。
func (r *Repository) Handle(objType uint16, msgType uint32, h object.Handler) error {
	if r.manager == nil {
		return ErrNoManager
	}
	r.manager.Handle(objType, msgType, h)
	return nil
}

// Send 给指定对象号发消息（需 Manager）。目标须已 Register。
// 派发顺序：优先 (对象类型, 消息类型) 绑定处理器 → 退化对象 OnMessage → ErrNoHandler；
// 目标不存在返回 ErrObjectNotFound（与 object.Manager.Send 一致）。
func (r *Repository) Send(ctx context.Context, id object.ObjectID, msg object.Message) error {
	if r.manager == nil {
		return ErrNoManager
	}
	return r.manager.Send(ctx, id, msg)
}

// ApplyResult 单对象批量操作的执行结果。
type ApplyResult struct {
	ID      object.ObjectID // 本次操作的对象号
	Applied bool            // true 表示该对象已成功加载 + 改 + 落库（变动已广播/落库）
	Err     error           // 非 nil 表示该对象在某一步失败（加载/改/存），其余对象不受影响
}

// BulkGet 批量加载一组对象（逐个 Load，并发安全）。缺失的对象返回空 GameObject（不报错），
// 调用方可结合 Exists 判断其是否存在。返回 id → 对象 的映射。
func (r *Repository) BulkGet(ctx context.Context, ids []object.ObjectID) (map[object.ObjectID]*gobject.GameObject, error) {
	out := make(map[object.ObjectID]*gobject.GameObject, len(ids))
	for _, id := range ids {
		g, err := r.Load(ctx, id)
		if err != nil {
			// 返回已加载的部分结果 + 错误：丢弃已加载对象会让「批量里单点失败 ⇒ 全批不可用」，
			// 调用方可按 out 里的内容继续处理成功项（与 BulkDelete 的部分返回语义一致）。
			return out, err
		}
		out[id] = g
	}
	return out, nil
}

// BulkApply 批量改：对一组 id 逐个（存在性检查 → Load → 应用 fn 改动 → Save 落库 + 自动同步）。
// 每个对象独立成败、互不影响；返回逐对象结果（含错误）。
// 全部对象均失败时，第二个返回值非 nil（errors.Join 汇总各对象错误）。

// fn 内直接改 g.Props() / Record / 子对象即可，无需手动 Save——BulkApply 统一落库。
// 不存在的 id 被跳过（记录 ErrNotFound，不创建新对象），与 BulkDelete 的「幂等跳过」一致；
// 不存在的 id 安全跳过，不会误建对象。
// 给一批军团改公告」等批量操作的地基；若仓库配了 notifier，改动经自动同步即时推到在线客户端。
func (r *Repository) BulkApply(ctx context.Context, ids []object.ObjectID, fn func(g *gobject.GameObject) error) ([]ApplyResult, error) {
	res := make([]ApplyResult, 0, len(ids))
	for _, id := range ids {
		ar := ApplyResult{ID: id}
		ok, err := r.Exists(ctx, id)
		if err != nil {
			ar.Err = err
			res = append(res, ar)
			continue
		}
		if !ok {
			ar.Err = data.ErrNotFound // 不存在则跳过，不创建
			res = append(res, ar)
			continue
		}
		g, err := r.Load(ctx, id)
		if err != nil {
			ar.Err = err
			res = append(res, ar)
			continue
		}
		if err := fn(g); err != nil {
			ar.Err = err
			res = append(res, ar)
			continue
		}
		if err := r.Save(ctx, g); err != nil {
			ar.Err = err
			res = append(res, ar)
			continue
		}
		ar.Applied = true
		res = append(res, ar)
	}
	// 逐对象细节在 res 里；但「整批全部失败」必须让只看 error 的调用方也能感知。
	applied, failed := 0, 0
	for _, ar := range res {
		if ar.Err != nil {
			failed++
			continue
		}
		if ar.Applied {
			applied++
		}
	}
	if applied == 0 && failed > 0 {
		errs := make([]error, 0, failed)
		for _, ar := range res {
			if ar.Err != nil {
				errs = append(errs, fmt.Errorf("object %s: %w", ar.ID, ar.Err))
			}
		}
		return res, errors.Join(errs...)
	}
	return res, nil
}

// BulkDelete 批量删除一组对象（递归删除 props/表/子对象 + 从 Manager 注销）。
// 删除是幂等的：不存在的对象安全跳过。返回「实际存在并被删除」的对象 id 列表。
func (r *Repository) BulkDelete(ctx context.Context, ids []object.ObjectID) ([]object.ObjectID, error) {
	deleted := make([]object.ObjectID, 0, len(ids))
	for _, id := range ids {
		ok, err := r.Exists(ctx, id)
		if err != nil {
			return deleted, err
		}
		if !ok {
			continue // 已不存在，幂等跳过
		}
		if err := r.Delete(ctx, id); err != nil {
			return deleted, err
		}
		deleted = append(deleted, id)
	}
	return deleted, nil
}
