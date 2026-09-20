// Package accessor 跨实体数据访问门面。
//
// 给定一个 data.Store（及可选的 Publisher），仅凭 (OwnerType, ID, Type) 三元键即
// 可对任意实体数据做「读 / 改 / 列 / 删」，无需知道实体内部结构：
//
// a := accessor.NewNotifyingAccessor(store, pub, subject)
// a.ReadJSON(ctx, data.OwnerPlayer, "1001", "profile", &p) // 读整个玩家档案
// a.Apply(ctx, data.OwnerPlayer, "1001", "profile", // 只改金币字段
// map[string]any{"gold": 9999}, msgID)
// a.ApplyString(ctx, data.OwnerPlayer, "1001", "player", // 按名改
// `{"name":"新名字"}`, 0)
// types, _ := a.ListTypes(ctx, data.OwnerPlayer) // 这玩家都有啥数据
//
// 改动经 data.Publisher 广播字段级 ChangeSet（{"patch":{字段:值}}，与 data.SyncEntity 同源线化），
// 在线客户端即时生效；不广播时仅落库。
//
// 这是「object 对象内核 + prop 标准数据格式 + data 同步」能力的统一入口，
// 对玩家 / 军团 / 服务器数据一视同仁。
package accessor

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/data/visibility"
	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/qw576483/clover-server-engine/pkg/shared/util"
)

// Accessor 跨实体数据访问门面。
//
// 仅持有 data.Store（及可选的 Publisher / subject），不绑定任何业务实体类型——
// 玩家 / 军团 / 服务器 / 场景……只要是 data.Key 能表达的 (ownerType,id,type)，都能读改。
type Accessor struct {
	store   *data.Store
	pub     data.Publisher
	subject string

	snapMu    sync.RWMutex
	snapCache map[snapKey]*snapEntry // 客户端可见快照缓存（按 ownerType+id），写操作时失效。
	// 快照失效 generation。SnapshotClient 「读库 → 写缓存」非原子：期间若有
	// Apply 落库并失效缓存，旧快照会被写回缓存并永久滞留。读库前记录 gen，写缓存时
	// gen 变了（发生过失效）则放弃写入。
	snapGen map[snapKey]uint64
	// snapEpoch 缓存淘汰代：trimSnapCacheLocked 淘汰条目时递增，使所有在途快照
	// 放弃写缓存 —— 被淘汰 key 的旧 gen 已无法再代表「期间无失效」（gen 被删除后
	// 读回 0，可能与在途快照记录的 0 相等而放行写回）。
	snapEpoch uint64
	keyMuPool [64]sync.Mutex // 分片锁，防止同 key 并发 Load-Modify-Save 数据竞争。
}

// maxSnapCacheEntries snapCache / snapGen 的条目上限。
// 只读快照过的实体此前缓存永不淘汰，随访问过的实体数无上限增长（长跑进程内存只增不减）；
// 超限时按随机序淘汰单条并推进 snapEpoch（见 trimSnapCacheLocked）。
const maxSnapCacheEntries = 8192

// keyLock 根据 (ownerType,id,typ) 获取分片锁。
func (a *Accessor) keyLock(ownerType data.OwnerType, id, typ string) *sync.Mutex {
	h := util.Fnv32(string(ownerType) + "/" + id + "/" + typ)
	return &a.keyMuPool[h&63]
}

type snapKey struct {
	ownerType data.OwnerType
	id        string
}

type snapEntry struct {
	snap    map[string]json.RawMessage // JSON 快照（SnapshotClient / 全量同步使用）
	snapBin map[string][]byte          // 二进制快照（视野协议使用，首次访问时懒转换并缓存）
}

// NewAccessor 构造不广播的访问门面（只读 / 仅落库场景）。
func NewAccessor(store *data.Store) *Accessor {
	return &Accessor{store: store, snapCache: make(map[snapKey]*snapEntry), snapGen: make(map[snapKey]uint64)}
}

// NewNotifyingAccessor 构造带广播的访问门面：改动经 pub 发布到 subject（data-event 自动同步）。
func NewNotifyingAccessor(store *data.Store, pub data.Publisher, subject string) *Accessor {
	return &Accessor{store: store, pub: pub, subject: subject, snapCache: make(map[snapKey]*snapEntry), snapGen: make(map[snapKey]uint64)}
}

// InvalidateSnapshotCache 手动失效 (ownerType,id) 的客户端可见快照缓存。
// 当外部绕过 Accessor 直接修改 data.Store 后，应调用此方法保证进视野快照不返回旧数据。
func (a *Accessor) InvalidateSnapshotCache(ownerType data.OwnerType, id string) {
	a.invalidateSnapshotCache(ownerType, id)
}

func (a *Accessor) invalidateSnapshotCache(ownerType data.OwnerType, id string) {
	key := snapKey{ownerType: ownerType, id: id}
	a.snapMu.Lock()
	delete(a.snapCache, key)
	a.snapGen[key]++ // 递增 generation，使正在进行的快照计算放弃写缓存
	a.snapMu.Unlock()
}

// trimSnapCacheLocked 缓存条目触顶时淘汰一条（调用方须持 snapMu 写锁）。
// 淘汰同时推进 snapEpoch：删除 gen 会让该 key 的读回值变回 0，可能与在途快照
// 记录的 0 相等而放行「失效后」的旧快照写回——epoch 变化保证在途快照一律放弃写入。
func (a *Accessor) trimSnapCacheLocked() {
	if len(a.snapCache) < maxSnapCacheEntries && len(a.snapGen) < maxSnapCacheEntries {
		return
	}
	evicted := false
	for k := range a.snapCache {
		delete(a.snapCache, k)
		delete(a.snapGen, k)
		evicted = true
		break
	}
	if !evicted {
		// snapGen 只被失效路径（写操作）创建，可能先于 snapCache 触顶：
		// 此时单独淘汰一条 gen 记录，防止「只写不读」的实体让 gen 表无界增长。
		for k := range a.snapGen {
			delete(a.snapGen, k)
			evicted = true
			break
		}
	}
	if evicted {
		a.snapEpoch++
	}
}

func (a *Accessor) key(ownerType data.OwnerType, id, typ string) data.Key {
	return data.Key{Owner: ownerType, ID: id, Type: typ}
}

// ReadRaw 读取原始字节（不存在返回 data.ErrNotFound）。
func (a *Accessor) ReadRaw(ctx context.Context, ownerType data.OwnerType, id, typ string) ([]byte, error) {
	return a.store.Load(ctx, a.key(ownerType, id, typ))
}

// ReadJSON 读取并反序列化到 v（任意结构体 / map 均兼容，全保真含嵌套）。
func (a *Accessor) ReadJSON(ctx context.Context, ownerType data.OwnerType, id, typ string, v any) error {
	return a.store.LoadJSON(ctx, a.key(ownerType, id, typ), v)
}

// Exists 该 (ownerType,id,typ) 数据是否存在。
func (a *Accessor) Exists(ctx context.Context, ownerType data.OwnerType, id, typ string) (bool, error) {
	_, err := a.store.Load(ctx, a.key(ownerType, id, typ))
	if err == data.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ListTypes 列出该 ownerType 下所有已存在的数据类型（"这个实体都有啥数据"）。
func (a *Accessor) ListTypes(ctx context.Context, ownerType data.OwnerType) ([]string, error) {
	return a.store.ListTypes(ctx, ownerType)
}

// Apply 按字段名合并改：将原数据与 patch 合并后整体落库；原数据缺失时直接以 patch 建数据；
// 原数据为合法 JSON 对象时按字段名浅合并（嵌套对象整体替换，不递归），否则整体替换为 patch。
// notifyMsgID!=0 且 pub 非空时，仅广播 patch 自身（字段级 ChangeSet，呼应 data.SyncEntity 增量同步）。
//
// 典型用法：
//
// a.Apply(ctx, data.OwnerPlayer, "1001", "profile", map[string]any{"gold": 9999}, msgID)
func (a *Accessor) Apply(ctx context.Context, ownerType data.OwnerType, id, typ string, patch map[string]any, notifyMsgID uint32) error {
	if len(patch) == 0 {
		return nil
	}
	// Load-Modify-Save 全程持分片锁；广播（网络发布）必须放到锁外执行：
	// 持锁发布会在下游阻塞时把同分片其它 key 的读写全部拖住（Replace/Mutate/DeleteField 同）。
	mu := a.keyLock(ownerType, id, typ)
	mu.Lock()
	var patchRaw []byte
	err := func() error {
		key := a.key(ownerType, id, typ)
		base := map[string]json.RawMessage{}
		raw, lerr := a.store.Load(ctx, key)
		if lerr != nil && lerr != data.ErrNotFound {
			return lerr
		}
		if len(raw) > 0 {
			// 原数据必须是 JSON 对象；无法解析时不能静默覆盖，否则会把数组/标量数据毁掉。
			if lerr := json.Unmarshal(raw, &base); lerr != nil {
				return fmt.Errorf("accessor: decode existing %s/%s/%s: %w", ownerType, id, typ, lerr)
			}
		}
		patchRaw, lerr = json.Marshal(patch)
		if lerr != nil {
			return fmt.Errorf("accessor: marshal patch: %w", lerr)
		}
		var pm map[string]json.RawMessage
		if lerr := json.Unmarshal(patchRaw, &pm); lerr != nil {
			return fmt.Errorf("accessor: decode patch: %w", lerr)
		}
		maps.Copy(base, pm)
		merged, lerr := json.Marshal(base)
		if lerr != nil {
			return fmt.Errorf("accessor: marshal merged: %w", lerr)
		}
		return a.store.Save(ctx, key, merged)
	}()
	mu.Unlock()
	if err != nil {
		return err
	}
	a.invalidateSnapshotCache(ownerType, id)
	if notifyMsgID != 0 {
		a.broadcast(ctx, ownerType, typ, id, patchRaw, notifyMsgID)
	}
	return nil
}

// ApplyString 同 Apply，但 patch 为紧凑 JSON 字符串（命令行友好：{"gold":999,"name":"bob"}，
// 数字按整/小推断、对象按 "type:seq"），类型推断复用 object.Bag.ApplyCompact 语义。
func (a *Accessor) ApplyString(ctx context.Context, ownerType data.OwnerType, id, typ, patchJSON string, notifyMsgID uint32) error {
	patch, err := parseCompactPatch(patchJSON)
	if err != nil {
		return err
	}
	return a.Apply(ctx, ownerType, id, typ, patch, notifyMsgID)
}

// Replace 整体覆盖（谨慎）：v 直接落库，不与原数据合并。notifyMsgID!=0 时整体广播。
func (a *Accessor) Replace(ctx context.Context, ownerType data.OwnerType, id, typ string, v any, notifyMsgID uint32) error {
	// 与 Apply/Mutate 同持分片锁：否则本盲写可插入 Apply 的 Load 与 Save 之间，
	// 被基于旧值合并的结果覆盖（Replace 的写入静默丢失）。
	// 广播在锁外执行（同 Apply）。
	mu := a.keyLock(ownerType, id, typ)
	mu.Lock()
	err := a.store.SaveJSON(ctx, a.key(ownerType, id, typ), v)
	mu.Unlock()
	if err != nil {
		return err
	}
	a.invalidateSnapshotCache(ownerType, id)
	if notifyMsgID != 0 {
		a.broadcastJSON(ctx, ownerType, typ, id, v, notifyMsgID)
	}
	return nil
}

// Delete 删除该 (ownerType,id,typ) 数据（若存在）。
func (a *Accessor) Delete(ctx context.Context, ownerType data.OwnerType, id, typ string) error {
	// 与 Apply/Mutate 同持分片锁：否则删除可发生在 Apply 的 Load 与 Save 之间，
	// 已删数据被旧值合并结果「复活」。
	mu := a.keyLock(ownerType, id, typ)
	mu.Lock()
	defer mu.Unlock()
	if err := a.store.Delete(ctx, a.key(ownerType, id, typ)); err != nil {
		return err
	}
	a.invalidateSnapshotCache(ownerType, id)
	return nil
}

// Read 泛型读取：把 (ownerType,id,typ) 数据直接反序列化为任意类型 T（结构体 / map 均可）。
// 不存在或反序列化失败均返回错误。
func Read[T any](ctx context.Context, a *Accessor, ownerType data.OwnerType, id, typ string) (T, error) {
	var zero T
	raw, err := a.store.Load(ctx, a.key(ownerType, id, typ))
	if err != nil {
		return zero, err
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return zero, fmt.Errorf("accessor: unmarshal %s/%s/%s: %w", ownerType, id, typ, err)
	}
	return v, nil
}

// Mutate 读改写：把当前数据读为 map，交给 fn 原地修改，再整体落库；
// fn 返回错误则放弃本次修改（数据不变）。notifyMsgID!=0 且 pub 非空时广播整体变更。
//
// a.Mutate(ctx, data.OwnerPlayer, "1001", "profile", func(p map[string]any) error {
// p["gold"] = int64(p["gold"].(float64)) + 100
// return nil
// }, msgID)
func (a *Accessor) Mutate(ctx context.Context, ownerType data.OwnerType, id, typ string, fn func(map[string]any) error, notifyMsgID uint32) error {
	// Load-Modify-Save 持分片锁；广播在锁外执行（同 Apply）。
	mu := a.keyLock(ownerType, id, typ)
	mu.Lock()
	var merged []byte
	err := func() error {
		key := a.key(ownerType, id, typ)
		base := map[string]any{}
		raw, lerr := a.store.Load(ctx, key)
		if lerr != nil && lerr != data.ErrNotFound {
			return lerr
		}
		if len(raw) > 0 {
			// 原数据必须是 JSON 对象；无法解析时不能静默覆盖。
			if lerr := json.Unmarshal(raw, &base); lerr != nil {
				return fmt.Errorf("accessor: decode existing %s/%s/%s: %w", ownerType, id, typ, lerr)
			}
		}
		if lerr := fn(base); lerr != nil {
			return lerr
		}
		merged, lerr = json.Marshal(base)
		if lerr != nil {
			return fmt.Errorf("accessor: marshal mutated %s/%s/%s: %w", ownerType, id, typ, lerr)
		}
		return a.store.Save(ctx, key, merged)
	}()
	mu.Unlock()
	if err != nil {
		return err
	}
	a.invalidateSnapshotCache(ownerType, id)
	if notifyMsgID != 0 {
		a.broadcast(ctx, ownerType, typ, id, merged, notifyMsgID)
	}
	return nil
}

// DeleteField 删除数据中的单个字段（GMT 删除某属性，如清掉某玩家的封禁标记）。
// 数据不存在 / 字段不存在时均为无操作成功；notifyMsgID!=0 且 pub 非空时广播删除（字段置 null）。
func (a *Accessor) DeleteField(ctx context.Context, ownerType data.OwnerType, id, typ, field string, notifyMsgID uint32) error {
	// Load-Modify-Save 必须与 Apply/Mutate 同持分片锁，否则并发下经典丢失更新。
	// 广播在锁外执行（同 Apply）。
	mu := a.keyLock(ownerType, id, typ)
	mu.Lock()
	changed := false
	err := func() error {
		key := a.key(ownerType, id, typ)
		raw, lerr := a.store.Load(ctx, key)
		if lerr == data.ErrNotFound {
			return nil
		}
		if lerr != nil {
			return lerr
		}
		base := map[string]json.RawMessage{}
		if len(raw) > 0 {
			if lerr := json.Unmarshal(raw, &base); lerr != nil {
				return fmt.Errorf("accessor: decode %s/%s/%s: %w", ownerType, id, typ, lerr)
			}
		}
		if _, ok := base[field]; !ok {
			return nil // 字段本就不存在
		}
		delete(base, field)
		merged, lerr := json.Marshal(base)
		if lerr != nil {
			return fmt.Errorf("accessor: marshal after delete %s/%s/%s: %w", ownerType, id, typ, lerr)
		}
		if lerr := a.store.Save(ctx, key, merged); lerr != nil {
			return lerr
		}
		changed = true
		return nil
	}()
	mu.Unlock()
	if err != nil {
		return err
	}
	if !changed {
		return nil // 数据/字段本就不存在：无操作
	}
	a.invalidateSnapshotCache(ownerType, id)
	if notifyMsgID != 0 {
		// 以 {field: null} 作为删除哨兵广播，客户端据此移除该字段。
		patch, _ := json.Marshal(map[string]any{field: nil})
		a.broadcast(ctx, ownerType, typ, id, patch, notifyMsgID)
	}
	return nil
}

// Snapshot 实体快照：批量加载该 (ownerType,id) 下全部数据类型及其原始字节。
// 使用编译期 Schema 注册表获取类型列表，并通过 LoadAll 执行单次批量查询。
// 返回 map[typ]json.RawMessage（typ→该类型原始数据）。
func (a *Accessor) Snapshot(ctx context.Context, ownerType data.OwnerType, id string) (map[string]json.RawMessage, error) {
	types := data.SchemaTypes(ownerType)
	if len(types) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	all, err := a.store.LoadAll(ctx, ownerType, id, types)
	if err != nil {
		return nil, err
	}
	out := make(map[string]json.RawMessage, len(all))
	for t, raw := range all {
		out[t] = raw
	}
	return out, nil
}

// SnapshotClient 「公开视角」快照：仅保留 ClientVisible（剔除 SelfOnly 与 ServerOnly），
// 供「推给别人」路径（进视野快照 / 房间广播）在源头统一裁剪。
// 「推给自己」的全量同步请用 SnapshotClientSelf（保留 SelfOnly，如背包）。
//
// 结果按 (ownerType,id) 缓存，直到对应实体数据被本 Accessor 的写方法修改或显式 InvalidateSnapshotCache。
func (a *Accessor) SnapshotClient(ctx context.Context, ownerType data.OwnerType, id string) (map[string]json.RawMessage, error) {
	key := snapKey{ownerType: ownerType, id: id}
	a.snapMu.RLock()
	if e := a.snapCache[key]; e != nil {
		out := cloneRawSnapshot(e.snap)
		a.snapMu.RUnlock()
		return out, nil
	}
	a.snapMu.RUnlock()

	// 读库前后各取 gen/epoch，变化则重试；
	// 返回快照同时透传 gen+epoch 供 SnapshotClientBinary 原子写入。
	snap, _, _, err := a.snapshotClientWithGen(ctx, ownerType, id)
	if err != nil {
		return nil, err
	}
	return cloneRawSnapshot(snap), nil
}

// snapshotClientWithGen 内部实现：读库、过滤、写缓存，并返回当时生效的 gen 与 epoch。
func (a *Accessor) snapshotClientWithGen(ctx context.Context, ownerType data.OwnerType, id string) (map[string]json.RawMessage, uint64, uint64, error) {
	key := snapKey{ownerType: ownerType, id: id}
	for retries := 0; retries < 3; retries++ {
		a.snapMu.RLock()
		gen := a.snapGen[key]
		epoch := a.snapEpoch
		a.snapMu.RUnlock()

		snap, err := a.Snapshot(ctx, ownerType, id)
		if err != nil {
			return nil, 0, 0, err
		}
		filtered := visibility.FilterPublicVisible(ownerType, snap)

		a.snapMu.Lock()
		if a.snapGen[key] == gen && a.snapEpoch == epoch {
			// gen/epoch 未变：完整写入包含 JSON 和二进制空槽的 snapEntry
			a.trimSnapCacheLocked()
			if a.snapCache[key] != nil {
				// 已有条目（由 SnapshotClientBinary 先创建）：只补 JSON
				a.snapCache[key].snap = filtered
			} else {
				a.snapCache[key] = &snapEntry{snap: filtered}
			}
			a.snapMu.Unlock()
			return filtered, gen, epoch, nil
		}
		// gen/epoch 已变：本次读取在写穿透（或缓存淘汰）期间发生，缓存写跳过
		// 但检查是否需要返回最新值：若缓存已有新数据则直接返回（由其他并发 SnapshotClient 写入）
		if e := a.snapCache[key]; e != nil && e.snap != nil {
			g := a.snapGen[key] // 必须持锁读：解锁后读 map 与写方构成数据竞争
			ep := a.snapEpoch
			a.snapMu.Unlock()
			return e.snap, g, ep, nil
		}
		a.snapMu.Unlock()
		// 重试
	}
	// 重试耗尽：最后一次尝试直接返回（不再写缓存）
	a.snapMu.RLock()
	gen := a.snapGen[key]
	epoch := a.snapEpoch
	a.snapMu.RUnlock()
	snap, err := a.Snapshot(ctx, ownerType, id)
	if err != nil {
		return nil, 0, 0, err
	}
	filtered := visibility.FilterPublicVisible(ownerType, snap)
	return filtered, gen, epoch, nil
}

// SnapshotClientSelf 「本人视角」快照：仅剔除 ServerOnly，保留 ClientSelfOnly。
// 供「推给自己」的全量同步（EPushPlayerFullSync）使用——若误用 SnapshotClient，
// 背包等 SelfOnly 数据会被剔除，本人客户端拿不到自己的私有数据。
// 全量同步频率低（登录/重连各一次），不走快照缓存。
func (a *Accessor) SnapshotClientSelf(ctx context.Context, ownerType data.OwnerType, id string) (map[string]json.RawMessage, error) {
	snap, err := a.Snapshot(ctx, ownerType, id)
	if err != nil {
		return nil, err
	}
	return visibility.FilterClientVisible(ownerType, snap), nil
}

// cloneRawSnapshot 深拷贝快照：复制外层 map，且逐个复制 json.RawMessage 底层字节，
// 使返回值与缓存完全隔离（调用方修改互不影响，且并发下无数据竞争）。
func cloneRawSnapshot(in map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(in))
	for typ, raw := range in {
		b := make(json.RawMessage, len(raw))
		copy(b, raw)
		out[typ] = b
	}
	return out
}

// cloneBinSnapshot 深拷贝二进制快照：复制外层 map，且逐个复制 []byte 底层字节。
func cloneBinSnapshot(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for typ, raw := range in {
		b := make([]byte, len(raw))
		copy(b, raw)
		out[typ] = b
	}
	return out
}

// SnapshotClientBinary 同 SnapshotClient，但返回紧凑二进制快照。
//
// 内部数据从 JSON 转为 object.Bag 二进制格式（JSON 对象 → Bag 二进制；非对象保持原 JSON 字节）。
// 二进制快照与 JSON 快照共用缓存，只在首次请求二进制时做一次转换，之后直接复用。
// 视野同步等高频路径应优先使用此方法，避免每次下发都走 json.Marshal。
func (a *Accessor) SnapshotClientBinary(ctx context.Context, ownerType data.OwnerType, id string) (map[string][]byte, error) {
	key := snapKey{ownerType: ownerType, id: id}
	a.snapMu.RLock()
	if e := a.snapCache[key]; e != nil && e.snapBin != nil {
		out := cloneBinSnapshot(e.snapBin)
		a.snapMu.RUnlock()
		return out, nil
	}
	a.snapMu.RUnlock()

	// 直接走 snapshotClientWithGen 获取 (snap, gen, epoch)，
	// 然后原子回填 JSON + 二进制，避免委托 SnapshotClient 时因 gen 变化跳过缓存写导致二进制缓存永不命中。
	snap, gen, epoch, err := a.snapshotClientWithGen(ctx, ownerType, id)
	if err != nil {
		return nil, err
	}
	snapBin := jsonSnapshotToBinary(snap)
	a.snapMu.Lock()
	// 原子写入：JSON 和二进制同时更新，基于相同 gen/epoch。
	if a.snapCache[key] != nil && a.snapGen[key] == gen && a.snapEpoch == epoch {
		a.snapCache[key].snapBin = snapBin
	} else if a.snapCache[key] == nil && a.snapGen[key] == gen && a.snapEpoch == epoch {
		a.trimSnapCacheLocked()
		a.snapCache[key] = &snapEntry{snap: snap, snapBin: snapBin}
	}
	a.snapMu.Unlock()
	return cloneBinSnapshot(snapBin), nil
}

// jsonSnapshotToBinary 把 map[type]json.RawMessage 转换为二进制快照。
// 每个 type 的数据：先尝试 object.Bag 标准 JSON 格式；失败再尝试紧凑 JSON；
// 仍失败则保持原 JSON 字节，供客户端按原 JSON 解析。
func jsonSnapshotToBinary(snap map[string]json.RawMessage) map[string][]byte {
	out := make(map[string][]byte, len(snap))
	for typ, raw := range snap {
		bag := object.NewBag()
		if err := bag.UnmarshalJSON(raw); err == nil {
			if b, err := bag.MarshalBinary(); err == nil {
				out[typ] = b
				continue
			}
		}
		if err := bag.ApplyCompact(raw); err == nil {
			if b, err := bag.MarshalBinary(); err == nil {
				out[typ] = b
				continue
			}
		}
		// 非对象或转换失败，透传原始 JSON 字节。
		out[typ] = raw
	}
	return out
}

// broadcast 经 Publisher 广播字段级 ChangeSet（patch 自身）：{"ownerType":..,"id":..,"patch":{...}}。
// 与 data.SyncEntity.SaveJSONPatch 的广播信封同源，客户端一致处理。
//
// 当 notifyMsgID>0（需要推客户端）时，patch 先经 data/visibility.FilterClientVisible
// 剥离 ServerOnly 类型——纯服务器数据整条不下发（绝不进线）。
//
// 广播目标为配置的 notify subject（如 player.sync），保持原契约（每次变更恰好 1 条广播）。
// ownerType/id 编入信封，供 mmo.EntitySync 订阅同一 subject 后按视野反向索引只推给视野内玩家。
func (a *Accessor) broadcast(_ context.Context, ownerType data.OwnerType, typ, id string, patchRaw []byte, notifyMsgID uint32) {
	if a.pub == nil || a.subject == "" {
		return
	}
	// 可见性过滤：ServerOnly 整条不下发（通用 notify 信道与旧契约一致，仅发客户端可见字段）。
	filtered := visibility.FilterClientVisible(ownerType, map[string]json.RawMessage{typ: patchRaw})
	clientPatch, ok := filtered[typ]
	if !ok {
		return // 纯服务器数据，不广播
	}
	env := struct {
		OwnerType string          `json:"owner_type"`
		ID        string          `json:"id"`
		Patch     json.RawMessage `json:"patch"`
	}{OwnerType: string(ownerType), ID: id, Patch: clientPatch}
	body, err := ujson.Marshal(env)
	if err != nil {
		return
	}
	a.publish(a.subject, id, body, notifyMsgID)
}

// NotifySubject 返回 Accessor 的广播 subject（供 mmo.EntitySync 订阅同一信道做视野路由）。
func (a *Accessor) NotifySubject() string { return a.subject }

// broadcastJSON 整体广播 v（用于 Replace）。
func (a *Accessor) broadcastJSON(_ context.Context, ownerType data.OwnerType, typ, id string, v any, notifyMsgID uint32) {
	if a.pub == nil || a.subject == "" {
		return
	}
	raw, err := ujson.Marshal(v)
	if err != nil {
		return
	}
	// 可见性过滤：ServerOnly 整体不下发（通用 notify 信道亦只发客户端可见）。
	filtered := visibility.FilterClientVisible(ownerType, map[string]json.RawMessage{typ: raw})
	clientRaw, ok := filtered[typ]
	if !ok {
		return // 纯服务器数据，不广播
	}
	env := struct {
		OwnerType string          `json:"owner_type"`
		ID        string          `json:"id"`
		Patch     json.RawMessage `json:"patch"`
	}{OwnerType: string(ownerType), ID: id, Patch: clientRaw}
	body, err := ujson.Marshal(env)
	if err != nil {
		return
	}
	a.publish(a.subject, id, body, notifyMsgID)
}

func (a *Accessor) publish(subject, id string, body []byte, notifyMsgID uint32) {
	push := proto.NotifyPush{
		Target:       id,
		MsgID:        notifyMsgID,
		Body:         body,
		DeliveryMode: proto.DeliveryModeReliable,
	}
	buf, err := proto.EncodeNotifyPush(&push)
	if err != nil {
		return
	}
	if err := a.pub.Publish(subject, buf); err != nil {
		logger.Errorf("accessor: publish to %s failed: %v", subject, err)
	}
}

// parseCompactPatch 用 object.Bag.ApplyCompact 语义把紧凑 JSON 字符串解析为 map[string]any
// （数字按整/小推断、布尔、字符串、对象 "type:seq"），供 ApplyString 使用。
func parseCompactPatch(s string) (map[string]any, error) {
	bag := object.NewBag()
	if err := bag.ApplyCompact([]byte(s)); err != nil {
		return nil, err
	}
	raw, err := bag.MarshalCompact()
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}
