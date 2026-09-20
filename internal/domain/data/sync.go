// Package data 的 data-event 同步基类（SyncEntity）。
//
// SyncEntity 是"可同步数据对象"的通用基类：持有三元键 Key + 脏标记。
// 业务对象（玩家档案、背包、房间状态…）组合或内嵌本结构即可获得：
// - 落库能力（复用 Store 的 redis / mysql / cache 三模式）
// - 自动广播能力：SaveJSON 落库后，可经消息总线发布一条数据变更事件，
// 由网关按 NotifyPush.Target 精准下发给在线目标（data-event 自动同步）。
//
// 本基类刻意不绑定任何游戏业务：
// - 广播目标经 Publisher 接口解耦（不依赖具体 NATS 客户端）；
// - 广播 subject 由构造参数传入（业务方自定义命名空间）；
// - 下发目标标识由 Targeter 决定（默认取 Key.ID，业务方可覆盖为房间/分线等自定义 ID）。
package data

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/internal/transport/pubsub"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// Publisher 发布接口（本体定义在 transport/pubsub）。
type Publisher = pubsub.Publisher

// Targeter 计算某条数据的"下发目标标识"。默认取 Key.ID（玩家 UID），
// 房间架构可覆盖为房间 ID、分线架构可覆盖为分线/视野 ID。
type Targeter func(key Key) string

// SyncEntity data-event 基类：持有数据三元键 + 脏标记 + 广播句柄。
// 业务对象（如玩家档案、背包）组合或内嵌本结构即可获得「落库 + 自动广播」能力。
type SyncEntity struct {
	store   *Store
	pub     Publisher
	subject string
	target  Targeter
	key     Key
	// dirty 会被业务 goroutine（MarkDirty）与 tick goroutine
	// （SaveJSON 系列清脏、Dirty 读）并发访问，必须用原子类型而非裸 bool。
	dirty atomic.Bool
}

// NewSyncEntity 构造同步实体。
// - store：数据存储抽象（cache 模式推荐）。
// - pub：发布器，可为 nil（不广播，仅落库）。
// - subject：广播 topic（pub 非 nil 时生效）。
// - key：三元数据键。
func NewSyncEntity(store *Store, pub Publisher, subject string, key Key) *SyncEntity {
	return &SyncEntity{
		store:   store,
		pub:     pub,
		subject: subject,
		target:  func(k Key) string { return k.ID },
		key:     key,
	}
}

// SetTargeter 覆盖下发目标标识计算（默认取 Key.ID）。房间/分线架构用于改为 room/line ID。
func (e *SyncEntity) SetTargeter(fn Targeter) {
	if fn != nil {
		e.target = fn
	}
}

// Key 返回三元键。
func (e *SyncEntity) Key() Key { return e.key }

// MarkDirty 标记脏（业务修改字段后调用）。
func (e *SyncEntity) MarkDirty() { e.dirty.Store(true) }

// Dirty 是否处于脏状态。
func (e *SyncEntity) Dirty() bool { return e.dirty.Load() }

// SaveJSON 序列化 v 落库（经 Store），清除脏标记，并按 notifyMsgID 广播变更（data-event）。
// notifyMsgID 为 0 表示仅落库不广播（如首次建号）。
// 广播失败时返回 error（落库已成功）并保留脏标记，下一次 tick 可安全重试广播，
// 避免"已清脏但未广播"导致客户端变更永久丢失。
func (e *SyncEntity) SaveJSON(ctx context.Context, v any, notifyMsgID uint32) error {
	if err := e.store.SaveJSON(ctx, e.key, v); err != nil {
		return err
	}
	if notifyMsgID != 0 {
		if err := e.broadcast(ctx, v, notifyMsgID); err != nil {
			logger.Errorf("sync: %v", err)
			return err
		}
	}
	e.dirty.Store(false)
	return nil
}

// LoadJSON 从 Store 读取并反序列化到 v。
func (e *SyncEntity) LoadJSON(ctx context.Context, v any) error {
	return e.store.LoadJSON(ctx, e.key, v)
}

// broadcast 经 Publisher 广播数据变更（data-event）。无发布器 / 无 subject 时静默跳过。
// 数据同步默认使用 Reliable（丢包=客户端状态不一致）。
// 返回 error（含 Publish 失败），供调用方感知广播结果。
func (e *SyncEntity) broadcast(ctx context.Context, v any, notifyMsgID uint32) error {
	if ctx != nil {
		// 尊重调用方 ctx：请求已取消 / 已超时时不再下发（此前该参数被忽略）。
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sync: broadcast %d canceled: %w", notifyMsgID, err)
		}
	}
	if e.pub == nil || e.subject == "" {
		return nil
	}
	body, err := ujson.Marshal(v)
	if err != nil {
		return fmt.Errorf("sync: json marshal notify %d: %w", notifyMsgID, err)
	}
	push := proto.NotifyPush{
		Target:       e.target(e.key),
		MsgID:        notifyMsgID,
		Body:         body,
		DeliveryMode: proto.DeliveryModeReliable,
	}
	buf, err := proto.EncodeNotifyPush(&push)
	if err != nil {
		return fmt.Errorf("sync: proto encode notify %d: %w", notifyMsgID, err)
	}
	if err := e.pub.Publish(e.subject, buf); err != nil {
		return fmt.Errorf("sync: publish %d to %s: %w", notifyMsgID, e.subject, err)
	}
	return nil
}

// SaveJSONPatch 序列化 v 落库（经 Store），清除脏标记，并仅广播脏字段 ChangeSet
// （呼应 MMO SyncRecChange「数据变动会自动推送哪个行变动」）。
//
// 与 SaveJSON（整体广播）互补：整体广播用于首次加载 / 全量同步；patch 仅推送变动字段，
// 带宽友好、且 GMT 改单字段即时生效。bag 为 nil 时退化为 SaveJSON 整体广播。
//
// 仅在广播成功后才消费 bag 脏标记（MarkClean），广播失败时保留变动信息以便重试。
func (e *SyncEntity) SaveJSONPatch(ctx context.Context, v any, bag *object.Bag, notifyMsgID uint32) error {
	if err := e.store.SaveJSON(ctx, e.key, v); err != nil {
		return err
	}
	broadcastOK := true
	if notifyMsgID != 0 {
		if bag == nil {
			if err := e.broadcast(ctx, v, notifyMsgID); err != nil {
				logger.Errorf("sync: savejsonpatch broadcast: %v", err)
				broadcastOK = false
			}
		} else {
			if !e.broadcastPatch(ctx, bag, notifyMsgID) {
				broadcastOK = false
			}
		}
	}
	// 广播成功才清 dirty；失败时保留，下一次 tick 可重新广播（数据已落库，广播可安全重试）。
	if broadcastOK {
		e.dirty.Store(false)
	}
	// 仅广播成功时才 MarkClean，避免"广播失败但标记已消"丢失变动。
	if bag != nil && broadcastOK {
		bag.MarkClean()
	}
	return nil
}

// BroadcastPatch 暴露「仅变动字段」广播能力，供外层（如 gobject 统一对象内核）复用，
// 避免重复实现 NotifyPush 线化与发布逻辑。无发布器 / 无 subject / 无变动时静默跳过。
// 成功广播后消费脏标记（MarkClean），因此「每次调用只推送自上次以来的增量」——
// 配合 object.Bag 的写即自动同步回调，即实现「改一个字段，立即只推这一个字段」的精细增量。
func (e *SyncEntity) BroadcastPatch(ctx context.Context, bag *object.Bag, notifyMsgID uint32) {
	if e.broadcastPatch(ctx, bag, notifyMsgID) && bag != nil {
		bag.MarkClean()
	}
}

// BroadcastRecordPatch 暴露「仅变动行」广播能力，供外层（如 gobject）复用。
// 无发布器 / 无 subject / 无变动时静默跳过；成功广播后消费脏标记（行级增量语义）。
func (e *SyncEntity) BroadcastRecordPatch(ctx context.Context, rec *Record, notifyMsgID uint32) {
	if e.broadcastRecordPatch(ctx, rec, notifyMsgID) && rec != nil {
		rec.MarkClean()
	}
}

// broadcastPatch 经 Publisher 广播「仅变动字段」的 ChangeSet。返回是否真的发布了广播。
// 数据同步默认使用 Reliable（丢包=客户端状态不一致）。
func (e *SyncEntity) broadcastPatch(ctx context.Context, bag *object.Bag, notifyMsgID uint32) bool {
	if ctx != nil {
		// ctx 此前被**完全忽略**（参数名是 `_`）：调用方取消 / 超时后广播照旧下发，
		// 自动同步回调（gobject 的写即推送）用 context.Background() 时更等于没有生命周期约束。
		// 这里显式尊重它：已取消就丢弃本次广播（脏标记保留，可重试）。
		if err := ctx.Err(); err != nil {
			return false
		}
	}
	if e.pub == nil || e.subject == "" || bag == nil {
		return false
	}
	changes := bag.Changes()
	if len(changes) == 0 {
		return false
	}
	env := struct {
		Patch map[string]object.Value `json:"patch"`
	}{Patch: changes}
	body, err := ujson.Marshal(env)
	if err != nil {
		return false
	}
	push := proto.NotifyPush{
		Target:       e.target(e.key),
		MsgID:        notifyMsgID,
		Body:         body,
		DeliveryMode: proto.DeliveryModeReliable,
	}
	buf, err := proto.EncodeNotifyPush(&push)
	if err != nil {
		return false
	}
	if err := e.pub.Publish(e.subject, buf); err != nil {
		// Publish 失败必须返回 false，否则调用方的 broadcastOK 保护形同虚设，
		// 变动脏标记会被误清、增量永久丢失（无法重试）。
		logger.Errorf("sync: publish %d to %s failed: %v", notifyMsgID, e.subject, err)
		return false
	}
	return true
}

// SaveJSONRecordPatch 序列化 v 落库（经 Store），清除脏标记，并仅广播脏行 ChangeSet
// （呼应 MMO SyncRecordChange 的行级变动推送：背包 / 邮件 / 任务等「多行同构」表数据）。
//
// rec 为 nil 时退化为 SaveJSON 整体广播。调用后 rec 的脏标记被清空。
func (e *SyncEntity) SaveJSONRecordPatch(ctx context.Context, v any, rec *Record, notifyMsgID uint32) error {
	if err := e.store.SaveJSON(ctx, e.key, v); err != nil {
		return err
	}
	broadcastOK := true
	if notifyMsgID != 0 {
		if rec == nil {
			// rec 为 nil 退化为全量广播，避免落库后无通知（与 SaveJSONPatch 对称）。
			if err := e.broadcast(ctx, v, notifyMsgID); err != nil {
				logger.Errorf("sync: savejsonrecordpatch broadcast: %v", err)
				broadcastOK = false
			}
		} else if !e.broadcastRecordPatch(ctx, rec, notifyMsgID) {
			broadcastOK = false
		}
	}
	// 仅广播成功时才清除脏标记，避免广播失败导致增量永久丢失。
	if broadcastOK {
		e.dirty.Store(false)
	}
	// 仅广播成功时才 MarkClean，避免广播失败却清除行级增量。
	if rec != nil && broadcastOK {
		rec.MarkClean()
	}
	return nil
}

// broadcastRecordPatch 经 Publisher 广播「仅变动行」的 ChangeSet（Full=true 表示结构性变动需整表重同步）。
// 数据同步默认使用 Reliable（丢包=客户端状态不一致）。
// 返回是否真的发布了广播。
func (e *SyncEntity) broadcastRecordPatch(ctx context.Context, rec *Record, notifyMsgID uint32) bool {
	if ctx != nil {
		// 同 broadcastPatch：ctx 此前被忽略，取消后广播照旧下发。
		if err := ctx.Err(); err != nil {
			return false
		}
	}
	if e.pub == nil || e.subject == "" || rec == nil {
		return false
	}
	dirtyRows, deletedRows, cleared, full := rec.Changes()
	if len(dirtyRows) == 0 && len(deletedRows) == 0 && !cleared && !full {
		return false
	}
	env := struct {
		Rows    map[int][]object.Value `json:"rows"`
		Deleted []int                  `json:"deleted,omitempty"`
		Cleared bool                   `json:"cleared,omitempty"`
		Full    bool                   `json:"full,omitempty"`
	}{Rows: dirtyRows, Deleted: deletedRows, Cleared: cleared, Full: full}
	body, err := ujson.Marshal(env)
	if err != nil {
		return false
	}
	push := proto.NotifyPush{
		Target:       e.target(e.key),
		MsgID:        notifyMsgID,
		Body:         body,
		DeliveryMode: proto.DeliveryModeReliable,
	}
	buf, err := proto.EncodeNotifyPush(&push)
	if err != nil {
		return false
	}
	if err := e.pub.Publish(e.subject, buf); err != nil {
		// 与 broadcastPatch 一致，Publish 失败返回 false 以保留行级脏标记供重试。
		logger.Errorf("sync: publish %d to %s failed: %v", notifyMsgID, e.subject, err)
		return false
	}
	return true
}
