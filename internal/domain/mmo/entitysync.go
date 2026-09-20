// entitysync.go 把「实体数据变更广播」桥接到「视野同步」：
//
// 1. 订阅 opts.viewSubject，解析 viewPush 的 enter/leave：
//   - 维护反向索引「谁在观察哪个实体」（map[objID]map[watcherID]bool）；
//   - **并把 enter/leave 下发给 watcher 客户端**（EPushDataSync + 外层 key = 事件名）。
//     少了这一步，客户端 Game.Sync.OnEntityEnter/OnEntityLeave 永远不触发，
//     且 Scene 预先取好的视图快照无人使用（历史缺陷，已修）。
//
// 2. 订阅 accessor.Accessor 的 notify subject（与 Accessor.NotifySubject 同源），
// accessor.Accessor.broadcast 把变更（kind/id 编入信封、已剔除 ServerOnly）发到该 subject；
// 本文件收到后用反向索引查出视野内的 watcher，仅对这些 player 经
// push.PushToPlayer 推送；若该实体本身是 player（kind==OwnerPlayer），也推给自己。
//
// 可见性过滤已在源头（accessor.Accessor.broadcast）完成，纯服务器数据根本不进线；
// 本文件只负责「按视野路由」，不重复过滤。
//
// 接入方式：在 NewSceneManager 之后调用 WireEntitySync(w, sub, pub)（用函数而非新增 SceneManager option，
// 以免改动 mmo.go）。sub 注入订阅能力（NATS Client 或测试替身），pub 用于把 patch 推给玩家。
package mmo

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"

	idata "clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/internal/transport/net/push"
	"clover-server-engine/internal/transport/pubsub"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/conv"
)

// Subscriber 订阅接口（统一定义在 transport/pubsub）。
type Subscriber = pubsub.Subscriber

// Unsubscriber 订阅反注册接口（统一定义在 transport/pubsub）。
// 可选能力：订阅方实现了它，实体同步管线 / 跨机迁移接收端才能在 Stop 时退回自己的订阅。
type Unsubscriber = pubsub.Unsubscriber

// EntityAccessor 提供实体快照与变更通知 subject（与 accessor.Accessor 对齐）。
type EntityAccessor interface {
	NotifySubject() string
}

// EntitySync 维护「谁在观察哪个实体」的反向索引，把实体数据变更只推给视野内的玩家。
type EntitySync struct {
	mu       sync.RWMutex
	watchers map[uint64]map[uint64]bool // objID -> {watcherID: true}
	pub      idata.Publisher            // 推送给玩家用的发布器（-> 网关 clover.notify）
	pushSubj string                     // 推送玩家用的 NATS subject（网关下行）

	// sub / subs 记录本管线注册过的**订阅**，供 Close 反注册：
	// 订阅生命周期必须与持有者一致，否则模块停了回调还在跑（改动已释放的状态）。
	sub  Subscriber
	subs []string
}

// WireEntitySync 把视野同步桥接到实体变更广播。应在 NewSceneManager 之后调用一次：
// - 订阅 viewSubject，维护反向索引；
// - 订阅 entityAcc 的 notify subject，把实体变更只推给视野内玩家（并推给自己若是 player）。
//
// viewSubject 是视野事件的 NATS channel；entityAcc 提供实体变更通知的 subject；
// sub 注入订阅能力（NATS Client 或测试替身）；pub 用于把过滤后的 patch 推给玩家。
func WireEntitySync(viewSubject string, entityAcc EntityAccessor, sub Subscriber, pub idata.Publisher) error {
	_, err := wireEntitySync(viewSubject, entityAcc, sub, pub)
	return err
}

// WireEntitySyncHandle 与 WireEntitySync 等价，但额外返回 *EntitySync 句柄。
//
// 给它句柄是为了**能反注册订阅**：调用方停止时必须 Close() 它（幂等），
// 否则 viewSubject / notify subject 两条订阅会一直挂着、回调在持有者停止后继续被派发。
// 由 Module 持有生命周期时不必显式调用（Module.Stop 已代为 Close）。
func WireEntitySyncHandle(viewSubject string, entityAcc EntityAccessor, sub Subscriber, pub idata.Publisher) (*EntitySync, error) {
	return wireEntitySync(viewSubject, entityAcc, sub, pub)
}

// wireEntitySync 是 WireEntitySync 的内部实现，额外返回 *EntitySync 句柄。
//
// 多返回一个句柄是为了「断线清理」：watchers 反向索引只在 AOI 的 enter/leave 事件里增删，
// 一旦 leave 事件丢失（节点崩溃 / 场景未 Leave 即销毁 / 订阅断档），objID→{watcher}
// 会永久残留成幽灵视野，必须给上层一个按 watcher 清理的入口。
func wireEntitySync(viewSubject string, entityAcc EntityAccessor, sub Subscriber, pub idata.Publisher) (*EntitySync, error) {
	es := &EntitySync{
		watchers: make(map[uint64]map[uint64]bool),
		pub:      pub,
		pushSubj: proto.NATSSubjectNotify,
	}
	// 与 accessor.Accessor 同源：广播发到 Accessor 的 notify subject，本处订阅同一信道。
	// ★ 先校验再订阅：反过来的话，notifySubj 为空时 viewSubject 的订阅已经注册成功，
	// 调用方拿到错误就走人，那条订阅会永远悬挂着（部分接线泄漏）。
	notifySubj := ""
	if !isNilEntityAccessor(entityAcc) {
		notifySubj = entityAcc.NotifySubject()
	}
	if notifySubj == "" {
		return nil, errors.New("entitysync: entity accessor has no notify subject")
	}
	if err := sub.Subscribe(viewSubject, es.onViewChange); err != nil {
		return nil, err
	}
	es.sub = sub
	es.subs = append(es.subs, viewSubject)
	if err := sub.Subscribe(notifySubj, es.onEntityChange); err != nil {
		// 第二条订阅失败：必须先把第一条退掉再返回错误，否则调用方拿到 error 就走人，
		// 那条 viewSubject 订阅会永远悬挂（部分接线泄漏，与上面「先校验再订阅」同一类）。
		es.Close()
		return nil, err
	}
	es.subs = append(es.subs, notifySubj)
	return es, nil
}

// Close 反注册本管线注册过的订阅（与 wireEntitySync 对称）。
//
// 为什么需要：wireEntitySync 订阅的是外部 subject（viewSubject + accessor 的 notify
// subject），订阅一旦注册就由 NATS 派发，**不会**因为调用方不再持有 *EntitySync 而停止。
// 不反注册 = 模块已停、回调仍在跑（改动已释放的状态），且只有整个 NATS 客户端 Close
// 才会真正退订 —— 长跑进程里这就是一条隐藏的泄漏。
//
// 幂等：重复调用只退一次（subs 清空后即空操作）。
// 订阅方未实现 Unsubscriber 时降级：只告警，不阻断（退化成旧行为）。
func (es *EntitySync) Close() {
	if es == nil {
		return
	}
	es.mu.Lock()
	sub := es.sub
	subjects := es.subs
	es.subs = nil
	es.mu.Unlock()
	if len(subjects) == 0 {
		return
	}
	u, ok := sub.(pubsub.Unsubscriber)
	if !ok {
		logger.Warnf("mmo: entitysync 的订阅方 %T 不支持反注册，%d 条订阅(%v)无法单独撤销，"+
			"只能等底层客户端 Close 才能释放", sub, len(subjects), subjects)
		return
	}
	for _, s := range subjects {
		if err := u.Unsubscribe(s); err != nil {
			logger.Warnf("mmo: entitysync 反注册订阅 %s 失败: %v", s, err)
			continue
		}
		logger.Infof("mmo: entitysync 已反注册订阅 %s", s)
	}
}

// isNilEntityAccessor 同时挡住「nil 接口」与「typed-nil 指针」。
//
// 调用方常写 `var acc *accessor.Accessor; WireEntitySync(subj, acc, ...)`：
// 此时接口值非 nil（动态类型是 *accessor.Accessor），`entityAcc != nil` 判不出来，
// 随后 entityAcc.NotifySubject() 解引用 nil 指针直接 panic。
func isNilEntityAccessor(v EntityAccessor) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
		return rv.IsNil()
	}
	return false
}

// dropWatcher 清理某个观察者留下的全部反向索引条目（断线 / leave 事件丢失时兜底）。
func (es *EntitySync) dropWatcher(watcherID uint64) {
	if watcherID == 0 {
		return
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	for objID, m := range es.watchers {
		if _, ok := m[watcherID]; !ok {
			continue
		}
		delete(m, watcherID)
		if len(m) == 0 {
			delete(es.watchers, objID)
		}
	}
}

// onViewChange 处理视野 enter/leave，更新反向索引。
// 线格式只有二进制 viewproto：批量（Scene 聚合后一次性发布）与 viewTypeDirect 单发。
// direct 消息直接路由到目标玩家连接。
func (es *EntitySync) onViewChange(_ string, payload []byte) {
	if p, ok, err := tryDecodeDirect(payload); err == nil && ok {
		es.routeDirect(p)
		return
	}
	batch, err := decodeViewPayload(payload)
	if err != nil {
		// 非预期分支必须留痕：payload 来自 NATS 视野 subject，编码侧与解码侧一旦
		// 线格式不一致，静默 return 的表现只是"玩家看不到实体"，现场无从排查。
		logger.Errorf("entitysync: decode view payload failed (len=%d): %v", len(payload), err)
		return
	}
	for _, vp := range batch {
		switch vp.Event {
		case "enter":
			es.addWatcher(vp.Object, vp.Watcher)
		case "leave":
			es.removeWatcher(vp.Object, vp.Watcher)
		default:
			// 理论上不可达（viewproto 只编 enter/leave）；真出现说明线格式与解析不一致，必须留痕。
			viewPushFailf("mmo: unknown view event %q (watcher=%d object=%d), dropped", vp.Event, vp.Watcher, vp.Object)
			continue
		}
		// 索引更新后立刻下发：客户端靠这条事件创建/销毁视野内实体。
		//
		// ⚠️ 已知限制（明确标注，不是漏写）：这里**丢弃了 vp.SnapshotBin**。
		// 该字段是 Scene 侧进视野时经 RPC 拉取并序列化的实体初始快照（map[kind]二进制，
		// 见 view_sync.go / accessor.SnapshotClientBinary），经 viewproto 一路传到本处，
		// 而本函数下发的 body 只带 entity_id。补齐它需要把快照塞进 EPushDataSync 的 body：
		//   - 快照值是 object.Bag 的**二进制**编码，客户端 WorldSync 的 enter 处理器读的是
		//     JSON 对象（attrs/properties），直接塞进去对端解不开；
		//   - 即"要让客户端能读"，必须改客户端可见的 payload 形状 / 新增二进制快照载体
		//     —— 属于**线格式变更**，与本轮「不改线格式」的约束冲突。
		// 因此本轮只做标注（保持现状：客户端进视野后由常规数据同步补齐实体属性）。
		// 若要补齐：与客户端一起做，把快照以 attrs 可解析的形状（或二进制字段 + 客户端解析）下发。
		es.pushViewEvent(vp.Watcher, vp.Event, vp.Object, vp.DeliveryMode)
	}
}

// pushViewEvent 把一次视野进出事件下发给 watcher（玩家）。
//
// 线格式（与客户端 clover-client-unity-engine/Runtime/Network/WorldSync.cs 的
// HandleDataSyncPush → DispatchEntityEvent 对齐）：
//   - 消息号必须是 EPushDataSync(4003) —— 客户端只在 4001/4003 上分派实体事件；
//   - body 的**外层 key 必须是事件名**（enter/leave），内层携带 entity_id；
//     外层 key 不是事件名时客户端会静默丢弃，因此这里不能改成 {event:..., data:...} 之类的形状。
func (es *EntitySync) pushViewEvent(watcherID uint64, event string, objID uint64, mode proto.DeliveryMode) {
	if es.pub == nil || watcherID == 0 || objID == 0 {
		return
	}
	body, err := json.Marshal(map[string]map[string]uint64{
		event: {"entity_id": objID},
	})
	if err != nil {
		viewPushFailf("mmo: marshal view event failed (event=%s watcher=%d object=%d): %v", event, watcherID, objID, err)
		return
	}
	if err := push.Push(&push.PlayerChannel{
		Pub:          es.pub,
		Subject:      es.pushSubj,
		PlayerID:     conv.FormatUint(watcherID),
		DeliveryMode: mode,
	}, proto.EPushDataSync, body); err != nil {
		viewPushFailf("mmo: push view event failed (event=%s watcher=%d object=%d): %v", event, watcherID, objID, err)
	}
}

// viewPushFailf 视野事件下发的失败日志（高频路径：首次全量，之后每 1000 次一条，既不刷屏也不静默）。
var viewPushFailCount atomic.Uint64

func viewPushFailf(format string, args ...any) {
	n := viewPushFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// routeDirect 把单发消息（SendTo）直接推给目标玩家连接。
func (es *EntitySync) routeDirect(p viewPush) {
	if p.Target == 0 {
		return
	}
	if err := push.Push(&push.PlayerChannel{
		Pub:          es.pub,
		Subject:      es.pushSubj,
		PlayerID:     conv.FormatUint(p.Target),
		DeliveryMode: p.DeliveryMode,
	}, p.MsgID, p.Body); err != nil {
		// 与 pushViewEvent 同一口径：direct 消息推送失败必须可观测（降频不静默）。
		viewPushFailf("mmo: push direct message failed (target=%d msg=%d): %v", p.Target, p.MsgID, err)
	}
}

func (es *EntitySync) addWatcher(objID, watcherID uint64) {
	if watcherID == 0 || objID == 0 {
		return
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.watchers[objID] == nil {
		es.watchers[objID] = make(map[uint64]bool)
	}
	es.watchers[objID][watcherID] = true
}

func (es *EntitySync) removeWatcher(objID, watcherID uint64) {
	es.mu.Lock()
	defer es.mu.Unlock()
	if m := es.watchers[objID]; m != nil {
		delete(m, watcherID)
		if len(m) == 0 {
			delete(es.watchers, objID)
		}
	}
}

// onEntityChange 处理实体数据变更：按反向索引只推给视野内玩家。
// 实体身份（ownerType/id）由推送信封携带（accessor.Accessor.broadcast 编入），不依赖 subject。
//
// 支持两种消息体格式：
// - Accessor 格式：{ownerType, id, patch}（accessor.Accessor.Apply/Replace 发出）
// - commitEdits 格式：直接是数据 diff JSON，无 {ownerType,id} 信封（LoadStruct/LoadRecord
// handler return 时由 commitEdits 自动发出）。此时用 NotifyPush.Target 作为实体 ID，
// 使视野内玩家也能立即收到变更，而不是等下一次 AOI snapshot。
func (es *EntitySync) onEntityChange(_ string, payload []byte) {
	np, err := proto.DecodeNotifyPush(payload)
	if err != nil {
		viewPushFailf("mmo: decode notify push failed (len=%d): %v", len(payload), err)
		return
	}
	var env struct {
		OwnerType string          `json:"owner_type"`
		ID        string          `json:"id"`
		Patch     json.RawMessage `json:"patch"`
	}
	if err := json.Unmarshal(np.Body, &env); err != nil {
		viewPushFailf("mmo: unmarshal notify envelope failed (target=%s msg=%d): %v", np.Target, np.MsgID, err)
		return
	}
	ownerType := idata.OwnerType(env.OwnerType)
	objID, err := strconv.ParseUint(env.ID, 10, 64)
	isFallback := false
	if err != nil {
		// 回退：commitEdits 格式（LoadStruct/LoadRecord handler return 自动同步）。
		// Body 不含 {ownerType,id} 信封，实体 ID 由 NotifyPush.Target 携带。
		if np.Target == "" {
			viewPushFailf("mmo: notify push has neither {owner_type,id} envelope nor Target (msg=%d), dropped", np.MsgID)
			return
		}
		var fbErr error
		objID, fbErr = strconv.ParseUint(np.Target, 10, 64)
		if fbErr != nil {
			viewPushFailf("mmo: parse notify push target %q failed: %v", np.Target, fbErr)
			return
		}
		ownerType = idata.OwnerPlayer // commitEdits 源于玩家侧 handler，变更归属为玩家实体
		isFallback = true
	}
	// 收集目标玩家：视野内的 watcher。若为 Accessor 格式且实体是 player，也包含自身。
	// commitEdits 回退路径不包含自身，因为网关已通过 Target 推送过。
	targets := make(map[uint64]bool)
	es.mu.RLock()
	for w := range es.watchers[objID] {
		targets[w] = true
	}
	es.mu.RUnlock()
	if !isFallback && ownerType == idata.OwnerPlayer {
		targets[objID] = true
	}
	for pid := range targets {
		if err := push.Push(&push.PlayerChannel{
			Pub:          es.pub,
			Subject:      es.pushSubj,
			PlayerID:     conv.FormatUint(pid),
			DeliveryMode: np.DeliveryMode,
		}, np.MsgID, np.Body); err != nil {
			viewPushFailf("mmo: push entity change failed (player=%d obj=%d msg=%d): %v", pid, objID, np.MsgID, err)
		}
	}
}
