// Package gobject 统一游戏对象内核。

// 本包把 base 已有对象内核能力合而为一，沉淀为「所有游戏实体（玩家 / 军团 / 怪物 / 场景 / 服务器…）
// 共用的统一对象」：

//  1. 身份（object.ObjectID）：稳定、带类型、可线化，可当 map 键、可网络传输。
//  2. 属性（object.Bag）：强类型、按字段名读写，标准化 JSON 线化，内建变更追踪
//     （金币 / 等级 / 各种标量）。
//  3. 表（data.Record）：强类型「列名 + 列类型」schema，单元格按 (行,列) 读写，行级变更追踪
//     （背包 / 邮件 / 任务…「多行同构」数据）。
//  4. 持久化（data.Store）：props 与每张表各自落一个三元键
//     Key{OwnerObject, id, "props"} / Key{OwnerObject, id, "rec:<name>"}，跨 redis/mysql/cache/memory/mmo 五模式。
//  5. 增量同步（data.SyncEntity）：Save 时仅广播「变动字段 / 变动行」ChangeSet
//     （数据变动自动推送，在线客户端即时生效）。
//  6. 消息路由（object.Manager）：GameObject 满足 object.Object，可被 Manager 按号注册；业务经
//     Manager.Handle(objType, msgType, h) 绑定处理器后，manager.Send(objID, msg) 即「给对象号发消息」。

// 这是「按 ID 读改任意对象 + 数据变动自动推送 + 给对象号发消息 + 客户端/服务器数据格式互通」
// 的统一地基。
package gobject

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/domain/object"
)

// GameObject 统一游戏对象（活体：线上对象加载后常驻内存，业务直接读写 Props/Records；
// 下线 / 周期调用 Save 落库 + 同步）。
type GameObject struct {
	id      object.ObjectID
	mu      sync.RWMutex // 保护 records / props / propMsg / recMsg / sync / autoSync / schema / components 的并发读写
	props   *object.Bag
	records map[string]*data.Record
	store   *data.Store

	// 同步配置（可选）：注入发布器后 Save 自动广播变动。
	sync    *data.SyncEntity
	propMsg uint32
	recMsg  uint32
	// 发布器与 subject 原样留一份：SetNotifier 只把 *SyncEntity 存进 sync，
	// 而 sync 的字段是私有的 —— 没有这两个就无法给**新建的对象**（级联加载出的子对象）
	// 复制同一套同步装配，只能眼巴巴看着它们"没接同步"（见 children.go loadChildren）。
	notifyPub     data.Publisher
	notifySubject string

	// autoSync：开启「写即自动同步」后，任意 Props/Records 变动立即经 sync 推送增量，
	// Save 仅落库不再重复广播（避免与自动同步撞车）。
	autoSync bool

	// schema：可选的标准化属性定义（客户端/服务器共用）。绑定后可按序号紧凑
	// 编解码、按 Flag 精确控制同步/存盘范围；未绑定时不影响任何既有行为。
	schema *object.Schema

	// 子对象归属（MMO 容器/嵌套对象精华）：slot → 子对象 id 列表（持久化）；
	// childPtrs 为内存中已挂子对象的 id → 实例映射（运行时装配，不持久化）。
	childIDs  map[string][]object.ObjectID
	childPtrs map[object.ObjectID]*GameObject

	// components：运行时组件槽（运行时装配，不持久化）。
	// 限流器、状态机、定时器、AI 行为树等运行时对象均通过 Component 接口挂载。
	components map[string]Component

	// syncCtx：自动同步广播（写即推送）的生命周期上下文。
	//
	// 为什么需要它：自动同步的回调是"对象活多久就挂多久"的（见 attachRecordNotifier），
	// 回调里原先是裸 context.Background() —— 既无超时、也无取消，
	// 持有者（会话 / 房间 / 停机流程）结束后广播照旧在跑。
	// 现在：本 ctx 由持有者经 SetSyncContext 绑定，取消后广播被丢弃；
	// 每次广播再叠加单次超时（见 broadcastCtx）。
	syncCtx context.Context

	// version：乐观并发版本（运行时装配，不持久化；持久化于 "ver" 键）。
	version int64
	// verMu 保护 loadVersion/compare/saveVersion 整条路径，确保 Save/SaveIfVersion 的
	// 版本单调递增且 SaveIfVersion 能原子地完成「读版本 → 比较 → 写数据 → 写版本」。
	verMu sync.Mutex
}

// 通常随后调用 Load 从 Store 载入，或直接纯内存使用（如单元测试 / 离线构造）。
func New(store *data.Store, id object.ObjectID) *GameObject {
	return &GameObject{
		id:         id,
		props:      object.NewBag(),
		records:    make(map[string]*data.Record),
		store:      store,
		childIDs:   make(map[string][]object.ObjectID),
		childPtrs:  make(map[object.ObjectID]*GameObject),
		components: make(map[string]Component),
		syncCtx:    context.Background(),
	}
}

// syncBroadcastTimeout 单次自动同步广播（写即推送）的时间上界。
//
// 取值理由：这条路径只做「序列化 + 发布到消息总线」，正常是微秒级；
// 5 秒已是"下游完全不可用"的量级 —— 超过它继续等只会把写路径一起拖住
// （回调是在业务写字段的调用栈里同步执行的）。
const syncBroadcastTimeout = 5 * time.Second

// SetSyncContext 绑定自动同步广播的生命周期上下文。
//
//   - 传入的 ctx 被取消后，**之后的自动同步广播会被丢弃**（不再下发）——
//     用于「持有者已结束」（会话断开 / 房间销毁 / 服务停机）的场景，
//     避免"持有者没了、回调还在跑"；
//   - 传 nil 恢复为 context.Background()（此时仅靠单次超时兜底）。
//
// 只影响**自动同步回调**（写即推送）；显式 SaveProps/SaveRecord/Save 仍用调用方自己的 ctx。
func (g *GameObject) SetSyncContext(ctx context.Context) {
	g.mu.Lock()
	if ctx == nil {
		ctx = context.Background()
	}
	g.syncCtx = ctx
	g.mu.Unlock()
}

// broadcastCtx 取自动同步广播用的上下文：持有者 ctx（若有）+ 单次超时。
// 返回的 cancel 必须由调用方在广播结束后调用（否则超时计时器要等到超时点才释放）。
func (g *GameObject) broadcastCtx() (context.Context, context.CancelFunc) {
	g.mu.RLock()
	parent := g.syncCtx
	g.mu.RUnlock()
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, syncBroadcastTimeout)
}

// ObjectID 返回对象号（满足 object.Object，可被 object.Manager 按号注册与派发）。
func (g *GameObject) ObjectID() object.ObjectID { return g.id }

// Props 返回属性袋（金币 / 等级 / 各种标量字段）。
func (g *GameObject) Props() *object.Bag {
	g.mu.RLock()
	p := g.props
	g.mu.RUnlock()
	return p
}

// AddRecord 定义一张命名表（如 "bag" / "mail" / "task"），返回该表指针以便后续读写。
// cols/colTypes 长度须一致（列名 + 列类型，强类型 schema）。需在 Load 前定义，Save 才会落它。
// 若已 EnableAutoSync，新表会自动挂上变动回调（无需再次调用）。
func (g *GameObject) AddRecord(name string, cols []string, colTypes []object.Type) *data.Record {
	r := data.NewRecord(cols, colTypes)
	g.mu.Lock()
	g.records[name] = r
	// 「是否自动同步」的判定必须在锁内一次取完：锁外先判 g.autoSync 再判 g.sync，
	// 中间可能被 SetNotifier(nil) / DisableAutoSync 挤进来，判到「开启 + 下一步已为 nil」的
	// 组合，回调挂上去后每次写表都会解引用空指针。
	auto := g.autoSync && g.sync != nil
	g.mu.Unlock()
	if auto {
		g.attachRecordNotifier(name, r)
	}
	return r
}

// attachRecordNotifier 给某张表挂「变动即推送」回调（写即自动同步）。
//
// 同步器为 nil 时直接不挂：回调是闭包捕获 sy，一旦捕获到 nil，
// 之后每一次写表都会在 BroadcastRecordPatch 上崩。
func (g *GameObject) attachRecordNotifier(_ string, r *data.Record) {
	if r == nil {
		return
	}
	g.mu.RLock()
	rm := g.recMsg
	sy := g.sync
	g.mu.RUnlock()
	if sy == nil {
		return
	}
	r.SetNotifier(func(_, _ int, _ object.Value, _ bool) {
		// 不再用裸 context.Background()：带持有者生命周期 + 单次超时（见 broadcastCtx）。
		ctx, cancel := g.broadcastCtx()
		defer cancel()
		sy.BroadcastRecordPatch(ctx, r, rm)
	})
}

// attachPropsNotifier 给 props 挂「变动即推送」回调（写即自动同步）。
// 同步器为 nil 时直接不挂（回调闭包捕获 sy，捕获到 nil 会在每次写 Bag 时崩）。
func (g *GameObject) attachPropsNotifier() {
	g.mu.RLock()
	pm := g.propMsg
	sy := g.sync
	g.mu.RUnlock()
	if sy == nil {
		return
	}
	g.props.SetNotifier(func(_ string, _ object.Value) {
		// 同 attachRecordNotifier：持有者生命周期 + 单次超时，不用裸 Background。
		ctx, cancel := g.broadcastCtx()
		defer cancel()
		sy.BroadcastPatch(ctx, g.props, pm)
	})
}

// Record 取命名表。
func (g *GameObject) Record(name string) (*data.Record, bool) {
	g.mu.RLock()
	r, ok := g.records[name]
	g.mu.RUnlock()
	return r, ok
}

// RecordNames 返回全部命名表名（升序），便于 Save 遍历与调试展示。
func (g *GameObject) RecordNames() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ns := make([]string, 0, len(g.records))
	for n := range g.records {
		ns = append(ns, n)
	}
	sort.Strings(ns)
	return ns
}

// SetNotifier 接入 data-event 自动同步：设置发布器 + 广播 subject + 两类消息号。
// 之后每次 Save 都会把 props 的变动字段、records 的变动行精准广播（呼应 MMO SyncRecordChange）。
// pub 传 nil 表示仅落库不广播（如离线批量改库、纯内存对象）。
func (g *GameObject) SetNotifier(pub data.Publisher, subject string, propMsg, recMsg uint32) {
	g.mu.Lock()
	g.propMsg = propMsg
	g.recMsg = recMsg
	if pub != nil {
		g.sync = data.NewSyncEntity(g.store, pub, subject, g.propKey())
		// 留一份原始装配，供级联加载出的子对象复制（见 wireSyncTo）。
		g.notifyPub = pub
		g.notifySubject = subject
		g.mu.Unlock()
		return
	}
	// pub == nil：仅落库不广播。必须同时解掉已挂的回调（props + 各 record），
	// 否则旧回调仍闭包捕获旧发布器 —— 写表照样广播，而 autoSync 已关的 Save 又不再广播，
	// 语义自相矛盾（表现为「明明关了同步，客户端却还在收到变更」）。
	g.sync = nil
	g.autoSync = false
	g.notifyPub = nil
	g.notifySubject = ""
	props := g.props
	recs := make([]*data.Record, 0, len(g.records))
	for _, r := range g.records {
		recs = append(recs, r)
	}
	g.mu.Unlock()
	if props != nil {
		props.SetNotifier(nil)
	}
	for _, r := range recs {
		r.SetNotifier(nil)
	}
}

// wireSyncTo 把本对象的同步装配（SetNotifier 的参数 + autoSync 开关）复制到另一个**新建**对象。
//
// 用途：级联加载出的子对象（loadChildren）必须是「和父对象同一套同步能力」的，
// 否则子对象写变动静默不同步 —— objstore.Repository 的 Create/Load 会给对象接同步，
// 但 gobject 内部按 id 补建的子对象不经过仓库，就会漏掉这一步（本缺陷的原形）。
//
// 语义：
//   - 本对象没接发布器（sync == nil）时**不动**子对象：保持"仅落库不广播"的默认语义；
//   - 子对象已接同步时不覆盖（避免把业务为子对象单独配的 subject 改掉）。
func (g *GameObject) wireSyncTo(child *GameObject) {
	if child == nil {
		return
	}
	g.mu.RLock()
	pub, subject, pm, rm, auto, sy := g.notifyPub, g.notifySubject, g.propMsg, g.recMsg, g.autoSync, g.sync
	g.mu.RUnlock()
	if sy == nil || pub == nil {
		return
	}
	child.mu.RLock()
	childWired := child.sync != nil
	child.mu.RUnlock()
	if childWired {
		return
	}
	child.SetNotifier(pub, subject, pm, rm)
	if auto {
		child.EnableAutoSync()
	}
}

// EnableAutoSync 开启「写即自动同步」（MMO 精髓：业务改一个字段，在线客户端即时生效，零侵入）。
// 须在 SetNotifier(pub!=nil) 之后调用。开启后：
//   - 任意 Props().SetXxx / Record.SetCell / AddRow / DeleteRow / Clear / AddColumn
//     都会立即经发布器推送增量（字段级 patch / 行级 rows），无需显式 Save；
//   - Save 仅落库（不再重复广播，避免和自动同步撞车）。

// pub 为 nil 时本方法无效（无发布器可推），保持默认「Save 才广播」语义。
func (g *GameObject) EnableAutoSync() {
	// 同步器判定与 autoSync 置位必须在**同一次持锁**里完成：
	// 先 RLock 读 g.sync、放开、再 Lock 置 autoSync —— 中间被 SetNotifier(nil) 挤进来时，
	// 会留下「autoSync=true 而 sync=nil」的组合，之后 Save 既不走自动同步、也不再广播，
	// 更新就静默不同步了（客户端一直看不到）。
	g.mu.Lock()
	sy := g.sync
	if sy == nil {
		// pub 为 nil 时本方法无效，保持默认「Save 才广播」语义。
		g.mu.Unlock()
		return
	}
	g.autoSync = true
	// 持锁只做快照：Record 自带锁，挂回调没必要嵌在 g.mu 里。
	names := make([]string, 0, len(g.records))
	recs := make([]*data.Record, 0, len(g.records))
	for n, r := range g.records {
		names = append(names, n)
		recs = append(recs, r)
	}
	g.mu.Unlock()

	g.attachPropsNotifier()
	for i := range recs {
		g.attachRecordNotifier(names[i], recs[i])
	}
}

func (g *GameObject) DisableAutoSync() {
	g.mu.Lock()
	g.autoSync = false
	g.props.SetNotifier(nil)
	recs := make([]*data.Record, 0, len(g.records))
	for _, r := range g.records {
		recs = append(recs, r)
	}
	g.mu.Unlock()
	for _, r := range recs {
		r.SetNotifier(nil)
	}
}

// 持久化键
func (g *GameObject) propKey() data.Key {
	return data.Key{Owner: data.OwnerObject, ID: g.id.String(), Type: "props"}
}

func (g *GameObject) recordKey(name string) data.Key {
	return data.Key{Owner: data.OwnerObject, ID: g.id.String(), Type: "rec:" + name}
}

// 加载 / 保存
// Load 从 Store 载入 props 与所有已定义 records；不存在的键视为空（不报错）。
// 载入后清除脏标记（避免把「加载」当成「变动」广播出去）。
// 注意：records 的 schema（列名 + 列类型）随数据一起从 Store 恢复，覆盖 AddRecord 时声明的 schema。
func (g *GameObject) Load(ctx context.Context) error {
	if err := g.store.LoadJSON(ctx, g.propKey(), g.props); err != nil && err != data.ErrNotFound {
		return err
	}
	g.props.MarkClean()
	// 先快照表名（持锁仅做内存拷贝，I/O 在锁外进行），避免与并发 AddRecord 竞争 map。
	g.mu.RLock()
	names := make([]string, 0, len(g.records))
	for n := range g.records {
		names = append(names, n)
	}
	g.mu.RUnlock()
	for _, name := range names {
		g.mu.RLock()
		rec, ok := g.records[name]
		g.mu.RUnlock()
		if !ok {
			continue
		}
		if err := g.store.LoadJSON(ctx, g.recordKey(name), rec); err != nil && err != data.ErrNotFound {
			return err
		}
		rec.MarkClean()
	}
	if err := g.loadChildren(ctx); err != nil {
		return err
	}
	v, err := g.loadVersion(ctx)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.version = v
	g.mu.Unlock()
	return nil
}

// Save 落库 props 与所有 records；若已 SetNotifier，则各自仅广播变动部分。
// 同时级联保存子对象并 bump 乐观并发版本号。
func (g *GameObject) Save(ctx context.Context) error {
	return g.withSaveLock(ctx, func() error { return g.saveBody(ctx) })
}

// saveBody 保存 props/records/children（不碰版本号）。调用方须已持有 verMu。
func (g *GameObject) saveBody(ctx context.Context) error {
	if err := g.saveProps(ctx); err != nil {
		return err
	}
	for _, name := range g.RecordNames() {
		if err := g.saveRecord(ctx, name); err != nil {
			return err
		}
	}
	return g.saveChildren(ctx)
}

func (g *GameObject) saveProps(ctx context.Context) error {
	if err := g.store.SaveJSON(ctx, g.propKey(), g.props); err != nil {
		return err
	}
	g.mu.RLock()
	sy := g.sync
	auto := g.autoSync
	pm := g.propMsg
	g.mu.RUnlock()
	if sy != nil && !auto {
		sy.BroadcastPatch(ctx, g.props, pm)
	}
	g.props.MarkClean()
	return nil
}

func (g *GameObject) saveRecord(ctx context.Context, name string) error {
	g.mu.RLock()
	rec, ok := g.records[name]
	g.mu.RUnlock()
	if !ok {
		return nil
	}
	if err := g.store.SaveJSON(ctx, g.recordKey(name), rec); err != nil {
		return err
	}
	g.mu.RLock()
	sy := g.sync
	auto := g.autoSync
	rm := g.recMsg
	g.mu.RUnlock()
	if sy != nil && !auto {
		sy.BroadcastRecordPatch(ctx, rec, rm)
	}
	rec.MarkClean()
	return nil
}

// SaveProps 仅落库 + 广播 props（细粒度，适合「只改了玩家标量属性」）。
func (g *GameObject) SaveProps(ctx context.Context) error {
	return g.withSaveLock(ctx, func() error { return g.saveProps(ctx) })
}

// SaveRecord 仅落库 + 广播某张表（细粒度，适合「只改了背包第 3 格」）。
func (g *GameObject) SaveRecord(ctx context.Context, name string) error {
	return g.withSaveLock(ctx, func() error { return g.saveRecord(ctx, name) })
}

// withSaveLock 获取全局 ID 写锁 + 版本写锁，执行 fn，成功后 bump 乐观并发版本号。
// Save/SaveProps/SaveRecord 均统一走此模板，锁协议集中管理。
func (g *GameObject) withSaveLock(ctx context.Context, fn func() error) error {
	gl := idLock(g.id.String())
	gl.Lock()
	defer gl.Unlock()
	g.verMu.Lock()
	defer g.verMu.Unlock()
	if err := fn(); err != nil {
		return err
	}
	return g.bumpVersionLocked(ctx)
}

// Delete 从 Store 删除 props 与所有 records（彻底移除该对象数据；不卸载内存中的对象）。
// 同时删除子对象归属索引并级联删除所有已挂子对象（递归）。
func (g *GameObject) Delete(ctx context.Context) error {
	if err := g.store.Delete(ctx, g.propKey()); err != nil && err != data.ErrNotFound {
		return err
	}
	for _, name := range g.RecordNames() {
		if err := g.store.Delete(ctx, g.recordKey(name)); err != nil && err != data.ErrNotFound {
			return err
		}
	}
	if err := g.deleteChildren(ctx); err != nil {
		return err
	}
	// 对象已彻底删除，回收全局 id 锁，防止 verLocks 无限膨胀。
	forgetVerLock(g.id.String())
	return nil
}

// ApplyPropsCompact 用紧凑 JSON（{字段名: 裸值}）按字段名合并改 props，变动标脏。
// 命令行友好入口：{"gold":999,"name":"bob"} 即可按名改任意字段；数字整/小推断、对象"type:seq"。
func (g *GameObject) ApplyPropsCompact(b []byte) error {
	return g.props.ApplyCompact(b)
}

// 标准化线化（客户端 / 服务器共用同一套编码）
type gameObjectWire struct {
	Props    *object.Bag             `json:"props"`
	Records  map[string]*data.Record `json:"records"`
	Children childSummary            `json:"children"`
}

// MarshalJSON 完整线化：{props: 属性袋, records: {表名: 表}, children: 子对象归属}。
// 便于调试快照、客户端/服务器一致解析。
// 注意：规范持久化仍是 Save 的按键分散存储；本方法主要用于「整体查看 / 快照」。
func (g *GameObject) MarshalJSON() ([]byte, error) {
	g.mu.RLock()
	recs := make(map[string]*data.Record, len(g.records))
	for k, v := range g.records {
		recs[k] = v
	}
	g.mu.RUnlock()
	return json.Marshal(gameObjectWire{Props: g.Props(), Records: recs, Children: g.childrenSummary()})
}

// UnmarshalJSON 从完整线化格式载入 props 与 records（覆盖式），并恢复子对象归属索引。
func (g *GameObject) UnmarshalJSON(b []byte) error {
	var w gameObjectWire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	g.mu.Lock()
	if w.Props != nil {
		g.props = w.Props
	}
	newRecNames := make([]string, 0, len(w.Records))
	for k, v := range w.Records {
		g.records[k] = v
		newRecNames = append(newRecNames, k)
	}
	// 反序列化换进来的是**新建的 Bag / Record 实例**，不带 onChange 回调；
	// 若此前已 EnableAutoSync，必须补挂，否则此后写这些字段静默不同步
	// （帧推照旧走，客户端却再也收不到变更）。
	auto := g.autoSync && g.sync != nil
	g.mu.Unlock()
	if auto {
		g.attachPropsNotifier()
		for _, name := range newRecNames {
			if rec, ok := g.Record(name); ok {
				g.attachRecordNotifier(name, rec)
			}
		}
	}
	g.applyChildrenSummary(w.Children)
	return nil
}
