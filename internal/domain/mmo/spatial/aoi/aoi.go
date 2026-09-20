// Package aoi 通用兴趣区域（Area of Interest）内核：基于格子的空间分区 + 范围查询 + 视野增量（进入/离开）。
//
// 空间模型（立体空间）：**索引按 (X,Z) 分格与分片，视野按三维球体判定**。
//   - 索引不切 Y 轴：多层楼的同一水平坐标落在同一格（一「列」），垂直方向对象稀疏，
//     再切一层只会白白增加层级与内存，分片数也不会随楼层数膨胀；
//   - 距离判定三维（X/Y/Z 全参与）：楼上楼下按真实空间距离计算，而不是投影到地面后互相可见。
//     2D 俯视玩法把 Y 固定为 0 即可，无需任何开关。
//
// 1. 格子空间分区（SceneGrid）：把地图的 (X,Z) 水平面按 cellSize 切成稀疏格子，每格挂一列对象。
// 对象的进入 / 移动 / 离开只更新所在格子，范围查询只扫描半径覆盖到的少量格子——把 O(N) 的
// 「谁在我周围」降到接近 O(1)，是场景同步的地基。
//
// 2. 视野增量（Viewport 的 GetVisualObjects → add_list / remove_list）：每个「观察者」维护一份
// 当前可见集合；移动或周围对象变化时，只算出「新进入视野」和「离开视野」两份增量，配合
// Observer 回调即时推送（视野变化自动推送）。
//
// 全部对象以 object.ObjectID 标识（与 Manager 注册 / 消息派发共用同一套身份），
// 因此 AOI 与消息内核天然打通：算出「新进入 A 视野的 B」后，可直接 manager.Send(A, EnterView{B}) 推送。
//
// 本包刻意不实现任何寻路 / 碰撞 / 战斗等业务，只提供「谁能看见谁」这一最通用的空间关系。
//
// 并发模型（分片锁）：按世界区域分片——把地图切成若干 super-shard（每片覆盖 superSize
// 见方的世界区域），每片持有自己的 RWMutex，只存落在该区域内的格子 / 对象 / 观察者状态。
// 一次移动仅锁定其影响区域覆盖到的少数几片（按坐标顺序加锁避免死锁），
// 互不重叠的区域可完全并行，避免全局锁争用。
package aoi

import (
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/geom"
)

// aoiFailf 是 AOI 内核的异常降频日志（首次全量 + 之后每 1000 条一条）。
// 视界/查询路径位于每帧热路径，异常分支直打日志会刷屏。
var aoiFailCount atomic.Uint64

func aoiFailf(format string, args ...any) {
	n := aoiFailCount.Add(1)
	if n == 1 || n%1000 == 0 {
		logger.Warnf(format+" (累计 %d 次)", append(args, n)...)
	}
}

// defaultCellsPerShard 每个 super-shard 在每轴覆盖的格子数，决定分片粒度。
// superSize = cellSize * defaultCellsPerShard。取值让单个分片覆盖约一个常见视野半径，
// 使绝大多数查询只触碰 1~4 个分片；视野半径极大时触碰更多分片（仍远小于全局锁）。
const defaultCellsPerShard = 32

// Position 三维坐标（x=东西, y=高度, z=南北）。
//
// 它是 geom.Vec3 的**别名**，不再自定义同构结构体：于是 `geom.Vec3` / `mmo.Vec3` /
// `engine.Vec3` / `aoi.Position` 在类型系统里是同一个类型 —— 业务可以把 `mmo.Vec3`
// 直接传给 AOI 接口，全程零转换（此前需要逐字段 copy）。
type Position = geom.Vec3

// Event 视野事件类型。
type Event int

const (
	// EnterView 目标进入观察者视野。
	EnterView Event = iota + 1
	// LeaveView 目标离开观察者视野。
	LeaveView
	// LeaveAll 群体离开（RemoveAll 广播用）。
	LeaveAll
)

// maxEventsPerDispatch 分批派发时单批的最大事件数，防止单帧突发消息撑爆下游队列。
const maxEventsPerDispatch = 8192

// maxQueryRadius 查询半径上限，防止大范围 O(n^2) 扫描。
const maxQueryRadius = 500.0

// cleanupInterval 空网格/空分片定期清理间隔。
const cleanupInterval = 30 * time.Second

// Observer 视野变化回调：在 watcher 的视野中，target 发生了 ev（进入 / 离开）。
// 回调总是在释放内部锁之后调用，业务可在回调里安全地再调用本包方法或推送网络消息，无死锁风险。
type Observer func(watcher, target object.ObjectID, ev Event)

// PermChecker 观察者权限校验回调，返回 watcher 是否有权观察 target。
// 若为 nil 则跳过权限检查（默认允许所有观察）。在 refreshWatcher 时对每个候选项调用。
type PermChecker func(watcher, target object.ObjectID) bool

// cellKey 稀疏格子索引（按格坐标，支持任意大 / 负坐标，无需预先申请整块地图内存）。
type cellKey struct {
	cx int
	cz int
}

// oidKey 是 object.ObjectID 的 uint64 线化形式（高 16 位 type + 低 48 位 seq），
// 用作 AOI 内部 map 键可降低结构体哈希与比较开销（profile 中 object.ObjectID 哈希占比高）。
type oidKey = uint64

// watchState 单个观察者的视野状态。
type watchState struct {
	radius  float64
	visible map[oidKey]struct{}
	// next 是刷新视野时复用的临时集合，避免每次 refresh 都新建 map。
	next map[oidKey]struct{}
}

// event 内部收集的待派发事件（先在锁内收集、锁外统一派发）。
// 使用 uint64 键避免锁内 object.ObjectID 结构体操作；仅在 dispatch 时还原。
type event struct {
	watcher oidKey
	target  oidKey
	ev      Event
	seq     uint64 // 全局递增序列号，保证消息有序
}

// oidKeySetPool 复用 aroundInShards / refreshWatcherInShards 产生的小型 oidKey 集合，
// 降低高 churn 场景下 map 分配与 GC 压力。
var oidKeySetPool = sync.Pool{New: func() any { return make(map[oidKey]struct{}) }}

// eventBufPool 复用视野事件收集过程中的 []event 缓冲，避免每次 Enter/Move/Leave/Watch
// 与每帧 EndBatch 都分配新切片，把视野事件热路径逼近零分配。
var eventBufPool = sync.Pool{New: func() any { b := make([]event, 0, 64); return &b }}

// oidKeySlicePool 复用 collectAffected 返回的 []oidKey 切片，降低查询路径分配。
var oidKeySlicePool = sync.Pool{New: func() any { b := make([]oidKey, 0, 16); return &b }}

func getEventBuf() []event {
	p := eventBufPool.Get().(*[]event)
	return (*p)[:0]
}
func putEventBuf(b []event) {
	p := b[:0]
	eventBufPool.Put(&p)
}
func getOidKeySlice() []oidKey {
	p := oidKeySlicePool.Get().(*[]oidKey)
	return (*p)[:0]
}
func putOidKeySlice(s []oidKey) {
	if s != nil {
		p := s[:0]
		oidKeySlicePool.Put(&p)
	}
}

// shardCoord super-shard 的世界坐标（每轴一个整数索引，可为负）。
type shardCoord struct {
	x int
	z int
}

// shard 一个世界区域分片：持有自己的锁与落在该区域内的格子 / 对象 / 观察者状态。
// 所有内部 map 均使用 oidKey，避免 object.ObjectID 结构体哈希开销。
// cells 使用 cx -> cz -> set 的双层 map，使范围查询只扫描覆盖到的格子，而不是整个分片的所有格子。
type shard struct {
	coord    shardCoord
	mu       sync.RWMutex
	cells    map[int]map[int]map[oidKey]struct{}
	pos      map[oidKey]Position
	cellOf   map[oidKey]cellKey
	watchers map[oidKey]*watchState
}

func newShard(coord shardCoord) *shard {
	return &shard{
		coord:    coord,
		cells:    make(map[int]map[int]map[oidKey]struct{}),
		pos:      make(map[oidKey]Position),
		cellOf:   make(map[oidKey]cellKey),
		watchers: make(map[oidKey]*watchState),
	}
}

// Grid 基于格子的 AOI 管理器，按世界区域分片，并发安全。
type Grid struct {
	cellSize  float64
	superSize float64

	shardsMu sync.Mutex
	shards   map[shardCoord]*shard

	muIndex sync.RWMutex // 保护 g.cellOf（对象 → 格子，全局快速定位）
	cellOf  map[oidKey]cellKey

	muCount sync.Mutex // 保护对象计数
	count   int

	muMax sync.RWMutex // 保护全局最大视野半径
	maxR  float64

	obsMu    sync.RWMutex
	observer Observer

	permChecker PermChecker // 观察者权限校验回调

	eventSeq atomic.Uint64 // 全局递增事件序列号

	// 定期清理空网格/空分片
	ticker   *time.Ticker
	stopCh   chan struct{}
	stopOnce sync.Once

	// 批量刷新模式：BeginBatch 后 Enter/Move/Leave/Watch/Unwatch 只做坐标/状态更新，
	// 并收集受影响的观察者；EndBatch 时统一刷新这些观察者并派发事件。
	// 用于把一帧内的多次移动合并为一次视野重算，把 O(moves × watchers) 降到 O(watchers)。
	// batching 用 atomic.Bool——Enter/Move/Leave/Watch/Unwatch 中在分片锁内
	// 直接读取该标志，若用普通 bool（仅 batchMu 保护写）会构成 data race。
	batchMu  sync.Mutex
	batching atomic.Bool
	dirty    map[oidKey]struct{}
	pending  []event // batch 期间已经确定的事件（如 Leave 时观察者自己的 leave）
}

// New 创建 AOI 网格；cellSize 为格子边长（建议取略大于常见视野半径的值，过小格子多、过大范围查询扫描多）。
// cellSize<=0 时回落为 1。
//
// 空间模型：对象**索引按 (X,Z) 分格与分片**（垂直方向不分桶，避免分片数随楼层数膨胀），
// 视野判定是**三维球体**（X/Y/Z 全参与距离）—— 因此「楼上楼下」按真实空间距离计算，
// 而不是投影到地面后互相可见。
//
// 若业务要「隔层完全不可见」（视线被楼板挡住），用 SetFilter / SetPermChecker
// 按层过滤即可，AOI 只负责距离范围，遮挡属视线判定。
func New(cellSize float64) *Grid {
	if cellSize <= 0 {
		cellSize = 1
	}
	super := cellSize * defaultCellsPerShard
	if super <= 0 {
		super = 1
	}
	g := &Grid{
		cellSize:  cellSize,
		superSize: super,
		shards:    make(map[shardCoord]*shard),
		cellOf:    make(map[oidKey]cellKey),
	}
	g.startCleanup()
	return g
}

// dist2 返回两点三维距离的平方（X/Y/Z 全参与）。
//
// 这是本包唯一的距离判定原语：视野刷新、受影响观察者收集、配额裁剪排序
// 三处都必须走它，避免出现「视野一套口径、配额另一套口径」的错配。
func (g *Grid) dist2(a, b Position) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	dz := a.Z - b.Z
	return dx*dx + dy*dy + dz*dz
}

// SetObserver 设置视野变化回调（覆盖旧值，传 nil 关闭）。
func (g *Grid) SetObserver(o Observer) {
	g.obsMu.Lock()
	g.observer = o
	g.obsMu.Unlock()
}

func (g *Grid) getObserver() Observer {
	g.obsMu.RLock()
	o := g.observer
	g.obsMu.RUnlock()
	return o
}

// SetPermChecker 设置观察者权限校验回调。nil=全部允许。
func (g *Grid) SetPermChecker(check PermChecker) {
	g.obsMu.Lock()
	g.permChecker = check
	g.obsMu.Unlock()
}

// getPermChecker 安全获取权限校验器。
func (g *Grid) getPermChecker() PermChecker {
	g.obsMu.RLock()
	c := g.permChecker
	g.obsMu.RUnlock()
	return c
}

// 说明：原 `SetRefreshRate` / `shouldRefresh`（含 refreshRate / lastRefresh / refreshMu 字段）
// 已**整体删除**。它们是一组从未接线的"预留 API"：全仓唯一出现位置就是定义处，
// refreshWatcher 路径从不调用 shouldRefresh，因此 SetRefreshRate 设了也不生效、
// 刷新频率实际不受限 —— 对外暴露一个静默失效的限流开关比没有更糟（调用方会以为限住了）。
// 将来若真要做刷新节流，必须实现成「节流 + 漏掉的 enter/leave 补偿」的完整语义，
// 而不是只加一个守卫（跳过刷新会让视野变化事件丢失）。

// startCleanup 启动定期清理协程，清除空分片与残留数据。
func (g *Grid) startCleanup() {
	g.stopCh = make(chan struct{})
	g.ticker = time.NewTicker(cleanupInterval)
	go func() {
		for {
			select {
			case <-g.ticker.C:
				g.cleanupEmptyShards()
			case <-g.stopCh:
				return
			}
		}
	}()
}

// Stop 停止 AOI 网格的定期清理 ticker，释放后台协程。
func (g *Grid) Stop() {
	g.stopOnce.Do(func() {
		if g.ticker != nil {
			g.ticker.Stop()
		}
		// 零值 Grid（未经 New 构造）的 stopCh 为 nil：close(nil) 直接 panic，
		// stopOnce 挡不住（第一次执行就崩）。必须判空。
		if g.stopCh != nil {
			close(g.stopCh)
		}
	})
}

// cleanupEmptyShards 清理空 cell 与不再包含任何对象的分片残留，释放内存。
//
// 并发约束（历史缺陷点）：
//   - 分片内部结构（cells/pos/cellOf/watchers）的读取与清理必须持 sh.mu——
//     此前在只持 shardsMu 时裸读 len(sh.*)，与 Enter/Move 的分片写锁路径构成 data race；
//   - 分片**不**从 g.shards 摘除：lockShardsBox 会先把分片指针交给调用方、之后调用方
//     才加 sh.mu 写入，删除条目会与这个「已发指针、未加锁」的窗口竞态，把对象写进一个
//     已脱离索引表的分片而永久不可见（无引用跟踪可消除该窗口）。分片壳本身体积很小
//     （几个 map 头），保留它换取无竞态。
func (g *Grid) cleanupEmptyShards() {
	// Phase 1：只复制分片指针列表（不读分片内部字段——那些字段受 sh.mu 保护）。
	g.shardsMu.Lock()
	shards := make([]*shard, 0, len(g.shards))
	for _, sh := range g.shards {
		shards = append(shards, sh)
	}
	g.shardsMu.Unlock()

	// Phase 2：逐片持写锁清理空 cell 与残留。
	for _, sh := range shards {
		sh.mu.Lock()
		for cx, row := range sh.cells {
			for cz, set := range row {
				if len(set) == 0 {
					delete(row, cz)
				}
			}
			if len(row) == 0 {
				delete(sh.cells, cx)
			}
		}
		// 清理 pos/cellOf 中不在任何 cell 的残留。
		// 全局索引（g.cellOf）与对象计数必须同步清理：只删分片内索引的话，
		// objectShard 仍会命中该分片，而 sh.pos 里已没有它——两份索引永久不一致。
		for key, ck := range sh.cellOf {
			stale := false
			if row, ok := sh.cells[ck.cx]; !ok {
				stale = true
			} else if set, ok := row[ck.cz]; !ok {
				stale = true
			} else if _, ok := set[key]; !ok {
				stale = true
			}
			if !stale {
				continue
			}
			delete(sh.pos, key)
			delete(sh.cellOf, key)
			g.muIndex.Lock()
			_, present := g.cellOf[key]
			if present {
				delete(g.cellOf, key)
			}
			g.muIndex.Unlock()
			if present {
				g.muCount.Lock()
				g.count--
				g.muCount.Unlock()
			}
		}
		sh.mu.Unlock()
	}
}

// RemoveAll 移除网格中的所有对象，为每个观察者广播 LeaveView 并返回全部对象 ID。
// 保留 watchers 状态以便复用。
func (g *Grid) RemoveAll() []object.ObjectID {
	g.shardsMu.Lock()
	allShards := make([]*shard, 0, len(g.shards))
	for _, sh := range g.shards {
		allShards = append(allShards, sh)
	}
	g.shardsMu.Unlock()

	sort.Slice(allShards, func(i, j int) bool {
		if allShards[i].coord.x != allShards[j].coord.x {
			return allShards[i].coord.x < allShards[j].coord.x
		}
		return allShards[i].coord.z < allShards[j].coord.z
	})
	for _, sh := range allShards {
		sh.mu.Lock()
	}
	unlock := func() {
		for _, sh := range allShards {
			sh.mu.Unlock()
		}
	}

	events := getEventBuf()
	// 返回值必须是**全部对象 ID**（含非观察者）：注释承诺"返回全部对象 ID"，
	// 上层据此清理自己的对象表——只收集 watchers 会让普通对象被永久丢失。
	// pos 是对象全集（watchers 正常是 pos 子集），取并集去重后统一排序。
	ids := make(map[oidKey]struct{}, len(allShards)*32)

	for _, sh := range allShards {
		for key := range sh.pos {
			ids[key] = struct{}{}
		}
		for wid := range sh.watchers {
			ids[wid] = struct{}{}
			w := sh.watchers[wid]
			if w != nil {
				for t := range w.visible {
					events = append(events, event{watcher: wid, target: t, ev: LeaveView})
				}
				w.visible = make(map[oidKey]struct{})
			}
		}
		sh.cells = make(map[int]map[int]map[oidKey]struct{})
		sh.pos = make(map[oidKey]Position)
		sh.cellOf = make(map[oidKey]cellKey)
		// 保留 watchers 以便复用
	}

	g.muIndex.Lock()
	g.cellOf = make(map[oidKey]cellKey)
	g.muIndex.Unlock()
	g.muCount.Lock()
	g.count = 0
	g.muCount.Unlock()

	o := g.getObserver()
	unlock()
	g.dispatch(o, events)
	putEventBuf(events)

	allIDs := make([]object.ObjectID, 0, len(ids))
	for k := range ids {
		allIDs = append(allIDs, object.FromUint64(k))
	}
	sortIDs(allIDs)
	return allIDs
}

// CellSize 返回格子边长。
func (g *Grid) CellSize() float64 { return g.cellSize }

// Count 返回当前在网格中的对象数量。
func (g *Grid) Count() int {
	g.muCount.Lock()
	n := g.count
	g.muCount.Unlock()
	return n
}

// Position 返回对象当前坐标；不在网格中返回 ok=false。
func (g *Grid) Position(id object.ObjectID) (Position, bool) {
	key := id.MarshalUint64()
	sh := g.objectShard(key)
	if sh == nil {
		return Position{}, false
	}
	sh.mu.RLock()
	p, ok := sh.pos[key]
	sh.mu.RUnlock()
	return p, ok
}

// shardCoordForWorld 计算世界坐标 (x,z) 所属 super-shard 坐标。
// 是 lockShardsBox / shardCoordForCell 共用的唯一分片映射，杜绝「按格坐标」与
// 「按世界坐标」两套换算在分片边界上产生不一致。
func (g *Grid) shardCoordForWorld(x, z float64) shardCoord {
	return shardCoord{
		x: int(math.Floor(x / g.superSize)),
		z: int(math.Floor(z / g.superSize)),
	}
}

// shardCoordForCell 计算某格子所属 super-shard 坐标。
// 把格子左下角换算成世界坐标后走 shardCoordForWorld，
// 与 lockShardsBox 的世界坐标分片映射完全一致，避免边界写入未加锁分片。
func (g *Grid) shardCoordForCell(ck cellKey) shardCoord {
	return g.shardCoordForWorld(float64(ck.cx)*g.cellSize, float64(ck.cz)*g.cellSize)
}

// getShard 在 shardsMu 保护下取 / 建指定坐标的分片。
func (g *Grid) getShard(sc shardCoord) *shard {
	g.shardsMu.Lock()
	sh := g.shards[sc]
	if sh == nil {
		sh = newShard(sc)
		g.shards[sc] = sh
	}
	g.shardsMu.Unlock()
	return sh
}

// objectShard 经全局 cellOf 快速定位对象所在分片（无锁保护调用方需自行加锁访问其字段）。
// cellOf 的读取与分片查找必须在同一把 shardsMu 下完成，否则两步之间对象
// 可能跨格/跨分片移动，导致返回一个不含该对象的分片（后续 shardOf 静默返回 nil）。
func (g *Grid) objectShard(key oidKey) *shard {
	g.shardsMu.Lock()
	defer g.shardsMu.Unlock()
	g.muIndex.RLock()
	ck, ok := g.cellOf[key]
	g.muIndex.RUnlock()
	if !ok {
		return nil
	}
	sc := g.shardCoordForCell(ck)
	return g.shards[sc]
}

// lockShardsBox 锁定覆盖世界矩形 [minX,minZ]-[maxX,maxZ] 的全部分片（写或读），返回分片列表与解锁函数。
// 分片按坐标确定性排序后加锁，避免跨分片死锁。分片在加锁前于 shardsMu 下确保存在。
func (g *Grid) lockShardsBox(minX, minZ, maxX, maxZ float64, write bool) ([]*shard, func()) {
	minSC := g.shardCoordForWorld(minX, minZ)
	maxSC := g.shardCoordForWorld(maxX, maxZ)
	minSX, minSZ := minSC.x, minSC.z
	maxSX, maxSZ := maxSC.x, maxSC.z

	g.shardsMu.Lock()
	var list []*shard
	seen := map[shardCoord]struct{}{}
	for sx := minSX; sx <= maxSX; sx++ {
		for sz := minSZ; sz <= maxSZ; sz++ {
			sc := shardCoord{sx, sz}
			if _, dup := seen[sc]; dup {
				continue
			}
			seen[sc] = struct{}{}
			sh := g.shards[sc]
			if sh == nil {
				sh = newShard(sc)
				g.shards[sc] = sh
			}
			list = append(list, sh)
		}
	}
	g.shardsMu.Unlock()

	// 按 (x,z) 稳定排序，保证任意操作加锁顺序一致 → 无死锁。
	sort.Slice(list, func(i, j int) bool {
		if list[i].coord.x != list[j].coord.x {
			return list[i].coord.x < list[j].coord.x
		}
		return list[i].coord.z < list[j].coord.z
	})

	for _, sh := range list {
		if write {
			sh.mu.Lock()
		} else {
			sh.mu.RLock()
		}
	}
	unlock := func() {
		for _, sh := range list {
			if write {
				sh.mu.Unlock()
			} else {
				sh.mu.RUnlock()
			}
		}
	}
	return list, unlock
}

// cellForLocked 计算坐标所属格子（调用方需持该片锁）。
func (g *Grid) cellForLocked(p Position) cellKey {
	return cellKey{
		cx: int(math.Floor(p.X / g.cellSize)),
		cz: int(math.Floor(p.Z / g.cellSize)),
	}
}

// insertInShards 把对象放入其坐标对应分片（调用方需持该片锁）。
func (g *Grid) insertInShards(shards []*shard, key oidKey, p Position) {
	c := g.cellForLocked(p)
	sc := g.shardCoordForCell(c)
	var sh *shard
	for _, s := range shards {
		if s.coord == sc {
			sh = s
			break
		}
	}
	if sh == nil {
		// 目标分片不在已加锁列表内说明加锁盒子未覆盖到——此时 getShard 拿到的是
		// 一把「未加锁」的分片，直接写入会与其它并发访问竞争。为避免静默的数据竞争，
		// 单独锁定该分片再写，并在返回前解锁（调用方仍持有 shards 列表的其它锁，
		// 顺序上此分片坐标必然落在盒子之外，不会与已持锁分片构成环）。
		sh = g.getShard(sc)
		sh.mu.Lock()
		defer sh.mu.Unlock()
	}
	row := sh.cells[c.cx]
	if row == nil {
		row = make(map[int]map[oidKey]struct{})
		sh.cells[c.cx] = row
	}
	set := row[c.cz]
	if set == nil {
		set = make(map[oidKey]struct{})
		row[c.cz] = set
	}
	set[key] = struct{}{}
	sh.pos[key] = p
	sh.cellOf[key] = c

	g.muIndex.Lock()
	g.cellOf[key] = c
	g.muIndex.Unlock()
	g.muCount.Lock()
	g.count++
	g.muCount.Unlock()
}

// removeInShards 把对象从其所在分片移除（调用方需持该片锁）。
func (g *Grid) removeInShards(shards []*shard, key oidKey) {
	sh := shardOf(shards, key)
	if sh == nil {
		return
	}
	c := sh.cellOf[key]
	if row := sh.cells[c.cx]; row != nil {
		if set := row[c.cz]; set != nil {
			delete(set, key)
			if len(set) == 0 {
				delete(row, c.cz)
				if len(row) == 0 {
					delete(sh.cells, c.cx)
				}
			}
		}
	}
	delete(sh.pos, key)
	delete(sh.cellOf, key)

	g.muIndex.Lock()
	delete(g.cellOf, key)
	g.muIndex.Unlock()
	g.muCount.Lock()
	g.count--
	g.muCount.Unlock()
}

// shardsContainCoord 判断分片列表是否已包含指定分片坐标（用于校验加锁盒子是否覆盖某位置）。
func shardsContainCoord(shards []*shard, sc shardCoord) bool {
	for _, sh := range shards {
		if sh.coord == sc {
			return true
		}
	}
	return false
}

// shardOf 在已加锁的分片列表中定位包含 key 的分片。
func shardOf(shards []*shard, key oidKey) *shard {
	for _, sh := range shards {
		if _, ok := sh.cellOf[key]; ok {
			return sh
		}
	}
	return nil
}

// cellRange 计算以 center 为圆心、radius 为半径的圆外接格子坐标范围。
// 限制查询半径上限，防止恶意大范围扫描导致 O(n²)。
func (g *Grid) cellRange(center Position, radius float64) (cx0, cx1, cz0, cz1 int) {
	if radius > maxQueryRadius {
		// 静默截断会让调用方无法区分「范围内没有对象」与「被限流截断」，
		// Around / Neighbors 返回不完整时必须有留痕。
		aoiFailf("aoi: 查询半径 %.1f 超过上限 %.1f，候选格已按上限截断", radius, maxQueryRadius)
		radius = maxQueryRadius
	}
	cx0 = int(math.Floor((center.X - radius) / g.cellSize))
	cx1 = int(math.Floor((center.X + radius) / g.cellSize))
	cz0 = int(math.Floor((center.Z - radius) / g.cellSize))
	cz1 = int(math.Floor((center.Z + radius) / g.cellSize))
	return
}

// aroundInShards 收集以 center 为圆心、radius 为半径的圆内所有对象（不含 exclude；调用方需持这些分片锁）。
// 返回 uint64 键集合，避免内部 object.ObjectID 结构体分配，也省去 refresh 时再转 map 的一次迭代。
// 使用 cells 双层 map 做范围查询：只扫描圆外接矩形覆盖到的格子，而不是整个分片。
func (g *Grid) aroundInShards(shards []*shard, center Position, radius float64, exclude oidKey) map[oidKey]struct{} {
	out := oidKeySetPool.Get().(map[oidKey]struct{})
	clear(out)
	g.aroundInShardsInto(shards, center, radius, exclude, out)
	return out
}

// aroundInShardsInto 同 aroundInShards，但把结果填入调用方提供的 out 集合，避免分配新 map。
// 调用方必须保证 out 已清空（本函数不会主动 clear）。
func (g *Grid) aroundInShardsInto(shards []*shard, center Position, radius float64, exclude oidKey, out map[oidKey]struct{}) {
	if radius <= 0 || out == nil {
		return
	}
	r2 := radius * radius
	cx0, cx1, cz0, cz1 := g.cellRange(center, radius)
	for _, sh := range shards {
		for cx := cx0; cx <= cx1; cx++ {
			row := sh.cells[cx]
			if row == nil {
				continue
			}
			for cz := cz0; cz <= cz1; cz++ {
				set := row[cz]
				if set == nil {
					continue
				}
				for key := range set {
					if key == exclude {
						continue
					}
					if g.dist2(sh.pos[key], center) <= r2 {
						out[key] = struct{}{}
					}
				}
			}
		}
	}
}

// refreshCommit 承载一次视野刷新的「写回动作」：把计算好的 next 集合置为观察者的新 visible。
// 由 EndBatch 在并行计算完成后串行执行，避免跨 worker 写 visible/next 的竞争。
type refreshCommit struct {
	w    *watchState
	next map[oidKey]struct{}
}

// apply 执行写回：旧 visible 归还对象池，next 成为新的 visible，w.next 清空复用。
func (c refreshCommit) apply() {
	if c.w == nil {
		return
	}
	if c.w.visible != nil {
		clear(c.w.visible)
		oidKeySetPool.Put(c.w.visible)
	}
	c.w.visible = c.next
	if c.w.next != nil {
		clear(c.w.next)
	}
}

// computeWatcherRefresh 只读地重算单个观察者的视野增量并收集事件，返回一个待串行写回的 commit。
// 不修改 w.visible/w.next（仅读取 w.visible 做 diff），因此可安全并行执行；
// 计算出的新集合放入独立的 next map（来自对象池），写回在 EndBatch 串行阶段完成。
func (g *Grid) computeWatcherRefresh(shards []*shard, watcher oidKey, events *[]event) (refreshCommit, bool) {
	sh := shardOf(shards, watcher)
	if sh == nil {
		return refreshCommit{}, false
	}
	w := sh.watchers[watcher]
	if w == nil {
		return refreshCommit{}, false
	}
	center := sh.pos[watcher]
	next := oidKeySetPool.Get().(map[oidKey]struct{})
	clear(next)
	g.aroundInShardsInto(shards, center, w.radius, watcher, next)
	// 预分配事件容量，按指数扩容避免 O(n²) 重复拷贝。
	need := len(next) + len(w.visible)
	if cap(*events)-len(*events) < need {
		newCap := cap(*events) * 2
		if newCap < len(*events)+need {
			newCap = len(*events) + need
		}
		newEvents := make([]event, len(*events), newCap)
		copy(newEvents, *events)
		*events = newEvents
	}
	// 获取权限校验器，过滤未授权观察者。
	checker := g.getPermChecker()
	for key := range next {
		if _, seen := w.visible[key]; !seen {
			if checker != nil && !checker(object.FromUint64(watcher), object.FromUint64(key)) {
				delete(next, key) // 权限不足，从候选集移除避免泄露
				continue
			}
			*events = append(*events, event{watcher: watcher, target: key, ev: EnterView})
		}
	}
	for key := range w.visible {
		if _, still := next[key]; !still {
			*events = append(*events, event{watcher: watcher, target: key, ev: LeaveView})
		}
	}
	return refreshCommit{w: w, next: next}, true
}

// refreshWatcherInShards 重算单个观察者（须在已加锁分片内）的视野增量，收集事件并立即写回。
// 供串行（非批量）路径使用；批量路径走 computeWatcherRefresh + 串行 commit。
func (g *Grid) refreshWatcherInShards(shards []*shard, watcher oidKey, events *[]event) {
	if c, ok := g.computeWatcherRefresh(shards, watcher, events); ok {
		c.apply()
	}
}

// collectAffectedInto 收集「视野可能包含 p 处对象」的观察者，直接填充 out（调用方需持这些分片锁）。
// 调用方已锁定覆盖 p 周围 maxRadius 的盒子，故分片内的对象均可能受影响；
// 这里利用 cells 双层索引，只扫描 p 周围 maxRadius 范围内的格子里的对象，再检查其是否为 watcher。
func (g *Grid) collectAffectedInto(shards []*shard, p Position, out map[oidKey]struct{}) {
	maxR := g.getMaxRadius()
	if maxR <= 0 {
		return
	}
	cx0, cx1, cz0, cz1 := g.cellRange(p, maxR)
	for _, sh := range shards {
		for cx := cx0; cx <= cx1; cx++ {
			row := sh.cells[cx]
			if row == nil {
				continue
			}
			for cz := cz0; cz <= cz1; cz++ {
				set := row[cz]
				if set == nil {
					continue
				}
				for wid := range set {
					if _, ok := out[wid]; ok {
						continue
					}
					w := sh.watchers[wid]
					if w == nil {
						continue
					}
					if g.dist2(sh.pos[wid], p) <= w.radius*w.radius {
						out[wid] = struct{}{}
					}
				}
			}
		}
	}
}

// collectAffected 收集「视野可能包含 p 处对象」的观察者，返回切片（供非批量路径使用）。
// 返回的切片来自 oidKeySlicePool，调用方用毕应 putOidKeySlice 归还。
func (g *Grid) collectAffected(shards []*shard, p Position) []oidKey {
	maxR := g.getMaxRadius()
	if maxR <= 0 {
		return nil
	}
	seen := oidKeySetPool.Get().(map[oidKey]struct{})
	clear(seen)
	g.collectAffectedInto(shards, p, seen)
	out := getOidKeySlice()
	out = out[:0]
	for wid := range seen {
		out = append(out, wid)
	}
	clear(seen)
	oidKeySetPool.Put(seen)
	return out
}

func (g *Grid) getMaxRadius() float64 {
	g.muMax.RLock()
	m := g.maxR
	g.muMax.RUnlock()
	return m
}

func (g *Grid) bumpMaxRadius(radius float64) {
	g.muMax.Lock()
	if radius > g.maxR {
		g.maxR = radius
	}
	g.muMax.Unlock()
}

// recomputeMaxRadius 重算全局最大视野半径（调用方需持全部分片锁）。
//
//lint:ignore U1000 hold-lock版本，由 recomputeMaxRadiusAll 调用方选择
func (g *Grid) recomputeMaxRadius(shards []*shard) {
	m := 0.0
	for _, sh := range shards {
		for _, w := range sh.watchers {
			if w.radius > m {
				m = w.radius
			}
		}
	}
	g.muMax.Lock()
	g.maxR = m
	g.muMax.Unlock()
}

// recomputeMaxRadiusAll 自行锁定全部分片并重算全局最大视野半径，避免调用方仅持有局部分片。
// 用于 Leave / Unwatch 等场景：若删除的 watcher 可能为全局最大半径持有者，需全量重算。
func (g *Grid) recomputeMaxRadiusAll() {
	g.shardsMu.Lock()
	allShards := make([]*shard, 0, len(g.shards))
	for _, sh := range g.shards {
		allShards = append(allShards, sh)
	}
	g.shardsMu.Unlock()

	sort.Slice(allShards, func(i, j int) bool {
		if allShards[i].coord.x != allShards[j].coord.x {
			return allShards[i].coord.x < allShards[j].coord.x
		}
		return allShards[i].coord.z < allShards[j].coord.z
	})
	for _, sh := range allShards {
		sh.mu.RLock()
	}
	m := 0.0
	for _, sh := range allShards {
		for _, w := range sh.watchers {
			if w.radius > m {
				m = w.radius
			}
		}
		sh.mu.RUnlock()
	}
	g.muMax.Lock()
	g.maxR = m
	g.muMax.Unlock()
}

// dispatch 锁外派发事件，支持序列号、去重、容量上限。
func (g *Grid) dispatch(o Observer, events []event) {
	if o == nil || len(events) == 0 {
		return
	}
	// 分配全局递增序列号，保证同一 watcher 的事件按发生顺序派发。
	for i := range events {
		events[i].seq = g.eventSeq.Add(1)
	}
	// 按序列号全局排序，保证跨 goroutine 事件的有序性。
	sort.Slice(events, func(i, j int) bool { return events[i].seq < events[j].seq })

	// 去重——合并同一 (watcher,target) 对的事件。
	// EnterView+LeaveView/LeaveAll 可相互抵消（同一帧进入又离开=无净变化）。
	type dedupKey struct {
		watcher oidKey
		target  oidKey
	}
	seen := make(map[dedupKey]int) // key -> index in deduped
	deduped := make([]event, 0, len(events))
	for i := range events {
		e := events[i]
		k := dedupKey{watcher: e.watcher, target: e.target}
		if prev, ok := seen[k]; ok {
			prevEv := deduped[prev]
			// 互斥事件抵消
			if (prevEv.ev == EnterView && (e.ev == LeaveView || e.ev == LeaveAll)) ||
				((prevEv.ev == LeaveView || prevEv.ev == LeaveAll) && e.ev == EnterView) {
				deduped[prev].ev = 0 // 标记删除
				delete(seen, k)
				continue
			}
			// 同类事件取最新
			deduped[prev] = e
			continue
		}
		seen[k] = len(deduped)
		deduped = append(deduped, e)
	}

	// 分批派发：maxEventsPerDispatch 只限制单批长度以平滑下游队列的瞬时压力，
	// 不用于丢弃事件——观察者的 visible 集合在派发前已提交，少发一条都会让
	// 下游维护的可见集合与网格永久不一致，且没有任何补发途径。
	for start := 0; start < len(deduped); start += maxEventsPerDispatch {
		end := start + maxEventsPerDispatch
		if end > len(deduped) {
			end = len(deduped)
		}
		for _, e := range deduped[start:end] {
			if e.ev == 0 {
				continue
			}
			o(object.FromUint64(e.watcher), object.FromUint64(e.target), e.ev)
		}
	}
}

// BeginBatch 开始批量刷新模式。在此模式下，Enter/Move/Leave/Watch/Unwatch 不会立即刷新观察者，
// 只更新内部坐标/成员并标记受影响的观察者为 dirty；必须配对调用 EndBatch 统一刷新。
// BeginBatch/EndBatch 不可重入（调用方需保证配对）。
func (g *Grid) BeginBatch() {
	g.batchMu.Lock()
	g.batching.Store(true)
	if g.dirty == nil {
		g.dirty = make(map[oidKey]struct{})
	}
	g.batchMu.Unlock()
}

// EndBatch 结束批量刷新模式，统一刷新所有 dirty 观察者并派发事件。
// 刷新阶段按 CPU 核数并行处理 dirty 观察者，每个 worker 只读写自己负责的对象视野状态，
// 共享的分片数据在刷新前已全部锁定，因此无数据竞争。
func (g *Grid) EndBatch() {
	g.batchMu.Lock()
	g.batching.Store(false)
	dirty := g.dirty
	pending := g.pending
	g.dirty = make(map[oidKey]struct{})
	g.pending = nil
	g.batchMu.Unlock()

	if len(dirty) == 0 {
		g.dispatch(g.getObserver(), pending)
		return
	}

	// 锁定所有 dirty 观察者所在分片（确定性排序避免死锁）。
	shards, unlock := g.lockDirtyShards(dirty)

	// 把 dirty 观察者列表化，便于按 worker 切分。
	keys := make([]oidKey, 0, len(dirty))
	for k := range dirty {
		keys = append(keys, k)
	}

	// 按 CPU 核数并行刷新，每个 worker 持有自己的 events 切片，最后合并。
	// 并行阶段严格只读共享分片、只写「本 worker 负责的 wid」自己的视野状态；
	// 但为杜绝跨 worker 对相邻 wid 视野状态（visible/next）的读写竞争风险，
	// 把「计算 next 集 + 生成增量事件」的只读密集部分并行化，
	// 而把真正修改 w.visible/w.next 的写回阶段收敛为串行执行（见 commitRefresh）。
	n := runtime.GOMAXPROCS(0)
	if n > len(keys) {
		n = len(keys)
	}
	var wg sync.WaitGroup
	results := make([][]event, n)
	commits := make([][]refreshCommit, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start := idx * len(keys) / n
			end := (idx + 1) * len(keys) / n
			if start >= end {
				return
			}
			// 预分配：每个 dirty 观察者平均约 8 个视野事件；缓冲来自事件池，避免每帧分配。
			evs := getEventBuf()
			cmts := make([]refreshCommit, 0, end-start)
			for _, wid := range keys[start:end] {
				if c, ok := g.computeWatcherRefresh(shards, wid, &evs); ok {
					cmts = append(cmts, c)
				}
			}
			results[idx] = evs
			commits[idx] = cmts
		}(i)
	}
	wg.Wait()

	// 串行写回视野状态，避免任何跨 worker 的 visible/next 写竞争。
	for _, cmts := range commits {
		for _, c := range cmts {
			c.apply()
		}
	}
	unlock()

	// 合并事件：pending 事件先行，再按 worker 顺序追加，最后去重并按 (watcher,target) 排序。
	total := len(pending)
	for _, evs := range results {
		total += len(evs)
	}
	events := getEventBuf()
	events = append(events, pending...)
	for _, evs := range results {
		events = append(events, evs...)
	}
	// 全局去重：同一 (watcher,target) 的 Enter/Leave 可能跨 worker 产生重复。
	events = dedupEvents(events)
	g.dispatch(g.getObserver(), events)
	putEventBuf(events)
	for _, evs := range results {
		putEventBuf(evs)
	}
}

// lockDirtyShards 锁定包含 dirty 观察者的全部分片，并扩展至各观察者视野半径覆盖的区域；
// 避免并行刷新时跨分片读取 pos/cells 产生数据竞争。
func (g *Grid) lockDirtyShards(dirty map[oidKey]struct{}) ([]*shard, func()) {
	coords := make(map[shardCoord]struct{})
	// 读取 dirty 观察者的数据时各有各的锁，不能裸读（与并发 Enter/Move/Leave 数据竞争）：
	//   - g.cellOf 由 muIndex 保护（此前只持 shardsMu 就读，是 data race）；
	//   - 分片内的 watchers / pos / radius 由 sh.mu 保护（此前尚未加锁就读）。
	// 锁序统一为 shardsMu → muIndex / sh.mu，与本包其它路径一致（不存在反向持锁路径）。
	g.shardsMu.Lock()
	for key := range dirty {
		// 通过全局 cellOf 定位观察者所在分片
		g.muIndex.RLock()
		ck, ok := g.cellOf[key]
		g.muIndex.RUnlock()
		if !ok {
			continue
		}
		watcherSC := g.shardCoordForCell(ck)
		coords[watcherSC] = struct{}{}

		// 扩展：该观察者的视野盒可能跨到相邻分片，一并锁定。
		sh := g.shards[watcherSC]
		if sh == nil {
			continue
		}
		sh.mu.RLock()
		w := sh.watchers[key]
		pos, has := sh.pos[key]
		var radius float64
		if w != nil {
			radius = w.radius
		}
		sh.mu.RUnlock()
		if w == nil || !has {
			continue
		}
		// 视野外接矩形 → super-shard 坐标范围。
		minSX := int(math.Floor((pos.X - radius) / g.superSize))
		maxSX := int(math.Floor((pos.X + radius) / g.superSize))
		minSZ := int(math.Floor((pos.Z - radius) / g.superSize))
		maxSZ := int(math.Floor((pos.Z + radius) / g.superSize))
		for sx := minSX; sx <= maxSX; sx++ {
			for sz := minSZ; sz <= maxSZ; sz++ {
				coords[shardCoord{sx, sz}] = struct{}{}
			}
		}
	}

	var list []*shard
	for sc := range coords {
		sh := g.shards[sc]
		if sh == nil {
			sh = newShard(sc)
			g.shards[sc] = sh
		}
		list = append(list, sh)
	}
	g.shardsMu.Unlock()

	sort.Slice(list, func(i, j int) bool {
		if list[i].coord.x != list[j].coord.x {
			return list[i].coord.x < list[j].coord.x
		}
		return list[i].coord.z < list[j].coord.z
	})
	for _, sh := range list {
		sh.mu.Lock()
	}
	unlock := func() {
		for _, sh := range list {
			sh.mu.Unlock()
		}
	}
	return list, unlock
}

// markDirty 在批量模式下把观察者标记为 dirty（非 batching 时无操作）。
func (g *Grid) markDirty(keys ...oidKey) {
	g.batchMu.Lock()
	if g.batching.Load() {
		for _, k := range keys {
			g.dirty[k] = struct{}{}
		}
	}
	g.batchMu.Unlock()
}

// Enter 让对象以坐标 p 进入网格（若已存在则等价于 Move）。
// 会刷新所有可能看到该对象的观察者视野；若该对象自身是观察者，也会刷新它自己的视野。
func (g *Grid) Enter(id object.ObjectID, p Position) {
	key := id.MarshalUint64()
	g.muIndex.RLock()
	_, exists := g.cellOf[key]
	g.muIndex.RUnlock()
	if exists {
		g.Move(id, p)
		return
	}

	maxR := g.getMaxRadius()
	// 覆盖 p 周围 maxR 范围，并补偿「cell 左下角近似」带来的 cellSize 偏移。
	minX, minZ, maxX, maxZ := expand(p, maxR+g.cellSize)
	shards, unlock := g.lockShardsBox(minX, minZ, maxX, maxZ, true)
	if shardOf(shards, key) != nil { // 极端竞态下已被插入，转交 Move
		unlock()
		g.Move(id, p)
		return
	}
	g.insertInShards(shards, key, p)

	if g.batching.Load() {
		affected := oidKeySetPool.Get().(map[oidKey]struct{})
		clear(affected)
		g.collectAffectedInto(shards, p, affected)
		for wid := range affected {
			g.markDirty(wid)
		}
		if sh := shardOf(shards, key); sh != nil {
			if _, isW := sh.watchers[key]; isW {
				g.markDirty(key)
			}
		}
		clear(affected)
		oidKeySetPool.Put(affected)
		unlock()
		return
	}

	events := getEventBuf()
	affected := g.collectAffected(shards, p)
	for _, wid := range affected {
		g.refreshWatcherInShards(shards, wid, &events)
	}
	putOidKeySlice(affected)
	if sh := shardOf(shards, key); sh != nil {
		if _, isW := sh.watchers[key]; isW {
			g.refreshWatcherInShards(shards, key, &events)
		}
	}

	o := g.getObserver()
	unlock()
	g.dispatch(o, events)
	putEventBuf(events)
}

// Move 更新对象坐标；刷新旧位置 / 新位置附近的观察者视野，以及（若是观察者）它自己的视野。
// 对象不存在时按 Enter 处理。
func (g *Grid) Move(id object.ObjectID, p Position) {
	key := id.MarshalUint64()
	g.muIndex.RLock()
	oldCK, existed := g.cellOf[key]
	g.muIndex.RUnlock()
	if !existed {
		g.Enter(id, p)
		return
	}
	// 用旧格世界坐标粗估影响盒子（精确旧坐标在加锁后读取）。
	oldApprox := Position{X: float64(oldCK.cx) * g.cellSize, Z: float64(oldCK.cz) * g.cellSize}

	maxR := g.getMaxRadius()
	// oldApprox 是旧格左下角，实际 oldPos 在 [oldApprox, oldApprox+cellSize] 内；
	// 用 maxR+cellSize 外扩即可覆盖 oldPos / p 周围 maxR 范围内的观察者。
	minX, minZ, maxX, maxZ := expandUnion(oldApprox, p, maxR+g.cellSize)
	shards, unlock := g.lockShardsBox(minX, minZ, maxX, maxZ, true)

	sh := shardOf(shards, key)
	if sh == nil { // 理论不可达（existed 已保证），退化为 Enter
		unlock()
		g.Enter(id, p)
		return
	}
	oldPos := sh.pos[key]
	// oldApprox 只是旧格左下角的近似，真实 oldPos 可能落在盒子未覆盖到的相邻分片；
	// 若真实旧位置所属分片不在已加锁列表中，跨格移动会漏锁 → 漏刷旧视野。
	// 此时用「真实 oldPos ∪ p」重新计算并加锁盒子，保证覆盖完整。
	oldSC := g.shardCoordForWorld(oldPos.X, oldPos.Z)
	if !shardsContainCoord(shards, oldSC) {
		unlock()
		minX, minZ, maxX, maxZ = expandUnion(oldPos, p, maxR+g.cellSize)
		shards, unlock = g.lockShardsBox(minX, minZ, maxX, maxZ, true)
		sh = shardOf(shards, key)
		if sh == nil {
			unlock()
			g.Enter(id, p)
			return
		}
		oldPos = sh.pos[key]
	}
	// 观察者状态随对象一起迁移分片：取出后从旧分片删除，插入后挂到新分片。
	var wstate *watchState
	if w, ok := sh.watchers[key]; ok {
		wstate = w
		delete(sh.watchers, key)
	}
	g.removeInShards(shards, key)
	g.insertInShards(shards, key, p)
	if wstate != nil {
		if newSh := shardOf(shards, key); newSh != nil {
			// 注意：保留 wstate.visible（旧视野集合），不要重置。
			// refreshWatcherInShards 依赖它计算「进入视野 / 离开视野」的增量；
			// 若此处清空，则已可见对象会被重复触发 EnterView，已离开对象丢失 LeaveView。
			newSh.watchers[key] = wstate
		} else {
			// 分片被并发回收等极端情况：回挂到原 key 所在 shard，避免视野状态静默丢失。
			sh.watchers[key] = wstate
		}
	}

	affected := oidKeySetPool.Get().(map[oidKey]struct{})
	clear(affected)
	g.collectAffectedInto(shards, oldPos, affected)
	g.collectAffectedInto(shards, p, affected)

	if g.batching.Load() {
		for wid := range affected {
			g.markDirty(wid)
		}
		if sh2 := shardOf(shards, key); sh2 != nil && sh2.watchers[key] != nil {
			g.markDirty(key)
		}
		clear(affected)
		oidKeySetPool.Put(affected)
		unlock()
		return
	}

	events := getEventBuf()
	for wid := range affected {
		g.refreshWatcherInShards(shards, wid, &events)
	}
	// 注意：对象已随 insertInShards 挂到「当前」分片（可能与 sh 不同）；
	// 必须用最新分片判断观察者自身视野，否则跨分片移动后其视野永不刷新。
	if sh2 := shardOf(shards, key); sh2 != nil && sh2.watchers[key] != nil {
		g.refreshWatcherInShards(shards, key, &events)
	}

	clear(affected)
	oidKeySetPool.Put(affected)
	o := g.getObserver()
	unlock()
	g.dispatch(o, events)
	putEventBuf(events)
}

// Leave 让对象离开网格；先通知附近观察者「目标离开视野」，再清除该对象自身的观察者状态。
func (g *Grid) Leave(id object.ObjectID) {
	key := id.MarshalUint64()
	g.muIndex.RLock()
	_, existed := g.cellOf[key]
	g.muIndex.RUnlock()
	if !existed {
		return
	}
	oldShard := g.objectShard(key)
	if oldShard == nil {
		return
	}
	oldShard.mu.RLock()
	oldPos := oldShard.pos[key]
	oldShard.mu.RUnlock()

	maxR := g.getMaxRadius()
	minX, minZ, maxX, maxZ := expand(oldPos, maxR+g.cellSize)
	shards, unlock := g.lockShardsBox(minX, minZ, maxX, maxZ, true)

	// 写锁后重验证：对象可能在锁外被并发移除或移动至另一分片。
	// 同时再次从已加锁的 shard 读取位置，确保采集的 oldPos 与锁定区域一致。
	g.muIndex.RLock()
	_, existed2 := g.cellOf[key]
	g.muIndex.RUnlock()
	if !existed2 {
		unlock()
		return
	}
	// 锁内重读位置：若与锁前不同，用最新值修正 oldPos（对象未移动时不变，移动时确保受影响分片正确）。
	if sh := shardOf(shards, key); sh != nil {
		if p, ok := sh.pos[key]; ok {
			oldPos = p
		}
	}

	events := getEventBuf()
	needRecompute := false
	// 若离开者本身是观察者：补发对其已见目标的 LeaveView，并移除其 watch 状态。
	if sh := shardOf(shards, key); sh != nil {
		if w := sh.watchers[key]; w != nil {
			for t := range w.visible {
				events = append(events, event{watcher: key, target: t, ev: LeaveView})
			}
			needRecompute = w.radius >= g.getMaxRadius()
			delete(sh.watchers, key)
		}
	}

	g.removeInShards(shards, key)

	if g.batching.Load() {
		g.batchMu.Lock()
		g.pending = append(g.pending, events...)
		g.batchMu.Unlock()
		putEventBuf(events)
		affected := oidKeySetPool.Get().(map[oidKey]struct{})
		clear(affected)
		g.collectAffectedInto(shards, oldPos, affected)
		for wid := range affected {
			g.markDirty(wid)
		}
		clear(affected)
		oidKeySetPool.Put(affected)
		unlock()
		if needRecompute {
			g.recomputeMaxRadiusAll()
		}
		return
	}

	affected := g.collectAffected(shards, oldPos)
	for _, wid := range affected {
		g.refreshWatcherInShards(shards, wid, &events)
	}
	putOidKeySlice(affected)

	o := g.getObserver()
	unlock()
	if needRecompute {
		g.recomputeMaxRadiusAll()
	}
	g.dispatch(o, events)
	putEventBuf(events)
}

// Watch 把对象登记为观察者并设定视野半径（radius<=0 视为取消观察）。
// 立即算出初始可见集合并返回；这些目标也会通过 Observer 以 EnterView 派发。
func (g *Grid) Watch(id object.ObjectID, radius float64) []object.ObjectID {
	key := id.MarshalUint64()
	if radius <= 0 {
		g.Unwatch(id)
		return nil
	}
	sh := g.objectShard(key)
	if sh == nil {
		return nil // 观察者必须先 Enter
	}
	sh.mu.RLock()
	center := sh.pos[key]
	sh.mu.RUnlock()

	minX, minZ, maxX, maxZ := expand(center, radius)
	shards, unlock := g.lockShardsBox(minX, minZ, maxX, maxZ, true)

	sh = shardOf(shards, key)
	if sh == nil {
		unlock()
		return nil
	}
	w := sh.watchers[key]
	if w == nil {
		w = &watchState{visible: make(map[oidKey]struct{})}
		sh.watchers[key] = w
	}
	// 视野半径必须有上限：Enter/Move/Leave 都按 maxR 枚举分片（lockShardsBox 双层循环），
	// 半径 1e6 时是 ~3.9e9 次枚举，CPU 卡死 + 内存爆炸。查询路径 cellRange 已夹紧到
	// maxQueryRadius，这里保持同一口径；超限降频留痕，调用方能看出半径被截断。
	if radius > maxQueryRadius {
		aoiFailf("aoi: Watch radius %.1f 超过上限 %.1f，已夹紧（obj=%d）", radius, maxQueryRadius, key)
		radius = maxQueryRadius
	}
	oldRadius := w.radius
	w.radius = radius
	needRecompute := false
	if radius > g.getMaxRadius() {
		g.bumpMaxRadius(radius)
	} else if oldRadius >= g.getMaxRadius() && radius < oldRadius {
		// 观察者缩小半径时，若原半径曾为全局最大，需全网重算，避免 maxR 永久偏高。
		needRecompute = true
	}

	if g.batching.Load() {
		g.markDirty(key)
		unlock()
		if needRecompute {
			g.recomputeMaxRadiusAll()
		}
		return nil // batch 期间不返回初始可见集，统一在 EndBatch 时生成事件
	}

	events := getEventBuf()
	entered, _ := g.refreshWatcherInShardsRet(shards, key, &events)
	o := g.getObserver()
	unlock()
	if needRecompute {
		g.recomputeMaxRadiusAll()
	}
	g.dispatch(o, events)
	putEventBuf(events)
	return oidKeysToIDs(entered)
}

// refreshWatcherInShardsRet 同 refreshWatcherInShards，但额外返回本次新进入视野的对象列表。
func (g *Grid) refreshWatcherInShardsRet(shards []*shard, watcher oidKey, events *[]event) ([]oidKey, []oidKey) {
	sh := shardOf(shards, watcher)
	if sh == nil {
		return nil, nil
	}
	w := sh.watchers[watcher]
	if w == nil {
		return nil, nil
	}
	center := sh.pos[watcher]
	if w.next == nil {
		w.next = oidKeySetPool.Get().(map[oidKey]struct{})
		clear(w.next)
	}
	g.aroundInShardsInto(shards, center, w.radius, watcher, w.next)
	// 预分配容量，按指数扩容避免 O(n²) 重复拷贝。
	need := len(w.next) + len(w.visible)
	if cap(*events)-len(*events) < need {
		newCap := cap(*events) * 2
		if newCap < len(*events)+need {
			newCap = len(*events) + need
		}
		newEvents := make([]event, len(*events), newCap)
		copy(newEvents, *events)
		*events = newEvents
	}
	var entered, left []oidKey
	// 获取权限校验器，过滤未授权观察者。
	checker := g.getPermChecker()
	for key := range w.next {
		if _, seen := w.visible[key]; !seen {
			if checker != nil && !checker(object.FromUint64(watcher), object.FromUint64(key)) {
				delete(w.next, key) // 权限不足，从候选集移除避免泄露
				continue
			}
			entered = append(entered, key)
			*events = append(*events, event{watcher: watcher, target: key, ev: EnterView})
		}
	}
	for key := range w.visible {
		if _, still := w.next[key]; !still {
			left = append(left, key)
			*events = append(*events, event{watcher: watcher, target: key, ev: LeaveView})
		}
	}
	// 复用 next 作为新的 visible，旧 visible 清空后充当下一轮 next。
	w.visible, w.next = w.next, w.visible
	clear(w.next)
	return entered, left
}

// Unwatch 取消对象的观察者身份（保留其在网格中的位置，只是不再维护视野）。
// 会对其当前可见目标补发 LeaveView。
func (g *Grid) Unwatch(id object.ObjectID) {
	key := id.MarshalUint64()
	sh := g.objectShard(key)
	if sh == nil {
		return
	}
	sh.mu.RLock()
	_, isW := sh.watchers[key]
	sh.mu.RUnlock()
	if !isW {
		return
	}

	// 取消观察只需锁住该对象所在分片即可。
	// 「读格子 → 加锁」之间对象可能并发 Move 跨分片（TOCTOU），锁到旧分片后
	// shardOf 找不到对象会静默失败，watcher 状态残留成幽灵视野。故加锁后重验证，
	// 未命中且对象仍在网格中则重读格子重试。
	var (
		boxSh     []*shard
		boxUnlock func()
	)
	for attempt := 0; ; attempt++ {
		sc := g.shardCoordForCell(g.cellOfSafe(key))
		boxSh, boxUnlock = g.lockShardsBoxRange(sc.x, sc.z, sc.x, sc.z, true)
		if shardOf(boxSh, key) != nil {
			break
		}
		boxUnlock()
		g.muIndex.RLock()
		_, still := g.cellOf[key]
		g.muIndex.RUnlock()
		if !still {
			// 对象已彻底离开网格（Leave 已清理其 watcher 状态）：正常路径，不打日志。
			return
		}
		if attempt >= 8 {
			// 对象仍在网格却反复锁不到其所在分片：直接返回会让 watcher 状态残留成
			// 幽灵视野，必须降频留痕。
			aoiFailf("aoi: Unwatch obj=%d 重试 %d 次仍未定位到其所在分片，已放弃（watcher 状态可能残留）", key, attempt)
			return
		}
	}

	events := getEventBuf()
	needRecompute := false
	if sh := shardOf(boxSh, key); sh != nil {
		if w := sh.watchers[key]; w != nil {
			for t := range w.visible {
				events = append(events, event{watcher: key, target: t, ev: LeaveView})
			}
			needRecompute = w.radius >= g.getMaxRadius()
			delete(sh.watchers, key)
		}
	}
	if g.batching.Load() {
		g.batchMu.Lock()
		g.pending = append(g.pending, events...)
		g.batchMu.Unlock()
		putEventBuf(events)
		boxUnlock()
		if needRecompute {
			g.recomputeMaxRadiusAll()
		}
		return
	}
	o := g.getObserver()
	boxUnlock()
	if needRecompute {
		g.recomputeMaxRadiusAll()
	}
	g.dispatch(o, events)
	putEventBuf(events)
}

// dedupEvents 对事件列表按 (watcher,target) 去重，保留每个 (watcher,target) 的最后一个事件。
func dedupEvents(events []event) []event {
	if len(events) <= 1 {
		return events
	}
	type key struct {
		watcher, target oidKey
	}
	seen := make(map[key]int, len(events))
	for i, ev := range events {
		k := key{watcher: ev.watcher, target: ev.target}
		seen[k] = i
	}
	result := getEventBuf()
	for i, ev := range events {
		k := key{watcher: ev.watcher, target: ev.target}
		if seen[k] == i {
			result = append(result, ev)
		}
	}
	// 输入缓冲来自调用方的 getEventBuf()，去重后不再被引用：归还回池，否则每帧
	// 批量路径泄漏一个池缓冲（调用方最后 putEventBuf 归还的是 result 这个新缓冲）。
	putEventBuf(events)
	return result
}

// cellOfSafe 安全取对象格子（不存在返回零值），供 Unwatch 定位分片。
func (g *Grid) cellOfSafe(key oidKey) cellKey {
	g.muIndex.RLock()
	ck := g.cellOf[key]
	g.muIndex.RUnlock()
	return ck
}

// Visible 返回观察者当前可见的对象列表（不是观察者返回 nil），结果按 id 稳定排序。
func (g *Grid) Visible(id object.ObjectID) []object.ObjectID {
	key := id.MarshalUint64()
	sh := g.objectShard(key)
	if sh == nil {
		return nil
	}
	sh.mu.RLock()
	w := sh.watchers[key]
	if w == nil {
		sh.mu.RUnlock()
		return nil
	}
	out := make([]object.ObjectID, 0, len(w.visible))
	for t := range w.visible {
		out = append(out, object.FromUint64(t))
	}
	sh.mu.RUnlock()
	sortIDs(out)
	return out
}

// Around 返回以 center 为圆心、radius 为半径的圆内所有对象，按 id 稳定排序。
func (g *Grid) Around(center Position, radius float64) []object.ObjectID {
	if radius <= 0 {
		return nil
	}
	minX, minZ, maxX, maxZ := expand(center, radius)
	shards, unlock := g.lockShardsBox(minX, minZ, maxX, maxZ, false)
	out := g.aroundInShards(shards, center, radius, 0)
	unlock()
	return oidKeySetToIDsSorted(out)
}

// Neighbors 返回 id 周围 radius 内的其他对象（不含自身），按 id 稳定排序；id 不在网格中返回 nil。
func (g *Grid) Neighbors(id object.ObjectID, radius float64) []object.ObjectID {
	if radius <= 0 {
		return nil
	}
	key := id.MarshalUint64()
	sh := g.objectShard(key)
	if sh == nil {
		return nil
	}
	sh.mu.RLock()
	center := sh.pos[key]
	sh.mu.RUnlock()
	minX, minZ, maxX, maxZ := expand(center, radius)
	shards, unlock := g.lockShardsBox(minX, minZ, maxX, maxZ, false)
	out := g.aroundInShards(shards, center, radius, key)
	unlock()
	return oidKeySetToIDsSorted(out)
}

// oidKeysToIDs 把 uint64 键列表还原为 ObjectID 列表。
func oidKeysToIDs(keys []oidKey) []object.ObjectID {
	out := make([]object.ObjectID, len(keys))
	for i, k := range keys {
		out[i] = object.FromUint64(k)
	}
	return out
}

// oidKeySetToIDsSorted 把 uint64 键集合还原为 ObjectID 列表并按 (Type, Seq) 排序；
// 完成后将集合归还对象池。
func oidKeySetToIDsSorted(set map[oidKey]struct{}) []object.ObjectID {
	out := make([]object.ObjectID, 0, len(set))
	for k := range set {
		out = append(out, object.FromUint64(k))
	}
	clear(set)
	oidKeySetPool.Put(set)
	sortIDs(out)
	return out
}

// lockShardsBoxRange 是 lockShardsBox 的整型坐标版本（Unwatch 仅锁单分片）。
func (g *Grid) lockShardsBoxRange(minSX, minSZ, maxSX, maxSZ int, write bool) ([]*shard, func()) {
	minX := float64(minSX) * g.superSize
	minZ := float64(minSZ) * g.superSize
	maxX := float64(maxSX+1) * g.superSize
	maxZ := float64(maxSZ+1) * g.superSize
	return g.lockShardsBox(minX, minZ, maxX, maxZ, write)
}

// expand 以 p 为中心、half 为半边长，返回世界矩形边界。
func expand(p Position, half float64) (minX, minZ, maxX, maxZ float64) {
	return p.X - half, p.Z - half, p.X + half, p.Z + half
}

// expandUnion 返回覆盖 old 与 p 两个中心、各自外扩 half 的包围矩形。
func expandUnion(old, p Position, half float64) (minX, minZ, maxX, maxZ float64) {
	minX = old.X - half
	minZ = old.Z - half
	maxX = old.X + half
	maxZ = old.Z + half
	if p.X-half < minX {
		minX = p.X - half
	}
	if p.Z-half < minZ {
		minZ = p.Z - half
	}
	if p.X+half > maxX {
		maxX = p.X + half
	}
	if p.Z+half > maxZ {
		maxZ = p.Z + half
	}
	return minX, minZ, maxX, maxZ
}

// sortIDs 按 (Type, Seq) 稳定排序，保证查询结果确定性。
func sortIDs(ids []object.ObjectID) {
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].Type != ids[j].Type {
			return ids[i].Type < ids[j].Type
		}
		return ids[i].Seq < ids[j].Seq
	})
}
