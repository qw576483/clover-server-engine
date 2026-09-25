package mmo

import (
	"context"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/internal/transport"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
)

// SceneManager 全局场景管理器：管理所有 Scene 的生命周期与跨 Scene 传输。
type SceneManager struct {
	mu       sync.RWMutex
	opts     options
	manager  *object.Manager
	scenes   map[uint64]*Scene
	objScene map[uint64]*Scene // objID→scene 索引，避免 O(n*m) 遍历

	// 跨机迁移的幂等 / 乱序校验：objID → 本节点已应用的**最大**迁移版本号。
	// 没有它，重投 / 乱序到达的迁移指令会被重复执行（RemoteTransfer.Version 必须
	// 赋值并校验，否则文档承诺的幂等语义完全落空）。
	//
	// 生命周期：对象从本节点任何场景 Leave / 实例被移除 / 场景被销毁时同步删除
	// （见 forgetXferVersionLocked），并额外带 xferVerTTL 兜底——
	// 只靠「离开时清理」仍会漏「迁入后再没被碰过、进程长跑」的条目，长跑下无上限增长。
	xferVer map[uint64]uint64
	// xferAt 上表每条记录的最后写入时间，仅用于 TTL 惰性清扫。
	xferAt  map[uint64]time.Time
	xferSeq uint64 // 本节点发出的迁移序号（单调递增）
	// xferOps 距上次 TTL 清扫的版本登记次数（摊还用，见 purgeXferVersionLocked）。
	xferOps int

	// remoteSubject 跨机迁移指令的订阅 subject（未开启接收端时为空串）。
	// 订阅必须与持有者同生命周期：Stop 时据此反注册（见 unsubscribeRemoteTransfer）——
	// 否则场景管理器已停、迁移回调仍在被 NATS 派发。
	remoteSubject string
}

// xferVerTTL 迁移版本记录的兜底存活时长。
//
// 取值依据：迁移指令的重投 / 乱序窗口只与「发送端重试退避 + 消息队列滞留」有关，
// 量级是秒到分钟；5 分钟足以覆盖任何合理重投，同时对长跑进程给出确定的上界。
const xferVerTTL = 5 * time.Minute

// xferPurgeEvery 每 N 次版本登记触发一次 TTL 清扫（摊还成本，避免每次迁移都扫全表）。
const xferPurgeEvery = 256

func NewSceneManager(opts ...Option) *SceneManager {
	o := options{tickRate: 50 * time.Millisecond, cellSize: 64, viewSubject: "mmo.view"}
	for _, opt := range opts {
		opt(&o)
	}
	objMgr := o.objMgr
	if objMgr == nil {
		objMgr = object.NewManager()
	}
	sm := &SceneManager{
		opts:     o,
		manager:  objMgr,
		scenes:   make(map[uint64]*Scene),
		objScene: make(map[uint64]*Scene),
		xferVer:  make(map[uint64]uint64),
		xferAt:   make(map[uint64]time.Time),
	}
	// 跨机迁移接收端接线（未同时注入路由表与订阅能力时空操作）。
	sm.wireRemoteTransfer()
	return sm
}

func (sm *SceneManager) Store() *data.Store             { return sm.opts.store }
func (sm *SceneManager) Publisher() Publisher           { return sm.opts.pub }
func (sm *SceneManager) ViewSubject() string            { return sm.opts.viewSubject }
func (sm *SceneManager) ObjectManager() *object.Manager { return sm.manager }

func (sm *SceneManager) CreateScene(id uint64, name string) *Scene {
	sm.mu.Lock()
	if old, ok := sm.scenes[id]; ok {
		// 同 id 重复创建：直接覆盖会让旧场景的 AOI 后台协程与场景路由登记
		// 永久泄漏（旧 Scene 再也等不到 Stop）。先停旧场景、注销它的路由，
		// 再登记新场景——与 DestroyScene 的清理口径保持一致。
		delete(sm.scenes, id)
		sm.mu.Unlock()
		old.Stop()
		for _, mid := range old.Members() {
			old.Leave(mid)
		}
		sm.unregisterSceneRoute(id)
		sm.mu.Lock()
	}
	s := newScene(sm, id, name)
	sm.scenes[id] = s
	sm.mu.Unlock()

	// 登记 scene→node 路由（跨机迁移的寻址依据）。放在锁外：这是一次全局 KV 写入，
	// 不能占着场景表锁做网络 IO。
	sm.registerSceneRoute(id)
	return s
}

func (sm *SceneManager) GetScene(id uint64) (*Scene, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.scenes[id]
	return s, ok
}

func (sm *SceneManager) DestroyScene(id uint64) {
	sm.mu.Lock()
	s, ok := sm.scenes[id]
	delete(sm.scenes, id)
	sm.mu.Unlock()
	// 注销路由：场景已不存在却仍留着登记，会让跨机迁移把对象发到「没有该场景」的节点。
	sm.unregisterSceneRoute(id)
	if ok {
		s.Stop()
		for _, id := range s.Members() {
			s.Leave(id)
		}
	}
}

// Transfer 同机跨场景迁移：pos 为完整三维落点（含高度，多层地图下不能只传水平坐标）。
func (sm *SceneManager) Transfer(srcSceneID, dstSceneID, objID uint64, pos Vec3) error {
	sm.mu.RLock()
	src, ok1 := sm.scenes[srcSceneID]
	dst, ok2 := sm.scenes[dstSceneID]
	sm.mu.RUnlock()
	if !ok1 || !ok2 {
		return ErrSceneNotFound
	}
	return src.TransferTo(dst, objID, pos)
}

// TransferRemote 跨机迁移：同机直接走 Transfer；异机先经路由表定位目标节点，再定向投递。
func (sm *SceneManager) TransferRemote(dstSceneID, objID uint64, pos Vec3) error {
	src, ok := sm.findSceneOf(objID)
	if !ok {
		return ErrSceneNotFound
	}
	if sm.isLocal(dstSceneID) {
		return sm.Transfer(src.ID(), dstSceneID, objID, pos)
	}
	if !sm.clusterEnabled() {
		// 「没开启」必须留痕：nodeID=0 或漏注入路由表时，跨机迁移会静默降级成
		// 一个 ErrNoRoute，业务侧只看到「迁移没生效」，查不出是配置漏了。
		logger.Warnf("mmo: TransferRemote obj=%d -> scene=%d 失败：跨机迁移未开启（route注入=%t nodeID=%d）",
			objID, dstSceneID, sm.opts.route != nil, sm.opts.nodeID)
		return ErrNoRoute
	}
	if sm.opts.pub == nil {
		return ErrNoPublisher
	}
	// 先查路由表定位目标场景所在节点：查不到就不发——目标场景可能不存在，
	// 或所在节点已崩溃（登记 TTL 过期被自动摘除）。盲发只会静默失败，
	// 源端还已经把对象 Leave 掉了，等于凭空丢对象。
	// 全局 KV 查询必须有超时，否则底层 IO 卡住就没有任何退出路径。
	lookupCtx, lookupCancel := sceneRouteCtx()
	defer lookupCancel()
	dstNode, found, err := sm.opts.route.LookupScene(lookupCtx, dstSceneID)
	if err != nil {
		return err
	}
	if !found {
		return ErrSceneRouteNotFound
	}
	if dstNode == sm.opts.nodeID {
		// 路由说目标在本节点，但上面 isLocal 未命中：登记已过期、或场景刚被销毁。
		return ErrSceneNotFound
	}
	rt := transport.RemoteTransfer{
		DstScene:  dstSceneID,
		ObjID:     objID,
		X:         pos.X,
		Y:         pos.Y,
		Z:         pos.Z,
		OwnerType: src.ownerTypeOf(objID),
		Instance:  src.instanceOfObj(objID),
		// 必须打上本节点单调递增的序号：接收端只接受比已应用版本更大的指令，
		// 否则重投 / 乱序的迁移指令会被重复执行。
		Version: sm.nextXferVersion(),
		SrcNode: conv.FormatUint(sm.opts.nodeID),
	}
	// 物理体随指令一起搬运：同机 TransferTo 会把 Body 整份拷贝过去并 AddBody，
	// 跨机若不带 Body，Mass / Radius / Velocity / Force / Static 就会静默丢失
	//（现象是"跨图后手感变了、击退不再生效"，现场极难定位）。
	if b := src.Body(objID); b != nil {
		rt.Body = &transport.RemoteBody{
			Mass:     b.Mass,
			Radius:   b.Radius,
			Position: [3]float64{b.Position.X, b.Position.Y, b.Position.Z},
			Velocity: [3]float64{b.Velocity.X, b.Velocity.Y, b.Velocity.Z},
			Force:    [3]float64{b.Force.X, b.Force.Y, b.Force.Z},
			Static:   b.Static,
		}
	}
	// 发布同样必须有界：JetStream 发布带重试/退避，用 Background 时调用方超时后它还在重试。
	pubCtx, pubCancel := sceneRouteCtx()
	defer pubCancel()
	if err := transport.PublishRemoteTransfer(pubCtx, sm.opts.pub, dstNode, rt); err != nil {
		return err
	}
	src.Leave(objID)
	return nil
}

// findSceneOf 经 objID→Scene 索引查找，O(1)。
func (sm *SceneManager) findSceneOf(objID uint64) (*Scene, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.objScene[objID]
	return s, ok
}

// SceneOf 经 objID→Scene 索引查找（门面 pkg/domain/mmo 透传用）。
//
// 与 findSceneOf 读的是同一张索引，导出它只为一件事：**业务不要再自建一份 objID→场景 映射**。
// 自建映射与这里的索引是两份真值，任何一处漏写都会让后续操作静默失效
// （典型症状：忘记登记 → Scene.Move 找不到对象 → 移动被无声忽略，服务端只当没收到）。
func (sm *SceneManager) SceneOf(objID uint64) (*Scene, bool) { return sm.findSceneOf(objID) }

// isLocal 判断目标场景是否在本节点。
//
// 必须走读锁：CreateScene / DestroyScene / Stop 都会并发写这张 map，
// 而 Go 的 map 并发读写是 fatal error（不可 recover），不是「偶发脏读」。
func (sm *SceneManager) isLocal(dstSceneID uint64) bool {
	sm.mu.RLock()
	_, ok := sm.scenes[dstSceneID]
	sm.mu.RUnlock()
	return ok
}

// nextXferVersion 取本节点下一个迁移序号（单调递增）。
func (sm *SceneManager) nextXferVersion() uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.xferSeq++
	return sm.xferSeq
}

// acceptXferVersion 判断该迁移指令是否应被应用（幂等 + 乱序保护）。
// version<=0 视为「旧发送端未打版本号」，放行（保持向后兼容）。
func (sm *SceneManager) acceptXferVersion(objID, version uint64) bool {
	if version <= 0 {
		return true
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if last, ok := sm.xferVer[objID]; ok && version <= last {
		return false
	}
	now := time.Now()
	sm.xferVer[objID] = version
	sm.xferAt[objID] = now
	sm.purgeXferVersionLocked(now)
	return true
}

// purgeXferVersionLocked 惰性清扫过期的迁移版本记录（调用方须持 sm.mu）。
//
// 必须清扫：正常应用过的条目不会随对象删除而释放，
// 长跑节点上「每个迁入过的对象」都留一条 —— 无上限增长。
// 摊还策略与 state/loginGuard 的 purgeIdleLocked 同款：每 xferPurgeEvery 次登记扫一遍。
func (sm *SceneManager) purgeXferVersionLocked(now time.Time) {
	sm.xferOps++
	if sm.xferOps < xferPurgeEvery {
		return
	}
	sm.xferOps = 0
	for objID, at := range sm.xferAt {
		if now.Sub(at) <= xferVerTTL {
			continue
		}
		delete(sm.xferVer, objID)
		delete(sm.xferAt, objID)
	}
}

// forgetXferVersionLocked 对象离开本节点时清掉它的迁移版本记录（调用方须持 sm.mu）。
//
// 场景侧四处 objID 归属变更（Leave / 实例移除 / RemoveInstance / 销毁）都会调用它，
// 与「对象离开」保持同步——否则同一个 objID 之后被复用时会残留历史版本号，
// 把新对象的正常迁移指令误判为「乱序」而丢弃。
func (sm *SceneManager) forgetXferVersionLocked(objID uint64) {
	delete(sm.xferVer, objID)
	delete(sm.xferAt, objID)
}

// rollbackXferVersion 在迁移**应用失败**时回滚已登记的版本号。
//
// acceptXferVersion 在「接收时」就记账，而其后 CreateInstanceIfNotExist /
// EnterOwnerTypeInstance 仍可能失败。若失败后不回滚，底层「至少一次」的同一版本补投
// 会被判为「重复/乱序」丢弃 —— 对象既没进新场景、源端又已 Leave，重试通道被幂等保护掐死。
func (sm *SceneManager) rollbackXferVersion(objID, version uint64) {
	if version <= 0 {
		return
	}
	sm.mu.Lock()
	// 仅回滚本次登记的版本：期间若有更大版本已应用，不得误删。
	if sm.xferVer[objID] == version {
		delete(sm.xferVer, objID)
		delete(sm.xferAt, objID)
	}
	sm.mu.Unlock()
}

func (sm *SceneManager) HandleRemoteTransfer(rt transport.RemoteTransfer) error {
	r, ok := sm.GetScene(rt.DstScene)
	if !ok {
		return ErrSceneNotFound
	}
	// 幂等 / 乱序校验：重投或先发后至的指令必须被挡掉，
	// 否则同一个对象会被重复 Enter（第二次必撞 ErrExists，或被当成新对象重建）。
	if !sm.acceptXferVersion(rt.ObjID, rt.Version) {
		logger.Warnf("mmo: 丢弃重复/乱序的跨机迁移 obj=%d dst_scene=%d version=%d（已应用更大版本）",
			rt.ObjID, rt.DstScene, rt.Version)
		return ErrExists
	}
	// 与同机 TransferTo 一致：目标实例不存在就先建。
	// 否则迁入非零 Instance 且目标场景没预建该实例时直接 ErrInstanceNotFound，迁移对象被丢弃。
	if _, err := r.CreateInstanceIfNotExist(rt.Instance); err != nil {
		// 应用失败必须回滚版本号，否则同一版本的补投会被误判为重复而丢弃（见 rollbackXferVersion）。
		sm.rollbackXferVersion(rt.ObjID, rt.Version)
		return err
	}
	// 三维落点原样还原：Y 是跨机指令里带过来的，不是 0。
	if err := r.EnterOwnerTypeInstance(rt.ObjID, rt.OwnerType, Vec3{X: rt.X, Y: rt.Y, Z: rt.Z}, rt.Instance); err != nil {
		sm.rollbackXferVersion(rt.ObjID, rt.Version)
		return err
	}
	// 物理体按快照重建：与同机 TransferTo 完全一致 —— 那边是「整份拷贝后 AddBody」，
	// 且**位置以本次迁移落点为准**（cp.Position = pos），而不是沿用源场景里的旧坐标。
	// 不做这一步，跨机迁移后 Mass/Radius/Velocity/Force/Static 会静默丢失。
	if rt.Body != nil {
		r.AddBody(rt.ObjID, &Body{
			Mass:   rt.Body.Mass,
			Radius: rt.Body.Radius,
			// 落点用本次迁移的坐标：源侧 Body.Position 是离开前的旧位置。
			Position: Vec3{X: rt.X, Y: rt.Y, Z: rt.Z},
			Velocity: Vec3{X: rt.Body.Velocity[0], Y: rt.Body.Velocity[1], Z: rt.Body.Velocity[2]},
			Force:    Vec3{X: rt.Body.Force[0], Y: rt.Body.Force[1], Z: rt.Body.Force[2]},
			Static:   rt.Body.Static,
		})
	}
	return nil
}

func (sm *SceneManager) Run(ctx context.Context) error {
	// 场景路由 TTL 续期：登记必须在场景存活期间持续刷新，否则 DefaultSceneTTL 之后过期，
	// 别的节点再想跨机迁移过来就 LookupScene 不到目标（表现是「迁移静默失败」）。
	tk := time.NewTicker(routeRefreshInterval())
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			// 只停「启动时快照」里的场景是不够的：运行期新建的场景
			// 永远等不到 Stop，其 AOI 后台协程与路由登记全部泄漏。
			// 这里统一走 sm.Stop()（它按**当前**场景表停，并注销路由）。
			sm.Stop()
			return ctx.Err()
		case <-tk.C:
			sm.refreshSceneRoutes(ctx)
		}
	}
}

func (sm *SceneManager) Stop() {
	sm.mu.Lock()
	list := make([]*Scene, 0, len(sm.scenes))
	ids := make([]uint64, 0, len(sm.scenes))
	for id, s := range sm.scenes {
		list = append(list, s)
		ids = append(ids, id)
	}
	sm.mu.Unlock()
	for _, s := range list {
		s.Stop()
	}
	// 路由只在 CreateScene 注册、只在 DestroyScene 注销：Stop 若不同步注销，
	// 场景已经没了却仍留在路由表里，跨机迁移会把对象发到一个没有该场景的节点。
	for _, id := range ids {
		sm.unregisterSceneRoute(id)
	}
	// 跨机迁移的接收端订阅同样要退：订阅比持有者活得久，回调就会改动已停的场景管理器。
	sm.unsubscribeRemoteTransfer()
}
