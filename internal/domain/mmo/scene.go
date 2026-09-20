package mmo

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/domain/object"
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/pkg/domain/mmo/collide"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/conv"
)

// Scene 是 SceneManager 内的一张地图：包含多个隔离 Instance + 共享物理碰撞 + 视野同步。
// 文件编排：Scene 定义+Instance管理+Enter/Leave/Move+查询 → scene.go

// 物理Body → physics.go  视野同步 → view_sync.go
type Scene struct {
	mu         sync.RWMutex
	sm         *SceneManager
	id         uint64
	name       string
	instances  map[uint32]*Instance // instance id → Instance
	instanceOf map[uint64]uint32    // objID → instance id
	cgrid      *collide.Grid        // 2D 水平面宽相（俯视玩法 / 贴地碰撞，跨 Instance 共享）
	cgrid3     *collide.Grid3       // 3D 宽相（含高度：多层地形 / 飞行 / 立体弹道避障）
	physicsOn  atomic.Bool
	beat       *SceneBeat

	viewMu    sync.Mutex
	viewBatch *viewBatch
	batching  atomic.Bool
	// batchMu 守卫「批处理窗口」本身（BeginBatch / EndBatch 配对）；
	// viewMu 只守卫 viewBatch 数据。两者必须分开：AOI 派发会同步回调 onViewChange 去拿 viewMu，
	// 若批窗口期间一直持 viewMu 就必然自锁死（见 BeginBatch 注释）。
	batchMu sync.Mutex
	// batchDepth 嵌套的批处理窗口深度（由 batchMu 保护）：
	// 0 = 无窗口；>1 表示被外层窗口包裹，内层不重复开关（见 BeginBatch 注释）。
	batchDepth int

	// 事件系统：eventType → handler，业务可通过 OnEvent 注册，SendEvent 同步派发。
	evtHandlers map[string]SceneEventHandler
	// evtExecMu 是 SendQueueEvent 的串行执行锁：保证同一场景上的事件互斥进入 handler。
	// 与 s.mu 互不嵌套（SendQueueEvent 先取 evtExecMu，handler 内再取 s.mu），无死锁风险。
	evtExecMu sync.Mutex
}

// moveOp 是一次 AOI 位置更新（攒起来在释放层锁后再写入网格，避免持锁阻塞该层读写）。
// pos 是完整三维坐标：物理积分产生的垂直位移同样要落回 AOI，否则起跳/落地不产生视野变化。
type moveOp struct {
	id  object.ObjectID
	pos Vec3
}

// MoveOp 对外暴露的移动操作。
type MoveOp struct {
	ID  uint64
	Pos Vec3
}

func newScene(sm *SceneManager, id uint64, name string) *Scene {
	s := &Scene{
		sm:         sm,
		id:         id,
		name:       name,
		instances:  make(map[uint32]*Instance),
		instanceOf: make(map[uint64]uint32),
		cgrid:      collide.NewGrid(sm.opts.cellSize),
		cgrid3:     collide.NewGrid3(sm.opts.cellSize),
	}
	s.createInstanceLocked(0) // 默认 instance 0
	return s
}

func (s *Scene) ID() uint64   { return s.id }
func (s *Scene) Name() string { return s.name }

// Instance 管理 API
func (s *Scene) CreateInstance(id uint32) (*Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.instances[id]; ok {
		return nil, errors.New("mmo: instance already exists")
	}
	return s.createInstanceLocked(id), nil
}

func (s *Scene) createInstanceLocked(id uint32) *Instance {
	l := newInstance(s, id)
	s.instances[id] = l
	return l
}

func (s *Scene) Instance(id uint32) *Instance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.instances[id]
}

func (s *Scene) Instances() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint32, 0, len(s.instances))
	for id := range s.instances {
		out = append(out, id)
	}
	return out
}

func (s *Scene) RemoveInstance(id uint32) error {
	if id == 0 {
		return errors.New("mmo: cannot remove default instance 0")
	}
	s.mu.Lock()
	l, ok := s.instances[id]
	if !ok {
		s.mu.Unlock()
		return ErrInstanceNotFound
	}
	delete(s.instances, id)
	members := make([]uint64, 0, 8)
	for objID, lid := range s.instanceOf {
		if lid == id {
			delete(s.instanceOf, objID)
			members = append(members, objID)
		}
	}
	// 成员列表必须在清空 kinds 之前抓：kinds 一置 nil，memberIDs() 就返回空，
	// 后面那圈 grid.Leave 会静默变成「什么都没做」（AOI 里留下一整批幽灵成员）。
	gridMembers := l.memberIDs()
	l.mu.Lock()
	bodyIDs := make([]uint64, 0, len(l.bodies))
	for objID := range l.bodies {
		bodyIDs = append(bodyIDs, objID)
	}
	l.bodies = nil
	l.kinds = nil
	l.mu.Unlock()
	// 该实例物理体挂在**场景级**宽相上，正常 Leave 会移除、RemoveInstance 之前没移除 →
	// 宽相里残留幽灵碰撞体（碰撞误判 + 内存不释放）。
	for _, objID := range bodyIDs {
		s.cgrid.Remove(strID(objID))
		s.cgrid3.Remove(strID(objID))
	}
	// objID→Scene 索引同样要清（正常 Leave 在 247-251 行会清）：
	// 否则 SceneOf 仍返回本场景，而 instanceOf 已删 → 之后 Move/Position 回落到默认
	// 实例 0，实体在两个实例之间漂移（幽灵实体）。
	s.sm.mu.Lock()
	for _, objID := range members {
		if s.sm.objScene[objID] == s {
			delete(s.sm.objScene, objID)
		}
		// 对象已离开本节点：迁移版本记录一并清掉（不清理会在长跑中无上限增长，
		// 且同一 objID 复用时残留版本号会把新对象的迁移误判为乱序）。
		s.sm.forgetXferVersionLocked(objID)
	}
	s.sm.mu.Unlock()
	s.mu.Unlock()
	l.grid.Stop()
	for _, member := range gridMembers {
		l.grid.Leave(member)
	}
	return nil
}

func (s *Scene) instanceOfObj(objID uint64) uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if lid, ok := s.instanceOf[objID]; ok {
		return lid
	}
	return 0
}

func (s *Scene) getInstance(id uint32) *Instance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.instances[id]
}

// Enter / Leave / Move
func (s *Scene) Enter(objID uint64, pos Vec3) error {
	return s.EnterOwnerType(objID, data.OwnerPlayer, pos)
}

func (s *Scene) EnterOwnerType(objID uint64, ownerType data.OwnerType, pos Vec3) error {
	return s.EnterOwnerTypeInstance(objID, ownerType, pos, DefaultInstance)
}

func (s *Scene) EnterOwnerTypeInstance(objID uint64, ownerType data.OwnerType, pos Vec3, instanceID uint32) error {
	l := s.getInstance(instanceID)
	if l == nil {
		return ErrInstanceNotFound
	}
	s.mu.Lock()
	if _, exists := s.instanceOf[objID]; exists {
		s.mu.Unlock()
		return ErrExists
	}
	// 还要挡「对象已属于别的场景」：objScene 是全局唯一归属索引，
	// 直接覆盖会让同一 objID 同时挂在两个 Scene 的 instanceOf / kinds / AOI 网格上，
	// 而 objScene 只记最后一次 → 旧场景的 Leave 清理不到它，双份成员永久残留。
	s.sm.mu.Lock()
	prev, prevOK := s.sm.objScene[objID]
	s.sm.mu.Unlock()
	if prevOK && prev != nil && prev != s {
		s.mu.Unlock()
		logger.Warnf("mmo: obj=%d 已属于场景 %d(%s)，拒绝重复进入场景 %d(%s)；请先从旧场景 Leave",
			objID, prev.id, prev.name, s.id, s.name)
		return ErrExists
	}
	s.instanceOf[objID] = instanceID
	s.sm.mu.Lock()
	s.sm.objScene[objID] = s
	s.sm.mu.Unlock()
	s.mu.Unlock()

	l.mu.Lock()
	l.kinds[objID] = ownerType
	l.mu.Unlock()

	// 玩家进入场景（含切场景 / 跨服转移落点）时，先下发场景标识，
	// 再放行 AOI 实体事件，保证客户端拿到场景归属后才有实体进入视野。
	if ownerType == data.OwnerPlayer {
		s.pushSceneInfo(objID, instanceID)
	}

	if !s.batching.Load() {
		s.beginViewBatch()
		defer s.endViewBatch()
	}
	id := object.NewObjectID(object.TypePlayer, objID)
	// Y 必须透传：AOI 按三维球体判定视野，丢掉 Y 会让「楼上楼下」退化成同一水平投影内互相可见。
	// pos 与 aoi.Position 同为 geom.Vec3，直接传即可。
	l.grid.Enter(id, pos)
	return nil
}

// pushSceneInfo 向玩家下发其当前所在的场景标识（EPushSceneInfo）。
// 这是客户端「服务端场景」（CloverScene）的唯一数据来源：客户端据此得知
// 「我在哪张地图、哪条分线」，再自行映射到本地 Unity 关卡。
// 载体语义与 mmo.Scene 一致，不做任何改名或语义包装。
func (s *Scene) pushSceneInfo(objID uint64, instanceID uint32) {
	if s.sm.opts.pub == nil {
		return
	}
	body, err := json.Marshal(proto.ESceneInfoNotify{
		SceneID:    s.id,
		InstanceID: instanceID,
		Name:       s.name,
	})
	if err != nil {
		logger.Warnf("mmo: marshal scene info failed (scene=%d obj=%d): %v", s.id, objID, err)
		return
	}
	if err := s.SendToWithMode(objID, proto.EPushSceneInfo, body, proto.DeliveryModeReliable); err != nil {
		logger.Warnf("mmo: push scene info failed (scene=%d obj=%d): %v", s.id, objID, err)
	}
}

func (s *Scene) Leave(objID uint64) {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		// 实例已不存在（典型：RemoveInstance 已把该实例删掉）：直接 return 会让
		// objID→Scene 索引与场景级宽相里的条目永久残留 —— SceneOf 继续返回本场景，
		// 后续 Move/Position 又因 instanceOf 已删而落到默认实例 0（幽灵实体）。
		s.cgrid.Remove(strID(objID))
		s.cgrid3.Remove(strID(objID))
		s.mu.Lock()
		delete(s.instanceOf, objID)
		s.sm.mu.Lock()
		if s.sm.objScene[objID] == s {
			delete(s.sm.objScene, objID)
		}
		s.sm.forgetXferVersionLocked(objID)
		s.sm.mu.Unlock()
		s.mu.Unlock()
		return
	}
	if !s.batching.Load() {
		s.beginViewBatch()
		defer s.endViewBatch()
	}
	id := object.NewObjectID(object.TypePlayer, objID)
	l.grid.Leave(id)
	// 宽相清理放在「归属映射删除」之前。
	// 顺序反过来的话：instanceOf 一删，并发的 Enter 就会认为对象不在场景里、
	// 把它放进自己的 instance —— 然后我们这两行 Remove 正好把**刚进入的对象**
	// 从碰撞网格里抹掉（有归属记录、却没有宽相条目，之后所有碰撞/查询都漏它）。
	s.cgrid.Remove(strID(objID))
	s.cgrid3.Remove(strID(objID))

	l.mu.Lock()
	delete(l.bodies, objID)
	delete(l.kinds, objID)
	l.mu.Unlock()

	s.mu.Lock()
	delete(s.instanceOf, objID)
	s.sm.mu.Lock()
	if s.sm.objScene[objID] == s {
		delete(s.sm.objScene, objID)
	}
	s.sm.forgetXferVersionLocked(objID)
	s.sm.mu.Unlock()
	s.mu.Unlock()
}

// Collider 返回场景的 2D 水平面碰撞宽相（业务可注册墙体/区域后再查询）。
func (s *Scene) Collider() *collide.Grid { return s.cgrid }

// Collider3 返回场景的 3D 碰撞宽相：业务往里注册三层墙体/楼板（Mask 含 GroupWall）后，
// 即可用 QueryRegion / QuerySphere / SweepCCD / Raycast 做**立体**的区域判定、
// 弹道避障与视线判定——这是 2D 版做不到的部分（2D 只看水平面）。
//
// 约定：Y 为高度；掩码用 collide.GroupWall 标记静态障碍，其余分组见 collide.CollisionGroup。
func (s *Scene) Collider3() *collide.Grid3 { return s.cgrid3 }

func (s *Scene) ownerTypeOf(objID uint64) data.OwnerType {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return data.OwnerPlayer
	}
	l.mu.RLock()
	k, ok := l.kinds[objID]
	l.mu.RUnlock()
	if !ok {
		return data.OwnerPlayer
	}
	return k
}

func (s *Scene) OwnerTypeOf(objID uint64) data.OwnerType { return s.ownerTypeOf(objID) }

// MemberKind 返回对象在本场景登记的实体类型；ok=false 表示该对象不在本场景（或未登记类型）。
//
// 与 OwnerTypeOf 的区别：那个在查不到时**返回 OwnerPlayer 当默认值**，把「不在场景」
// 与「确实是玩家」混成了同一个答案。需要按类型过滤（例如只给玩家推消息）时必须用本方法，
// 否则一个已经离场的 objID 会被当成玩家。
func (s *Scene) MemberKind(objID uint64) (data.OwnerType, bool) {
	l := s.getInstance(s.instanceOfObj(objID))
	if l == nil {
		return data.OwnerPlayer, false
	}
	l.mu.RLock()
	k, ok := l.kinds[objID]
	l.mu.RUnlock()
	return k, ok
}

func (s *Scene) TransferTo(dst *Scene, objID uint64, pos Vec3) error {
	if s == dst {
		s.Move(objID, pos)
		return nil
	}
	lid := s.instanceOfObj(objID)
	ownerType := s.ownerTypeOf(objID)
	srcInstance := s.getInstance(lid)

	var body *Body
	if srcInstance != nil {
		srcInstance.mu.RLock()
		if b, ok := srcInstance.bodies[objID]; ok {
			cp := *b
			// 落点必须同步进拷贝：cp 保留的是**源**位置，直接用它在新场景重建宽相，
			// 会出现「AOI 网格在 pos、物理体在旧位置」的分裂（违反下方 Move 的同源约定）。
			cp.Position = pos
			body = &cp
		}
		srcInstance.mu.RUnlock()
	}
	if _, err := dst.CreateInstanceIfNotExist(lid); err != nil {
		return err
	}
	// 先从源场景离场：EnterOwnerTypeInstance 会拒绝「已属于别的场景」的重复进入，
	// 顺序反了会让对象在两帧之间同时挂在两个场景上（双份成员 + objScene 只认最后一个）。
	s.Leave(objID)
	if err := dst.EnterOwnerTypeInstance(objID, ownerType, pos, lid); err != nil {
		// 已经离场却没进新场景 = 对象凭空消失，必须留痕。
		logger.Errorf("mmo: transfer obj=%d 从场景 %d(%s) 到 %d(%s) 失败（已离场但未入场）: %v",
			objID, s.id, s.name, dst.id, dst.name, err)
		return err
	}
	if body != nil {
		dst.AddBody(objID, body)
	}
	return nil
}

func (s *Scene) CreateInstanceIfNotExist(id uint32) (*Instance, error) {
	if l := s.Instance(id); l != nil {
		return l, nil
	}
	return s.CreateInstance(id)
}

func (s *Scene) Move(objID uint64, pos Vec3) {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return
	}
	if !s.batching.Load() {
		s.beginViewBatch()
		defer s.endViewBatch()
	}
	id := object.NewObjectID(object.TypePlayer, objID)
	l.grid.Move(id, pos)
	l.mu.Lock()
	if b, ok := l.bodies[objID]; ok {
		// 整体同步（含 Y）：Body 与 AOI 位置必须同源，否则物理与视野各走一套会漂移。
		b.Position = pos
		// 场景级宽相同样要跟着走（与 physicsStep 一致）：只改 b.Position 不改 cgrid/cgrid3，
		// 碰撞查询拿到的仍是旧 AABB —— 表现为「看着已经走开了，还是被打到 / 卡住」。
		// 必须在仍持有 l.mu 时写回，避免与并发 Leave/RemoveBody 交错出幽灵碰撞体。
		key := strID(objID)
		s.cgrid.Insert(key, bodyAABB(b))
		s.cgrid3.Insert(key, bodyAABB3(b))
		s.cgrid3.SetSphere(key, collide.Sphere{Center: b.Position, R: b.Radius})
	}
	l.mu.Unlock()
}

func (s *Scene) MoveBatch(moves []MoveOp) {
	if len(moves) == 0 {
		return
	}
	// BeginBatch/EndBatch 现在是**可嵌套**的（batchDepth 计数），
	// 因此在已有批处理窗口里调用 MoveBatch 不会再自我死锁，内层的
	// 开关退化为加/减深度，由最外层统一 flush。
	s.BeginBatch()
	defer s.EndBatch()
	for _, m := range moves {
		s.Move(m.ID, m.Pos)
	}
}

// 视图批处理
//
// 锁分工（关键，历史缺陷点）：batchMu 守卫「批处理窗口」本身（Begin/End 配对），
// viewMu 只守卫 viewBatch 数据，且**只在读写 viewBatch 的瞬间持有**。
// 不能像过去那样让 viewMu 跨整个窗口：l.grid.EndBatch() 会**同步**回调 Scene.onViewChange，
// 那里要拿 viewMu，同一 goroutine 反复加同一把非重入锁 = 必然死锁。
//
// 批窗口是**可嵌套**的：深度由 batchDepth 记录，只有最外层真正开/关窗口，
// 内层只累加深度。这样 Enter/Leave/Move 内部的 begin/flush 与业务手写的
// BeginBatch/EndBatch 可以任意嵌套，不会自我死锁、也不会互相踩掉 viewBatch。
func (s *Scene) BeginBatch() {
	s.batchMu.Lock()
	s.batchDepth++
	if s.batchDepth > 1 {
		s.batchMu.Unlock()
		return
	}
	s.viewMu.Lock()
	s.batching.Store(true)
	b, _ := viewBatchPool.Get().(*viewBatch)
	if b == nil {
		b = &viewBatch{}
	}
	wm, _ := watchersMapPool.Get().(map[uint64]*watcherBatch)
	if wm == nil {
		wm = make(map[uint64]*watcherBatch)
	}
	sm, _ := snapMapPool.Get().(map[snapCacheKey]cachedSnap)
	if sm == nil {
		sm = make(map[snapCacheKey]cachedSnap)
	}
	b.watchers = wm
	b.snaps = sm
	s.viewBatch = b
	s.viewMu.Unlock()
	s.batchMu.Unlock()
	// 实例列表先快照再开窗口 —— 不在持 s.mu 时遍历写操作，避免与并发建/删实例纠缠。
	for _, l := range s.instanceSnapshot() {
		l.grid.BeginBatch()
	}
}

// EndBatch 结束手动批处理（与 BeginBatch 配对）。
//
// 顺序是硬要求：先让 AOI 派发（**此时既不持 viewMu 也不持 s.mu** ——
// l.grid.EndBatch() 会同步回调 Scene.onViewChange，它要拿 viewMu；
// dispatch 链路上又会调 s.Instance() 拿 s.mu.RLock，持着就自锁死），
// 派发期间进出视野事件累积进 viewBatch，最后持 viewMu 关闭窗口并统一发布。
func (s *Scene) EndBatch() {
	// 深度判定必须在关网格窗口**之前**：AOI 侧的 BeginBatch/EndBatch 不可重入，
	// 只有最外层才允许配对关闭。内层若也调用 EndBatch（BeginBatch 内层已只累加深度），
	// 会把最外层开的网格批窗口提前关死，此后外层窗口内的网格操作不再批处理。
	s.batchMu.Lock()
	if s.batchDepth == 0 {
		s.batchMu.Unlock()
		return
	}
	s.batchDepth--
	outermost := s.batchDepth == 0
	s.batchMu.Unlock()
	if !outermost {
		return
	}

	for _, l := range s.instanceSnapshot() {
		l.grid.EndBatch()
	}

	s.batchMu.Lock()
	s.viewMu.Lock()
	s.batching.Store(false)
	pending := s.takeViewBatchLocked()
	s.viewMu.Unlock()
	// 快照拉取（数据库 IO）与 Publish 必须在锁外：见 takeViewBatchLocked 注释。
	s.flushViewBatch(pending)
	s.batchMu.Unlock()
}

// instanceSnapshot 在 s.mu 下抓一份实例列表快照（供锁外遍历使用）。
func (s *Scene) instanceSnapshot() []*Instance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Instance, 0, len(s.instances))
	for _, l := range s.instances {
		out = append(out, l)
	}
	return out
}

func (s *Scene) SetViewRadius(objID uint64, radius float64) {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return
	}
	if !s.batching.Load() {
		s.beginViewBatch()
		defer s.endViewBatch()
	}
	id := object.NewObjectID(object.TypePlayer, objID)
	l.grid.Watch(id, radius)
}

// 查询
func (s *Scene) Members() []uint64 {
	var out []uint64
	s.mu.RLock()
	for _, l := range s.instances {
		out = append(out, l.Members()...)
	}
	s.mu.RUnlock()
	return out
}

func (s *Scene) Position(objID uint64) (Vec3, bool) {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return Vec3{}, false
	}
	id := object.NewObjectID(object.TypePlayer, objID)
	p, ok := l.grid.Position(id)
	if !ok {
		return Vec3{}, false
	}
	// 三维原样返回：这里曾被压成 {X, Z}，使业务拿不到高度（跳跃/飞行位置查询全失真）。
	return p, true
}

func (s *Scene) Neighbors(objID uint64, radius float64) []uint64 {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return nil
	}
	id := object.NewObjectID(object.TypePlayer, objID)
	ids := l.grid.Neighbors(id, radius)
	out := make([]uint64, 0, len(ids))
	for _, v := range ids {
		out = append(out, v.Seq)
	}
	return out
}

func (s *Scene) Around(objID uint64, radius float64) []uint64 {
	lid := s.instanceOfObj(objID)
	l := s.getInstance(lid)
	if l == nil {
		return nil
	}
	id := object.NewObjectID(object.TypePlayer, objID)
	p, ok := l.grid.Position(id)
	if !ok {
		return nil
	}
	ids := l.grid.Around(p, radius)
	out := make([]uint64, 0, len(ids))
	for _, v := range ids {
		out = append(out, v.Seq)
	}
	return out
}

// 消息广播
func (s *Scene) Broadcast(msgID uint32, body []byte) {
	s.BroadcastWithMode(msgID, body, proto.DeliveryModeReliable)
}

// broadcastFailCount 广播失败计数（降频用）：下游不可用时按成员数打印会瞬间刷屏，
// 把真正有用的前几条日志淹掉。这里「首次全量 + 之后每 1000 条一条」，与
// entitysync 的 viewPushFailf 同一口径。
var broadcastFailCount atomic.Uint64

func (s *Scene) BroadcastWithMode(msgID uint32, body []byte, mode proto.DeliveryMode) {
	for _, id := range s.Members() {
		if err := s.SendToWithMode(id, msgID, body, mode); err != nil {
			if n := broadcastFailCount.Add(1); n == 1 || n%1000 == 0 {
				logger.Warnf("mmo: scene %d broadcast to obj %d failed: %v (累计 %d 次)", s.id, id, err, n)
			}
		}
	}
}

func (s *Scene) SendTo(objID uint64, msgID uint32, body []byte) error {
	// 直接发给指定玩家的消息默认使用可靠传输（任务完成、奖励发放等）
	return s.SendToWithMode(objID, msgID, body, proto.DeliveryModeReliable)
}

func (s *Scene) SendToWithMode(objID uint64, msgID uint32, body []byte, mode proto.DeliveryMode) error {
	if s.sm.opts.pub == nil {
		return ErrNoPublisher
	}
	p := viewPush{Target: objID, MsgID: msgID, Body: body, DeliveryMode: mode}
	b := encodeDirectMessage(p)
	return s.sm.opts.pub.Publish(s.sm.opts.viewSubject, b)
}

// 生命周期
func (s *Scene) Stop() {
	s.mu.Lock()
	s.beat = nil
	s.mu.Unlock()
	s.mu.RLock()
	for _, l := range s.instances {
		l.grid.Stop()
	}
	s.mu.RUnlock()
}

func (s *Scene) EnablePhysics()  { s.physicsOn.Store(true) }
func (s *Scene) DisablePhysics() { s.physicsOn.Store(false) }

func (s *Scene) AttachBeat(b *SceneBeat) {
	s.mu.Lock()
	s.beat = b
	s.mu.Unlock()
}

func (s *Scene) Tick(dt time.Duration) {
	s.mu.RLock()
	b := s.beat
	instances := make([]*Instance, 0, len(s.instances))
	for _, l := range s.instances {
		instances = append(instances, l)
	}
	s.mu.RUnlock()
	if b != nil {
		b.Tick(dt)
	}
	// 物理步进随主循环驱动（physicsOn 默认关闭，开启后每帧步进各 Instance）。
	// ★ 物理产生的 AOI 移动必须包在**批窗口**内：否则每次 grid.Move 都会触发
	// 非批路径的 onViewChange —— 逐条做快照拉取（DB IO）与单事件 Publish，
	// 把 tick 线程钉在同步 IO 上。批窗口把一帧内所有移动合并为一次刷新与发布。
	// （批窗口可嵌套：外层已有窗口时本处只累加深度，由最外层统一 flush。）
	if len(instances) > 0 && !s.batching.Load() {
		s.beginViewBatch()
		defer s.endViewBatch()
	}
	for _, l := range instances {
		s.tickInstance(l, dt)
	}
}

// strID 将 uint64 转字符串（碰撞网格 key 用）。
func strID(id uint64) string {
	return conv.FormatUint(id)
}
