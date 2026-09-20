package frame

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Service 统一管理多个 frame room。通过 roomID 索引，持有一个公用的 Broadcaster。
// 结构体与行为留在 internal；对外类型 / 接口 / 错误真身在 pkg/domain/room/frame。
type Service struct {
	mu             sync.RWMutex        // 保护 rooms map
	rooms          map[string]*Room    // roomID → Room
	push           Broadcaster         // 消息推送函数，由业务层注入
	pushReliable   BroadcasterWithMode // 可靠消息推送函数，由业务层注入
	now            func() time.Time    // 时间源，默认 time.Now，可注入 mock 时钟
	takeoverHook   func(roomID string) // 房间操作前的接管检查钩子，由引擎层注入
	onRoomCreated  func(roomID string) // 房间创建回调，引擎借此自动 RegisterRoom
	onRoomDestroy  func(roomID string) // 房间销毁回调，引擎借此自动 UnregisterRoom
	inputApplier   InputApplier        // 输入应用器，由业务层注入（生产环境必须设置）
	defaultRoomCfg *Config             // 新建房间的默认配置（nil 表示用 DefaultConfig()）
	metrics        Metrics             // 运行时指标，通过 Metrics() 获取
}

// NewService 构造一个纯内存 frame room 服务。
// 两阶段：先用 ServiceOption 填充 pkg 的 ServiceConfig，再落到本结构体字段，
// 使选项类型真身得以留在 pkg，而不暴露本结构体的私有字段。
func NewService(opts ...ServiceOption) *Service {
	cfg := ServiceConfig{Now: time.Now}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Service{
		rooms:          make(map[string]*Room),
		push:           cfg.Push,
		pushReliable:   cfg.PushReliable,
		now:            cfg.Now,
		takeoverHook:   cfg.TakeoverHook,
		onRoomCreated:  cfg.OnRoomCreated,
		onRoomDestroy:  cfg.OnRoomDestroy,
		inputApplier:   cfg.InputApplier,
		defaultRoomCfg: cfg.DefaultRoomConfig,
	}
}

// NewRoom 创建一个房间。已存在则返回 ErrRoomExists。
func (s *Service) NewRoom(roomID string, opts ...Option) (*Room, error) {
	if roomID == "" {
		return nil, ErrRoomNotFound
	}
	s.mu.Lock()
	r, err := s.newRoomLocked(roomID, opts...)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.fireRoomCreated(roomID)
	return r, nil
}

// fireRoomCreated 在**不持 Service 锁**的状态下调用房间创建回调。
// 该回调在 Module 里是 Owner.RegisterRoom → CallMaster 同步网络调用：
// 持锁调用会让建房期间阻塞所有房间操作，回调若再进 Service 即死锁。
func (s *Service) fireRoomCreated(roomID string) {
	if s.onRoomCreated != nil {
		s.onRoomCreated(roomID)
	}
}

// newRoomLocked 在写锁保护下创建房间，供 NewRoom 和 EnsureRoom 复用。
func (s *Service) newRoomLocked(roomID string, opts ...Option) (*Room, error) {
	if _, ok := s.rooms[roomID]; ok {
		return nil, ErrRoomExists
	}
	cfg := DefaultConfig()
	if s.defaultRoomCfg != nil {
		// 服务级默认房间配置：逐项覆盖 DefaultConfig()（不是整体替换），房间级 Option 再在其之上覆盖。
		applyRoomConfigOverrides(&cfg, *s.defaultRoomCfg)
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	normalizeConfig(&cfg)

	r := &Room{
		svc:          s,
		id:           roomID,
		cfg:          cfg,
		inputApplier: s.inputApplier,
		players:      make(map[string]*PlayerState),
		presence:     make(map[string]*PresenceState),
		pending:      make(map[int64]map[string]Input),
		stop:         &roomStop{ch: make(chan struct{})},
	}
	if cfg.HistoryLimit > 0 {
		r.history = make([]FrameDelta, 0, cfg.HistoryLimit)
	}
	if cfg.SnapshotLimit > 0 {
		r.snapshots = make([]Snapshot, 0, cfg.SnapshotLimit)
	}

	s.rooms[roomID] = r
	atomic.AddInt64(&s.metrics.RoomCount, 1)
	// 创建回调由调用方在**释放 s.mu 之后**触发（见 fireRoomCreated）。
	return r, nil
}

// EnsureRoom 确保房间存在；不存在时创建，存在则返回已有房间。
// 使用写锁保证 check-and-create 原子性，避免并发 NewRoom 竞态。
func (s *Service) EnsureRoom(roomID string, opts ...Option) (*Room, error) {
	s.mu.Lock()
	if r, ok := s.rooms[roomID]; ok {
		s.mu.Unlock()
		return r, nil
	}
	room, err := s.newRoomLocked(roomID, opts...)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.fireRoomCreated(roomID)
	return room, nil
}

// Get 返回房间句柄。
func (s *Service) Get(roomID string) (*Room, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rooms[roomID]
	return r, ok
}

// MustGet 语义化地返回房间或错误。
func (s *Service) MustGet(roomID string) (*Room, error) {
	r, ok := s.Get(roomID)
	if !ok {
		return nil, ErrRoomNotFound
	}
	return r, nil
}

// SetInputApplier 设置所有后续创建房间的默认输入应用器。
// 已存在的房间不受影响，仅在 NewRoom/EnsureRoom 时继承。
//
// 必须持写锁：newRoomLocked 在 s.mu 保护下读取该字段，无锁写会构成数据竞争。
func (s *Service) SetInputApplier(fn InputApplier) {
	s.mu.Lock()
	s.inputApplier = fn
	s.mu.Unlock()
}

// Join 让玩家加入房间。
func (s *Service) Join(roomID, playerID string) error {
	s.maybeActivateTakeover(roomID)
	r, err := s.MustGet(roomID)
	if err != nil {
		return err
	}
	return r.Join(playerID)
}

// Leave 让玩家离开房间。
func (s *Service) Leave(roomID, playerID string) error {
	s.maybeActivateTakeover(roomID)
	r, err := s.MustGet(roomID)
	if err != nil {
		return err
	}
	return r.Leave(playerID)
}

// Input 投递一帧输入。
func (s *Service) Input(roomID, playerID string, input Input) error {
	s.maybeActivateTakeover(roomID)
	r, err := s.MustGet(roomID)
	if err != nil {
		return err
	}
	return r.Input(playerID, input)
}

// Destroy 销毁一个房间。
// onRoomDestroy 回调由 destroyWithReason 统一触发（所有销毁路径共用一处）。
func (s *Service) Destroy(roomID string) error {
	r, err := s.MustGet(roomID)
	if err != nil {
		return err
	}
	r.destroyWithReason("destroyed")
	s.removeRoom(roomID)
	return nil
}

// Snapshot 返回该房间最近一份快照；若无快照则返回当前状态快照。
func (s *Service) Snapshot(roomID string) (Snapshot, error) {
	r, err := s.MustGet(roomID)
	if err != nil {
		return Snapshot{}, err
	}
	return r.Snapshot(), nil
}

// ExportState 导出房间完整运行态，供 owner 接管或节点迁移恢复使用。
func (s *Service) ExportState(roomID string) (RoomState, error) {
	r, err := s.MustGet(roomID)
	if err != nil {
		return RoomState{}, err
	}
	return r.ExportState(), nil
}

// ImportState 导入完整房间状态。房间不存在则创建，存在则覆盖其运行态。
func (s *Service) ImportState(state RoomState) (*Room, error) {
	if state.RoomID == "" {
		return nil, ErrRoomNotFound
	}
	if room, ok := s.Get(state.RoomID); ok {
		if err := room.ImportState(state); err != nil {
			return nil, err
		}
		return room, nil
	}
	cfg := state.Config
	normalizeConfig(&cfg)
	r := &Room{
		svc:      s,
		id:       state.RoomID,
		cfg:      cfg,
		players:  make(map[string]*PlayerState),
		presence: make(map[string]*PresenceState),
		pending:  make(map[int64]map[string]Input),
		stop:     &roomStop{ch: make(chan struct{})},
	}
	s.mu.Lock()
	if existing, ok := s.rooms[state.RoomID]; ok {
		s.mu.Unlock()
		if err := existing.ImportState(state); err != nil {
			return nil, err
		}
		return existing, nil
	}
	s.rooms[state.RoomID] = r
	// 指标与创建回调必须与 NewRoom 路径一致：漏了递增时该房间不计入 RoomCount，
	// 且下方失败路径的 removeRoom 会递减一个从未递增过的计数（RoomCount 变负）。
	atomic.AddInt64(&s.metrics.RoomCount, 1)
	s.mu.Unlock()
	if err := r.ImportState(state); err != nil {
		s.removeRoom(state.RoomID)
		return nil, err
	}
	s.fireRoomCreated(state.RoomID)
	return r, nil
}

// BuildTakeoverRecovery 根据导出的房间运行态构造统一恢复包。
// takeover 场景下，新 owner 可直接把该恢复包下发给客户端做快照恢复与追帧。
func BuildTakeoverRecovery(state RoomState) RecoveryPack {
	var snap Snapshot
	if n := len(state.Snapshots); n > 0 {
		snap = cloneSnapshot(state.Snapshots[n-1])
	} else {
		snap = Snapshot{
			RoomID:    state.RoomID,
			Frame:     state.Frame,
			Hash:      state.LastHash,
			Players:   clonePlayers(state.Players),
			CreatedAt: time.Now(),
		}
	}
	deltas := make([]FrameDelta, 0, len(state.History))
	for _, delta := range state.History {
		if delta.Frame <= snap.Frame {
			continue
		}
		deltas = append(deltas, cloneDelta(delta))
	}
	return RecoveryPack{
		RoomID:         state.RoomID,
		Snapshot:       snap,
		Deltas:         deltas,
		RecoverFrom:    snap.Frame + 1,
		RecoveredUntil: state.Frame,
		FrameHash:      state.LastHash,
	}
}

// Recovery 返回完整恢复包。
func (s *Service) Recovery(roomID, playerID string) (RecoveryPack, error) {
	s.maybeActivateTakeover(roomID)
	r, err := s.MustGet(roomID)
	if err != nil {
		return RecoveryPack{}, err
	}
	return r.Recovery(playerID)
}

// Reconnect 标记玩家重连，并返回恢复包。
func (s *Service) Reconnect(roomID, playerID string) (RecoveryPack, error) {
	s.maybeActivateTakeover(roomID)
	r, err := s.MustGet(roomID)
	if err != nil {
		return RecoveryPack{}, err
	}
	return r.Reconnect(playerID)
}

// Disconnect 仅标记玩家断线，不立即离房。
func (s *Service) Disconnect(roomID, playerID string) error {
	s.maybeActivateTakeover(roomID)
	r, err := s.MustGet(roomID)
	if err != nil {
		return err
	}
	return r.MarkDisconnected(playerID)
}

// Info 返回房间摘要。
func (s *Service) Info(roomID string) (RoomInfo, error) {
	r, err := s.MustGet(roomID)
	if err != nil {
		return RoomInfo{}, err
	}
	return r.Info(), nil
}

// ListRooms 返回全部房间 ID。
func (s *Service) ListRooms() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.rooms))
	for id := range s.rooms {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Close 停止并清空所有房间。
//
// 房间被直接移出 rooms map（不经 removeRoom），因此必须在这里统一补上：
//   - RoomCount 递减（否则服务关闭后计数虚高）；
//   - onRoomDestroy（由 destroyWithReason 触发，注销 master 侧 owner）。
func (s *Service) Close() {
	s.mu.Lock()
	rooms := make([]*Room, 0, len(s.rooms))
	for _, r := range s.rooms {
		rooms = append(rooms, r)
	}
	s.rooms = make(map[string]*Room)
	s.mu.Unlock()
	if len(rooms) > 0 {
		atomic.AddInt64(&s.metrics.RoomCount, -int64(len(rooms)))
	}
	for _, r := range rooms {
		r.destroyWithReason("service_closed")
	}
}

// maybeActivateTakeover 在房间操作前触发接管钩子（引擎内部调用）。
// 钩子幂等：仅当本节点是 takeover 目标节点时才执行 claim + import。
func (s *Service) maybeActivateTakeover(roomID string) {
	if s.takeoverHook != nil {
		s.takeoverHook(roomID)
	}
}

// removeRoom 从 rooms map 中移除指定房间。
// 仅当房间确实存在于 map 中时才递减计数器，避免 Close 与 Leave 并发时重复递减。
func (s *Service) removeRoom(roomID string) {
	s.mu.Lock()
	_, existed := s.rooms[roomID]
	delete(s.rooms, roomID)
	s.mu.Unlock()
	if existed {
		atomic.AddInt64(&s.metrics.RoomCount, -1)
	}
}

// Metrics 返回当前运行时指标的原子快照。
func (s *Service) Metrics() Metrics {
	return s.metrics.Snapshot()
}

// applyRoomConfigOverrides 把 src 的「非零值」字段逐项覆盖到 dst 上。
//
// 语义说明：服务级默认房间配置是「逐项覆盖 DefaultConfig()」，零值表示该项不覆盖。
// 因此无法用它把 AutoStart / AutoDestroyEmpty 关成 false ——
// 要显式关闭请在该房间的创建 Option 里用 frame.WithAutoStart(false) / frame.WithAutoDestroyEmpty(false)。
// 之所以不能整体替换：Config 有 11 个字段，业务只想改帧率时不该被迫填满其余字段，
// 否则 AutoStart 变 false 会让房间永远不推进（静默失效）。
func applyRoomConfigOverrides(dst *Config, src Config) {
	if src.TargetFPS > 0 {
		dst.TargetFPS = src.TargetFPS
	}
	if src.SnapshotEvery > 0 {
		dst.SnapshotEvery = src.SnapshotEvery
	}
	if src.SnapshotLimit > 0 {
		dst.SnapshotLimit = src.SnapshotLimit
	}
	if src.HistoryLimit > 0 {
		dst.HistoryLimit = src.HistoryLimit
	}
	if src.RecoveryMaxFrames > 0 {
		dst.RecoveryMaxFrames = src.RecoveryMaxFrames
	}
	if src.InputTimeoutTicks > 0 {
		dst.InputTimeoutTicks = src.InputTimeoutTicks
	}
	if src.IdleTimeoutTicks > 0 {
		dst.IdleTimeoutTicks = src.IdleTimeoutTicks
	}
	if src.DisconnectRetentionFrames > 0 {
		dst.DisconnectRetentionFrames = src.DisconnectRetentionFrames
	}
	if src.MaxInputLead > 0 {
		dst.MaxInputLead = src.MaxInputLead
	}
	if src.PushMessageID != 0 {
		dst.PushMessageID = src.PushMessageID
	}
	if src.CloseMessageID != 0 {
		dst.CloseMessageID = src.CloseMessageID
	}
	if src.AutoStart {
		dst.AutoStart = true
	}
	if src.AutoDestroyEmpty {
		dst.AutoDestroyEmpty = true
	}
}

// normalizeConfig 将未配置的字段补为默认值。创建房间前调用。
func normalizeConfig(cfg *Config) {
	if cfg.TargetFPS <= 0 {
		cfg.TargetFPS = 30
	}
	if cfg.SnapshotEvery <= 0 {
		cfg.SnapshotEvery = 15
	}
	if cfg.SnapshotLimit <= 0 {
		cfg.SnapshotLimit = 8
	}
	// 硬上限：防止快照无限堆积导致内存泄漏。
	if cfg.SnapshotLimit > 64 {
		cfg.SnapshotLimit = 64
	}
	if cfg.HistoryLimit <= 0 {
		cfg.HistoryLimit = 120
	}
	if cfg.RecoveryMaxFrames <= 0 {
		cfg.RecoveryMaxFrames = cfg.HistoryLimit
	}
	if cfg.InputTimeoutTicks <= 0 {
		cfg.InputTimeoutTicks = 90
	}
	if cfg.IdleTimeoutTicks <= 0 {
		cfg.IdleTimeoutTicks = 900
	}
	if cfg.DisconnectRetentionFrames <= 0 {
		cfg.DisconnectRetentionFrames = 180
	}
	if cfg.MaxInputLead <= 0 {
		cfg.MaxInputLead = 3
	}
}
