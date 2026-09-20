// #nosec G115 -- 帧序号/时间戳按定长拆分与重组（取低 N 位），语义即截断，不存在溢出语义。

// Package frame 实现锁步帧同步房间。
//
// 核心模型：
//   - Room：单个帧同步房间，维护玩家状态、待处理输入、帧历史和快照
//   - Service：管理多个 Room 实例的容器，提供创建/加入/离开等操作界面
//
// 帧同步采用 wait-for-all lockstep 模型：每帧收集所有玩家的输入后再推进，
// 超时未提交的输入自动填 fallback。支持断线重连（快照 + 增量追帧）和节点接管（ExportState / ImportState）。
package frame

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// roomStop 把「关闭信号通道 + 只关一次」封装为一个整体对象。
//
// ImportState 会在锁内整体替换它；各销毁路径必须先在锁内取快照、再在锁外按快照关闭。
// 若像以前那样分开读写 stopCh / stopOnce 两个字段，替换瞬间可能读到
// 「新通道 + 旧 once」的不一致组合，导致关错通道（新的被误关 / 旧的不关）。
type roomStop struct {
	ch   chan struct{}
	once sync.Once
}

// stopSignal 幂等地关闭一个关闭信号（nil 安全）。调用方应传入锁内取好的快照。
func (r *Room) stopSignal(stop *roomStop) {
	if stop != nil {
		stop.once.Do(func() { close(stop.ch) })
	}
}

// Room 是单个运行中的锁步帧同步房间。
//
// 采用经典的 lockstep 模型：
//   - 每帧收集所有玩家的 Input 才能推进（wait-for-all）
//   - 超时未收到输入的玩家自动填 fallback Input（collectTimedOutLocked）
//   - 断线玩家在一定帧数内保留槽位，超时后移除（pruneDisconnectedLocked）
//   - 支持快照 + 历史增量两种恢复路径（Snapshot / RecoveryPack）
//   - 支持完整运行态导出/导入，用于节点接管与故障恢复（ExportState / ImportState）
type Room struct {
	svc *Service // 所属的房间服务

	mu           sync.RWMutex               // 保护所有房间状态
	id           string                     // 房间唯一 ID
	cfg          Config                     // 房间运行配置（FPS、超时阈值等）
	inputApplier InputApplier               // 输入应用器，由业务层注入（生产环境必须设置）
	players      map[string]*PlayerState    // 当前房间内的玩家状态（位置、HP 等）
	presence     map[string]*PresenceState  // 玩家在线/断线存在性
	pending      map[int64]map[string]Input // frame → playerID → Input，未推进的待处理输入
	history      []FrameDelta               // 最近 N 帧的历史记录，用于追帧恢复
	snapshots    []Snapshot                 // 定期快照，用于断线重连起点
	frame        int64                      // 当前已完成帧号
	lastHash     uint64                     // 上一帧的玩家状态哈希，用于校验一致性
	ticker       *time.Ticker               // 游戏主循环定时器
	running      bool                       // 主循环是否运行中
	destroyed    bool                       // 房间是否已销毁

	lastTimedOut      []string  // 上一帧超时的玩家列表
	lastProgressFrame int64     // 最后一次产生实际输入/推进的帧号
	closeReason       string    // 房间关闭原因
	stop              *roomStop // 关闭信号（通道 + once 整体），ImportState 时在锁内整体替换
	loopGen           int64     // tickLoop 世代号，防止 ImportState 后双 tickLoop

	// inputWaitTicks：玩家 → 连续未提交输入的 step 次数（InputTimeoutTicks 兜底的判定基准）。
	// 用「step 次数」而非帧号差：帧卡住时 step 仍按时触发、计数继续增长，兜底才真正生效
	// （帧号差在卡帧时恒为 0/1，阈值永不满足 ⇒ 帧自锁）。
	inputWaitTicks map[string]int64

	// pushFailCount：本房间广播推送累计失败次数（高频路径日志降噪用：首条 + 之后每 100 条一条）。
	pushFailCount uint64
}

// ID 返回房间 ID。
func (r *Room) ID() string { return r.id }

// Config 返回房间配置副本。
func (r *Room) Config() Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg
}

// Frame 返回当前已完成帧号。
func (r *Room) Frame() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.frame
}

// Join 向房间加入一个玩家。
func (r *Room) Join(playerID string) error {
	if playerID == "" {
		return ErrPlayerNotIn
	}
	r.mu.Lock()
	if r.destroyed {
		r.mu.Unlock()
		return ErrRoomDestroyed
	}
	if _, ok := r.players[playerID]; ok {
		// 玩家已在线则拒绝重复加入。
		if p, exists := r.presence[playerID]; exists && p.Connected {
			atomic.AddInt64(&r.svc.metrics.JoinErrors, 1)
			r.mu.Unlock()
			return ErrPlayerExists
		}
		// 重连流：玩家存在但断线，恢复连接状态。
		if p, exists := r.presence[playerID]; exists {
			p.Connected = true
			p.LastSeenFrame = r.frame
			p.DisconnectAtFrame = 0
		}
		// 与 Reconnect 保持一致：清掉该玩家的残留待处理输入（断线期间积压的过期/未来帧），
		// 否则走 Join 重连会带着脏输入继续推进（两条重连路径语义必须对称）。
		r.dropPlayerPendingLocked(playerID)
		// 重连广播所需的快照全部在锁内取好，推送本身放到锁外：
		// 网络下发不得占用房锁（否则会阻塞 tickLoop 与所有 Join/Leave/Input）。
		memberIDs := r.memberIDsLocked()
		playerCount := len(r.players)
		pushMsgID := r.cfg.PushMessageID
		r.mu.Unlock()
		// 广播重连事件，通知其他成员该玩家已重连。
		r.broadcastReconnect(memberIDs, playerID, playerCount, pushMsgID)
		return nil
	}
	// 人数上限（<=0 表示不限）。放在重连分支**之后**：已在房内的玩家必须始终放行，
	// 否则满员时掉线的人就再也回不来了；这里只拦真正的新玩家。
	if r.cfg.MaxPlayers > 0 && len(r.players) >= r.cfg.MaxPlayers {
		atomic.AddInt64(&r.svc.metrics.JoinErrors, 1)
		r.mu.Unlock()
		return ErrRoomFull
	}
	r.players[playerID] = &PlayerState{PlayerID: playerID, HP: 100}
	r.presence[playerID] = &PresenceState{Connected: true, LastSeenFrame: r.frame}
	atomic.AddInt64(&r.svc.metrics.PlayerCount, 1)
	r.lastProgressFrame = r.frame
	if r.cfg.AutoStart {
		r.ensureStartedLocked()
	}
	r.mu.Unlock()
	return nil
}

// Leave 从房间移除一个玩家。
func (r *Room) Leave(playerID string) error {
	r.mu.Lock()
	if r.destroyed {
		r.mu.Unlock()
		return ErrRoomDestroyed
	}
	if _, ok := r.players[playerID]; !ok {
		r.mu.Unlock()
		return ErrPlayerNotIn
	}
	delete(r.players, playerID)
	delete(r.presence, playerID)
	delete(r.inputWaitTicks, playerID)
	r.dropPlayerPendingLocked(playerID)
	atomic.AddInt64(&r.svc.metrics.PlayerCount, -1)
	empty := len(r.players) == 0
	autoDestroy := r.cfg.AutoDestroyEmpty
	if empty {
		r.stopTickerLocked()
	}
	// 原子标记 destroyed，防止并发 Join 在 unlock ~ destroyWithReason 窗口内重新加入已决定销毁的房间。
	if empty && autoDestroy {
		r.destroyed = true
		r.closeReason = "empty"
	}
	// 关闭信号必须在锁内取快照、锁外消费：ImportState 会在锁内整体替换它，
	// 分开的无锁读可能关错通道。
	stop := r.stop
	r.mu.Unlock()
	if empty && autoDestroy {
		// 房间已空，无需广播关闭通知，但必须保证关闭信号只关一次，
		// 避免与 destroyWithReason 并发 close 同一 channel 导致 panic。
		r.stopSignal(stop)
		r.svc.removeRoom(r.id)
		if r.svc.onRoomDestroy != nil {
			r.svc.onRoomDestroy(r.id)
		}
	}
	return nil
}

// Input 投递一帧输入；Frame<=0 时自动落到下一待推进帧。
func (r *Room) Input(playerID string, input Input) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.destroyed {
		return ErrRoomDestroyed
	}
	if _, ok := r.players[playerID]; !ok {
		return ErrPlayerNotIn
	}
	// 先归一化再校验（Frame<=0 落到下一待推进帧）。
	// 在线态与 LastInputFrame 只在校验通过、输入真正入队后刷新：
	// 被拒输入（过期/重复/超远）不得刷新在线态，也不得污染 prune 的保留计时基准。
	if input.Frame <= 0 {
		input.Frame = r.frame + 1
	}
	// 使用 < 而非 <=，允许客户端重发当前帧的输入（上一帧刚推进时客户端可能重发同帧号）。
	if input.Frame < r.frame {
		atomic.AddInt64(&r.svc.metrics.InputDropped, 1)
		return ErrFrameTooOld
	}
	if input.Frame > r.frame+r.cfg.MaxInputLead {
		atomic.AddInt64(&r.svc.metrics.InputDropped, 1)
		return ErrFrameTooFar
	}
	if _, ok := r.pending[input.Frame]; !ok {
		r.pending[input.Frame] = make(map[string]Input)
	}
	if _, exists := r.pending[input.Frame][playerID]; exists {
		atomic.AddInt64(&r.svc.metrics.InputDropped, 1)
		return ErrInputDuplicated
	}
	r.pending[input.Frame][playerID] = input
	if p, ok := r.presence[playerID]; ok {
		p.Connected = true
		p.LastSeenFrame = r.frame
		p.DisconnectAtFrame = 0
		p.LastInputFrame = input.Frame
	}
	if r.cfg.AutoStart {
		r.ensureStartedLocked()
	}
	return nil
}

// MarkDisconnected 标记玩家断线但暂不移出房间，允许后续重连继续追帧。
func (r *Room) MarkDisconnected(playerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.players[playerID]; !ok {
		return ErrPlayerNotIn
	}
	p, ok := r.presence[playerID]
	if !ok {
		p = &PresenceState{}
		r.presence[playerID] = p
	}
	p.Connected = false
	p.DisconnectAtFrame = r.frame
	return nil
}

// Reconnect 重新标记玩家在线，清除该玩家的过期/未来帧输入，并返回完整恢复包。
func (r *Room) Reconnect(playerID string) (RecoveryPack, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.players[playerID]; !ok {
		return RecoveryPack{}, ErrPlayerNotIn
	}
	p, ok := r.presence[playerID]
	if !ok {
		p = &PresenceState{}
		r.presence[playerID] = p
	}
	p.Connected = true
	p.LastSeenFrame = r.frame
	p.DisconnectAtFrame = 0
	// 清除该玩家所有过期（<=frame）和未来帧的待处理输入，避免重连后残留脏数据。
	r.dropPlayerPendingLocked(playerID)
	return r.buildRecoveryLocked(), nil
}

// Recovery 返回当前完整恢复包。
func (r *Room) Recovery(playerID string) (RecoveryPack, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.players[playerID]; !ok {
		return RecoveryPack{}, ErrPlayerNotIn
	}
	return r.buildRecoveryLocked(), nil
}

// Snapshot 返回房间最近一份快照；若还没有历史快照，则即时构造一份。
func (r *Room) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n := len(r.snapshots); n > 0 {
		return cloneSnapshot(r.snapshots[n-1])
	}
	return r.makeSnapshotLocked()
}

// ExportState 导出完整房间运行态，供 owner 接管与节点迁移恢复使用。
func (r *Room) ExportState() RoomState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.exportStateLocked()
}

// ImportState 使用导出状态覆盖当前房间运行态。
func (r *Room) ImportState(state RoomState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.destroyed {
		return ErrRoomDestroyed
	}
	r.stopTickerLocked()
	// 关闭旧信号并整体替换（通道与 once 一起换，杜绝「新通道 + 旧 once」的组合）。
	r.stopSignal(r.stop)
	r.stop = &roomStop{ch: make(chan struct{})}
	r.loopGen++ // 递增世代号，使旧 tickLoop 退出
	r.applyStateLocked(state)
	if state.Running && len(r.players) > 0 {
		r.ensureStartedLocked()
	}
	return nil
}

// Info 返回房间摘要。
func (r *Room) Info() RoomInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	players := make([]string, 0, len(r.players))
	disconnected := make([]string, 0)
	for pid := range r.players {
		players = append(players, pid)
		if p, ok := r.presence[pid]; ok && !p.Connected {
			disconnected = append(disconnected, pid)
		}
	}
	sort.Strings(players)
	sort.Strings(disconnected)
	missing, reason := r.waitStateLocked(r.frame + 1)
	recoverableFrom := int64(0)
	if len(r.history) > 0 {
		recoverableFrom = r.history[0].Frame
	}
	return RoomInfo{
		RoomID:          r.id,
		Frame:           r.frame,
		TargetFPS:       r.cfg.TargetFPS,
		PlayerCount:     len(r.players),
		Players:         players,
		Running:         r.running,
		Waiting:         reason != "",
		WaitingReason:   reason,
		Missing:         missing,
		Disconnected:    disconnected,
		TimedOut:        append([]string(nil), r.lastTimedOut...),
		SnapshotSize:    len(r.snapshots),
		HistorySize:     len(r.history),
		NextFrame:       r.frame + 1,
		FrameHash:       r.lastHash,
		RecoverableFrom: recoverableFrom,
		CloseReason:     r.closeReason,
	}
}

func (r *Room) ensureStartedLocked() {
	if r.running || r.destroyed || len(r.players) == 0 {
		return
	}
	interval := time.Second / time.Duration(r.cfg.TargetFPS)
	if interval <= 0 {
		interval = time.Second / 30
	}
	r.ticker = time.NewTicker(interval)
	r.running = true
	r.loopGen++
	go r.tickLoop(r.loopGen)
}

// tickLoop 是房间的主循环 goroutine，由 ensureStartedLocked 启动。
// 每次 ticker 触发时调用 step() 推进一帧，收到 stopCh 信号时退出。
// gen 参数为启动时的世代号，若与当前 loopGen 不一致则立即退出，防止双 tickLoop。
func (r *Room) tickLoop(gen int64) {
	for {
		r.mu.RLock()
		ticker := r.ticker
		stop := r.stop
		currentGen := r.loopGen
		r.mu.RUnlock()
		if ticker == nil || stop == nil || gen != currentGen {
			return
		}
		select {
		case <-ticker.C:
			r.step()
		case <-stop.ch:
			return
		}
	}
}

// step 是帧同步的核心推进逻辑，每帧执行一次：
//  1. 检查房间是否为空或应销毁（空房间 / 空闲超时）
//  2. 收集所有玩家对下一帧的输入（waitStateLocked）
//  3. 对超时未提交输入的玩家填入 fallback 输入（collectTimedOutLocked）
//  4. 如果仍有玩家缺输入 → 广播 waiting 帧推，本帧不推进
//  5. 如果所有输入到位 → 应用输入更新玩家状态，推进帧号，记录历史/快照，广播帧推
func (r *Room) step() {
	var (
		memberIDs     []string
		pushMsgID     uint32
		push          FramePush
		autoDestroy   bool
		destroyNow    bool
		destroyReason string
	)

	r.mu.Lock()
	if r.destroyed {
		r.mu.Unlock()
		return
	}
	if len(r.players) == 0 {
		r.stopTickerLocked()
		autoDestroy = r.cfg.AutoDestroyEmpty
		if autoDestroy {
			r.destroyed = true
			r.closeReason = "empty"
		}
		stop := r.stop
		r.mu.Unlock()
		if autoDestroy {
			r.stopSignal(stop)
			r.svc.removeRoom(r.id)
			// 与 Leave 的空房路径一致：销毁房间必须通知 onRoomDestroy（注销 master 侧 owner），
			// 否则经 prune 清空玩家后由本分支销毁的房间 owner 永不注销。
			if r.svc.onRoomDestroy != nil {
				r.svc.onRoomDestroy(r.id)
			}
		}
		return
	}

	// 有未消费的待处理输入 = 有玩家在活跃提交：刷新空闲计时。
	// 该副作用由 step 显式完成（只此一处），不再藏在 shouldCloseForIdleLocked 判定函数里，
	// 避免任何查询式调用误重置空闲计时。
	for _, pend := range r.pending {
		if len(pend) > 0 {
			r.lastProgressFrame = r.frame
			break
		}
	}
	if r.shouldCloseForIdleLocked() {
		destroyNow = true
		destroyReason = "idle_timeout"
	}
	if destroyNow {
		r.mu.Unlock()
		r.destroyWithReason(destroyReason)
		r.svc.removeRoom(r.id)
		return
	}

	next := r.frame + 1
	missing, reason := r.waitStateLocked(next)
	timedOut := r.collectTimedOutLocked(next)
	if len(timedOut) > 0 {
		if _, ok := r.pending[next]; !ok {
			r.pending[next] = make(map[string]Input)
		}
		for _, pid := range timedOut {
			if _, exists := r.pending[next][pid]; !exists {
				r.pending[next][pid] = Input{Frame: next}
			}
		}
		missing, reason = r.waitStateLocked(next)
	}
	if reason != "" {
		r.lastTimedOut = append([]string(nil), timedOut...)
		// 有超时玩家提交了 fallback 输入，视为有活动，更新 lastProgressFrame 防止误触发空闲超时。
		if len(timedOut) > 0 {
			r.lastProgressFrame = r.frame
		}
		memberIDs = r.memberIDsLocked()
		pushMsgID = r.cfg.PushMessageID
		push = FramePush{
			RoomID:        r.id,
			Frame:         next,
			FPS:           r.cfg.TargetFPS,
			Waiting:       true,
			WaitingReason: reason,
			Missing:       missing,
			TimedOut:      append([]string(nil), timedOut...),
			Disconnected:  r.disconnectedPlayersLocked(),
			PlayerCount:   len(r.players),
			Players:       clonePlayers(r.players),
		}
		r.mu.Unlock()
		r.broadcast(memberIDs, pushMsgID, push)
		return
	}

	inputs := r.pending[next]
	delete(r.pending, next)
	r.frame = next
	atomic.AddInt64(&r.svc.metrics.TotalFrames, 1)
	r.lastProgressFrame = r.frame

	// ── 阶段一结束：把业务回调需要的一切取成快照，然后**放锁** ──
	//
	// 本帧的推进（帧号 + 待处理输入出队）已经生效；「把输入应用到游戏状态」这一步
	// 交给业务实现，是**任意时长的用户代码**（含读库 / 计算 / 甚至是慢日志），
	// 继续持房写锁会阻塞该房 tickLoop 与所有 Join/Leave/Input/Info。
	applier := r.inputApplier
	roomID := r.id
	gen := r.loopGen
	frameNo := next
	r.mu.Unlock()

	// ── 阶段二：回调在**锁外**执行 ──
	//
	// ⚠️ 契约（与旧版相反，旧版要求实现方"禁止回调本房间任何方法"）：
	//   - 本回调**不再持房锁** ⇒ 可以安全调用 Join / Leave / Input / Info / Push 等方法；
	//   - 但因此**不再与那些方法串行**：期间可能有玩家进出房间、也可能发生
	//     ImportState（节点接管）/ 房间销毁。实现方若需要一遍一致的房间视图，
	//     应在自己的业务状态里取，而不是"假定期间没人动房间"。
	//   - 返回的 `*PlayerState` 在返回之后**不得再被业务改写**（引擎会把它直接放进
	//     房间玩家表，其他 goroutine 会并发读它）——需要继续复用请返回副本。
	var newStates map[string]*PlayerState
	if applier != nil {
		newStates = applier(roomID, frameNo, inputs)
	}

	// ── 阶段三：回锁提交 ──
	r.mu.Lock()
	if r.destroyed || r.loopGen != gen || r.frame != frameNo {
		// 回调期间房间被销毁 / 被 ImportState 整体重置（接管）：本次结果已经不再属于
		// 当前这份房间状态，强行写入会把新状态覆盖成旧的。丢弃并留痕。
		r.mu.Unlock()
		logger.Warnf("frame: room=%s frame=%d 输入应用结果已失效，丢弃（destroyed=%t loopGen=%d→%d frame=%d→%d）",
			roomID, frameNo, r.destroyed, gen, r.loopGen, frameNo, r.frame)
		return
	}
	if newStates != nil {
		// **按玩家逐个合并**，不做整体替换（旧实现是 r.players = newStates）：
		//   - 回调期间离场的玩家已不在 r.players，逐个合并天然不会把 TA "复活"；
		//   - 回调期间新进房的玩家不在 newStates 里，保留其刚加入时的状态
		//     （他本来就没参与本帧的 wait-for-all，本帧没有他的输入）；
		//   - 因此 metrics.PlayerCount 与 presence 始终和 players 一致，
		//     不需要在这里做增删补偿。
		for pid := range r.players {
			ns, ok := newStates[pid]
			if !ok || ns == nil {
				continue
			}
			r.players[pid] = ns
		}
		// 业务返回了房间里没有的玩家：不新增成员（房间成员只能经 Join 进入），留痕。
		extra := 0
		for pid := range newStates {
			if _, ok := r.players[pid]; !ok {
				extra++
			}
		}
		if extra > 0 {
			logger.Warnf("frame: room=%s frame=%d 输入应用器返回了 %d 个不在房间内的玩家，已忽略（成员只能经 Join 进入）",
				roomID, frameNo, extra)
		}
	}
	for pid := range r.players {
		if p, ok := r.presence[pid]; ok {
			p.LastSeenFrame = next
		}
		// 回填 PlayerState.LastFrame：供追帧恢复/快照比对使用；
		// 输入超时兜底已改用 inputWaitTicks（step 次数），不再依赖该字段。
		if ps, ok := r.players[pid]; ok {
			ps.LastFrame = next
		}
	}
	r.lastTimedOut = append([]string(nil), timedOut...)
	r.lastHash = hashPlayers(r.players)
	r.appendHistoryLocked(FrameDelta{Frame: next, Inputs: cloneInputs(inputs), State: clonePlayers(r.players), Hash: r.lastHash})
	r.pruneDisconnectedLocked()

	snapshotFrame := int64(0)
	if r.cfg.SnapshotEvery > 0 && r.frame%int64(r.cfg.SnapshotEvery) == 0 {
		snap := r.makeSnapshotLocked()
		r.snapshots = append(r.snapshots, snap)
		if max := r.cfg.SnapshotLimit; max > 0 && len(r.snapshots) > max {
			r.snapshots = append([]Snapshot(nil), r.snapshots[len(r.snapshots)-max:]...)
		}
		snapshotFrame = snap.Frame
	}

	memberIDs = r.memberIDsLocked()
	pushMsgID = r.cfg.PushMessageID
	push = FramePush{
		RoomID:         r.id,
		Frame:          r.frame,
		FPS:            r.cfg.TargetFPS,
		TimedOut:       append([]string(nil), timedOut...),
		Disconnected:   r.disconnectedPlayersLocked(),
		PlayerCount:    len(r.players),
		SnapshotFrame:  snapshotFrame,
		FrameHash:      r.lastHash,
		RecoveredUntil: r.frame,
		Players:        clonePlayers(r.players),
	}
	r.mu.Unlock()

	r.broadcast(memberIDs, pushMsgID, push)
}

// logPushFailure 对帧推失败做降频日志：首条必打，之后每 100 条打一条。
//
// 帧推是「每帧 × 每成员」的高频路径，下游不可用时逐条打印会造成日志风暴；
// 完全不打印则会让推送失败彻底不可观测（表现为"帧在推进但客户端收不到"）。
func (r *Room) logPushFailure(kind, playerID string, msgID uint32, err error) {
	n := atomic.AddUint64(&r.pushFailCount, 1)
	if n == 1 || n%100 == 0 {
		logger.Warnf("frame: %s push to %s failed (msgID=%d, total=%d): %v", kind, playerID, msgID, n, err)
	}
}

// broadcast 通过 Service 的 Push 函数向所有成员广播帧推消息（BestEffort）。
//
// ⚠️ pushMsgID == 0 时**直接静默返回**：不报错、不打日志 ⇒ 表现是"帧在正常推进，
// 客户端却永远收不到帧推"，看起来像"Broadcast 失败"，实际是**没配推帧消息号**。
// 默认配置（DefaultConfig）的 PushMessageID 就是 0，所以建房间时必须显式
// frame.WithPushMessageID(<业务帧推消息号>)；等待态与重连/接管路径同样依赖它
// （见 broadcastReconnect 的 pushMsgID 判定）。回归见 room_test.go。
//
// push 取 any：帧推（FramePush）与房间关闭通知（RoomClosedPush）走同一条广播路径，
// broadcastReliable 的「可靠通道不可用时降级到普通 broadcast」才对两者都成立。
func (r *Room) broadcast(memberIDs []string, pushMsgID uint32, push any) {
	if pushMsgID == 0 || r.svc.push == nil || len(memberIDs) == 0 {
		return
	}
	for _, pid := range memberIDs {
		if err := r.svc.push(pid, pushMsgID, push); err != nil {
			r.logPushFailure("broadcast", pid, pushMsgID, err)
		}
	}
}

// broadcastReliable 通过 Service 的 PushReliable 函数向所有成员广播帧推消息（Reliable）。
// 用于结算、开局等关键生命周期事件。
func (r *Room) broadcastReliable(memberIDs []string, pushMsgID uint32, push any) {
	if pushMsgID == 0 || r.svc.pushReliable == nil || len(memberIDs) == 0 {
		// 降级到普通 broadcast（对任意载体都生效，含 RoomClosedPush）。
		r.broadcast(memberIDs, pushMsgID, push)
		return
	}
	for _, pid := range memberIDs {
		if err := r.svc.pushReliable(pid, pushMsgID, push, proto.DeliveryModeReliable); err != nil {
			r.logPushFailure("broadcastReliable", pid, pushMsgID, err)
		}
	}
}

// broadcastReconnect 向所有成员广播玩家重连事件。
//
// 必须在**锁外**调用：memberIDs / playerCount / pushMsgID 由调用方在锁内取好快照后传入，
// 网络下发不得占用房锁（否则阻塞 tickLoop 与所有 Join/Leave/Input）。
func (r *Room) broadcastReconnect(memberIDs []string, playerID string, playerCount int, pushMsgID uint32) {
	if pushMsgID == 0 || r.svc.push == nil || len(memberIDs) == 0 {
		return
	}
	type reconnectPush struct {
		RoomID      string `json:"room_id"`
		PlayerID    string `json:"player_id"`
		PlayerCount int    `json:"player_count"`
	}
	push := reconnectPush{
		RoomID:      r.id,
		PlayerID:    playerID,
		PlayerCount: playerCount,
	}
	for _, pid := range memberIDs {
		if err := r.svc.push(pid, pushMsgID, push); err != nil {
			r.logPushFailure("reconnect", pid, pushMsgID, err)
		}
	}
}

// waitStateLocked 检查房间是否处于等待状态，返回缺失输入的玩家列表及等待原因。
// 等待原因："waiting_input" = 部分玩家未提交输入。
// 所有输入到位（返回空字符串）表示可以推进。
func (r *Room) waitStateLocked(frame int64) ([]string, string) {
	if len(r.players) == 0 {
		return nil, ""
	}
	inputs := r.pending[frame]
	missing := make([]string, 0)
	for pid := range r.players {
		if _, ok := inputs[pid]; !ok {
			missing = append(missing, pid)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return missing, "waiting_input"
	}
	return nil, ""
}

// disconnectedPlayersLocked 返回当前已标记为断线的玩家 ID 列表（已排序）。
func (r *Room) disconnectedPlayersLocked() []string {
	ids := make([]string, 0)
	for pid, p := range r.presence {
		if !p.Connected {
			ids = append(ids, pid)
		}
	}
	sort.Strings(ids)
	return ids
}

// collectTimedOutLocked 检查当前帧的待提交输入，返回连续 InputTimeoutTicks 次 step 都没提交新输入的超时玩家列表。
//
// 判据用「连续未提交的 step 次数」（inputWaitTicks）而非帧号差 `frame - ps.LastFrame`：
// 帧号差在**卡帧**时恒为 0/1（wait-for-all 模型下没人提交就没人推进），阈值永不满足 ⇒
// 兜底永不触发、帧永久自锁。step 次数由 ticker 驱动，卡帧时仍按时增长，兜底才真正生效。
// 超时玩家在本次 step 广播中会标记为 timed_out，用于 UI 提示，但不会强制离开。
func (r *Room) collectTimedOutLocked(frame int64) []string {
	if r.cfg.InputTimeoutTicks <= 0 {
		return nil
	}
	if r.inputWaitTicks == nil {
		// ImportState / 零值 Room 可能未初始化；懒建避免构造点遗漏。
		r.inputWaitTicks = make(map[string]int64)
	}
	inputs := r.pending[frame]
	timedOut := make([]string, 0)
	threshold := int64(r.cfg.InputTimeoutTicks)
	for pid := range r.players {
		if _, ok := inputs[pid]; ok {
			// 本帧已提交：清零连续未提交计数。
			delete(r.inputWaitTicks, pid)
			continue
		}
		if r.players[pid] == nil {
			continue
		}
		n := r.inputWaitTicks[pid] + 1
		r.inputWaitTicks[pid] = n
		if n >= threshold {
			timedOut = append(timedOut, pid)
		}
	}
	sort.Strings(timedOut)
	return timedOut
}

// appendHistoryLocked 将一帧执行记录加入历史，超出 HistoryLimit 时裁剪。
// 历史帧是追帧恢复（RecoveryPack）的数据来源。
func (r *Room) appendHistoryLocked(rec FrameDelta) {
	r.history = append(r.history, rec)
	if max := r.cfg.HistoryLimit; max > 0 && len(r.history) > max {
		r.history = append([]FrameDelta(nil), r.history[len(r.history)-max:]...)
	}
}

// pruneDisconnectedLocked 清理超出 DisconnectRetentionFrames 阈值的断线玩家。
// 超时后从 rooms.players 和 rooms.presence 中移除，同时清空其待处理输入。
func (r *Room) pruneDisconnectedLocked() {
	limit := r.cfg.DisconnectRetentionFrames
	if limit <= 0 {
		return
	}
	for pid, p := range r.presence {
		if p == nil || p.Connected || p.DisconnectAtFrame == 0 {
			continue
		}
		// 使用 max(DisconnectAtFrame, lastInputFrame) 作为基准帧，
		// 防止 disconnect 与 prune 间同帧 Input 重置 Retention 计时导致过早踢出。
		baseFrame := p.DisconnectAtFrame
		if p.LastInputFrame > baseFrame {
			baseFrame = p.LastInputFrame
		}
		if r.frame-baseFrame >= limit {
			// 该玩家已彻底移出房间：players / presence / 兜底计数 / 待处理输入 / 指标
			// 必须一起清理（Leave 走的就是同一套），否则 PlayerCount 虚高且不回正。
			delete(r.players, pid)
			delete(r.presence, pid)
			delete(r.inputWaitTicks, pid)
			r.dropPlayerPendingLocked(pid)
			atomic.AddInt64(&r.svc.metrics.PlayerCount, -1)
		}
	}
}

// memberIDsLocked 返回房间内所有玩家 ID（已排序），用于广播目标列表。
func (r *Room) memberIDsLocked() []string {
	ids := make([]string, 0, len(r.players))
	for pid := range r.players {
		ids = append(ids, pid)
	}
	sort.Strings(ids)
	return ids
}

// dropPlayerPendingLocked 从所有待处理帧中移除指定玩家的输入，玩家离开后调用。
func (r *Room) dropPlayerPendingLocked(playerID string) {
	for frame := range r.pending {
		delete(r.pending[frame], playerID)
		if len(r.pending[frame]) == 0 {
			delete(r.pending, frame)
		}
	}
}

// stopTickerLocked 停止主循环 ticker 并标记房间为非运行状态。
func (r *Room) stopTickerLocked() {
	if r.ticker != nil {
		r.ticker.Stop()
		r.ticker = nil
	}
	r.running = false
}

// destroyWithReason 以指定原因销毁房间：标记 destroyed、停止 ticker、发出关闭信号、
// 向所有成员广播房间关闭通知，并清理资源。
func (r *Room) destroyWithReason(reason string) {
	var closePush RoomClosedPush
	var memberIDs []string
	var closeMsgID uint32

	r.mu.Lock()
	if r.destroyed {
		r.mu.Unlock()
		return
	}
	r.destroyed = true
	r.closeReason = reason
	memberIDs = r.memberIDsLocked()
	closeMsgID = r.cfg.CloseMessageID
	closePush = RoomClosedPush{RoomID: r.id, Frame: r.frame, Reason: reason}
	r.stopTickerLocked()
	// 关闭信号必须在锁内取快照、锁外消费：ImportState 会在锁内整体替换它，
	// 无锁直读可能关到已被替换掉的新通道（旧的不关 → tickLoop 泄漏）。
	stop := r.stop
	r.mu.Unlock()
	r.stopSignal(stop)
	// 房间关闭通知必须可靠送达
	r.broadcastReliable(memberIDs, closeMsgID, closePush)
	// 注销 master 侧 owner：所有销毁路径（idle_timeout / step 空房自毁 / Service.Destroy /
	// Service.Close）都必须经过本函数，否则房间已销毁而 master 仍把请求路由到本节点。
	if r.svc.onRoomDestroy != nil {
		r.svc.onRoomDestroy(r.id)
	}
}

// buildRecoveryLocked 构建追帧恢复包：选定最近一次快照，附加其后所有历史增量帧，
// 拼接为完整的 RecoveryPack 供断线重连玩家从 snap.Frame+1 开始追帧。
func (r *Room) buildRecoveryLocked() RecoveryPack {
	var snap Snapshot
	if n := len(r.snapshots); n > 0 {
		snap = cloneSnapshot(r.snapshots[n-1])
	} else {
		snap = r.makeSnapshotLocked()
	}
	maxFrames := r.cfg.RecoveryMaxFrames
	if maxFrames <= 0 || maxFrames > len(r.history) {
		maxFrames = len(r.history)
	}
	start := len(r.history) - maxFrames
	if start < 0 {
		start = 0
	}
	deltas := make([]FrameDelta, 0, len(r.history)-start)
	for i := start; i < len(r.history); i++ {
		rec := r.history[i]
		if rec.Frame <= snap.Frame {
			continue
		}
		deltas = append(deltas, FrameDelta{
			Frame:  rec.Frame,
			Inputs: cloneInputs(rec.Inputs),
			State:  clonePlayers(rec.State),
			Hash:   rec.Hash,
		})
	}
	return RecoveryPack{
		RoomID:         r.id,
		Snapshot:       snap,
		Deltas:         deltas,
		RecoverFrom:    snap.Frame + 1,
		RecoveredUntil: r.frame,
		FrameHash:      r.lastHash,
	}
}

// shouldCloseForIdleLocked 判断是否因房间空闲超时而应关闭。
// 返回 true 表示已超过 IdleTimeoutTicks 帧没有任何玩家提交新输入。
//
// 本函数是**纯判定**：lastProgressFrame 的刷新统一由 step 显式完成（见其注释），
// 不得在此处写状态 —— 否则任何查询式调用都会误重置空闲计时。
func (r *Room) shouldCloseForIdleLocked() bool {
	if r.cfg.IdleTimeoutTicks <= 0 {
		return false
	}
	for _, pend := range r.pending {
		if len(pend) > 0 {
			return false
		}
	}
	return r.frame-r.lastProgressFrame >= int64(r.cfg.IdleTimeoutTicks)
}

// makeSnapshotLocked 生成当前帧完整房间快照，包含所有玩家状态及帧号。
func (r *Room) makeSnapshotLocked() Snapshot {
	return Snapshot{
		RoomID:    r.id,
		Frame:     r.frame,
		Hash:      r.lastHash,
		Players:   clonePlayers(r.players),
		CreatedAt: r.svc.now(),
	}
}

// exportStateLocked 导出房间完整运行态，用于节点接管（takeover）时的状态迁移。
// 包含配置、所有玩家状态、presence、待处理输入、历史帧和快照。
func (r *Room) exportStateLocked() RoomState {
	history := make([]FrameDelta, 0, len(r.history))
	for _, rec := range r.history {
		history = append(history, FrameDelta{
			Frame:  rec.Frame,
			Inputs: cloneInputs(rec.Inputs),
			State:  clonePlayers(rec.State),
			Hash:   rec.Hash,
		})
	}
	snapshots := make([]Snapshot, 0, len(r.snapshots))
	for _, snap := range r.snapshots {
		snapshots = append(snapshots, cloneSnapshot(snap))
	}
	presence := make(map[string]PresenceState, len(r.presence))
	for pid, p := range r.presence {
		if p == nil {
			continue
		}
		presence[pid] = PresenceState{
			Connected:         p.Connected,
			LastSeenFrame:     p.LastSeenFrame,
			DisconnectAtFrame: p.DisconnectAtFrame,
			LastInputFrame:    p.LastInputFrame,
		}
	}
	return RoomState{
		RoomID:            r.id,
		Config:            r.cfg,
		Frame:             r.frame,
		LastHash:          r.lastHash,
		Players:           clonePlayers(r.players),
		Presence:          presence,
		Pending:           clonePendingInputs(r.pending),
		History:           history,
		Snapshots:         snapshots,
		LastTimedOut:      append([]string(nil), r.lastTimedOut...),
		LastProgressFrame: r.lastProgressFrame,
		CloseReason:       r.closeReason,
		Running:           r.running,
	}
}

// applyStateLocked 从导出状态恢复房间：覆盖 Room 中的所有字段，包括配置、玩家、presence、
// 待处理输入、历史帧和快照。恢复后房间处于非运行状态，由调用方决定是否重启 ticker。
func (r *Room) applyStateLocked(state RoomState) {
	cfg := state.Config
	normalizeConfig(&cfg)
	oldPlayerCount := int64(len(r.players))
	r.id = state.RoomID
	r.cfg = cfg
	r.frame = state.Frame
	r.lastHash = state.LastHash
	r.players = clonePlayers(state.Players)
	if r.players == nil {
		r.players = make(map[string]*PlayerState)
	}
	// 指标修正：整体替换玩家表必须把 PlayerCount 按差值调整，
	// 否则接管迁移后计数漂移（旧玩家不递减、新玩家不递增）。
	if delta := int64(len(r.players)) - oldPlayerCount; delta != 0 {
		atomic.AddInt64(&r.svc.metrics.PlayerCount, delta)
	}
	r.presence = make(map[string]*PresenceState, len(state.Presence))
	for pid, p := range state.Presence {
		// 丢弃不在 players 中的孤立 presence：否则 disconnectedPlayersLocked 会把
		// 已不存在的玩家列进断线列表（导出态可能来自更早的时刻）。
		if _, ok := r.players[pid]; !ok {
			continue
		}
		cp := p
		r.presence[pid] = &PresenceState{
			Connected:         cp.Connected,
			LastSeenFrame:     cp.LastSeenFrame,
			DisconnectAtFrame: cp.DisconnectAtFrame,
			LastInputFrame:    cp.LastInputFrame,
		}
	}
	for pid := range r.players {
		if _, ok := r.presence[pid]; !ok {
			r.presence[pid] = &PresenceState{Connected: true, LastSeenFrame: r.frame}
		}
	}
	r.pending = clonePendingInputs(state.Pending)
	if r.pending == nil {
		r.pending = make(map[int64]map[string]Input)
	}
	r.history = make([]FrameDelta, 0, len(state.History))
	for _, rec := range state.History {
		r.history = append(r.history, FrameDelta{
			Frame:  rec.Frame,
			Inputs: cloneInputs(rec.Inputs),
			State:  clonePlayers(rec.State),
			Hash:   rec.Hash,
		})
	}
	r.snapshots = make([]Snapshot, 0, len(state.Snapshots))
	for _, snap := range state.Snapshots {
		r.snapshots = append(r.snapshots, cloneSnapshot(snap))
	}
	r.lastTimedOut = append([]string(nil), state.LastTimedOut...)
	r.lastProgressFrame = state.LastProgressFrame
	if r.lastProgressFrame == 0 && r.frame > 0 {
		r.lastProgressFrame = r.frame
	}
	r.closeReason = state.CloseReason
	r.destroyed = false
	r.running = false
	r.ticker = nil
}

func clonePlayers(src map[string]*PlayerState) map[string]*PlayerState {
	out := make(map[string]*PlayerState, len(src))
	for k, v := range src {
		// inputApplier 由业务注入，返回值可能含 nil 值（本包对 nil player 已有防御），
		// 这里必须判空后再解引用，否则一步 clone 就 panic。
		if v == nil {
			continue
		}
		cp := *v
		out[k] = &cp
	}
	return out
}

func cloneSnapshot(src Snapshot) Snapshot {
	return Snapshot{
		RoomID:    src.RoomID,
		Frame:     src.Frame,
		Hash:      src.Hash,
		Players:   clonePlayers(src.Players),
		CreatedAt: src.CreatedAt,
	}
}

func cloneInputs(src map[string]Input) map[string]Input {
	out := make(map[string]Input, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func clonePendingInputs(src map[int64]map[string]Input) map[int64]map[string]Input {
	out := make(map[int64]map[string]Input, len(src))
	for frame, inputs := range src {
		out[frame] = cloneInputs(inputs)
	}
	return out
}

func cloneDelta(src FrameDelta) FrameDelta {
	return FrameDelta{
		Frame:  src.Frame,
		Inputs: cloneInputs(src.Inputs),
		State:  clonePlayers(src.State),
		Hash:   src.Hash,
	}
}

// hashPlayers 计算房间内所有玩家状态的确定性哈希值（id 排序后逐一混合）。
// 用于客户端校验逻辑一致性：客户端计算本地 hash 与服务端广播的 FrameHash 对比。
func hashPlayers(src map[string]*PlayerState) uint64 {
	ids := make([]string, 0, len(src))
	for id := range src {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var h uint64 = 1469598103934665603
	const prime uint64 = 1099511628211
	mix := func(v uint64) {
		h ^= v
		h *= prime
	}
	for _, id := range ids {
		ps := src[id]
		if ps == nil {
			// 同 clonePlayers：nil player 直接跳过（不 panic），哈希仍确定性。
			continue
		}
		for i := 0; i < len(id); i++ {
			mix(uint64(id[i]))
		}
		mix(uint64(ps.PosX + 1_000_000))
		mix(uint64(ps.PosY + 1_000_000))
		mix(uint64(ps.HP + 1_000_000))
		mix(uint64(ps.LastFrame))
		for i := 0; i < len(ps.LastAction); i++ {
			mix(uint64(ps.LastAction[i]))
		}
	}
	return h
}
