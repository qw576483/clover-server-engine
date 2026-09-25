package room

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// ===========================================================================
// 状态同步房间参考实现（room.Kernel 的第二款内核）
//
// 引擎内置的帧同步内核解决的问题是「每帧收输入 + 锁步推进」；
// 本文件解决的是另一类房间：「房间**状态本身**怎么同步」——
// 服务端持有唯一权威状态，任何一次变化都把**完整状态**推给房内真人，
// 客户端不做逐帧确定性模拟，只按收到的全量状态重建画面（状态同步）。
//
// 三条语义在本实现里都保留：
//
//	① 房主移交            房主离房时由 reassignHostLocked 指派：**座位号最小的真人**接任；
//	                      机器人座位不接任；房里没有真人 ⇒ 房主清空，
//	                      下一次真人进房时由 Join 顶上（见 Join）。
//	② 座位号 = 队伍号     座位在 RoomSnapshot.Seats 里的**下标就是队伍号**（见 SeatOf）。
//	                      因此对局进行中拒绝一切会移动座位号的操作（Join / Leave），
//	                      掉线只做标记（SetOffline），座位号原地保留，结算不会认错人。
//	③ 观察者版本号兜底补推 全局版本号 + 每个观察者的「已下发版本」；
//	                      Watch 注册即返回完整列表（首帧兜底），之后每次变化即时推送，
//	                      推送失败 / 漏推的观察者由 FlushWatchers 按版本号补推。
//
// 关键约定：
//   - 座位用**固定槽**：切片长度 = 座位数，下标即队伍号，删人不移动其它座位。
//   - 观察者的「已下发版本」只在**推送成功之后**记录。
//   - 玩法 / 展示字段（昵称、头像、阵容、资源、战绩……）不进内核状态：
//     内核只持有影响**同步与结算**的字段（谁坐几号座位、是否房主、是否准备、
//     是否在线、是否机器人）。
// ===========================================================================

// DefaultStateSyncSeats 状态同步房间的默认座位数（= 队伍数）。
const DefaultStateSyncSeats = 2

var (
	errStateSyncNil    = errors.New("room: 状态同步内核未构造")
	errStateSyncClosed = errors.New("room: 状态同步内核已关闭")
)

// StateSyncConfig 是 StateSyncKernel 的构造参数。
type StateSyncConfig struct {
	// Seats 座位数 = 队伍数（座位号即队伍号）。<= 0 时取 DefaultStateSyncSeats。
	Seats int

	// Pusher 向指定玩家推送消息；nil = 不接网络，内核只维护状态
	// （业务自行读取 Snapshot / RoomList 下发）。
	Pusher func(playerID string, msgID uint32, v any) error
	// StateMsgID 房间状态（RoomSnapshot）推送的消息号。
	// Pusher 非 nil 而本值为 0 时跳过状态推送并打日志（不静默）。
	StateMsgID uint32
	// ListMsgID 房间列表（RoomList）推送的消息号；同上。
	ListMsgID uint32
}

// Seat 是一个座位。
//
// **座位号 = 座位在 RoomSnapshot.Seats 里的下标 = 队伍号**（见 SeatOf）。
type Seat struct {
	// PlayerID 占座玩家；空串表示该座位空着。
	PlayerID string `json:"player_id,omitempty"`
	// Ready 准备态（房主与机器人不参与准备态校验）。
	Ready bool `json:"ready,omitempty"`
	// IsHost 是否为房主（一个房间至多一个）。
	IsHost bool `json:"is_host,omitempty"`
	// Bot 机器人座位：不接收推送，也不接任房主。
	Bot bool `json:"bot,omitempty"`
	// Offline 对局进行中掉线的座位：保留座位号（= 队伍号），结算时由 SetRunning(false) 清理。
	Offline bool `json:"offline,omitempty"`
}

// RoomSnapshot 是一个房间的完整状态（状态同步推给客户端的正是它）。
type RoomSnapshot struct {
	RoomID string `json:"room_id"`
	// Host 当前房主；空串 = 房间里还没有真人房主。
	Host string `json:"host,omitempty"`
	// Running 对局是否进行中。
	Running bool `json:"running,omitempty"`
	// Version 本房间的状态版本，每次变化 +1。
	Version int64 `json:"version"`
	// Seats 固定长度 = 座位数；下标即座位号（= 队伍号）。
	Seats []Seat `json:"seats"`
}

// RoomInfo 房间列表里的一行。
type RoomInfo struct {
	RoomID  string `json:"room_id"`
	Host    string `json:"host,omitempty"`
	Cur     int    `json:"cur"`
	Max     int    `json:"max"`
	Running bool   `json:"running,omitempty"`
}

// RoomList 房间列表快照；Version 是全局房间版本（任何房间的任何变化都 +1）。
type RoomList struct {
	Version int64      `json:"version"`
	Rooms   []RoomInfo `json:"rooms"`
}

// stateSyncExport 是可迁移态（ExportPack.State 的内容）。
type stateSyncExport struct {
	RoomID  string `json:"room_id"`
	Host    string `json:"host,omitempty"`
	Running bool   `json:"running,omitempty"`
	Version int64  `json:"version"`
	Seats   []Seat `json:"seats"`
}

// stateSyncRoom 单个房间的运行态。所有字段只在持有 StateSyncKernel.mu 时读写。
type stateSyncRoom struct {
	id      string
	host    string
	running bool
	seats   []Seat // 长度固定 = 座位数；下标 = 座位号 = 队伍号
	version int64
}

// seatOf 返回玩家的座位号（= 队伍号）；-1 = 不在房内。调用方须持锁。
func (st *stateSyncRoom) seatOf(playerID string) int {
	if playerID == "" {
		return -1
	}
	for i := range st.seats {
		if st.seats[i].PlayerID == playerID {
			return i
		}
	}
	return -1
}

// firstFreeSeat 返回第一个空座位号；-1 = 满员。调用方须持锁。
func (st *stateSyncRoom) firstFreeSeat() int {
	for i := range st.seats {
		if st.seats[i].PlayerID == "" {
			return i
		}
	}
	return -1
}

// occupiedLocked 已占座位数。调用方须持锁。
func (st *stateSyncRoom) occupiedLocked() int {
	n := 0
	for i := range st.seats {
		if st.seats[i].PlayerID != "" {
			n++
		}
	}
	return n
}

// humanIDsLocked 房内真人玩家（机器人不发推送）。调用方须持锁。
func (st *stateSyncRoom) humanIDsLocked() []string {
	out := make([]string, 0, len(st.seats))
	for i := range st.seats {
		if p := st.seats[i].PlayerID; p != "" && !st.seats[i].Bot {
			out = append(out, p)
		}
	}
	return out
}

// copySeatsLocked 座位副本（出锁后仍可安全读写）。调用方须持锁。
func (st *stateSyncRoom) copySeatsLocked() []Seat {
	seats := make([]Seat, len(st.seats))
	copy(seats, st.seats)
	return seats
}

// snapshotLocked 完整状态副本。调用方须持锁。
func (st *stateSyncRoom) snapshotLocked() RoomSnapshot {
	return RoomSnapshot{
		RoomID:  st.id,
		Host:    st.host,
		Running: st.running,
		Version: st.version,
		Seats:   st.copySeatsLocked(),
	}
}

// infoLocked 列表行。调用方须持锁。
func (st *stateSyncRoom) infoLocked() RoomInfo {
	return RoomInfo{
		RoomID:  st.id,
		Host:    st.host,
		Cur:     st.occupiedLocked(),
		Max:     len(st.seats),
		Running: st.running,
	}
}

// reassignHostLocked 语义①：重新指派房主 —— **座位号最小的真人**接任；
// 没有真人（空房 / 只剩机器人）⇒ 房主清空，等下一次真人进房时由 Join 顶上。
// 调用方须持锁。
func (st *stateSyncRoom) reassignHostLocked() {
	st.host = ""
	for i := range st.seats {
		st.seats[i].IsHost = false
	}
	for i := range st.seats { // 座位号小者优先 ⇒ 结果稳定可复现
		if st.seats[i].PlayerID != "" && !st.seats[i].Bot {
			st.seats[i].IsHost = true
			st.host = st.seats[i].PlayerID
			return
		}
	}
}

// healHostLocked 导入态自愈：导出态里的房主仍在座位上 ⇒ 保留；否则按语义①重新指派。
// 调用方须持锁。
func (st *stateSyncRoom) healHostLocked() {
	if st.host != "" {
		for i := range st.seats {
			if st.seats[i].PlayerID == st.host && !st.seats[i].Bot {
				for j := range st.seats {
					st.seats[j].IsHost = st.seats[j].PlayerID == st.host
				}
				return
			}
		}
	}
	st.reassignHostLocked()
}

// ===========================================================================
// StateSyncKernel
// ===========================================================================

// StateSyncKernel 是 `Kernel` 的**状态同步**实现：房间持有权威状态，
// 任何变化都向房内真人推完整状态；大厅观察者按版本号兜底补推房间列表。
//
// 用法（与帧同步内核共用同一套外壳 API）：
//
//	mod := room.NewModule(room.Config{
//	    MasterCaller: g,
//	    Pusher: func(pid string, msgID uint32, v any) error { return g.PushToPlayer(pid, msgID, v) },
//	    NodeAddr:     g.Addr(),
//	    Kernel: room.NewStateSyncKernel(room.StateSyncConfig{
//	        Seats:      2, // 座位数 = 队伍数
//	        Pusher:     func(pid string, msgID uint32, v any) error { return g.PushToPlayer(pid, msgID, v) },
//	        StateMsgID: def.PushRoomState,
//	        ListMsgID:  def.PushRoomList,
//	    }),
//	})
//
// ⚠️ 构造返回的是**具体类型**（不是 Kernel 接口）：除 8 个内核方法外，
// 座位 / 大厅相关操作（SeatOf / Watch / FlushWatchers …）只有具体类型才有。
// 直接赋给 Config.Kernel 即可使用（*StateSyncKernel 实现 Kernel）。
type StateSyncKernel struct {
	cfg StateSyncConfig

	mu       sync.Mutex
	closed   bool
	rooms    map[string]*stateSyncRoom
	order    []string
	version  int64            // 全局房间版本：任何房间的任何变化 +1
	watchers map[string]bool  // 大厅观察者
	pushed   map[string]int64 // 观察者已**成功下发**的房间列表版本（语义③）
}

// 编译期断言：StateSyncKernel 就是一款房间内核。
var _ Kernel = (*StateSyncKernel)(nil)

// NewStateSyncKernel 构造状态同步内核。cfg.Seats <= 0 时取 DefaultStateSyncSeats。
func NewStateSyncKernel(cfg StateSyncConfig) *StateSyncKernel {
	if cfg.Seats <= 0 {
		cfg.Seats = DefaultStateSyncSeats
	}
	return &StateSyncKernel{
		cfg:      cfg,
		rooms:    make(map[string]*stateSyncRoom),
		watchers: make(map[string]bool),
		pushed:   make(map[string]int64),
	}
}

// Seats 返回本内核房间的座位数（= 队伍数）。
func (k *StateSyncKernel) Seats() int {
	if k == nil {
		return 0
	}
	return k.cfg.Seats
}

// Version 返回全局房间版本号（任何房间的任何变化都会 +1）。
func (k *StateSyncKernel) Version() int64 {
	if k == nil {
		return 0
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.version
}

// usable 前置校验：内核已构造且未关闭。
func (k *StateSyncKernel) usable() error {
	if k == nil {
		return errStateSyncNil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return errStateSyncClosed
	}
	return nil
}

// ===========================================================================
// Kernel 接口实现
// ===========================================================================

// EnsureRoom 确保房间存在（幂等：已存在直接返回 nil）。
func (k *StateSyncKernel) EnsureRoom(roomID string) error {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync ensure room=%s rejected: %v", roomID, err)
		return err
	}
	if roomID == "" {
		logger.Warnf("room: state-sync ensure rejected: empty room id")
		return errors.New("room: 房间 ID 不能为空")
	}
	k.mu.Lock()
	if _, ok := k.rooms[roomID]; ok {
		k.mu.Unlock()
		return nil // 幂等
	}
	k.rooms[roomID] = &stateSyncRoom{id: roomID, seats: make([]Seat, k.cfg.Seats)}
	k.order = append(k.order, roomID)
	k.version++
	k.mu.Unlock()

	logger.Infof("room: state-sync ensure room=%s seats=%d", roomID, k.cfg.Seats)
	k.broadcastList()
	return nil
}

// Join 玩家进房。已在房内视为成功；房内还没有房主时（首人进房 / 前任房主已离开），
// 进房者接任房主并留在自己占到的座位上（语义①）。
func (k *StateSyncKernel) Join(roomID, playerID string) error {
	return k.join(roomID, playerID, false)
}

// JoinBot 让一个机器人占座（第一个空座位）。机器人座位不接收推送、不接任房主。
func (k *StateSyncKernel) JoinBot(roomID, botID string) error {
	return k.join(roomID, botID, true)
}

// join 是 Join / JoinBot 的公共实现。
func (k *StateSyncKernel) join(roomID, playerID string, bot bool) error {
	who := "join"
	if bot {
		who = "join bot"
	}
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync %s room=%s player=%s rejected: %v", who, roomID, playerID, err)
		return err
	}
	if playerID == "" {
		logger.Warnf("room: state-sync %s room=%s rejected: empty player id", who, roomID)
		return errors.New("room: 玩家 ID 不能为空")
	}
	k.mu.Lock()
	st := k.rooms[roomID]
	if st == nil {
		k.mu.Unlock()
		logger.Warnf("room: state-sync %s room=%s player=%s rejected: room not found", who, roomID, playerID)
		return errors.New("room: 房间不存在")
	}
	if st.seatOf(playerID) >= 0 {
		k.mu.Unlock()
		return nil // 已在房内视为成功
	}
	if st.running {
		k.mu.Unlock()
		logger.Warnf("room: state-sync %s room=%s player=%s rejected: match running", who, roomID, playerID)
		return errors.New("room: 对局已开始")
	}
	seat := st.firstFreeSeat()
	if seat < 0 {
		k.mu.Unlock()
		logger.Warnf("room: state-sync %s room=%s player=%s rejected: full (%d seats)", who, roomID, playerID, len(st.seats))
		return fmt.Errorf("room: 房间已满（%d 个座位）", len(st.seats))
	}
	st.seats[seat] = Seat{PlayerID: playerID, Bot: bot}
	if st.host == "" && !bot {
		st.seats[seat].IsHost = true
		st.host = playerID
	}
	st.version++
	k.version++
	snap := st.snapshotLocked()
	k.mu.Unlock()

	logger.Infof("room: state-sync %s room=%s player=%s seat=%d host=%s bot=%v",
		who, roomID, playerID, seat, snap.Host, bot)
	k.pushState(snap)
	k.broadcastList()
	return nil
}

// Leave 玩家离房（不在房内视为成功）。
//
// 语义②：**对局进行中拒绝离房** —— 摘掉一个座位会让其后座位号（= 队伍号）前移，
// 结算就会认错人。对局中要走请用 SetOffline 保住座位号。
// 语义①：离开的是房主时，由座位号最小的真人接任（见 reassignHostLocked）。
func (k *StateSyncKernel) Leave(roomID, playerID string) error {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync leave room=%s player=%s rejected: %v", roomID, playerID, err)
		return err
	}
	k.mu.Lock()
	st := k.rooms[roomID]
	if st == nil {
		k.mu.Unlock()
		logger.Warnf("room: state-sync leave room=%s player=%s rejected: room not found", roomID, playerID)
		return errors.New("room: 房间不存在")
	}
	seat := st.seatOf(playerID)
	if seat < 0 {
		k.mu.Unlock()
		return nil // 不在房内视为成功
	}
	if st.running {
		k.mu.Unlock()
		logger.Warnf("room: state-sync leave room=%s player=%s seat=%d rejected: match running（对局中请用 SetOffline 保住座位号）", roomID, playerID, seat)
		return errors.New("room: 对局进行中不允许离房（会改变队伍号）")
	}
	wasHost := st.host == playerID || st.seats[seat].IsHost
	st.seats[seat] = Seat{}
	if wasHost {
		st.reassignHostLocked() // 语义①
	}
	st.version++
	k.version++
	snap := st.snapshotLocked()
	k.mu.Unlock()

	logger.Infof("room: state-sync leave room=%s player=%s seat=%d wasHost=%v newHost=%s",
		roomID, playerID, seat, wasHost, snap.Host)
	k.pushState(snap)
	k.broadcastList()
	return nil
}

// Destroy 销毁房间（幂等：房间不存在返回 nil）。
func (k *StateSyncKernel) Destroy(roomID string) error {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync destroy room=%s rejected: %v", roomID, err)
		return err
	}
	k.mu.Lock()
	if _, ok := k.rooms[roomID]; !ok {
		k.mu.Unlock()
		return nil // 幂等
	}
	delete(k.rooms, roomID)
	for i, id := range k.order {
		if id == roomID {
			k.order = append(k.order[:i:i], k.order[i+1:]...)
			break
		}
	}
	k.version++
	k.mu.Unlock()

	logger.Infof("room: state-sync destroy room=%s", roomID)
	k.broadcastList()
	return nil
}

// ExportState 导出可迁移态：State = 内核恢复运行态用的导出结构；
// Recovery = 客户端重建画面用的完整状态。
//
// 与帧同步内核的差别：状态同步房间的迁移态本身就是客户端恢复包，
// 不需要额外派生「追帧增量」。
func (k *StateSyncKernel) ExportState(roomID string) (ExportPack, error) {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync export room=%s rejected: %v", roomID, err)
		return ExportPack{}, err
	}
	k.mu.Lock()
	st := k.rooms[roomID]
	if st == nil {
		k.mu.Unlock()
		logger.Warnf("room: state-sync export room=%s rejected: room not found", roomID)
		return ExportPack{}, errors.New("room: 房间不存在")
	}
	ex := stateSyncExport{
		RoomID:  st.id,
		Host:    st.host,
		Running: st.running,
		Version: st.version,
		Seats:   st.copySeatsLocked(),
	}
	snap := st.snapshotLocked()
	k.mu.Unlock()

	stateRaw, err := json.Marshal(ex)
	if err != nil {
		logger.Errorf("room: state-sync marshal state room=%s failed: %v", roomID, err)
		return ExportPack{}, err
	}
	pack := ExportPack{State: stateRaw}
	// 恢复包序列化失败不阻断接管：State 仍可恢复运行态，只是客户端少一次快照下发。
	if recRaw, err := json.Marshal(snap); err != nil {
		logger.Warnf("room: state-sync marshal recovery room=%s failed: %v", roomID, err)
	} else {
		pack.Recovery = recRaw
	}
	logger.Infof("room: state-sync export room=%s seats=%d host=%s running=%v",
		roomID, len(snap.Seats), snap.Host, snap.Running)
	return pack, nil
}

// ImportState 导入可迁移态（房间不存在时先创建）。
//
// 对局实例**不随状态迁移**：导入后房间回到未开打态、掉线标记清除、准备态清零
// （要一段 JSON 无损搬运正在跑的模拟，只会得到一个「看起来搬过来了」的假房间）。
// 语义①：导出态里的房主已不在座位上时，由座位号最小的真人接任。
func (k *StateSyncKernel) ImportState(roomID string, state json.RawMessage) error {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync import room=%s rejected: %v", roomID, err)
		return err
	}
	if roomID == "" {
		logger.Warnf("room: state-sync import rejected: empty room id")
		return errors.New("room: 房间 ID 不能为空")
	}
	var ex stateSyncExport
	if len(state) > 0 {
		if err := json.Unmarshal(state, &ex); err != nil {
			logger.Errorf("room: state-sync unmarshal state room=%s failed: %v", roomID, err)
			return fmt.Errorf("room: 房间状态解析失败: %w", err)
		}
	}
	k.mu.Lock()
	st := k.rooms[roomID]
	if st == nil {
		st = &stateSyncRoom{id: roomID, seats: make([]Seat, k.cfg.Seats)}
		k.rooms[roomID] = st
		k.order = append(k.order, roomID)
	}
	seats := make([]Seat, k.cfg.Seats)
	for i := range seats {
		if i >= len(ex.Seats) {
			break // 座位数比导出态多 ⇒ 多出来的座位保持空
		}
		seats[i] = ex.Seats[i]
		seats[i].Offline = false
		seats[i].Ready = false
	}
	st.seats = seats
	st.host = ex.Host
	st.running = false // 对局实例不迁移
	st.healHostLocked()
	st.version++
	k.version++
	snap := st.snapshotLocked()
	k.mu.Unlock()

	logger.Infof("room: state-sync import room=%s seats=%d host=%s（对局态不回填，房间回到未开打态）",
		roomID, len(seats), snap.Host)
	k.pushState(snap)
	k.broadcastList()
	return nil
}

// Players 返回房内的**真人**玩家（机器人不发推送；外壳据此下发接管恢复包）。
func (k *StateSyncKernel) Players(roomID string) []string {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync players room=%s rejected: %v", roomID, err)
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	st := k.rooms[roomID]
	if st == nil {
		logger.Warnf("room: state-sync players room=%s rejected: room not found", roomID)
		return nil
	}
	return st.humanIDsLocked()
}

// Close 关闭内核并释放资源（幂等）。
func (k *StateSyncKernel) Close() {
	if k == nil {
		return
	}
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return
	}
	k.closed = true
	k.rooms = make(map[string]*stateSyncRoom)
	k.order = nil
	k.watchers = make(map[string]bool)
	k.pushed = make(map[string]int64)
	k.mu.Unlock()
	logger.Infof("room: state-sync kernel closed")
}

// ===========================================================================
// 语义②：座位号 = 队伍号
// ===========================================================================

// SeatOf 返回玩家在房间里的座位号，ok=false 表示不在房内。
//
// **座位号 = 队伍号**：座位 0 属于队伍 0，座位 1 属于队伍 1，依此类推 ——
// 业务不需要另建「玩家 → 队伍」映射，结算时也不用反查；
// 座位是**固定槽**，别人离开或掉线都不会让自己的座位号前移。
func (k *StateSyncKernel) SeatOf(roomID, playerID string) (int, bool) {
	if k == nil {
		return -1, false
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	st := k.rooms[roomID]
	if st == nil {
		return -1, false
	}
	seat := st.seatOf(playerID)
	return seat, seat >= 0
}

// SetOffline 标记 / 取消座位的掉线态。
//
// 语义②：**对局进行中的掉线不摘座位** —— 只打标记，座位号（= 队伍号）原地保留。
// 对局结束后由 SetRunning(roomID, false) 统一清理掉线标记与准备态。
func (k *StateSyncKernel) SetOffline(roomID, playerID string, offline bool) error {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync set offline room=%s player=%s rejected: %v", roomID, playerID, err)
		return err
	}
	k.mu.Lock()
	st := k.rooms[roomID]
	if st == nil {
		k.mu.Unlock()
		logger.Warnf("room: state-sync set offline room=%s player=%s rejected: room not found", roomID, playerID)
		return errors.New("room: 房间不存在")
	}
	seat := st.seatOf(playerID)
	if seat < 0 {
		k.mu.Unlock()
		logger.Warnf("room: state-sync set offline room=%s player=%s rejected: not seated", roomID, playerID)
		return errors.New("room: 玩家不在房间内")
	}
	if st.seats[seat].Offline == offline {
		k.mu.Unlock()
		return nil
	}
	st.seats[seat].Offline = offline
	if offline {
		st.seats[seat].Ready = false
	}
	st.version++
	k.version++
	snap := st.snapshotLocked()
	k.mu.Unlock()

	logger.Infof("room: state-sync set offline room=%s player=%s seat=%d offline=%v", roomID, playerID, seat, offline)
	k.pushState(snap)
	return nil
}

// SetReady 更新座位的准备态（房主与机器人不参与准备态校验；对局进行中拒绝改动）。
func (k *StateSyncKernel) SetReady(roomID, playerID string, ready bool) error {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync set ready room=%s player=%s rejected: %v", roomID, playerID, err)
		return err
	}
	k.mu.Lock()
	st := k.rooms[roomID]
	if st == nil {
		k.mu.Unlock()
		logger.Warnf("room: state-sync set ready room=%s player=%s rejected: room not found", roomID, playerID)
		return errors.New("room: 房间不存在")
	}
	seat := st.seatOf(playerID)
	if seat < 0 {
		k.mu.Unlock()
		logger.Warnf("room: state-sync set ready room=%s player=%s rejected: not seated", roomID, playerID)
		return errors.New("room: 玩家不在房间内")
	}
	if st.running {
		k.mu.Unlock()
		logger.Warnf("room: state-sync set ready room=%s player=%s rejected: match running", roomID, playerID)
		return errors.New("room: 对局已开始")
	}
	if st.seats[seat].Ready == ready {
		k.mu.Unlock()
		return nil
	}
	st.seats[seat].Ready = ready
	st.version++
	k.version++
	snap := st.snapshotLocked()
	k.mu.Unlock()

	logger.Infof("room: state-sync set ready room=%s player=%s seat=%d ready=%v", roomID, playerID, seat, ready)
	k.pushState(snap)
	k.broadcastList()
	return nil
}

// SetRunning 开关「对局进行中」。
//
// 开始时要求每个座位都有人（**玩法层的前置校验** —— 阵容 / 准备态 / 资源 ——
// 不属于房间内核，由业务在调用本方法之前自己做）。
// 语义②：对局进行中座位号冻结（Join / Leave 被拒），以保证座位号 = 队伍号稳定；
// 结束时清理掉线标记与准备态，房间回到未开打态。
func (k *StateSyncKernel) SetRunning(roomID string, running bool) error {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync set running room=%s rejected: %v", roomID, err)
		return err
	}
	k.mu.Lock()
	st := k.rooms[roomID]
	if st == nil {
		k.mu.Unlock()
		logger.Warnf("room: state-sync set running room=%s rejected: room not found", roomID)
		return errors.New("room: 房间不存在")
	}
	if st.running == running {
		k.mu.Unlock()
		return nil
	}
	if running {
		if seat := st.firstFreeSeat(); seat >= 0 {
			k.mu.Unlock()
			logger.Warnf("room: state-sync set running room=%s rejected: seat %d empty (%d/%d)",
				roomID, seat, st.occupiedLocked(), len(st.seats))
			return fmt.Errorf("room: 座位 %d 空着，无法开始对局（需要坐满 %d 个座位）", seat, len(st.seats))
		}
	} else {
		for i := range st.seats {
			st.seats[i].Offline = false
			st.seats[i].Ready = false
		}
	}
	st.running = running
	st.version++
	k.version++
	snap := st.snapshotLocked()
	k.mu.Unlock()

	logger.Infof("room: state-sync set running room=%s running=%v", roomID, running)
	k.pushState(snap)
	k.broadcastList()
	return nil
}

// Host 返回当前房主；ok=false 表示房间不存在。房主为空串 = 房里还没有真人房主。
func (k *StateSyncKernel) Host(roomID string) (string, bool) {
	if k == nil {
		return "", false
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	st := k.rooms[roomID]
	if st == nil {
		return "", false
	}
	return st.host, true
}

// Snapshot 返回房间完整状态副本；ok=false 表示房间不存在。
func (k *StateSyncKernel) Snapshot(roomID string) (RoomSnapshot, bool) {
	if k == nil {
		return RoomSnapshot{}, false
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	st := k.rooms[roomID]
	if st == nil {
		return RoomSnapshot{}, false
	}
	return st.snapshotLocked(), true
}

// ===========================================================================
// 语义③：观察者版本号兜底补推
// ===========================================================================

// RoomList 返回当前房间列表快照（拉取式）。
func (k *StateSyncKernel) RoomList() RoomList {
	if k == nil {
		return RoomList{}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.listLocked()
}

// Watch 登记一个大厅观察者，并返回当前**完整**房间列表（语义③的首帧兜底）。
//
// 调用方把返回值回给客户端，即完成「新加入的观察者拿到完整状态」；
// 内核据此把该观察者标记为已同步到当前版本 —— 之后的每次变化由内核即时推送
// （**推送成功**才记版本），推送失败 / 漏推的观察者由 FlushWatchers 按版本号补推。
func (k *StateSyncKernel) Watch(playerID string) (RoomList, error) {
	if playerID == "" {
		logger.Warnf("room: state-sync watch rejected: empty observer id")
		return RoomList{}, errors.New("room: 观察者 ID 不能为空")
	}
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync watch player=%s rejected: %v", playerID, err)
		return RoomList{}, err
	}
	k.mu.Lock()
	k.watchers[playerID] = true
	list := k.listLocked()
	// 返回值就是首帧下发载体，因此这里标记为已同步到当前版本；
	// 之后的版本由推送路径按成功与否记录。
	if list.Version > k.pushed[playerID] {
		k.pushed[playerID] = list.Version
	}
	k.mu.Unlock()

	logger.Infof("room: state-sync watch player=%s rooms=%d version=%d", playerID, len(list.Rooms), list.Version)
	return list, nil
}

// Unwatch 注销大厅观察者。
func (k *StateSyncKernel) Unwatch(playerID string) {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.watchers, playerID)
	delete(k.pushed, playerID)
}

// FlushWatchers 语义③的兜底补推：把当前房间列表补推给所有「已下发版本 < 当前版本」的观察者。
//
// 典型触发场景：某次推送失败（网络抖动 / 连接正在迁移），失败的那次不会记版本 ⇒
// 观察者停在旧版本上，由本方法（业务侧定时器，例如 3s 一次）补推自愈。
// 返回本轮**实际补推成功**的观察者 ID（已排序，便于日志与断言稳定）。
//
// 未接入推送（Pusher 或 ListMsgID 未配置）时返回 nil 并打日志 —— 无从投递，
// 不谎报已补推。
func (k *StateSyncKernel) FlushWatchers() []string {
	if err := k.usable(); err != nil {
		logger.Warnf("room: state-sync flush watchers rejected: %v", err)
		return nil
	}
	if k.cfg.Pusher == nil || k.cfg.ListMsgID == 0 {
		logger.Warnf("room: state-sync flush watchers skipped: 未接入推送（Pusher / ListMsgID 未配置）")
		return nil
	}
	k.mu.Lock()
	list := k.listLocked()
	behind := make([]string, 0, len(k.watchers))
	for pid := range k.watchers {
		if k.pushed[pid] < list.Version {
			behind = append(behind, pid)
		}
	}
	k.mu.Unlock()

	sort.Strings(behind)
	flushed := make([]string, 0, len(behind))
	for _, pid := range behind {
		if k.pushList(pid, list) {
			k.recordListPushed(pid, list.Version)
			flushed = append(flushed, pid)
		}
	}
	if len(behind) > 0 {
		logger.Infof("room: state-sync flush watchers behind=%d flushed=%d version=%d",
			len(behind), len(flushed), list.Version)
	}
	return flushed
}

// ===========================================================================
// 推送（全部在锁外做网络 I/O）
// ===========================================================================

// listLocked 组装房间列表。调用方须持锁。
func (k *StateSyncKernel) listLocked() RoomList {
	rooms := make([]RoomInfo, 0, len(k.order))
	for _, id := range k.order {
		if st := k.rooms[id]; st != nil {
			rooms = append(rooms, st.infoLocked())
		}
	}
	return RoomList{Version: k.version, Rooms: rooms}
}

// pushState 把房间完整状态推给房内所有真人（锁外调用）。
func (k *StateSyncKernel) pushState(snap RoomSnapshot) {
	if k.cfg.Pusher == nil {
		return
	}
	if k.cfg.StateMsgID == 0 {
		logger.Warnf("room: state-sync state push skipped room=%s: Pusher 已设置但 StateMsgID=0", snap.RoomID)
		return
	}
	for i := range snap.Seats {
		s := snap.Seats[i]
		if s.PlayerID == "" || s.Bot {
			continue // 空座位 / 机器人不接收推送
		}
		if err := k.cfg.Pusher(s.PlayerID, k.cfg.StateMsgID, snap); err != nil {
			logger.Warnf("room: state-sync state push room=%s player=%s failed: %v", snap.RoomID, s.PlayerID, err)
		}
	}
}

// broadcastList 把最新房间列表推给所有观察者（锁外调用），成功的才记版本。
func (k *StateSyncKernel) broadcastList() {
	if k.cfg.Pusher == nil || k.cfg.ListMsgID == 0 {
		return
	}
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return
	}
	list := k.listLocked()
	targets := make([]string, 0, len(k.watchers))
	for pid := range k.watchers {
		targets = append(targets, pid)
	}
	k.mu.Unlock()

	sort.Strings(targets) // 排序只为日志与输出稳定
	for _, pid := range targets {
		if k.pushList(pid, list) {
			k.recordListPushed(pid, list.Version)
		}
	}
}

// pushList 把列表推给一个观察者；返回是否真的送达。
func (k *StateSyncKernel) pushList(playerID string, list RoomList) bool {
	if k.cfg.Pusher == nil {
		return false
	}
	if k.cfg.ListMsgID == 0 {
		logger.Warnf("room: state-sync list push skipped player=%s: Pusher 已设置但 ListMsgID=0", playerID)
		return false
	}
	if err := k.cfg.Pusher(playerID, k.cfg.ListMsgID, list); err != nil {
		logger.Warnf("room: state-sync list push player=%s failed: %v", playerID, err)
		return false
	}
	return true
}

// recordListPushed 记录观察者已成功下发的版本（单调递增；观察者已注销则不记）。
func (k *StateSyncKernel) recordListPushed(playerID string, version int64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.watchers[playerID]; !ok {
		return
	}
	if version > k.pushed[playerID] {
		k.pushed[playerID] = version
	}
}
