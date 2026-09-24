package room

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// ===========================================================================
// 测试替身：记录推送的假 Pusher（可指定某个玩家「拒收」以模拟推送失败）
// ===========================================================================

type pushRec struct {
	msgID   uint32
	payload any
}

type fakePusher struct {
	mu      sync.Mutex
	sent    map[string][]pushRec
	failing map[string]bool
}

func newFakePusher() *fakePusher {
	return &fakePusher{sent: make(map[string][]pushRec), failing: make(map[string]bool)}
}

func (p *fakePusher) push(playerID string, msgID uint32, v any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failing[playerID] {
		return fmt.Errorf("fake pusher: %s 拒收", playerID)
	}
	p.sent[playerID] = append(p.sent[playerID], pushRec{msgID: msgID, payload: v})
	return nil
}

func (p *fakePusher) fail(playerID string, fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fail {
		p.failing[playerID] = true
		return
	}
	delete(p.failing, playerID)
}

func (p *fakePusher) count(playerID string, msgID uint32) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.sent[playerID] {
		if r.msgID == msgID {
			n++
		}
	}
	return n
}

func (p *fakePusher) last(playerID string, msgID uint32) any {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out any
	for _, r := range p.sent[playerID] {
		if r.msgID == msgID {
			out = r.payload
		}
	}
	return out
}

func mustEnsureRoom(t *testing.T, k *StateSyncKernel, roomID string) {
	t.Helper()
	if err := k.EnsureRoom(roomID); err != nil {
		t.Fatalf("EnsureRoom(%s): %v", roomID, err)
	}
}

func mustJoin(t *testing.T, k *StateSyncKernel, roomID, playerID string) {
	t.Helper()
	if err := k.Join(roomID, playerID); err != nil {
		t.Fatalf("Join(%s,%s): %v", roomID, playerID, err)
	}
}

// ===========================================================================
// 语义①：房主移交
// ===========================================================================

func TestStateSyncKernelHostHandover(t *testing.T) {
	k := NewStateSyncKernel(StateSyncConfig{Seats: 3})
	mustEnsureRoom(t, k, "r1")
	mustJoin(t, k, "r1", "p1")

	// 第一个进房者 = 房主，占 0 号座位。
	if h, ok := k.Host("r1"); !ok || h != "p1" {
		t.Fatalf("首人进房应成为房主，得到 host=%q ok=%v", h, ok)
	}
	if seat, ok := k.SeatOf("r1", "p1"); !ok || seat != 0 {
		t.Fatalf("房主应占 0 号座位，得到 seat=%d ok=%v", seat, ok)
	}
	mustJoin(t, k, "r1", "p2")
	if err := k.JoinBot("r1", "bot-1"); err != nil {
		t.Fatalf("JoinBot: %v", err)
	}

	// 房主离开 ⇒ 座位号最小的**真人**（p2，座位 1）接任；机器人（座位 2）不接任。
	if err := k.Leave("r1", "p1"); err != nil {
		t.Fatalf("Leave p1: %v", err)
	}
	if h, _ := k.Host("r1"); h != "p2" {
		t.Fatalf("房主离开后应由座位号最小的真人接任，得到 host=%q", h)
	}
	snap, ok := k.Snapshot("r1")
	if !ok {
		t.Fatal("Snapshot: 房间应存在")
	}
	if len(snap.Seats) != 3 {
		t.Fatalf("座位数应固定为 3，得到 %d", len(snap.Seats))
	}
	if snap.Seats[0].PlayerID != "" {
		t.Fatalf("0 号座位应已空出，得到 %q", snap.Seats[0].PlayerID)
	}
	if snap.Seats[1].PlayerID != "p2" || !snap.Seats[1].IsHost {
		t.Fatalf("1 号座位应为接任房主 p2，得到 %+v", snap.Seats[1])
	}
	if snap.Seats[2].PlayerID != "bot-1" || !snap.Seats[2].Bot || snap.Seats[2].IsHost {
		t.Fatalf("2 号座位应是机器人且不接任房主，得到 %+v", snap.Seats[2])
	}

	// 最后一个真人离开 ⇒ 房主清空（房里只剩机器人）。
	if err := k.Leave("r1", "p2"); err != nil {
		t.Fatalf("Leave p2: %v", err)
	}
	if h, _ := k.Host("r1"); h != "" {
		t.Fatalf("房里没有真人时应清空房主，得到 host=%q", h)
	}

	// 再有真人进房 ⇒ 顶上房主（占第一个空座位 0）。
	mustJoin(t, k, "r1", "p3")
	if h, _ := k.Host("r1"); h != "p3" {
		t.Fatalf("房主空缺时进房者应顶上，得到 host=%q", h)
	}
	if seat, _ := k.SeatOf("r1", "p3"); seat != 0 {
		t.Fatalf("p3 应占第一个空座位 0，得到 %d", seat)
	}
	// 接管恢复包只含真人：机器人不算 Players。
	if players := k.Players("r1"); len(players) != 1 || players[0] != "p3" {
		t.Fatalf("Players 应只含真人，得到 %v", players)
	}
}

// ===========================================================================
// 语义②：座位号 = 队伍号
// ===========================================================================

func TestStateSyncKernelSeatIsTeam(t *testing.T) {
	k := NewStateSyncKernel(StateSyncConfig{Seats: 3})
	mustEnsureRoom(t, k, "r1")
	mustJoin(t, k, "r1", "p1") // 座位 0（= 队伍 0）
	mustJoin(t, k, "r1", "p2") // 座位 1（= 队伍 1）

	// 座位号就是切片下标 —— 业务无需另建「玩家 → 队伍」映射。
	if seat, ok := k.SeatOf("r1", "p2"); !ok || seat != 1 {
		t.Fatalf("p2 应在 1 号座位（= 队伍 1），得到 seat=%d ok=%v", seat, ok)
	}
	mustJoin(t, k, "r1", "p3") // 座位 2

	// 中间的人离开 ⇒ 后面的座位号（= 队伍号）**不前移**。
	if err := k.Leave("r1", "p2"); err != nil {
		t.Fatalf("Leave p2: %v", err)
	}
	if seat, ok := k.SeatOf("r1", "p3"); !ok || seat != 2 {
		t.Fatalf("他人离开不应改变我的座位号：p3 应为 2，得到 seat=%d ok=%v", seat, ok)
	}

	// 开打前必须坐满（有空座位 ⇒ 队伍缺人）。玩法层的前置校验不在这里。
	if err := k.SetRunning("r1", true); err == nil {
		t.Fatal("座位 1 空着，开打应被拒绝")
	}
	mustJoin(t, k, "r1", "p4") // 补上 1 号座位
	if seat, _ := k.SeatOf("r1", "p4"); seat != 1 {
		t.Fatalf("补位应占用空出的 1 号座位，得到 %d", seat)
	}
	if err := k.SetRunning("r1", true); err != nil {
		t.Fatalf("坐满后应能开打: %v", err)
	}

	// 对局进行中：离房被拒（摘座位会让其后座位号前移 ⇒ 结算认错人）。
	err := k.Leave("r1", "p1")
	if err == nil || !strings.Contains(err.Error(), "队伍号") {
		t.Fatalf("对局进行中离房应被拒（并说明队伍号原因），得到 err=%v", err)
	}
	if seat, _ := k.SeatOf("r1", "p1"); seat != 0 {
		t.Fatalf("被拒的离房不应改变座位号，得到 %d", seat)
	}
	// 对局进行中进房同样被拒。
	if err := k.Join("r1", "p5"); err == nil || !strings.Contains(err.Error(), "对局") {
		t.Fatalf("对局进行中进房应被拒，得到 err=%v", err)
	}

	// 对局中掉线：只打标记，座位号（= 队伍号）原地保留。
	if err := k.SetOffline("r1", "p1", true); err != nil {
		t.Fatalf("SetOffline: %v", err)
	}
	if seat, ok := k.SeatOf("r1", "p1"); !ok || seat != 0 {
		t.Fatalf("掉线不应摘座位：p1 应仍在 0 号座位，得到 seat=%d ok=%v", seat, ok)
	}
	snap, ok := k.Snapshot("r1")
	if !ok {
		t.Fatal("Snapshot: 房间应存在")
	}
	if !snap.Seats[0].Offline {
		t.Fatalf("0 号座位应标记掉线，得到 %+v", snap.Seats[0])
	}

	// 结算：掉线标记与准备态清零，座位保留（房间回到未开打态）。
	if err := k.SetRunning("r1", false); err != nil {
		t.Fatalf("SetRunning(false): %v", err)
	}
	snap, _ = k.Snapshot("r1")
	if snap.Running || snap.Seats[0].Offline {
		t.Fatalf("结算后应清掉对局态与掉线标记，得到 running=%v seat0=%+v", snap.Running, snap.Seats[0])
	}
	if seat, ok := k.SeatOf("r1", "p1"); !ok || seat != 0 {
		t.Fatalf("结算后座位仍应保留，得到 seat=%d ok=%v", seat, ok)
	}
}

// ===========================================================================
// 语义③：观察者版本号兜底补推
// ===========================================================================

func TestStateSyncKernelWatcherVersionFallback(t *testing.T) {
	p := newFakePusher()
	k := NewStateSyncKernel(StateSyncConfig{
		Seats:      2,
		Pusher:     p.push,
		StateMsgID: 1001,
		ListMsgID:  1002,
	})
	mustEnsureRoom(t, k, "r1")
	mustJoin(t, k, "r1", "p1")

	// 新观察者注册 ⇒ Watch 直接返回**完整**列表（首帧兜底），不等推送。
	list, err := k.Watch("w1")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if len(list.Rooms) != 1 || list.Rooms[0].RoomID != "r1" || list.Rooms[0].Cur != 1 {
		t.Fatalf("Watch 应返回完整列表，得到 %+v", list)
	}
	if n := p.count("w1", 1002); n != 0 {
		t.Fatalf("Watch 只返回、不回推，应 0 次，得到 %d", n)
	}
	// 已同步到当前版本 ⇒ 无需补推。
	if flushed := k.FlushWatchers(); len(flushed) != 0 {
		t.Fatalf("没有落后的观察者时不应补推，得到 %v", flushed)
	}

	// 房间变化 ⇒ 即时推给观察者。
	mustJoin(t, k, "r1", "p2")
	if n := p.count("w1", 1002); n != 1 {
		t.Fatalf("房间变化应即时推列表，得到 %d 次", n)
	}
	if flushed := k.FlushWatchers(); len(flushed) != 0 {
		t.Fatalf("推送成功即已同步，不应补推，得到 %v", flushed)
	}

	// 推送失败 ⇒ 不记版本 ⇒ 由兜底按版本号补推回来。
	p.fail("w1", true)
	if err := k.SetReady("r1", "p1", true); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	if n := p.count("w1", 1002); n != 1 {
		t.Fatalf("推送失败不应计数，得到 %d", n)
	}
	p.fail("w1", false)
	flushed := k.FlushWatchers()
	if len(flushed) != 1 || flushed[0] != "w1" {
		t.Fatalf("推送失败的观察者应由兜底补推，得到 %v", flushed)
	}
	if n := p.count("w1", 1002); n != 2 {
		t.Fatalf("补推应真的发出，得到 %d 次", n)
	}
	if again := k.FlushWatchers(); len(again) != 0 {
		t.Fatalf("补推成功后不应再补推，得到 %v", again)
	}

	// 之后注册的观察者同样拿到完整状态（含当前人数）。
	list2, err := k.Watch("w2")
	if err != nil {
		t.Fatalf("Watch w2: %v", err)
	}
	if len(list2.Rooms) != 1 || list2.Rooms[0].Cur != 2 {
		t.Fatalf("新观察者应拿到完整状态（Cur 应为 2），得到 %+v", list2)
	}

	// 注销后不再收到推送。
	k.Unwatch("w2")
	mustEnsureRoom(t, k, "r2")
	if n := p.count("w2", 1002); n != 0 {
		t.Fatalf("已注销的观察者不应再收到推送，得到 %d 次", n)
	}
	if n := p.count("w1", 1002); n != 3 {
		t.Fatalf("在册观察者应收到新房子的推送，得到 %d 次", n)
	}

	// 状态同步 = 推全量：房内玩家收到的载荷是完整 RoomSnapshot。
	if got := p.last("p1", 1001); got == nil {
		t.Fatal("房内玩家应收到房间状态推送")
	} else if snap, ok := got.(RoomSnapshot); !ok || len(snap.Seats) != 2 || snap.Seats[0].PlayerID != "p1" {
		t.Fatalf("状态推送载荷应为完整 RoomSnapshot，得到 %#v", got)
	}
}

// ===========================================================================
// 迁移：ExportState / ImportState
// ===========================================================================

func TestStateSyncKernelExportImport(t *testing.T) {
	// 座位数默认值：不传 Seats 时取 DefaultStateSyncSeats。
	if got := NewStateSyncKernel(StateSyncConfig{}).Seats(); got != DefaultStateSyncSeats {
		t.Fatalf("默认座位数应为 %d，得到 %d", DefaultStateSyncSeats, got)
	}

	k := NewStateSyncKernel(StateSyncConfig{Seats: 2})
	mustEnsureRoom(t, k, "r1")
	mustJoin(t, k, "r1", "p1")
	mustJoin(t, k, "r1", "p2")
	if err := k.SetRunning("r1", true); err != nil {
		t.Fatalf("SetRunning: %v", err)
	}
	if err := k.SetOffline("r1", "p2", true); err != nil {
		t.Fatalf("SetOffline: %v", err)
	}

	pack, err := k.ExportState("r1")
	if err != nil {
		t.Fatalf("ExportState: %v", err)
	}
	if len(pack.State) == 0 || len(pack.Recovery) == 0 {
		t.Fatalf("导出应同时给出迁移态与客户端恢复包：state=%d recovery=%d", len(pack.State), len(pack.Recovery))
	}
	// Recovery 就是客户端重建画面用的完整状态。
	var rec RoomSnapshot
	if err := json.Unmarshal(pack.Recovery, &rec); err != nil {
		t.Fatalf("恢复包解析失败: %v", err)
	}
	if rec.RoomID != "r1" || len(rec.Seats) != 2 || rec.Seats[0].PlayerID != "p1" || !rec.Running {
		t.Fatalf("恢复包内容不对：%+v", rec)
	}

	// 新内核接管。
	k2 := NewStateSyncKernel(StateSyncConfig{Seats: 2})
	if err := k2.ImportState("r1", pack.State); err != nil {
		t.Fatalf("ImportState: %v", err)
	}
	snap, ok := k2.Snapshot("r1")
	if !ok {
		t.Fatal("导入后房间应存在")
	}
	if snap.Running {
		t.Fatal("对局实例不应随状态迁移（导入后应回到未开打态）")
	}
	if h, _ := k2.Host("r1"); h != "p1" {
		t.Fatalf("导出态里的房主应保留，得到 host=%q", h)
	}
	if seat, ok := k2.SeatOf("r1", "p2"); !ok || seat != 1 {
		t.Fatalf("导入应保留座位号（= 队伍号），得到 seat=%d ok=%v", seat, ok)
	}
	if snap.Seats[1].Offline {
		t.Fatal("导入后应清除掉线标记")
	}
	if snap.Seats[1].Ready {
		t.Fatal("导入后应清零准备态")
	}
	if players := k2.Players("r1"); len(players) != 2 {
		t.Fatalf("导入后应有两个真人，得到 %v", players)
	}

	// 语义①的自愈：导出态里的房主已不在座位表上 ⇒ 由座位号最小的真人接任。
	raw, err := json.Marshal(stateSyncExport{
		RoomID: "r9",
		Host:   "ghost",
		Seats:  []Seat{{PlayerID: "a"}, {PlayerID: "b"}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := k2.ImportState("r9", raw); err != nil {
		t.Fatalf("ImportState r9: %v", err)
	}
	if h, _ := k2.Host("r9"); h != "a" {
		t.Fatalf("房主不在座位表上时应由座位号最小的真人接任，得到 host=%q", h)
	}
	// 房间不存在时导入应先创建。
	if snap, ok := k2.Snapshot("r9"); !ok || len(snap.Seats) != 2 {
		t.Fatalf("导入应创建房间，得到 ok=%v snap=%+v", ok, snap)
	}

	// 关闭后不再接受操作；Close 幂等。
	k2.Close()
	if err := k2.EnsureRoom("r1"); err == nil {
		t.Fatal("关闭后不应再能建房")
	}
	k2.Close()
}
