package app

import (
	"fmt"
	"strings"
	"testing"

	irroom "clover-server-engine/internal/domain/room"
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/internal/transport/tcpmsg"
	proom "clover-server-engine/pkg/domain/room"
	pevent "clover-server-engine/pkg/transport/event"
)

// TestMasterRoomHandlersRegisterInternalIDs 复现缺陷 S2（引擎内建房间消息号被业务号守卫拒绝）。
//
// 复现什么缺陷：master 侧房间协议的 4 条消息号（EMasterRoomRegister/Unregister/Find/TakeoverClaim
// = 6001..6004）是**引擎内建**的（见 pkg/shared/proto/msg.go），但 room.NewMasterHandlers(mg).Register()
// 原先经业务注册门面（MasterGameFacade.OnMsg → tcpMsgBridge.OnMsg）注册，而那里有一道
// 「msgID 必须 > InternalMsgMax(10000)」的守卫 —— 引擎自己的号撞上了自己给业务的约束。
//
// 修复前什么现象：注册即 panic，master 侧永远没有房间 handler：
//
//	app: business message id must be > 10000; got 6001
//
// （后果：game 侧 CallMaster(6001) 同步等回包，master 回 "unknown msgID=6001" 或连接被掐，
// 建房 handler 卡住不回包 —— 业务侧只能把 room.Config.MasterCaller 传 nil 绕开，跨节点接管失效。）
//
// 修复后什么断言：4 条内建号全部注册成功 —— 既进 Logic 派发表（能被派发），
// 也登记了 TCP handler（TCP 帧到达才有入口），两者缺一都是「注册了但不工作」。
func TestMasterRoomHandlersRegisterInternalIDs(t *testing.T) {
	mg := newMasterGame(&Config{}, nil)

	proom.NewMasterHandlers(&MasterGameFacade{mg}).Register() // 修复前：此处 panic

	ids := []uint32{
		proto.EMasterRoomRegister,
		proto.EMasterRoomUnregister,
		proto.EMasterRoomFind,
		proto.EMasterRoomTakeoverClaim,
	}
	for _, id := range ids {
		if !mg.Logic.HasHandler(id) {
			t.Errorf("内建房间消息号 %d 未进入 Logic 派发表", id)
		}
	}
	// TCP 侧登记必须同样发生：master 的 handler 只有同时挂到 TCP 服务端才会被真正派发。
	// 按 msgID 去重统计，不假设注册调用次数：构造 NewMasterHandlers 内部已 Register 一次，
	// 业务再显式 Register() 一次会再登记一遍（同 msgID 重复登记，本用例只验「登记到位」）。
	mg.bridge.mu.Lock()
	pend := append([]pendingTCPHandler(nil), mg.bridge.pend...)
	mg.bridge.mu.Unlock()
	got := map[uint32]bool{}
	for _, p := range pend {
		got[p.msgID] = true
	}
	for _, id := range ids {
		if !got[id] {
			t.Errorf("内建房间消息号 %d 未登记 TCP handler（只进 Logic ⇒ 收到帧也无人派发）", id)
		}
	}
}

// TestMasterRoomHandlerRoundTripOverTCP 给「修好后 MasterCaller 能不能换回 g」一个**判据而不是猜**：
// 真起一条 master TCP 通道（tcpmsg.Server + Listen）+ 真客户端（tcpmsg.Dial → Call，
// 与 `Game.CallMaster` 同一路径：Dial + Call），用引擎内建号 6001 走一次完整往返：
//
//	客户端 Call(EMasterRoomRegister=6001) → master TCP → Logic 派发 → room handler
//	→ MasterRegistry.Register → 回包 OwnerResp{OK:true}
//
// 断言：① 往返成功（无 unknown msgID）；② 回包 OK=true；③ master 注册表里真能查到该 room 的 owner。
// 这三点成立 ⇒ 业务侧 room.Config.MasterCaller 传 g 可行（剩下的唯一前提是 master 角色真的挂了
// room.NewMasterHandlers）。
func TestMasterRoomHandlerRoundTripOverTCP(t *testing.T) {
	mg := newMasterGame(&Config{}, nil)
	// 与 runMaster 的顺序一致：先 setServer（建 TCP 服务端），再挂业务（这里就是房间 handler）。
	srv := tcpmsg.NewServer("127.0.0.1:0")
	mg.setServer(srv)
	if err := srv.Listen(); err != nil {
		t.Fatalf("master TCP Listen: %v", err)
	}
	defer func() { _ = srv.Stop() }()
	proom.NewMasterHandlers(&MasterGameFacade{mg}).Register()

	cli, err := tcpmsg.Dial(srv.Addr())
	if err != nil {
		t.Fatalf("dial master: %v", err)
	}
	defer func() { _ = cli.Close() }()

	const roomID, nodeAddr = "room-rt-1", "node-a"
	var resp irroom.OwnerResp
	if err := cli.Call(proto.EMasterRoomRegister, &irroom.OwnerRegisterReq{RoomID: roomID, NodeAddr: nodeAddr}, &resp); err != nil {
		t.Fatalf("CallMaster(6001) 往返失败: %v（修复前这里回 unknown msgID=6001）", err)
	}
	if !resp.OK {
		t.Fatalf("注册回包 OK=false: %+v", resp)
	}
	if got := mg.MasterRegistry().Find(roomID); got != nodeAddr {
		t.Fatalf("master 注册表里 room=%s 的 owner = %q，期望 %q（handler 没真正执行）", roomID, got, nodeAddr)
	}
}

// TestMasterGameOnMsgStillRejectsInternalIDs 对照组：**业务**注册路径的硬约束不许被放宽。
// 修 S2 只给「引擎内部注册路径」开了口子；业务经 MasterGame.OnMsg 注册 ≤10000 的号
// 仍必须 panic —— 否则业务可以覆盖 EMsgLogin / EMasterRoom* 等引擎内建 handler。
func TestMasterGameOnMsgStillRejectsInternalIDs(t *testing.T) {
	mg := newMasterGame(&Config{}, nil)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MasterGame.OnMsg(6001) 未 panic：业务消息号守卫被放宽了（这是给业务用户的保护）")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "business message id must be > 10000") {
			t.Fatalf("panic 信息变了：%q（期望仍是 app: business message id must be > 10000; got 6001）", msg)
		}
	}()
	(&MasterGameFacade{mg}).OnMsg(proto.EMasterRoomRegister, func(c pevent.Ctx) error { return nil })
	t.Fatal("不可达：OnMsg(6001) 应当 panic")
}
