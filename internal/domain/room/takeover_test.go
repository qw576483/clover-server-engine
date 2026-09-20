package room

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"clover-server-engine/internal/shared/proto"
	proom "clover-server-engine/pkg/domain/room"
)

// fakeMaster 是 CallMaster 的桩：模拟 master 侧的房间注册表 + 暂存接管状态。
type fakeMaster struct {
	mu            sync.Mutex
	owner         string // 当前 owner（Reassign 后 = takeover 目标节点）
	state         TakeoverState
	registerOK    bool
	registerCalls int
	claimCalls    int
}

func (f *fakeMaster) CallMaster(msgID uint32, req, resp any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	out, _ := resp.(*OwnerResp)
	switch msgID {
	case proto.EMasterRoomFind:
		r, _ := req.(*OwnerFindReq)
		if out != nil {
			*out = OwnerResp{OK: f.owner != "", RoomID: r.RoomID, NodeAddr: f.owner}
		}
	case proto.EMasterRoomTakeoverClaim:
		f.claimCalls++
		if out != nil {
			*out = OwnerResp{OK: true, State: f.state.State, Recovery: f.state.Recovery, Frame: f.state.Frame, Hash: f.state.Hash}
		}
		f.state = TakeoverState{} // master 认领即弹出
	case proto.EMasterRoomRegister:
		f.registerCalls++
		if out != nil {
			*out = OwnerResp{OK: f.registerOK}
		}
	default:
		return fmt.Errorf("unexpected msgID %d", msgID)
	}
	return nil
}

func (f *fakeMaster) SwitchUpstream(connID, targetAddr string) error { return nil }

func (f *fakeMaster) setRegisterOK(ok bool) {
	f.mu.Lock()
	f.registerOK = ok
	f.mu.Unlock()
}

func (f *fakeMaster) calls() (register, claim int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registerCalls, f.claimCalls
}

// fakeKernel 记录 ImportState 调用次数，并可指定房间玩家（恢复包收件人）。
type fakeKernel struct {
	mu          sync.Mutex
	importCalls int
	players     []string
}

func (k *fakeKernel) EnsureRoom(roomID string) error      { return nil }
func (k *fakeKernel) Join(roomID, playerID string) error  { return nil }
func (k *fakeKernel) Leave(roomID, playerID string) error { return nil }
func (k *fakeKernel) Destroy(roomID string) error         { return nil }
func (k *fakeKernel) ExportState(roomID string) (proom.ExportPack, error) {
	return proom.ExportPack{}, nil
}
func (k *fakeKernel) ImportState(roomID string, state json.RawMessage) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.importCalls++
	return nil
}
func (k *fakeKernel) Players(roomID string) []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.players...)
}
func (k *fakeKernel) Close() {}

func (k *fakeKernel) imports() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.importCalls
}

// TestTakeoverRecoveryOnlyPackDoesNotLoop：只有 Recovery、没有 State 的接管包
// 必须走完流程（跳过导入 + 注册 owner + 下发恢复包 + 清 pending），
// 不能拿空 State 反复调内核 ImportState（会被内核拒绝）导致每次 Activate 重试、pendingState 永久滞留。
func TestTakeoverRecoveryOnlyPackDoesNotLoop(t *testing.T) {
	master := &fakeMaster{owner: "node-b", registerOK: true}
	master.state = TakeoverState{Recovery: json.RawMessage(`{"snap":1}`)}
	kernel := &fakeKernel{players: []string{"p1"}}
	var pushed []string
	pusher := func(playerID string, msgID uint32, payload interface{}) error {
		if msgID != proto.EPushRoomTakeover {
			t.Errorf("推送消息号 = %d, 期望 %d", msgID, proto.EPushRoomTakeover)
		}
		pushed = append(pushed, playerID)
		return nil
	}
	m := NewRoomTakeoverManager(TakeoverDeps{
		Kernel:     func() proom.Kernel { return kernel },
		CallMaster: master.CallMaster,
		Pusher:     pusher,
		NodeAddr:   "node-b",
		Owner:      NewOwnerClient(master),
	})

	if err := m.Activate("room-1"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if got := kernel.imports(); got != 0 {
		t.Fatalf("空 State 不应调用 ImportState，实际 %d 次", got)
	}
	if register, _ := master.calls(); register == 0 {
		t.Fatal("owner 未注册（RegisterRoom 未被调用）")
	}
	if len(pushed) != 1 || pushed[0] != "p1" {
		t.Fatalf("恢复包未下发到房间玩家，pushed=%v", pushed)
	}
	if m.hasPending("room-1") {
		t.Fatal("pendingState 未清理（会永久滞留并反复重试）")
	}

	// 房间操作前会反复触发 Activate：不应产生任何重复副作用。
	if err := m.Activate("room-1"); err != nil {
		t.Fatalf("Activate#2: %v", err)
	}
	if got := kernel.imports(); got != 0 {
		t.Fatalf("重复激活不应导入，实际 %d 次", got)
	}
	if len(pushed) != 1 {
		t.Fatalf("重复激活不应重复推送恢复包，pushed=%v", pushed)
	}
}

// TestTakeoverRegisterRetryDoesNotReimport：注册 owner 失败时 pendingState 必须保留
// （等下次 Activate 重试注册），且重试**不得重复导入内核态**（重复导入会把房间回退到旧帧）。
func TestTakeoverRegisterRetryDoesNotReimport(t *testing.T) {
	master := &fakeMaster{owner: "node-b", registerOK: false}
	master.state = TakeoverState{State: json.RawMessage(`{"frame":3}`)}
	kernel := &fakeKernel{}
	m := NewRoomTakeoverManager(TakeoverDeps{
		Kernel:     func() proom.Kernel { return kernel },
		CallMaster: master.CallMaster,
		Pusher:     func(playerID string, msgID uint32, payload interface{}) error { return nil },
		NodeAddr:   "node-b",
		Owner:      NewOwnerClient(master),
	})

	if err := m.Activate("room-2"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if got := kernel.imports(); got != 1 {
		t.Fatalf("应导入 1 次，实际 %d 次", got)
	}
	if !m.hasPending("room-2") {
		t.Fatal("注册失败时 pendingState 被丢弃，owner 注册无法重试")
	}

	// 注册恢复后重试：只重试注册，不重复导入。
	master.setRegisterOK(true)
	if err := m.Activate("room-2"); err != nil {
		t.Fatalf("Activate#2: %v", err)
	}
	if got := kernel.imports(); got != 1 {
		t.Fatalf("重试注册不应重复导入，ImportState 调用 %d 次", got)
	}
	if m.hasPending("room-2") {
		t.Fatal("注册成功后 pendingState 应清理")
	}
}
