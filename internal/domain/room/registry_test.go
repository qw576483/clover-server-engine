package room

import "testing"

// Register 不允许覆盖已有 owner。
//
// 缺陷形态：原实现无条件 `r.rooms[roomID] = nodeAddr`。多节点并发创建同一房间时，
// 后到的会把 owner 改写成自己，两个节点都认为自己持有该房间 —— 客户端随机落到其中一个，
// 房间状态从此分裂（且没有任何一方会发现）。
func TestRegisterDoesNotStealOwner(t *testing.T) {
	r := NewMasterRegistry()

	if !r.Register("room-1", "node-a") {
		t.Fatal("首个 owner 注册应成功")
	}
	if r.Register("room-1", "node-b") {
		t.Fatal("已有 owner 时被别的节点注册成功（覆盖了 owner）")
	}
	if got := r.Find("room-1"); got != "node-a" {
		t.Fatalf("owner = %q，应仍为 node-a", got)
	}

	// 同一节点重复注册（重连 / 消息重投）必须幂等成功。
	if !r.Register("room-1", "node-a") {
		t.Fatal("同一节点重复注册应幂等成功")
	}

	// 注销后可以被新节点接手。
	if !r.Unregister("room-1", "node-a") {
		t.Fatal("注销失败")
	}
	if !r.Register("room-1", "node-b") {
		t.Fatal("owner 注销后应可重新注册")
	}
	if got := r.Find("room-1"); got != "node-b" {
		t.Fatalf("owner = %q，应为 node-b", got)
	}
}
