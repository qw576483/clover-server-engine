package gobject

import (
	"context"
	"sync"
	"testing"

	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/domain/object"
)

// ============================================================================
// 自动同步广播的生命周期上下文回归
//
// 守的历史缺陷：写即推送（attachRecordNotifier / attachPropsNotifier）的回调里用的是
// 裸 context.Background() —— 既无超时、也无取消，且下游 data.SyncEntity 的
// broadcast* 把 ctx 参数**直接忽略**（形参名是 `_`）。
// 结果：对象持有者（会话 / 房间 / 停机流程）结束后广播照旧下发；
// 下游不可用时写路径也没有任何时间上界。
//
// 现在：持有者经 SetSyncContext 绑定 ctx（取消即停止广播）+ 单次超时；
// data 侧真正尊重 ctx.Err()。
// ============================================================================

// countPub 只统计发布次数。
type countPub struct {
	mu sync.Mutex
	n  int
}

func (p *countPub) Publish(_ string, _ []byte) error {
	p.mu.Lock()
	p.n++
	p.mu.Unlock()
	return nil
}

func (p *countPub) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

func TestAutoSyncBroadcastStopsAfterSyncContextCanceled(t *testing.T) {
	pub := &countPub{}
	g := New(nil, object.NewObjectID(object.TypePlayer, 1))
	g.SetNotifier(pub, "test.subject", 1, 2)
	g.EnableAutoSync()

	// 基线：持有者存活时，写字段即广播。
	g.Props().SetInt("gold", 1)
	if got := pub.count(); got != 1 {
		t.Fatalf("开启写即同步后应广播 1 次，实际 %d", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.SetSyncContext(ctx)
	g.Props().SetInt("gold", 2)
	if got := pub.count(); got != 2 {
		t.Fatalf("持有者 ctx 未取消时应继续广播，实际 %d", got)
	}

	// 持有者结束：之后的写不该再广播（原先用 Background ⇒ 照旧下发）。
	cancel()
	g.Props().SetInt("gold", 3)
	if got := pub.count(); got != 2 {
		t.Fatalf("持有者 ctx 取消后不该再广播，实际 %d 次", got)
	}

	// 重新绑定一个存活 ctx：广播恢复（可恢复，说明只是被拦截而不是被永久关掉）。
	g.SetSyncContext(context.Background())
	g.Props().SetInt("gold", 4)
	if got := pub.count(); got != 3 {
		t.Fatalf("重新绑定 ctx 后应恢复广播，实际 %d 次", got)
	}
}

// 自动同步广播用的 ctx 必须带超时（原先的裸 Background 既无超时也无取消）。
func TestAutoSyncBroadcastContextHasDeadline(t *testing.T) {
	g := New(nil, object.NewObjectID(object.TypePlayer, 2))
	ctx, cancel := g.broadcastCtx()
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("自动同步广播的 context 必须带超时（裸 context.Background() 无超时也无取消）")
	}
}

// data 侧必须真正尊重 ctx：已取消的 ctx 不许再发布。
func TestDataBroadcastHonorsCanceledContext(t *testing.T) {
	pub := &countPub{}
	se := data.NewSyncEntity(nil, pub, "test.subject", data.Key{
		Owner: data.OwnerObject, ID: "1", Type: "props",
	})
	bag := object.NewBag()
	bag.SetInt("gold", 1)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	se.BroadcastPatch(canceled, bag, 1)
	if got := pub.count(); got != 0 {
		t.Fatalf("ctx 已取消时不该发布，实际 %d 次", got)
	}
	// 脏标记必须保留（广播被丢弃 ≠ 变动已同步），换一个正常 ctx 必须能补发。
	se.BroadcastPatch(context.Background(), bag, 1)
	if got := pub.count(); got != 1 {
		t.Fatalf("换用正常 ctx 应补发变动，实际 %d 次", got)
	}
}
