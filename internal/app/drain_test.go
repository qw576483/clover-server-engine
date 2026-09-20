package app

import (
	"strconv"
	"sync"
	"testing"
)

// 排空统计的正确性 + 锁范围。
//
// 缺陷形态：remainingLocked 在持有 d.mu 的情况下遍历全局连接表（万级连接就分配万级切片），
// 而 drain 期间每一帧 BeforeDispatch 的 drainTarget 都要取 d.mu —— 于是「数还剩几个人」
// 会把消息派发一起堵住。
//
// 修复后：全表遍历在锁外做，锁内只做减法 / 过滤。本用例守住口径（活跃 − 已迁出）
// 与并发可用性。
func TestDrainerPendingAndRemaining(t *testing.T) {
	g := &Game{connMgr: NewConnectionManager(0)}
	for _, id := range []string{"c-1", "c-2", "c-3"} {
		g.connMgr.EnsureBag(id)
	}
	d := &Drainer{g: g, mig: map[string]bool{"c-1": true}}

	if got := d.remaining(); got != 2 {
		t.Fatalf("remaining = %d，应为 2（3 条活跃 − 1 条已迁出）", got)
	}
	if got := d.pending(1); len(got) != 1 {
		t.Fatalf("pending(1) 返回 %d 条，应为 1", len(got))
	}
	all := d.pending(10)
	if len(all) != 2 {
		t.Fatalf("pending(10) 返回 %d 条，应为 2", len(all))
	}
	for _, id := range all {
		if id == "c-1" {
			t.Fatal("已迁出的连接不应再出现在 pending 里")
		}
	}

	// 并发压：读者（remaining / pending）与写者（markMigrated / setPhase）并行，
	// 覆盖 d.mu 的读写协议（原用例只有只读调用，无写者争锁）；不应死锁，
	// 也不该算出负数或漏掉待迁连接。
	g.drainer = d // markMigrated 经 g.drainer 取到本编排器（并发开始前赋值）
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				if n := d.remaining(); n < 0 || n > 3 {
					t.Errorf("remaining = %d，越界（0..3）", n)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				if n := len(d.pending(2)); n > 2 {
					t.Errorf("pending(2) 返回 %d 条，超过上限", n)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				d.setPhase("kicking")
				g.markMigrated("w-" + strconv.Itoa(j))
			}
		}()
	}
	wg.Wait()
}
