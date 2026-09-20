package mmo

import (
	"sync"
	"testing"
)

// isLocal 无锁读 sm.scenes 时，与 CreateScene / DestroyScene 的并发写会触发
// Go 运行时的 `fatal error: concurrent map read and map write`（这条不需要 -race 也能抓到，
// 而且是不可 recover 的进程级崩溃）。
//
// 本用例把「跨机迁移的目标判定」与「场景增删」并发跑起来，守住 isLocal 的读锁。
func TestSceneManagerIsLocalConcurrentWithSceneLifecycle(t *testing.T) {
	sm := NewSceneManager()
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < 5000; i++ {
			id := uint64(i%8 + 1)
			if s := sm.CreateScene(id, "s"); s == nil {
				t.Error("CreateScene 返回 nil")
				return
			}
			sm.DestroyScene(id)
		}
	}()

	// 读侧：TransferRemote 的入口判定。只压这一处，让它尽可能与写侧的 map 写窗口重叠。
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				_ = sm.isLocal(1)
			}
		}()
	}
	wg.Wait()

	// 读侧并行查询也应安全（走锁，无 map 竞争）。
	var wg2 sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for i := 0; i < 500; i++ {
				_, _ = sm.GetScene(uint64(i%8 + 1))
			}
		}()
	}
	wg2.Wait()
}
