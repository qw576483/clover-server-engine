package app

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"clover-server-engine/pkg/runtime/timer"
)

// 共享调度器必须**只建一次**。
//
// 缺陷形态：ensure() 无同步，并发首次调用（Scheduler / Every / After / Cron …）各建一个
// Scheduler —— 而 NewScheduler 会立即起一条调度 goroutine；后建的覆盖字段，
// 先建的那个连同其上注册的任务一起失联（任务永不触发、goroutine 无人关闭）。
func TestTimeEventSchedulerIsCreatedOnce(t *testing.T) {
	te := NewTimeEvent(time.UTC)
	const n = 32

	got := make([]*timer.Scheduler, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i] = te.Scheduler()
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if got[i] != got[0] {
			t.Fatalf("第 %d 个 goroutine 拿到了另一个 Scheduler：并发首次调用建了多份", i)
		}
	}

	// 并发注册的具名任务必须都落在同一个调度器上：StopTimer 找得到即为证明
	// （任务若落在被覆盖的调度器上，这个名字就查不到）。
	const jobs = 16
	var wg2 sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg2.Add(1)
		go func(name string) {
			defer wg2.Done()
			te.Every(name, time.Hour, func() {})
		}("job-" + strconv.Itoa(i))
	}
	wg2.Wait()
	for i := 0; i < jobs; i++ {
		if !te.StopTimer("job-" + strconv.Itoa(i)) {
			t.Fatalf("job-%d 不在当前调度器上（任务丢了）", i)
		}
	}
	te.Close()
}
