package gobject

import (
	"sync"
	"testing"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
)

// fakePub 只用于让同步实体非 nil（本用例不关心投递结果）。
type fakePub struct{}

func (fakePub) Publish(string, []byte) error { return nil }

// AddRecord 与 SetNotifier / EnableAutoSync / DisableAutoSync 并发。
//
// 缺陷形态：「是否自动同步」的判定若在锁外取，就能取到「已开启 + 同步器随后被置 nil」
// 的组合，于是把 nil 同步器捕获进表回调 —— 之后任何一次写表都在
// BroadcastRecordPatch 上崩（空指针，业务侧表现为偶发进程崩溃）。
//
// 修复后：判定在锁内一次取完，且 attachRecordNotifier 对 nil 同步器直接不挂回调。
func TestAddRecordConcurrentWithNotifierSwitch(t *testing.T) {
	g := New(nil, object.NewObjectID(object.TypePlayer, 1))
	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < 2000; i++ {
			r := g.AddRecord("bag", []string{"n"}, []object.Type{object.TypeInt})
			if r == nil {
				t.Error("AddRecord 返回 nil")
				return
			}
			// 触发一次表变动：同步器若被错误地捕获为 nil，崩溃点就在这一行里面。
			r.AddRow()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			g.SetNotifier(fakePub{}, "subject", 1, 2)
			g.EnableAutoSync()
			g.SetNotifier(nil, "", 0, 0)
			g.DisableAutoSync()
		}
	}()
	wg.Wait()
}
