package logger

import (
	"sync"
	"sync/atomic"
	"testing"
)

// 只有一个 goroutine 能拿到「首报」—— 并发下调频闸门不能被穿。
func TestOnce_FirstCallWinsUnderConcurrency(t *testing.T) {
	OnceResetAll()
	const key = "test.once.concurrent"

	var firsts int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if Oncef(key, "同一 key 只应打一条") {
				atomic.AddInt64(&firsts, 1)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&firsts); got != 1 {
		t.Fatalf("首报次数 = %d, 期望 1（否则高频路径仍会刷屏）", got)
	}
	if !OnceSeen(key) {
		t.Fatal("OnceSeen 应为 true")
	}
}

// key 之间互不干扰：一处报过不该把另一处也静默掉。
func TestOnce_KeysAreIndependent(t *testing.T) {
	OnceResetAll()
	if !Oncef("test.once.a", "a") {
		t.Fatal("a 首次应输出")
	}
	if Oncef("test.once.a", "a") {
		t.Fatal("a 第二次不该输出")
	}
	if !Oncef("test.once.b", "b") {
		t.Fatal("b 是另一个 key，首次应输出（不能被 a 连坐）")
	}
	if got := OnceLen(); got != 2 {
		t.Fatalf("OnceLen = %d, 期望 2", got)
	}
}

// 空 key 不做去重：宁可多打，不可把后续完全不同的失败一起静默掉。
func TestOnce_EmptyKeyAlwaysLogs(t *testing.T) {
	OnceResetAll()
	if !Oncef("", "第一次") {
		t.Fatal("空 key 首次应输出")
	}
	if !Oncef("", "第二次") {
		t.Fatal("空 key 不做去重，第二次也应输出")
	}
	if OnceLen() != 0 {
		t.Fatalf("空 key 不该写进去重表，OnceLen = %d", OnceLen())
	}
}

// OnceReset 让 key 重新可报（按对象拼 key 的场景靠它避免无界增长）。
func TestOnce_Reset(t *testing.T) {
	OnceResetAll()
	const key = "test.once.reset"
	Oncef(key, "x")
	if Oncef(key, "x") {
		t.Fatal("第二次不该输出")
	}
	OnceReset(key)
	if !Oncef(key, "x") {
		t.Fatal("Reset 之后应重新输出")
	}
	if OnceSeen(key) == false {
		t.Fatal("Reset 后再报一次，OnceSeen 应为 true")
	}
}

// 级别变体都要走同一套去重（否则会出现"Warn 报过、Error 又报一遍"）。
func TestOnce_LevelVariantsShareGate(t *testing.T) {
	OnceResetAll()
	const key = "test.once.level"
	if !OnceWarnf(key, "warn") {
		t.Fatal("Warn 首次应输出")
	}
	if OnceErrorf(key, "error") {
		t.Fatal("同一 key 已被 Warn 占用，Error 不该再输出")
	}
	OnceResetAll()
	if !OnceErrorf(key, "error") {
		t.Fatal("ResetAll 之后 Error 应输出")
	}
}
