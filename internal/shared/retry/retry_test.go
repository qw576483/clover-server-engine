package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 抖动必须在 MaxDelay 截断之后仍不突破上限；且「不设上限 + 抖动」不得溢出为 0（忙等）。
func TestNextDelayJitterRespectsCap(t *testing.T) {
	// 有上限：任何抖动取值都不得超过 MaxDelay。
	p := Policy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 10 * time.Second, Multiplier: 2, Jitter: 1}
	for attempt := 1; attempt <= 12; attempt++ {
		for i := 0; i < 200; i++ {
			if d := NextDelay(p, attempt); d > p.MaxDelay {
				t.Fatalf("attempt=%d 退避 %s 超过 MaxDelay=%s（抖动突破上限）", attempt, d, p.MaxDelay)
			}
		}
	}

	// 不设上限 + 抖动：结果必须为正（溢出为负会修正为 0 → 忙等）。
	// attempt 取足够大，让无抖动基值本身已越过 int64 上限。
	unbounded := Policy{MaxAttempts: 5, BaseDelay: time.Second, Multiplier: 2, Jitter: 1}
	for _, attempt := range []int{64, 200, 100000} {
		for i := 0; i < 50; i++ {
			if d := NextDelay(unbounded, attempt); d <= 0 {
				t.Fatalf("attempt=%d 不设上限时退避不应为 0/负（忙等）：%s", attempt, d)
			}
		}
	}
}

// Permanent 标记不得被 Do 剥离：外层再套一层重试时仍须识别为不可重试，
// 同时 errors.Is 仍能穿透到原始错误。
func TestDoKeepsPermanentMark(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("参数非法")
	first := func(attempt int) error { return Permanent(sentinel) }

	err := Do(ctx, Policy{MaxAttempts: 3, BaseDelay: time.Millisecond}, first)
	if err == nil {
		t.Fatal("Permanent 错误应原样返回")
	}
	if !IsPermanent(err) {
		t.Fatalf("Do 返回的错误丢失了 Permanent 标记：%v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is 应能穿透到原始错误：%v", err)
	}

	// 把该错误再交给外层 Do：必须立即终止（只调用外层一次），不重新重试。
	outerCalls := 0
	_ = Do(ctx, Policy{MaxAttempts: 3, BaseDelay: time.Millisecond}, func(int) error {
		outerCalls++
		return err
	})
	if outerCalls != 1 {
		t.Fatalf("外层 Do 应因 Permanent 标记立即终止（调用次数 = %d）", outerCalls)
	}
}
