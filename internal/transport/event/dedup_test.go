package event

import (
	"context"
	"testing"
	"time"
)

// 接收侧判重的两条语义必须同时成立：
//   - Contains：**只读**判重，用于 dispatch 之前拦截重复（不能占位，否则失败重投会被误判为重复）；
//   - Seen：原子「检查并占位」，用于 dispatch 成功之后登记。
func TestDeduperSeenAndContains(t *testing.T) {
	ctx := context.Background()
	d := NewLocalDeduper(8, time.Minute)

	if d.Contains(ctx, "m1") {
		t.Fatal("未登记的 msgID 不应被判为已见")
	}
	if d.Seen(ctx, "m1") {
		t.Fatal("首次 Seen 应返回 false（并完成登记）")
	}
	if !d.Contains(ctx, "m1") {
		t.Fatal("Seen 登记后 Contains 应为 true")
	}
	if !d.Seen(ctx, "m1") {
		t.Fatal("重复 Seen 应返回 true")
	}

	// Contains 不得产生登记副作用：未登记的键查过之后，Seen 仍应判为首次。
	if d.Contains(ctx, "m2") {
		t.Fatal("未登记的 msgID 不应被判为已见")
	}
	if d.Seen(ctx, "m2") {
		t.Fatal("Contains 不应登记：Seen 仍应返回 false")
	}

	// 空 msgID 直接放行（无从去重）。
	if d.Contains(ctx, "") || d.Seen(ctx, "") {
		t.Fatal("空 msgID 不应参与去重")
	}

	// 链式：任一级命中即判已见。
	chain := NewChainDeduper(d, NopDeduper())
	if !chain.Contains(ctx, "m1") {
		t.Fatal("链式 Contains 应命中外层已登记的键")
	}
	if chain.Contains(ctx, "m3") {
		t.Fatal("链式 Contains 对未登记键应返回 false")
	}
}
