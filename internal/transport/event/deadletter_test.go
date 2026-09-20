package event

import (
	"context"
	"fmt"
	"testing"
)

// MemoryDLQ 的容量淘汰改成「前移 head + 摊还压缩」后，行为必须与原来完全一致：
// 容量封顶、丢最老、同 ID 覆盖、List 倒序、Remove 可从队首或中间删、索引始终自洽。
// 本用例就是这次内部改写的守卫。
func TestMemoryDLQCapacityAndOrder(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryDLQ(3)

	for _, id := range []string{"a", "b", "c"} {
		if err := q.Push(ctx, DeadLetter{ID: id}); err != nil {
			t.Fatalf("Push %s 失败: %v", id, err)
		}
	}
	if got := q.Len(); got != 3 {
		t.Fatalf("Len = %d，应为 3", got)
	}

	// 第 4 条：最老的 a 被淘汰。
	if err := q.Push(ctx, DeadLetter{ID: "d"}); err != nil {
		t.Fatalf("Push d 失败: %v", err)
	}
	if got := q.Len(); got != 3 {
		t.Fatalf("淘汰后 Len = %d，应仍为 3", got)
	}
	if got := q.Dropped(); got != 1 {
		t.Fatalf("Dropped = %d，应为 1", got)
	}
	assertIDs(t, q, "d", "c", "b")

	// 同 ID 覆盖：不增长、不改变顺序。
	if err := q.Push(ctx, DeadLetter{ID: "b", MsgID: "overwritten"}); err != nil {
		t.Fatalf("覆盖 Push 失败: %v", err)
	}
	if got := q.Len(); got != 3 {
		t.Fatalf("同 ID 覆盖后 Len = %d，应为 3", got)
	}
	list, _ := q.List(ctx, 0)
	if list[2].MsgID != "overwritten" {
		t.Fatalf("最老一条未被覆盖：%+v", list[2])
	}

	// 删中间一条（b）。
	if err := q.Remove(ctx, "b"); err != nil {
		t.Fatalf("Remove b 失败: %v", err)
	}
	assertIDs(t, q, "d", "c")

	// 删最老一条（head 路径）。
	if err := q.Remove(ctx, "c"); err != nil {
		t.Fatalf("Remove c 失败: %v", err)
	}
	assertIDs(t, q, "d")

	// 大量涌入，跨过多次「摊还压缩」与淘汰，索引必须始终自洽：
	// 每条 List 出来的记录都应能被 Remove。
	for i := 0; i < 200; i++ {
		if err := q.Push(ctx, DeadLetter{ID: fmt.Sprintf("n%d", i)}); err != nil {
			t.Fatalf("Push n%d 失败: %v", i, err)
		}
	}
	if got := q.Len(); got != 3 {
		t.Fatalf("高压后 Len = %d，应为 3", got)
	}
	list, _ = q.List(ctx, 0)
	for _, dl := range list {
		if err := q.Remove(ctx, dl.ID); err != nil {
			t.Fatalf("List 返回的 %s 无法删除（索引与数组不一致）: %v", dl.ID, err)
		}
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("全部删除后 Len = %d，应为 0", got)
	}
}

func assertIDs(t *testing.T, q *MemoryDLQ, want ...string) {
	t.Helper()
	list, err := q.List(context.Background(), 0)
	if err != nil {
		t.Fatalf("List 失败: %v", err)
	}
	if len(list) != len(want) {
		t.Fatalf("List 条数 = %d，应为 %d（%v）", len(list), len(want), want)
	}
	for i, id := range want {
		if list[i].ID != id {
			t.Fatalf("List[%d] = %s，应为 %s（最新在前）", i, list[i].ID, id)
		}
	}
}
