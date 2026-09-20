package state

import (
	"context"
	"testing"
)

// 节点换类型重注册时，旧类型的索引必须一并摘掉。
//
// 缺陷形态：RegisterNode 只清 byTag，byType 只做 add；而 NodesByType / NodeIDsByType
// 只校验「节点还在」、不校验类型一致 —— 于是旧类型下会长期挂着一个幽灵节点，
// 按类型选节点做转发时就会投到一台不干这活儿的机器上。
func TestRegisterNodeTypeChangeClearsOldTypeIndex(t *testing.T) {
	st := NewState()
	ctx := context.Background()

	if err := st.RegisterNode(ctx, Node{ID: "n1", Addr: "n1", Type: NodeTypeGame, Tags: []string{"a"}}); err != nil {
		t.Fatalf("首次注册失败: %v", err)
	}
	if got := st.NodeIDsByType(NodeTypeGame); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("首次注册后 game 索引 = %v，应为 [n1]", got)
	}

	// 同一个 nodeID 换类型重注册（tags 也一并换掉）。
	if err := st.RegisterNode(ctx, Node{ID: "n1", Addr: "n1", Type: NodeTypeBattle, Tags: []string{"b"}}); err != nil {
		t.Fatalf("换类型重注册失败: %v", err)
	}
	if got := st.NodeIDsByType(NodeTypeGame); len(got) != 0 {
		t.Fatalf("换类型后 game 索引残留幽灵节点: %v", got)
	}
	if got := st.NodesByType(NodeTypeGame); len(got) != 0 {
		t.Fatalf("换类型后 NodesByType(game) = %v，应为空", got)
	}
	if got := st.NodeIDsByType(NodeTypeBattle); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("battle 索引 = %v，应为 [n1]", got)
	}
	if got := st.NodesByType(NodeTypeBattle); len(got) != 1 || got[0].Type != NodeTypeBattle {
		t.Fatalf("NodesByType(battle) = %v，应只有一个 battle 节点", got)
	}

	// 旧的 tag 索引同样要清干净。
	if got := st.NodeIDsByTag("a"); len(got) != 0 {
		t.Fatalf("旧 tag 索引残留: %v", got)
	}
	if got := st.NodeIDsByTag("b"); len(got) != 1 {
		t.Fatalf("新 tag 索引 = %v，应为 1 个", got)
	}

	// 摘除后三类索引都不能留残渣。
	if err := st.RemoveNode(ctx, "n1"); err != nil {
		t.Fatalf("摘除失败: %v", err)
	}
	if len(st.AllNodes()) != 0 || len(st.NodeIDsByType(NodeTypeBattle)) != 0 || len(st.NodeIDsByTag("b")) != 0 {
		t.Fatal("摘除后仍有索引残留")
	}
}
