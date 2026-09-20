package mmo

import (
	"errors"
	"reflect"
	"testing"

	"clover-server-engine/pkg/domain/data"
	"clover-server-engine/pkg/shared/proto"
)

// viewersStubScene 只实现本文件用到的两个方法（其余靠嵌入接口占位）。
type viewersStubScene struct {
	Scene
	around map[uint64][]uint64
	kinds  map[uint64]data.OwnerType
}

func (s *viewersStubScene) Around(id uint64, _ float64) []uint64 { return s.around[id] }
func (s *viewersStubScene) MemberKind(id uint64) (data.OwnerType, bool) {
	k, ok := s.kinds[id]
	return k, ok
}

func newViewersScene() *viewersStubScene {
	return &viewersStubScene{
		around: map[uint64][]uint64{
			1001: {1001, 1002, 2001, 2002, 2003}, // 2 个玩家 + 3 个假人（无连接）
		},
		kinds: map[uint64]data.OwnerType{
			1001: data.OwnerPlayer,
			1002: data.OwnerPlayer,
			2001: data.OwnerObject,
			2002: data.OwnerObject,
			2003: data.OwnerObject, // 非玩家类型（对象）也要被排除（未登记 ok=false 的分支由另一用例 delete 构造）
		},
	}
}

// fakePusher 记录推送目标；fail 里的 id 一律返回错误（模拟掉线）。
type fakePusher struct {
	calls []string
	msgID uint32
	fail  map[string]bool
}

func (f *fakePusher) PushToPlayer(playerID string, msgID uint32, _ any, _ ...proto.DeliveryMode) error {
	f.calls = append(f.calls, playerID)
	f.msgID = msgID
	if f.fail[playerID] {
		return errors.New("连接已断开")
	}
	return nil
}

// 只看观看者：假人没有连接，把它当推送目标不会报错但会重复投递
// （实测客户端因此同一条事件收到 4 份）。这条用例把过滤钉死。
func TestPlayerViewers_FiltersNonPlayers(t *testing.T) {
	s := newViewersScene()
	got := PlayerViewers(s, 1001, 96)
	want := []uint64{1001, 1002}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PlayerViewers = %v, 期望 %v（假人必须被过滤掉）", got, want)
	}
}

// 未登记类型的对象（MemberKind ok=false）同样排除：宁可不推，也不要把对象当人。
func TestPlayerViewers_SkipsUnregisteredObjects(t *testing.T) {
	s := newViewersScene()
	delete(s.kinds, 2003)
	for _, id := range PlayerViewers(s, 1001, 96) {
		if id == 2003 {
			t.Fatal("未登记实体类型的对象不该出现在观看者列表里")
		}
	}
}

// 推送目标必须是字符串（网关下行路由按字符串索引）。
func TestViewerIDs_StringForm(t *testing.T) {
	s := newViewersScene()
	got := ViewerIDs(s, 1001, 96)
	want := []string{"1001", "1002"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ViewerIDs = %v, 期望 %v", got, want)
	}
}

// 半径非法 / 场景为空时返回 nil，不 panic（业务漏传参数不该炸在推送路径上）。
func TestPlayerViewers_GuardsInvalidInput(t *testing.T) {
	if ids := PlayerViewers(nil, 1001, 96); ids != nil {
		t.Fatalf("场景为 nil 应返回 nil，实际 %v", ids)
	}
	if ids := PlayerViewers(newViewersScene(), 1001, 0); ids != nil {
		t.Fatalf("半径 <=0 应返回 nil，实际 %v", ids)
	}
}

// 一个玩家失败不中断其余推送（掉线的人不该让在场的人收不到）。
func TestPushToViewers_OneFailureDoesNotStopOthers(t *testing.T) {
	s := newViewersScene()
	p := &fakePusher{fail: map[string]bool{"1002": true}}

	sent, failed := PushToViewers(p, s, 1001, 96, DataSyncMsgID, map[string]any{"x": 1})
	if sent != 1 || failed != 1 {
		t.Fatalf("sent/failed = %d/%d, 期望 1/1", sent, failed)
	}
	if len(p.calls) != 2 {
		t.Fatalf("两个观看者都应被尝试推送，实际 %v", p.calls)
	}
	if p.msgID != DataSyncMsgID {
		t.Fatalf("消息号应为 %d，实际 %d", DataSyncMsgID, p.msgID)
	}
}

// watchers 已算好的复用路径：不重复算 AOI，只推给定名单。
func TestPushToViewersOf_UsesGivenWatchers(t *testing.T) {
	p := &fakePusher{fail: map[string]bool{}}
	sent, failed := PushToViewersOf(p, []uint64{7, 8}, 4003, map[string]any{})
	if sent != 2 || failed != 0 {
		t.Fatalf("sent/failed = %d/%d, 期望 2/0", sent, failed)
	}
	if len(p.calls) != 2 || p.calls[0] != "7" {
		t.Fatalf("推送目标应为给定 watchers 的字符串形式，实际 %v", p.calls)
	}
}

// nil 推送器不 panic（装配期可能还没接上）。
func TestPushToViewers_NilPusher(t *testing.T) {
	if sent, failed := PushToViewers(nil, newViewersScene(), 1001, 96, 1, nil); sent != 0 || failed != 0 {
		t.Fatal("nil 推送器应返回 0/0")
	}
}
