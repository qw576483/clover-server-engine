package mmo

import (
	"encoding/json"
	"testing"

	"clover-server-engine/pkg/shared/proto"
)

// 消息号必须钉死在数值 4003（= 引擎推送 EPushDataSync）：客户端 WorldSync 按字面量消费，
// 换号即静默收不到。
//
// 不能只比较 `DataSyncMsgID != proto.EPushDataSync`：DataSyncMsgID 本就定义为
// proto.EPushDataSync（datasync.go），左右是同一常量、条件恒 false —— 即便
// EPushDataSync 被整体改号（双端契约被破坏），该断言依旧全绿，测不出任何问题。
// 这里同时断言字面数值与「与 proto 常量一致」，改号任一环节都会红。
func TestDataSyncMsgIDIsEnginePush(t *testing.T) {
	const wireDataSyncMsgID = 4003
	if DataSyncMsgID != wireDataSyncMsgID {
		t.Fatalf("DataSyncMsgID = %d, 必须等于线上契约值 %d", DataSyncMsgID, wireDataSyncMsgID)
	}
	if DataSyncMsgID != proto.EPushDataSync {
		t.Fatalf("DataSyncMsgID = %d, 必须等于 proto.EPushDataSync(%d)", DataSyncMsgID, proto.EPushDataSync)
	}
	if DataSyncEventMove != "move" || DataSyncEventProperty != "property" {
		t.Fatalf("事件名被改动（客户端按事件名分派）：move=%q property=%q",
			DataSyncEventMove, DataSyncEventProperty)
	}
}

// 位置包壳的**线上形状**逐个字段钉死：外层 key 是事件名，内层 entity_id + position{x,y,z}。
// 用 JSON 往返断言（而不是比较 map 结构），因为真正上线的是 JSON。
func TestNewMovePatchWireShape(t *testing.T) {
	body := NewMovePatch(1001, Vec3{X: 12.5, Y: 1, Z: -3.25})
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	var got struct {
		Move struct {
			EntityID uint64 `json:"entity_id"`
			Position struct {
				X, Y, Z float64
			} `json:"position"`
		} `json:"move"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("反序列化失败: %v (raw=%s)", err, raw)
	}
	if got.Move.EntityID != 1001 {
		t.Errorf("entity_id = %d, 期望 1001", got.Move.EntityID)
	}
	if got.Move.Position.X != 12.5 || got.Move.Position.Y != 1 || got.Move.Position.Z != -3.25 {
		t.Errorf("position = (%v,%v,%v), 期望 (12.5,1,-3.25)",
			got.Move.Position.X, got.Move.Position.Y, got.Move.Position.Z)
	}
}

// 属性包壳：外层 key=property，内层 entity_id + properties{…}（键名就是客户端的属性名）。
func TestNewPropertyPatchWireShape(t *testing.T) {
	body := NewPropertyPatch(2001, map[string]any{"hp": 88, "max_hp": 100})
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	var got struct {
		Property struct {
			EntityID   uint64         `json:"entity_id"`
			Properties map[string]any `json:"properties"`
		} `json:"property"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("反序列化失败: %v (raw=%s)", err, raw)
	}
	if got.Property.EntityID != 2001 {
		t.Errorf("entity_id = %d, 期望 2001", got.Property.EntityID)
	}
	if len(got.Property.Properties) != 2 || got.Property.Properties["hp"] != float64(88) {
		t.Errorf("properties = %v, 期望 hp=88 + max_hp=100", got.Property.Properties)
	}
}

// 两种包壳的外层 key 不能串（接错会让位置被当属性派发，反之亦然）。
func TestPatchKindsDoNotMix(t *testing.T) {
	move := NewMovePatch(1, Vec3{})
	if _, hasProp := move[DataSyncEventProperty]; hasProp {
		t.Fatal("位置包壳里不该出现 property 键")
	}
	prop := NewPropertyPatch(1, map[string]any{"hp": 1})
	if _, hasMove := prop[DataSyncEventMove]; hasMove {
		t.Fatal("属性包壳里不该出现 move 键")
	}
}
