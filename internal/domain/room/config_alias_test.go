package room

import (
	"reflect"
	"testing"

	iframe "github.com/qw576483/clover-server-engine/internal/domain/room/frame"
	proom "github.com/qw576483/clover-server-engine/pkg/domain/room"
	pframe "github.com/qw576483/clover-server-engine/pkg/domain/room/frame"
)

// configFieldNames 是 room.Config 的对外字段契约（顺序即声明顺序）。
// 业务按这些字段名构造成 Config，改名/删字段都是破坏性变更，必须在这里炸掉。
var configFieldNames = []string{
	"MasterCaller", "Pusher", "NodeAddr", "Kernel", "FrameCfg", "FrameSvc", "FrameSvcOpts",
}

// TestConfigIsPkgTypeAlias 钉住「Config 真身只有一份」这条不变量：
// 把别名再拆回 struct 时本测试立刻失败（那会引入字段静默漂移）。
func TestConfigIsPkgTypeAlias(t *testing.T) {
	local, pkg := reflect.TypeOf(Config{}), reflect.TypeOf(proom.Config{})
	if local != pkg {
		t.Fatalf("internal room.Config 必须是 pkg/domain/room.Config 的类型别名；"+
			"当前是两份不同类型（%v / %v）—— 重新拆出 struct 会引入字段静默漂移", local, pkg)
	}
}

// TestPkgConfigPublicContract 钉住 Config 的对外可用面：字段名与 FrameSvc 的类型。
// FrameSvc 必须是门面接口 frame.Service。
func TestPkgConfigPublicContract(t *testing.T) {
	pkg := reflect.TypeOf(proom.Config{})
	if pkg.NumField() != len(configFieldNames) {
		t.Fatalf("Config 字段数 = %d，期望 %d（对外契约变更需同步本测试）", pkg.NumField(), len(configFieldNames))
	}
	for i, name := range configFieldNames {
		if got := pkg.Field(i).Name; got != name {
			t.Fatalf("Config 第 %d 个字段 = %s，期望 %s", i, got, name)
		}
	}
	f, ok := pkg.FieldByName("FrameSvc")
	if !ok {
		t.Fatal("Config 缺少 FrameSvc 字段（对外契约被改）")
	}
	if want := reflect.TypeOf((*pframe.Service)(nil)).Elem(); f.Type != want {
		t.Fatalf("Config.FrameSvc 的类型 = %v，期望门面接口 %v", f.Type, want)
	}
}

// TestModuleFacadeKeepsPrebuiltFrameService 覆盖 Config.FrameSvc 的还原路径。
//
// internal 的 Config 改成 pkg 别名后，「门面接口 frame.Service → 内部具体 *iframe.Service」
// 的还原从 pkgfacade 的逐字段拷贝搬进了 NewModule（unwrapFrameService）。
// 这一步若做漏，业务预构造的服务会被静默丢弃：Module 不报错、另建一个空 Service，
// 业务注入的 InputApplier / 监听全部消失 —— 故必须有回归。
func TestModuleFacadeKeepsPrebuiltFrameService(t *testing.T) {
	inner := iframe.NewService(iframe.WithDefaultRoomConfig(iframe.DefaultConfig()))
	svc := iframe.Facade(inner)

	mod := newModuleFacade(proom.Config{NodeAddr: "node-a", FrameSvc: svc})
	if mod == nil {
		t.Fatal("newModuleFacade 返回 nil")
	}
	facadeFrame := mod.Frame()
	if facadeFrame == nil {
		t.Fatal("预构造的 FrameSvc 被丢弃：Module 未挂帧同步内核")
	}
	got, ok := iframe.UnwrapService(facadeFrame)
	if !ok || got != inner {
		t.Fatal("Frame() 还原出的不是业务预构造的那个服务实例（FrameSvc 传递丢失）")
	}
	if err := mod.EnsureRoom("room-prebuilt"); err != nil {
		t.Fatalf("EnsureRoom（预构造服务路径）失败：%v", err)
	}
}
