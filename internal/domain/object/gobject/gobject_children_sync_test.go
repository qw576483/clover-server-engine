package gobject

import (
	"context"
	"sync"
	"testing"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/object"
)

// ============================================================================
// 级联加载出的子对象必须有同步装配（回归）
//
// 契约：loadChildren 按持久化的子对象 id 补建 *GameObject 并挂进 childPtrs 时，
// 必须一并接上同步装配（objstore.Repository 的 Create/Load 都会接）。
// 否则：父对象写变动客户端立刻能看到，子对象（背包道具 / 容器里的卡 / 军团成员）
// 改了客户端一直不变，且没有任何报错 —— 静默不同步最难查的一种。
// ============================================================================

// recordPub 记录每次发布（subject + 是否含内容）。
type recordPub struct {
	mu    sync.Mutex
	subjs []string
}

func (p *recordPub) Publish(subject string, _ []byte) error {
	p.mu.Lock()
	p.subjs = append(p.subjs, subject)
	p.mu.Unlock()
	return nil
}

func (p *recordPub) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.subjs)
}

func newMemStore(t *testing.T) *data.Store {
	t.Helper()
	st, err := data.NewStore(data.MemoryConfig())
	if err != nil {
		t.Fatalf("NewStore(memory) 失败: %v", err)
	}
	return st
}

// 用「写子对象 → 应产生广播」来判定装配是否真的接上了（比查内部字段更接近症状）。
func TestLoadChildrenWiresSyncForCascadedChildren(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	pub := &recordPub{}

	parentID := object.NewObjectID(object.TypePlayer, 9001)
	childID := object.NewObjectID(object.TypeScene, 9002)

	// 1) 造一个"落库过"的父对象（本步只关心落库，不关心广播）：加一个子对象然后 Save。
	parent := New(store, parentID)
	parent.SetNotifier(pub, "test.subject", 11, 12)
	parent.Props().SetInt("level", 1)
	child := New(store, childID)
	child.AddRecord("bag", []string{"item"}, []object.Type{object.TypeInt})
	parent.AddChild("bag", child)
	if err := parent.Save(ctx); err != nil {
		t.Fatalf("parent.Save 失败: %v", err)
	}

	// 2) 模拟重启：新对象只从库里 Load（不经过 objstore.Repository，即走 loadChildren 补建路径）。
	reloaded := New(store, parentID)
	reloaded.SetNotifier(pub, "test.subject", 11, 12)
	reloaded.EnableAutoSync()
	if err := reloaded.Load(ctx); err != nil {
		t.Fatalf("reloaded.Load 失败: %v", err)
	}

	// 校准：父对象自身的「写即同步」必须生效 —— 否则下面的失败分不清是父还是子的问题。
	base := pub.count()
	reloaded.Props().SetInt("level", 2)
	if got := pub.count(); got <= base {
		t.Fatalf("用例校准失败：父对象写即同步没生效（%d → %d），先修这里", base, got)
	}

	kids := reloaded.Children("bag")
	if len(kids) != 1 {
		t.Fatalf("级联加载应挂上 1 个子对象，实际 %d", len(kids))
	}
	loadedKid := kids[0]

	// 3) 判定：写级联加载出来的子对象，必须产生广播。
	afterLoad := pub.count()
	loadedKid.Props().SetInt("hp", 5)
	if got := pub.count(); got <= afterLoad {
		t.Fatalf("级联加载出的子对象写变动没有广播（%d → %d）：loadChildren 没给它接同步装配", afterLoad, got)
	}

	// 4) 表级写即同步同样必须生效（AddRecord 在 autoSync 已开启时自动挂变动回调）。
	//
	// 注：级联加载出的子对象**没有** records schema —— 表结构是业务代码用 AddRecord 声明的，
	// gobject 只按已声明的表去库里读数据。这里显式声明后再验证"写表即广播"，
	// 用来确认「子对象确实处于 autoSync 模式」（只有 loadChildren 复制了装配才会如此）。
	beforeRec := pub.count()
	rec := loadedKid.AddRecord("bag", []string{"item"}, []object.Type{object.TypeInt})
	rec.AddRowValues([]object.Value{object.NewInt(1)})
	if got := pub.count(); got <= beforeRec {
		t.Fatalf("级联加载出的子对象表变动没有广播（%d → %d）：子对象未处于写即同步模式", beforeRec, got)
	}
}

// 父对象**没接**发布器时，级联加载出的子对象也必须保持"仅落库不广播"
// （不许凭空把广播能力塞给子对象）。
func TestLoadChildrenWithoutNotifierStaysSilent(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	pub := &recordPub{}

	parentID := object.NewObjectID(object.TypePlayer, 9101)
	childID := object.NewObjectID(object.TypeScene, 9102)

	// 先落库一份带子对象的数据（用"没接发布器"的方式写）。
	seed := New(store, parentID)
	kid := New(store, childID)
	seed.AddChild("bag", kid)
	if err := seed.Save(ctx); err != nil {
		t.Fatalf("seed.Save 失败: %v", err)
	}

	// 父对象不接发布器地 Load。
	parent := New(store, parentID)
	if err := parent.Load(ctx); err != nil {
		t.Fatalf("parent.Load 失败: %v", err)
	}
	kids := parent.Children("bag")
	if len(kids) != 1 {
		t.Fatalf("级联加载应挂上 1 个子对象，实际 %d", len(kids))
	}
	kids[0].Props().SetInt("hp", 1)
	if got := pub.count(); got != 0 {
		t.Fatalf("父对象未接发布器时子对象不该广播，实际 %d 次", got)
	}
}
