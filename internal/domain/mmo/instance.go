package mmo

import (
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	internalaoi "github.com/qw576483/clover-server-engine/internal/domain/mmo/spatial/aoi"
	"github.com/qw576483/clover-server-engine/internal/domain/object"
)

// Instance 是 Scene 内的一个隔离实例：独立的 AOI 网格 + 物理体 + 实体类型记录。
// 同一 Instance 内实体通过 AOI 互相可见；跨 Instance 互不可见但共享 Scene 物理碰撞。
type Instance struct {
	id     uint32
	scene  *Scene
	grid   *internalaoi.Grid         // 独立的 AOI 网格（视野 + 成员）
	bodies map[uint64]*Body          // 物理体
	kinds  map[uint64]data.OwnerType // 实体类型
	mu     sync.RWMutex
}

func newInstance(scene *Scene, id uint32) *Instance {
	i := &Instance{
		id:    id,
		scene: scene,
		// AOI 按三维球体判定视野（Y 参与距离）；2D 玩法把 Y 固定为 0 即可。
		grid:   internalaoi.New(scene.sm.opts.cellSize),
		bodies: make(map[uint64]*Body),
		kinds:  make(map[uint64]data.OwnerType),
	}
	i.grid.SetObserver(func(watcher, target object.ObjectID, ev internalaoi.Event) {
		scene.onViewChange(i.id, watcher, target, ev)
	})
	return i
}

// ID 返回实例 id。
func (i *Instance) ID() uint32 { return i.id }

// Bodies 返回物理体快照。
//
// 返回的是 **Body 值拷贝**（map[uint64]*Body 的指针仍指向新对象），不是浅拷贝：
// 浅拷贝会把 physicsStep / ApplyForce / SetVelocity 正在 l.mu 内改写的同一个 *Body 交给
// 调用方，调用方在锁外读 Position/Velocity 就是与物理线程并发读写同一块内存。
func (i *Instance) Bodies() map[uint64]*Body {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make(map[uint64]*Body, len(i.bodies))
	for k, v := range i.bodies {
		if v == nil {
			out[k] = nil
			continue
		}
		cp := *v
		out[k] = &cp
	}
	return out
}

// Members 返回本实例全部成员的 objID（与 kinds 记录一致）。
func (i *Instance) Members() []uint64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]uint64, 0, len(i.kinds))
	for id := range i.kinds {
		out = append(out, id)
	}
	return out
}

// memberIDs 返回本实例成员的 object.ObjectID 列表（用于 AOI 网格操作）。
func (i *Instance) memberIDs() []object.ObjectID {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]object.ObjectID, 0, len(i.kinds))
	for id := range i.kinds {
		out = append(out, object.NewObjectID(object.TypePlayer, id))
	}
	return out
}
