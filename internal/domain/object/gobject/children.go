// 子对象（容器 / 嵌套对象）支持：MMO 精髓的延伸。

// MMO 的 GameObject 不是扁平的——它挂着子对象：玩家背包里的道具、卡牌容器里的卡、军团成员、
// 场景里的实体……每个子对象本身仍是完整的 GameObject（自带 ObjectID/props/records/schema/同步），
// 可被 Manager 按号注册。父对象只维护「槽位(slot) → 子对象 id 列表」的归属关系，
// 并在 Save/Load/Delete 时级联到子对象。

// slot 类似 MMO 的「容器类型 / script」：一个玩家可以同时有 "bag"（背包道具）、"hero"（卡牌）等槽位；
// 每个槽位下挂若干子对象。子对象的数据仍按其自身 ObjectID 落库（Key{OwnerObject, childID, ...}），
// 与父对象解耦——这正是「按 id 改任意对象」统一基础的自然延伸。
// 嵌套对象（如某玩家背包第 7 格道具）」变得 trivial。
package gobject

import (
	"context"
	"sort"
	"sync"

	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/domain/object"
	"clover-server-engine/pkg/shared/util"
)

// childIndexWire 子对象归属索引的线化结构：slot → 子对象 id（"type:seq"）列表。
type childIndexWire struct {
	Slots map[string][]string `json:"slots"`
}

// childGraphMu 串行化「跨对象的子对象挂载」。

// 环检测需要同时查看多个对象（child 及其子孙）的子对象表。若一边持有本对象的写锁、一边去获取
// 其它对象的锁，双向并发挂载（A 挂 B 与 B 挂 A）就会形成 A→B 与 B→A 的锁序反转而死锁。

// 故所有 AddChild 统一先取本锁再取对象锁，锁序恒定为 childGraphMu → 对象 mu；
// 且挂载过程中除本对象外一律只取读锁，绝不嵌套等待其它对象的写锁。
// 单对象操作（RemoveChild / Children / 各级联持久化）不经过本锁，不受影响。
var childGraphMu sync.Mutex

// AddChild 把 child 挂到命名槽位 slot（如 "bag" / "hero"）。
// child 须已构造好（含自身 ObjectID 与 records 定义）。同一 (slot, child.ObjectID()) 不重复加入。
// 挂上后父对象的 Save/Load/Delete 会级联到它；child 自身仍是独立 GameObject，可单独 Manager 注册 / 同步。

// 检测到会形成环（child 的子孙中已含 g）时静默放弃挂载，不改动任何状态。
func (g *GameObject) AddChild(slot string, child *GameObject) {
	if child == nil {
		return
	}
	if g == child {
		return
	}
	cid := child.ObjectID()
	// 先取全局子图锁：环检测与挂载作为一个整体串行执行，
	// 既避免跨对象加锁的锁序反转，也避免两个相反方向的挂载各自通过检测后拼出环。
	childGraphMu.Lock()
	defer childGraphMu.Unlock()
	// 环检测：遍历 child 的子孙，检查是否引用了 g
	if g.detectCycleLocked(child, g) {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// 同一 (slot, cid) 去重
	for _, id := range g.childIDs[slot] {
		if id == cid {
			g.childPtrs[cid] = child
			return
		}
	}
	g.childIDs[slot] = append(g.childIDs[slot], cid)
	g.childPtrs[cid] = child
}

// detectCycleLocked 在 childGraphMu 已持有的前提下，BFS 遍历 child 的子孙，检查是否有节点引用 self。
// 遍历途中对每个对象只取读锁，且读锁绝不嵌套等待另一把写锁。
func (g *GameObject) detectCycleLocked(child *GameObject, self *GameObject) bool {
	visited := make(map[object.ObjectID]bool)
	queue := []*GameObject{child}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		// 先读快照
		cur.mu.RLock()
		var ids []object.ObjectID
		for _, cs := range cur.childIDs {
			ids = append(ids, cs...)
		}
		for _, id := range ids {
			c, ok := cur.childPtrs[id]
			if !ok || visited[id] {
				continue
			}
			visited[id] = true
			if c == self {
				cur.mu.RUnlock()
				return true
			}
			queue = append(queue, c)
		}
		cur.mu.RUnlock()
	}
	return false
}

// Child 取槽位 slot 下 id 为 cid 的子对象（仅返回内存中已挂上的；未挂返回 false）。
func (g *GameObject) Child(slot string, cid object.ObjectID) (*GameObject, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	c, ok := g.childPtrs[cid]
	return c, ok && util.Contains(g.childIDs[slot], cid)
}

// Children 返回槽位 slot 下所有已挂子对象，按子对象 id 升序（便于稳定展示 / 遍历）。
func (g *GameObject) Children(slot string) []*GameObject {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := g.childIDs[slot]
	out := make([]*GameObject, 0, len(ids))
	for _, id := range ids {
		if c, ok := g.childPtrs[id]; ok {
			out = append(out, c)
		}
	}
	return out
}

// ChildSlots 返回所有「有子对象」的槽位名（升序）。
func (g *GameObject) ChildSlots() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]string, 0, len(g.childIDs))
	for s, ids := range g.childIDs {
		if len(ids) > 0 {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// ChildIDs 返回槽位 slot 下全部子对象 id（含持久化归属，无论当前是否挂在内存）。
// 业务可据此遍历、加载或寻址任意嵌套对象。
func (g *GameObject) ChildIDs(slot string) []object.ObjectID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := g.childIDs[slot]
	out := make([]object.ObjectID, len(ids))
	copy(out, ids)
	return out
}

// RemoveChild 从槽位 slot 移除 id 为 cid 的子对象（仅移除归属关系，不删其数据；
// 要删数据请对子对象调用 child.Delete）。返回是否确实有移除。
func (g *GameObject) RemoveChild(slot string, cid object.ObjectID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	ids := g.childIDs[slot]
	kept := ids[:0]
	found := false
	for _, id := range ids {
		if id == cid {
			found = true
			continue
		}
		kept = append(kept, id)
	}
	if !found {
		return false
	}
	g.childIDs[slot] = kept
	delete(g.childPtrs, cid)
	return true
}

// 子对象归属索引的持久化
func (g *GameObject) childIndexKey() data.Key {
	return data.Key{Owner: data.OwnerObject, ID: g.id.String(), Type: "childindex"}
}

// saveChildren 持久化 slot→childIDs 归属索引，并级联 Save 所有内存中已挂的子对象
// （递归：子对象若也挂了孙对象，会一并落库）。
func (g *GameObject) saveChildren(ctx context.Context) error {
	// 先快照归属索引与子对象指针（持锁仅做内存拷贝），I/O 在锁外进行，避免持锁做递归 Save。
	g.mu.RLock()
	w := childIndexWire{Slots: make(map[string][]string, len(g.childIDs))}
	for slot, ids := range g.childIDs {
		if len(ids) == 0 {
			continue
		}
		ss := make([]string, 0, len(ids))
		for _, id := range ids {
			ss = append(ss, id.String())
		}
		w.Slots[slot] = ss
	}
	kids := make([]*GameObject, 0, len(g.childPtrs))
	for _, c := range g.childPtrs {
		kids = append(kids, c)
	}
	g.mu.RUnlock()
	if err := g.store.SaveJSON(ctx, g.childIndexKey(), &w); err != nil {
		return err
	}
	for _, c := range kids {
		if err := c.Save(ctx); err != nil {
			return err
		}
	}
	return nil
}

// loadChildren 载入 slot→childIDs 归属索引（使 ChildIDs/ChildSlots 反映持久化状态），
// 并把持久化的每个子对象装配进内存（childPtrs）后递归 Load，
// 保证重启后从库恢复的对象其子树完整挂载——否则后续 Save/Delete 会漏掉这些子对象。
func (g *GameObject) loadChildren(ctx context.Context) error {
	var w childIndexWire
	if err := g.store.LoadJSON(ctx, g.childIndexKey(), &w); err != nil && err != data.ErrNotFound {
		return err
	}
	// I/O 完成后，清空槽位后重建归属索引，避免与内存已挂子对象重复。
	// 仅重建 childIDs（归属关系）；已挂 childPtrs 的实例保留复用，缺失的按持久化 id 补建。
	var toLoad []*GameObject
	var newKids []*GameObject
	g.mu.Lock()
	if w.Slots != nil {
		for slot := range g.childIDs {
			delete(g.childIDs, slot)
		}
		for slot, ss := range w.Slots {
			ids := make([]object.ObjectID, 0, len(ss))
			for _, s := range ss {
				id, err := object.ParseObjectID(s)
				if err != nil {
					continue
				}
				ids = append(ids, id)
				// 持久化子对象若尚未挂入内存，则按其自身 ObjectID 构造实例并挂上，
				// 使其在本对象生命周期内可被寻址、级联 Save/Delete。
				if _, ok := g.childPtrs[id]; !ok {
					child := New(g.store, id)
					g.childPtrs[id] = child
					// 记下来，锁外补同步装配（wireSyncTo 要取 g.mu 读锁，锁内调会自死锁）。
					newKids = append(newKids, child)
				}
			}
			g.childIDs[slot] = ids
		}
	}
	// 收集全部已挂子对象（含本次补建的），锁外递归 Load，避免持锁做 I/O。
	for _, c := range g.childPtrs {
		toLoad = append(toLoad, c)
	}
	g.mu.Unlock()

	// ★ 给本次补建的子对象接上同一套同步装配（与 objstore.Repository.Create/Load 的口径一致）。
	// 少了这一步：级联加载出来的子对象写变动**不会自动同步** —— 表现是「父对象改了客户端立刻能看到，
	// 子对象（背包道具 / 军团成员 / 容器里的卡）改了客户端一直不变」，且没有任何报错。
	// 必须在 Load 之前接：Load 会 MarkClean，接上之后才不会有"加载即广播"的假变动；
	// 子对象自身的 loadChildren 会继续把装配传给孙对象（递归生效）。
	for _, c := range newKids {
		g.wireSyncTo(c)
	}
	for _, c := range toLoad {
		if err := c.Load(ctx); err != nil {
			return err
		}
	}
	return nil
}

// deleteChildren 级联删除所有子对象数据，再删除归属索引（递归）。

// 关键：删除必须依据「持久化的 childIDs」而非仅内存中的 childPtrs。
// repo.Delete 走 gobject.New(id).Delete()，此时 childPtrs 为空——若只删内存指针，
// 重启后子对象数据将永远无人删除而成孤儿。故先读持久化归属索引，
// 对每个持久化子 id 构造实例递归 Delete（其孙对象由子对象的 deleteChildren 继续级联）。
func (g *GameObject) deleteChildren(ctx context.Context) error {
	// 从持久化读出归属索引，得到全部子对象 id。
	var w childIndexWire
	if err := g.store.LoadJSON(ctx, g.childIndexKey(), &w); err != nil && err != data.ErrNotFound {
		return err
	}
	// 合并「内存已挂子对象」与「持久化子 id」，按 id 去重，得到待删除子对象集合。
	g.mu.RLock()
	kids := make(map[object.ObjectID]*GameObject, len(g.childPtrs))
	for id, c := range g.childPtrs {
		kids[id] = c
	}
	g.mu.RUnlock()
	for _, ss := range w.Slots {
		for _, s := range ss {
			id, err := object.ParseObjectID(s)
			if err != nil {
				continue
			}
			if _, ok := kids[id]; !ok {
				kids[id] = New(g.store, id) // 未挂内存的持久化子对象，按 id 构造以便删除其数据
			}
		}
	}
	for _, c := range kids {
		if err := c.Delete(ctx); err != nil {
			return err
		}
	}
	if err := g.store.Delete(ctx, g.childIndexKey()); err != nil && err != data.ErrNotFound {
		return err
	}
	return nil
}

// 快照线化（含子对象清单）
// childSummary 快照中的子对象摘要。
type childSummary struct {
	Slots map[string][]string `json:"slots"`
}

// childrenSummary 生成当前子对象归属摘要（不含子对象自身属性，避免快照无限膨胀；
// 子对象属性由其自身 GameObject 的快照承载，业务按需递归读取）。
func (g *GameObject) childrenSummary() childSummary {
	g.mu.RLock()
	defer g.mu.RUnlock()
	s := childSummary{Slots: make(map[string][]string, len(g.childIDs))}
	for slot, ids := range g.childIDs {
		if len(ids) == 0 {
			continue
		}
		ss := make([]string, 0, len(ids))
		for _, id := range ids {
			ss = append(ss, id.String())
		}
		s.Slots[slot] = ss
	}
	return s
}

// applyChildrenSummary 从快照摘要恢复子对象归属索引（id 维度；子对象实例由业务按 id 加载）。
func (g *GameObject) applyChildrenSummary(s childSummary) {
	if s.Slots == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for slot, ss := range s.Slots {
		for _, str := range ss {
			id, err := object.ParseObjectID(str)
			if err != nil {
				continue
			}
			// 去重：避免重复反序列化导致 childIDs 膨胀
			dup := false
			for _, eid := range g.childIDs[slot] {
				if eid == id {
					dup = true
					break
				}
			}
			if !dup {
				g.childIDs[slot] = append(g.childIDs[slot], id)
			}
		}
	}
}
