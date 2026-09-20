package fsm

import (
	"fmt"
	"sync"
)

// HFSM 层次状态机（Hierarchical Finite State Machine），在扁平 Machine 之上
// 增加父子状态嵌套关系。它内嵌 *Machine 并接管转移逻辑，使其具备层级感知：
//
//   - 转移到某个子状态时，其全部祖先一并进入（OnEnter 自底向上触发：最深的新状态先，
//     再其父，逐级向上）。
//   - 离开某个父状态时，其全部后代一并退出（OnExit 自顶向下触发：最靠根的退出状态先，
//     再其子孙，逐级向下）。
//
// 通过 RegisterState 注册在底层 Machine 上的既有钩子（OnChange / OnEnter / OnExit）照常工作。
type HFSM struct {
	*Machine

	hmu      sync.RWMutex
	parent   map[State]State   // 子 → 父
	children map[State][]State // 父 → []子
}

// NewHFSM 创建层次状态机，初始态为 initial（透传给底层 Machine）。
func NewHFSM(initial State) *HFSM {
	return &HFSM{
		Machine:  New(initial),
		parent:   make(map[State]State),
		children: make(map[State][]State),
	}
}

// AddState 声明一对父子嵌套关系。parent 与 child 都必须已通过 RegisterState
// 登记到底层 Machine 上。一个子状态最多只能有一个父状态（重复调用会覆盖旧关系）。
//
// 层级成环（parent 是 child 自身或 child 的后代）会让沿父链的遍历永不终止，
// 这里直接拒绝并 panic——与 ancestors 的环检测口径一致（层级环属编程错误）。
func (h *HFSM) AddState(parent, child State) {
	h.hmu.Lock()
	defer h.hmu.Unlock()

	if h.wouldCycleLocked(parent, child) {
		panic(fmt.Sprintf("fsm: AddState(%v, %v) would create a cycle in HFSM hierarchy", parent, child))
	}

	// 从旧父节点上摘除（若有）
	if oldParent, ok := h.parent[child]; ok {
		h.removeChild(oldParent, child)
	}
	h.parent[child] = parent
	h.children[parent] = append(h.children[parent], child)
}

// wouldCycleLocked 报告「把 parent 设为 child 的父」是否成环：
// parent 等于 child，或 parent 沿父链向上可达 child（child 将成为自己的祖先）。
// 调用方须已持有 hmu。
func (h *HFSM) wouldCycleLocked(parent, child State) bool {
	if parent == child {
		return true
	}
	cur := parent
	seen := make(map[State]bool)
	for cur != NoState {
		if cur == child {
			return true
		}
		if seen[cur] {
			// 历史遗留的环（本函数保证新边不成环）：不能再走下去，视为成环并拒绝新边。
			return true
		}
		seen[cur] = true
		p, ok := h.parent[cur]
		if !ok {
			break
		}
		cur = p
	}
	return false
}

// removeChild 从 parent 的子列表中移除 child。调用方须已持有 hmu。
func (h *HFSM) removeChild(parent, child State) {
	children := h.children[parent]
	for i, c := range children {
		if c == child {
			// 清尾再缩容：直接 append(children[:i], children[i+1:]...) 会把末尾元素的
			// 副本残留在底层数组里（被删 State 的引用不释放）。
			copy(children[i:], children[i+1:])
			children[len(children)-1] = NoState
			h.children[parent] = children[:len(children)-1]
			return
		}
	}
}

// Trigger 按事件名尝试转移（先过守卫）。它覆写 Machine.Trigger 以遵循状态层级：
// 祖先按自顶向下退出，新祖先按自底向上进入。
func (h *HFSM) Trigger(event string, payload any) error {
	return h.doTrigger(event, payload, false)
}

// Force 与 Trigger 类似，但绕过守卫。祖先的退出 / 进入钩子照常触发。
func (h *HFSM) Force(event string, payload any) error {
	return h.doTrigger(event, payload, true)
}

func (h *HFSM) doTrigger(event string, payload any, force bool) error {
	// 在 Machine 锁内读取转移边
	h.Machine.mu.RLock()
	cur := h.Machine.current
	es := h.Machine.edges[cur]
	var t transition
	var ok bool
	if es != nil {
		t, ok = es[event]
	}
	h.Machine.mu.RUnlock()

	if !ok {
		return ErrUnknownEvent
	}

	// 守卫校验（force 时跳过）
	if !force && t.guard != nil && !t.guard(h.Machine, cur, payload) {
		return ErrGuard
	}

	to := t.to

	// 校验目标态已登记
	h.Machine.mu.RLock()
	_, toRegistered := h.Machine.states[to]
	h.Machine.mu.RUnlock()
	if !toRegistered {
		return ErrUnknownState
	}

	// 计算层级退出链 / 进入链
	exitChain, enterChain := h.computeChains(cur, to)

	// 原子提交状态变更
	h.Machine.mu.Lock()
	if h.Machine.current != cur {
		h.Machine.mu.Unlock()
		return ErrRace
	}
	h.Machine.current = to
	action := t.action
	h.Machine.mu.Unlock()

	// 执行动作（沿用 Machine 的既有约定：先于退出钩子触发）
	if action != nil {
		runHook("action", func() {
			action(h.Machine, payload)
		})
	}

	// 退出钩子自顶向下：从最靠根的退出祖先一路到叶子（cur）。
	// exitChain 已是「根 → 叶」的自顶向下顺序。
	for _, s := range exitChain {
		if def, ok := h.Machine.states[s]; ok && def.OnExit != nil {
			exitFn := def.OnExit
			base := h.Machine
			runHook("exit", func() {
				exitFn(base, to, payload)
			})
		}
	}

	// 进入钩子自底向上：从最深的新状态（to）一路到最浅的新祖先。
	// enterChain 是「根 → 叶」顺序，这里倒序遍历以自底向上触发。
	for i := len(enterChain) - 1; i >= 0; i-- {
		s := enterChain[i]
		if def, ok := h.Machine.states[s]; ok && def.OnEnter != nil {
			enterFn := def.OnEnter
			base := h.Machine
			runHook("enter", func() {
				enterFn(base, cur, payload)
			})
		}
	}

	// 通知 OnChange 监听者
	h.Machine.notify(cur, to, payload)

	return nil
}

// computeChains 计算一次 from → to 转移的退出链与进入链，两条链都包含 from / to 自身。
//
// 退出链：正在离开的状态，自顶向下（最靠根者在前 → 最靠叶者在后）。
// 进入链：正在进入的状态，「根 → 叶」顺序（调用方按需倒序以自底向上触发）。
func (h *HFSM) computeChains(from, to State) (exitChain, enterChain []State) {
	fromAncestors := h.ancestors(from) // 根 → … → from
	toAncestors := h.ancestors(to)     // 根 → … → to

	// 找到两条路径第一个分叉点
	lcaIdx := 0
	for lcaIdx < len(fromAncestors) && lcaIdx < len(toAncestors) &&
		fromAncestors[lcaIdx] == toAncestors[lcaIdx] {
		lcaIdx++
	}

	// 退出：fromAncestors[lcaIdx:] — 正在离开的状态，已是自顶向下顺序
	// （从最靠根的退出态到最靠叶的退出态）。
	exitChain = fromAncestors[lcaIdx:]

	// 进入：toAncestors[lcaIdx:] — 正在进入的状态，自顶向下顺序。
	// 调用方倒序后即为自底向上触发。
	enterChain = toAncestors[lcaIdx:]

	return exitChain, enterChain
}

// ancestors 返回 s 的祖先链（含 s 自身），顺序为「根在前」。
// 若 s 无父状态，链中只有 [s]。
//
// 检测到环时 panic（层级成环属编程错误）。
func (h *HFSM) ancestors(s State) []State {
	h.hmu.RLock()
	defer h.hmu.RUnlock()

	// 先自叶向根收集
	var chain []State
	cur := s
	visited := make(map[State]bool)
	for cur != NoState {
		if visited[cur] {
			panic(fmt.Sprintf("fsm: cycle detected in HFSM hierarchy at state %v", cur))
		}
		visited[cur] = true
		chain = append(chain, cur)
		parent, hasParent := h.parent[cur]
		if !hasParent {
			break
		}
		cur = parent
	}

	// 反转为「根 → 叶」
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain
}

// Parent 返回状态 s 的父状态；s 为顶层状态时返回 NoState。
func (h *HFSM) Parent(s State) State {
	h.hmu.RLock()
	defer h.hmu.RUnlock()
	return h.parent[s]
}

// Children 返回状态 s 的直接子状态；无子状态时返回 nil。
func (h *HFSM) Children(s State) []State {
	h.hmu.RLock()
	defer h.hmu.RUnlock()
	// 返回副本，避免调用方改动内部切片
	children := h.children[s]
	if len(children) == 0 {
		return nil
	}
	out := make([]State, len(children))
	copy(out, children)
	return out
}

// IsAncestor 报告 ancestor 是否为 s 的祖先（s 自身也算）。
func (h *HFSM) IsAncestor(ancestor, s State) bool {
	h.hmu.RLock()
	defer h.hmu.RUnlock()
	seen := make(map[State]bool)
	cur := s
	for cur != NoState {
		if cur == ancestor {
			return true
		}
		if seen[cur] {
			// 历史遗留的层级环（AddState 现已拒绝新环）：按「不是祖先」处理，避免永不终止。
			return false
		}
		seen[cur] = true
		parent, hasParent := h.parent[cur]
		if !hasParent {
			return false
		}
		cur = parent
	}
	return false
}
