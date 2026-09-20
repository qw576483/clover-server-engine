package gobject

// Component 是 GameObject 可挂载的运行时组件。
//
// 组件运行时装配、不持久化；跨节点迁移时通过 Dump/Import 序列化状态。
// 业务层可自定义实现本接口，把限流器、AI 行为树、冷却队列等运行时对象
// 挂载到 GameObject 上，统一生命周期与迁移语义。
type Component interface {
	// Name 返回组件唯一标识，同一 GameObject 上不可重复。
	Name() string

	// OnAttach 在组件挂载到对象时调用。
	OnAttach(obj *GameObject)

	// OnDetach 在组件从对象卸载时调用（对象销毁、迁移离线等场景）。
	OnDetach()

	// Dump 导出组件运行状态，用于跨节点迁移；nil 或空切片表示无状态。
	Dump() []byte

	// Import 从 Dump 数据恢复组件运行状态。
	Import(data []byte)
}

// Attach 挂载一个运行时组件。
// 若已存在同 Name 的组件，会先调用旧组件的 OnDetach 并替换。
func (g *GameObject) Attach(c Component) {
	if c == nil {
		return
	}
	g.mu.Lock()
	if g.components == nil {
		g.components = make(map[string]Component)
	}
	old, hadOld := g.components[c.Name()]
	g.components[c.Name()] = c
	g.mu.Unlock()
	// 旧组件的 OnDetach 是用户代码，必须在锁外调用（与 Detach / StopComponents 一致）：
	// 持写锁回调时，OnDetach 内回读对象（Props / Component / Record…）会再取同一把锁 → 自死锁。
	if hadOld {
		old.OnDetach()
	}
	c.OnAttach(g)
}

// Detach 按名称卸载组件；若存在则调用其 OnDetach。
func (g *GameObject) Detach(name string) {
	g.mu.Lock()
	c, ok := g.components[name]
	if ok {
		delete(g.components, name)
	}
	g.mu.Unlock()
	if ok {
		c.OnDetach()
	}
}

// Component 按名称取组件；不存在返回 nil。
func (g *GameObject) Component(name string) Component {
	g.mu.RLock()
	c := g.components[name]
	g.mu.RUnlock()
	return c
}

// ComponentNames 返回已挂载组件名（顺序不定）。
func (g *GameObject) ComponentNames() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	names := make([]string, 0, len(g.components))
	for n := range g.components {
		names = append(names, n)
	}
	return names
}

// DumpComponents 导出所有组件状态，返回 map[组件名]状态数据。
func (g *GameObject) DumpComponents() map[string][]byte {
	g.mu.RLock()
	comps := make([]Component, 0, len(g.components))
	for _, c := range g.components {
		comps = append(comps, c)
	}
	g.mu.RUnlock()

	out := make(map[string][]byte, len(comps))
	for _, c := range comps {
		if d := c.Dump(); len(d) > 0 {
			out[c.Name()] = d
		}
	}
	return out
}

// ImportComponents 从迁移数据恢复所有组件状态。
// 只恢复 GameObject 上已挂载的组件；未挂载的组件数据会被静默忽略。
func (g *GameObject) ImportComponents(data map[string][]byte) {
	if len(data) == 0 {
		return
	}
	for name, d := range data {
		if c := g.Component(name); c != nil {
			c.Import(d)
		}
	}
}

// StopComponents 调用所有组件的 OnDetach（对象销毁/迁移离线时使用）。
func (g *GameObject) StopComponents() {
	g.mu.RLock()
	comps := make([]Component, 0, len(g.components))
	for _, c := range g.components {
		comps = append(comps, c)
	}
	g.mu.RUnlock()
	for _, c := range comps {
		c.OnDetach()
	}
}
