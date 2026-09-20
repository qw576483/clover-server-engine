// facade.go 是 Buff 容器的**门面包装真身**：把 internal 的具体 *Container 适配成
// `pkg/domain/mmo/buff` 的公开契约（`buff.Container` 接口）。
//
// 为什么包装必须放在 internal：`pkg/**` 只允许做门面（别名 / 转发 / 极薄适配），
// 见 `结构规则.md` §5.1；这里的包装是行为实现体，所以真身在 internal，
// `pkg/domain/mmo/mmo.go` 只做变量转发。
package buff

import (
	object "clover-server-engine/internal/domain/object"
	pkbuff "clover-server-engine/pkg/domain/mmo/buff"
)

// containerFacade 门面 buff.Container 接口的实现：包装 internal 的 *Container。
// 语义与内部容器一一对应，仅做接口形态的适配（门面返回接口，真身是具体类型）。
type containerFacade struct{ inner *Container }

func (b *containerFacade) Apply(def pkbuff.Def)       { b.inner.Apply(def) }
func (b *containerFacade) Remove(id uint32)           { b.inner.Remove(id) }
func (b *containerFacade) Tick(dt float64)            { b.inner.Tick(dt) }
func (b *containerFacade) HasFlag(f pkbuff.Flag) bool { return b.inner.HasFlag(f) }
func (b *containerFacade) Active() []pkbuff.Def       { return b.inner.Active() }
func (b *containerFacade) Clear()                     { b.inner.Clear() }

// 编译期断言：包装满足门面接口。
var _ pkbuff.Container = (*containerFacade)(nil)

// 编译期断言：内部 *Container 自身也满足门面接口（FromBuffContainer 直通它）。
var _ pkbuff.Container = (*Container)(nil)

// NewContainerFacade 以属性集构造 Buff 容器并包装为门面接口；nil 属性集按内部构造语义处理。
// 业务侧可见名 = `pkg/domain/mmo.NewBuffContainer`。
func NewContainerFacade(set *object.AttrSet) pkbuff.Container {
	return &containerFacade{inner: NewContainer(set)}
}

// FromContainerFacade 把 internal 的 *Container 以门面接口返回；nil 返回 nil 接口。
// 业务侧可见名 = `pkg/domain/mmo.FromBuffContainer`。
func FromContainerFacade(c *Container) pkbuff.Container {
	if c == nil {
		return nil
	}
	return c
}

// InternalContainerFacade 把门面 Container 还原为 internal 的 *Container；非底层则 ok=false。
// 业务侧可见名 = `pkg/domain/mmo.InternalBuffContainer`。
func InternalContainerFacade(c pkbuff.Container) (*Container, bool) {
	v, ok := c.(*Container)
	return v, ok
}
