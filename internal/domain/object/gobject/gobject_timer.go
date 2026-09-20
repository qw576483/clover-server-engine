package gobject

import (
	"github.com/qw576483/clover-server-engine/pkg/runtime/timer"
)

const timerComponentName = "timer"

// timerComponent 是定时任务组的组件封装。
type timerComponent struct {
	group *timer.Group
}

func (c *timerComponent) Name() string { return timerComponentName }

func (c *timerComponent) OnAttach(_ *GameObject) {}

func (c *timerComponent) OnDetach() {
	if c.group != nil {
		c.group.Stop()
	}
}

func (c *timerComponent) Dump() []byte {
	if c.group == nil {
		return nil
	}
	return c.group.DumpScope()
}

func (c *timerComponent) Import(data []byte) {
	if c.group == nil || len(data) == 0 {
		return
	}
	c.group.ImportScope(data)
}

// AttachTimer 给本对象挂载定时任务组组件（可选能力）。
//
// 挂上后，业务可通过 obj.Timer() 取到 *timer.Group 直接注册定时任务：
//
//	obj.Timer().After("task-name", 30*time.Second, func() { ... })
//	obj.Timer().Every("heartbeat", 5*time.Second, func() { ... })
//
// scope 自动取 ObjectID，对象销毁时调 Stop() 可一键清理该对象全部定时任务。
func (g *GameObject) AttachTimer(grp *timer.Group) {
	if grp == nil {
		return
	}
	g.Attach(&timerComponent{group: grp})
}

// Timer 返回已挂载的定时任务组（未挂载返回 nil）。
func (g *GameObject) Timer() *timer.Group {
	c := g.Component(timerComponentName)
	if c == nil {
		return nil
	}
	if tc, ok := c.(*timerComponent); ok {
		return tc.group
	}
	return nil
}
