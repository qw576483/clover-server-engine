package gobject

import (
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/runtime/fsm"
)

const fsmComponentName = "fsm"

// fsmComponent 是有限状态机的组件封装。
type fsmComponent struct {
	machine *fsm.Machine
}

func (c *fsmComponent) Name() string { return fsmComponentName }

func (c *fsmComponent) OnAttach(obj *GameObject) {
	if c.machine == nil {
		return
	}
	c.machine.SetContext(obj)
	c.machine.OnChange(func(_ *fsm.Machine, _, to fsm.State, _ any) {
		obj.Props().SetString("fsm_state", string(to))
	})
}

func (c *fsmComponent) OnDetach() {}

func (c *fsmComponent) Dump() []byte {
	if c.machine == nil {
		return nil
	}
	return c.machine.Dump()
}

func (c *fsmComponent) Import(data []byte) {
	if c.machine == nil || len(data) == 0 {
		return
	}
	// 导入失败必须留痕：否则对象状态机与持久化数据不一致时完全无从排查。
	if err := c.machine.Import(data); err != nil {
		logger.Warnf("gobject: fsm component import failed: %v", err)
	}
}

// AttachFSM 给本对象挂载有限状态机组件（可选能力）。
//
// 挂上后：
//   - 任意状态变化（Trigger 或 Force）都会把当前态写回 props 字段 "fsm_state"（字符串）。
//   - 若该对象已 EnableAutoSync，写回会经既有 props notifier 自动广播到在线客户端——
//     即「服务器改态 → 客户端即时知道」。
//   - 钩子内可通过 fsm.Machine.Context() 取回本 *GameObject，实现「状态联动改属性」
//     （如进入 Fighting 态自动加攻击_buff）。
//
// Force 同样写回并广播，无需业务层配合。
func (g *GameObject) AttachFSM(m *fsm.Machine) {
	if m == nil {
		return
	}
	g.Attach(&fsmComponent{machine: m})
}

// FSM 返回已挂载的状态机（未挂载返回 nil）。
func (g *GameObject) FSM() *fsm.Machine {
	c := g.Component(fsmComponentName)
	if c == nil {
		return nil
	}
	if fc, ok := c.(*fsmComponent); ok {
		return fc.machine
	}
	return nil
}
