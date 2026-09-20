// facade.go 是怪物/NPC 管理器的**门面包装真身**：把 internal 的具体 *MobManager
// 适配成 `pkg/domain/mmo/mob` 的公开契约（`mob.MobManager` 接口）。
//
// 为什么包装必须放在 internal：`pkg/**` 只允许做门面（别名 / 转发 / 极薄适配），
// 见 `结构规则.md` §5.1；这里是行为实现体，所以真身在 internal，
// `pkg/domain/mmo/mmo.go` 只做变量转发。
//
// 为什么在本包（而不是 internal/domain/mmo/facade.go）：本包 import `internal/domain/mmo`
// （MobScene 的 *mmo.Scene 真身、Vec3 等），若主门面反向 import 本包就成环
// （`import cycle not allowed`）。放在能力自己的包里既不成环，也符合
// 「对口包装落到对应 internal 包」的落点规则。
package mob

import (
	pkgmob "clover-server-engine/pkg/domain/mmo/mob"
)

// NewMobManagerFacade 以给定场景构造怪物/NPC 管理器，并以门面接口返回。
// 内部用 btree 行为树驱动每只怪的「追击 / 攻击 / 巡逻」决策。
// 业务侧可见名 = `pkg/domain/mmo.NewMobManager`。
func NewMobManagerFacade(scene pkgmob.MobScene) pkgmob.MobManager {
	return NewMobManager(scene)
}

// FromMobManagerFacade 把 internal 的 *MobManager 以门面接口返回。
// 业务侧可见名 = `pkg/domain/mmo.FromMobManager`。
func FromMobManagerFacade(m *MobManager) pkgmob.MobManager { return m }

// InternalMobManagerFacade 把门面 MobManager 还原为 internal 的 *MobManager；非底层则 ok=false。
// 业务侧可见名 = `pkg/domain/mmo.InternalMobManager`。
func InternalMobManagerFacade(m pkgmob.MobManager) (*MobManager, bool) {
	v, ok := m.(*MobManager)
	return v, ok
}

// 编译期断言：内部 *MobManager 满足门面接口。
var _ pkgmob.MobManager = (*MobManager)(nil)
