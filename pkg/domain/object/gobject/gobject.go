// Package gobject 统一游戏对象内核的公开门面。
//
// 本包是**门面包**（见 `结构规则.md` §5.1）：真身全部在 `internal/domain/object/gobject`
// （`facade.go` 里是 `GameObjectFacade` 接口 + 包装实现 + 工厂），这里只有
// **类型别名**（`type X = internal.X`）与**变量转发**（`var F = internal.F`），不含实现体。
//
// GameObject 在业务侧以接口形式暴露（接口 = 具体类型的方法集 + 协变 wrapper），
// 底层是 internal 的 *GameObject。工厂返回接口；如需还原 internal 侧，用
// FromGameObject / InternalGameObject 在两种形式间转换。
// F12 停在 `GameObject` 上即跳进 internal 看到完整方法集与注释。
package gobject

import (
	igobject "clover-server-engine/internal/domain/object/gobject"
)

// Component 是 GameObject 可挂载的运行时组件（运行时装配、迁移时 Dump/Import）。
type Component = igobject.Component

// GameObject 统一游戏对象（身份 + 版本 + 乐观并发写 + 增量同步 + 运行时组件槽）。
//
// 说明：为保持单向依赖与最小暴露，本接口只收编业务可达的方法。以下「引擎装配型」方法未编入接口，
// 它们返回/接收引擎内部装配类型（*object.Bag / *object.Schema / *fsm.Machine / 子对象树），仅 internal
// 装配与仓储层使用，业务无句柄：
//   - Props / AddRecord / Record / RecordNames（返回内部 Bag / Record）
//   - SetSchema / Schema / ValidateProps / MarshalPropsIndexed / ApplyPropsIndexed / SyncPropsPatch /
//     SavePropsSnapshot（返回内部 *object.Schema）
//   - AttachFSM / FSM（返回内部 *fsm.Machine）
//   - AddChild / Child / Children / ChildSlots / ChildIDs / RemoveChild（子对象树装配）
//   - SyncPropsPatch / ApplyPropsCompact / SavePropsSnapshot 等序列化内部格式
type GameObject = igobject.GameObjectFacade

// New 构造一个空 GameObject（仅身份 + 空属性袋）。
// store 为数据存储抽象接口（data.Store）；id 为带类型的对象号（object.ObjectID）。
// store 必须是 data.Store 的 internal 底层实现，否则返回 nil（typed-nil 安全）。
var New = igobject.NewGameObjectFacade

// FromGameObject 将 internal 的 *GameObject 包装为门面 GameObject 接口。
var FromGameObject = igobject.FromGameObjectFacade

// InternalGameObject 将门面 GameObject 接口还原为 internal 的 *GameObject；非底层则 ok=false。
var InternalGameObject = igobject.InternalGameObjectFacade

// ErrVersionConflict 乐观并发冲突：SaveIfVersion 检测到对象自加载后被他人改动。
// 据此提示「数据已被修改，请重新拉取」。
var ErrVersionConflict = igobject.ErrVersionConflict
