// Package bridge 打破 pkg/domain/data ↔ internal/domain/data 的 import cycle。
// 提供 InternalStore 解包函数，供 pkg 子系统（gobject / objstore / idgen / mmo）
// 将门面 data.Store 还原为 internal 具体实现。
//
// 本包是**薄门面**：实现位于 internal/domain/data（`bridge.go`），这里只做变量转发，
// import 路径与函数签名保持不变。业务不应直接调用。
package bridge

import (
	idata "github.com/qw576483/clover-server-engine/internal/domain/data"
)

// InternalStore 将门面 Store 接口还原为 internal 的具体 *Store；若底层不是则 ok=false。
// 真身见 internal/domain/data/bridge.go 的同名函数。
var InternalStore = idata.InternalStore
