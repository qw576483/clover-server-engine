package data

import (
	pdata "clover-server-engine/pkg/domain/data"
)

// bridge.go 持有原本落在 pkg/domain/data/bridge 的**解包实现**。
//
// 为什么实现搬到这里：pkg/domain/data/bridge 原先是 pkg 包，里面写着
// `s.(*idata.Store)` 这种「把门面接口还原成 internal 具体类型」的逻辑 ——
// 那是引擎侧实现，不是业务契约，按结构规则「pkg 只留门面（别名 / 转发）」必须落在 internal。
// pkg/domain/data/bridge 现在只是本函数的变量别名，import 路径与 API 保持不变。

// InternalStore 将门面 Store 接口还原为 internal 的具体 *Store；若底层不是则 ok=false。
// 供 pkg 内 object / mmo 等子系统的门面工厂使用，业务不应直接调用。
func InternalStore(s pdata.Store) (*Store, bool) {
	is, ok := s.(*Store)
	return is, ok
}

// compile-time 断言：internal 具体类型满足门面接口。
var (
	_ pdata.Store  = (*Store)(nil)
	_ pdata.Record = (*Record)(nil)
)
