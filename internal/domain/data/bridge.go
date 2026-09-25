package data

import (
	pdata "github.com/qw576483/clover-server-engine/pkg/domain/data"
)

// bridge.go 持有 pkg/domain/data/bridge 的**解包实现**。
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
