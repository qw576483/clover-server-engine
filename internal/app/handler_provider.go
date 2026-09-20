package app

import (
	"sync/atomic"
)

// currentAdminServer 当前进程的 admin server（可能为 nil = 已禁用 / 未启动）。
var currentAdminServer atomic.Pointer[AdminServer]

// setCurrentAdminServer 设置 / 清空全局 admin server 引用。
func setCurrentAdminServer(as *AdminServer) {
	if as == nil {
		currentAdminServer.Store(nil)
		return
	}
	currentAdminServer.Store(as)
}

// mountAdminRoutes 把业务在 RegisterMount 里通过 g.OnAdminHTTP 注册的探针路由，
// 统一挂到 admin server。必须在 admin server Start 之前调用。
func mountAdminRoutes(as *AdminServer, g *Game) {
	if as == nil || g == nil {
		return
	}
	for _, r := range g.adminRoutes {
		as.HandleFunc(r.pattern, r.h)
	}
}
