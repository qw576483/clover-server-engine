package player

import "errors"

// 业务错误（未导出变量，供 pkg 层 var 重新导出）。
var (
	errPlayerNotFound = errors.New("player: not found")
	errPlayerExists   = errors.New("player: already exists")
)
