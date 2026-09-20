package order

import "errors"

// 业务错误（未导出变量，供 pkg 层 var 重新导出）。
var (
	errOrderNotFound = errors.New("order: not found")
)
