package account

import "errors"

// 业务错误（未导出变量，供 pkg 层 var 重新导出）。
var (
	errAccountExists   = errors.New("account: already exists")
	errAccountNotFound = errors.New("account: not found")
	errWrongPassword   = errors.New("account: wrong password")
	errInvalidToken    = errors.New("account: invalid token")
	errTokenExpired    = errors.New("account: token expired")
	errChannelNotFound = errors.New("account: channel not found")
	errChannelExists   = errors.New("account: channel already bound")
)
