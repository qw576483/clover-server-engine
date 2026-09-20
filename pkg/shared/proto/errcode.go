// 错误码契约（业务可见）。
//
// 设计原则：错误回包 EErrorReply 同时携带「机器可读错误码」与「人类可读描述」，
// 客户端据此做统一处理（如 401 未认证 → 回到登录流程），而不是匹配错误文案
// ——文案会变，码不会。
//
// 与引擎其它对外契约一样，本文件的常量是**真身**，业务 import 本包即可点到。
package proto

import "errors"

// 引擎级错误码。
//
// 采用 HTTP 语义的数字，便于跨端 / 跨团队沟通；业务自定义错误码请从 1000 起，
// 避免与引擎段（0 与 4xx/5xx）冲突。
const (
	// ErrCodeNone 无错误码（缺省值）。引擎内部未分类 error（如 errors.New）与
	// 旧版服务端都会落在这个值上，客户端应回退到「按文案展示」而非逻辑判断。
	ErrCodeNone int32 = 0

	// ErrCodeBadRequest 请求不合法（参数缺失 / 格式错误 / 状态不允许）。
	ErrCodeBadRequest int32 = 400

	// ErrCodeUnauthenticated 未认证：未登录、会话失效或登录门禁拒绝。
	// 客户端收到后应回到登录流程（网关登录门禁也使用本码）。
	ErrCodeUnauthenticated int32 = 401

	// ErrCodeForbidden 已认证但无权限（如操作不属于自己的房间）。
	ErrCodeForbidden int32 = 403

	// ErrCodeNotFound 目标不存在（房间已解散、玩家不在线等）。
	ErrCodeNotFound int32 = 404

	// ErrCodeTooManyRequests 频率超限。
	ErrCodeTooManyRequests int32 = 429

	// ErrCodeInternal 服务端内部错误。
	ErrCodeInternal int32 = 500
)

// BizError 携带错误码的业务错误。
//
// handler 或 OnBeforeDispatch 钩子返回 *BizError 时，引擎回包 EErrorReply 会带上 Code：
//
//	return proto.NewBizError(proto.ErrCodeForbidden, "不在该房间")
//
// 返回普通 error（非 *BizError）时 Code 为 ErrCodeNone，客户端按文案展示。
type BizError struct {
	Code int32  // 机器可读错误码（ErrCode* 常量或业务自定义码）
	Msg  string // 人类可读描述（日志与调试面板）
}

// Error 实现 error 接口；错误码请经 Code 字段或 ErrorCodeOf 读取。
func (e *BizError) Error() string { return e.Msg }

// NewBizError 构造带错误码的业务错误。
func NewBizError(code int32, msg string) *BizError {
	return &BizError{Code: code, Msg: msg}
}

// Unauthenticated 构造未认证错误（ErrCodeUnauthenticated）。
func Unauthenticated(msg string) *BizError {
	return &BizError{Code: ErrCodeUnauthenticated, Msg: msg}
}

// Forbidden 构造无权限错误（ErrCodeForbidden）。
func Forbidden(msg string) *BizError {
	return &BizError{Code: ErrCodeForbidden, Msg: msg}
}

// BadRequest 构造请求不合法错误（ErrCodeBadRequest）。
func BadRequest(msg string) *BizError {
	return &BizError{Code: ErrCodeBadRequest, Msg: msg}
}

// ErrorCodeOf 从 error 链中提取错误码；非 *BizError（或 nil）返回 ErrCodeNone。
// 引擎在回包时用本函数决定 EErrorReply.Code。
func ErrorCodeOf(err error) int32 {
	var be *BizError
	if errors.As(err, &be) {
		return be.Code
	}
	return ErrCodeNone
}
