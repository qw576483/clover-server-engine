package proto

import pproto "clover-server-engine/pkg/shared/proto"

// 错误码与 BizError 的**真身在 pkg/shared/proto**（业务可见，业务返回带码错误时用那一份）。
// 此处转发，供引擎内部（派发内核 / 网关）在构造 EErrorReply 时取码，避免两处各写一套常量。

const (
	// ErrCodeNone 无错误码（缺省值）。
	ErrCodeNone = pproto.ErrCodeNone
	// ErrCodeBadRequest 请求不合法。
	ErrCodeBadRequest = pproto.ErrCodeBadRequest
	// ErrCodeUnauthenticated 未认证：未登录 / 会话失效 / 登录门禁拒绝。
	ErrCodeUnauthenticated = pproto.ErrCodeUnauthenticated
	// ErrCodeForbidden 已认证但无权限。
	ErrCodeForbidden = pproto.ErrCodeForbidden
	// ErrCodeNotFound 目标不存在。
	ErrCodeNotFound = pproto.ErrCodeNotFound
	// ErrCodeTooManyRequests 频率超限。
	ErrCodeTooManyRequests = pproto.ErrCodeTooManyRequests
	// ErrCodeInternal 服务端内部错误。
	ErrCodeInternal = pproto.ErrCodeInternal
)

// BizError 带错误码的业务错误，真身在 pkg/shared/proto。
type BizError = pproto.BizError

// ErrorCodeOf 从 error 链提取错误码，真身在 pkg/shared/proto。
func ErrorCodeOf(err error) int32 { return pproto.ErrorCodeOf(err) }

// EMsgError 通用错误回包 opcode，真身在 pkg/shared/proto。
const EMsgError = pproto.EMsgError
