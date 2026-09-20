package state

// Kind 领域错误分类。transport 层（server 子包）据此映射 HTTP 状态码——
// **业务层不感知 HTTP**，只管「错在哪一类」。
type Kind uint8

// 领域错误分类。命名按「业务原因」而非「HTTP 状态」，
// 这样换传输层（比如将来加消息通道版本）时无需改业务代码。
const (
	// KindBadParam 请求参数非法（缺字段 / 格式错 / 超长）。
	KindBadParam Kind = iota + 1
	// KindAccountExists 注册时账号已存在。
	KindAccountExists
	// KindBadCredential 凭证错误（账号不存在与密码错误共用，避免账号枚举）。
	KindBadCredential
	// KindAccountLocked 账号因连续失败被撞库防护锁定。
	KindAccountLocked
	// KindChannelUnsupported 账号服未接入该渠道（业务未注册 ChannelVerifier）。
	KindChannelUnsupported
	// KindChannelUnavailable 渠道链路不可用（缺渠道表等）。
	KindChannelUnavailable
	// KindTicketInvalid 渠道票据校验失败。
	KindTicketInvalid
	// KindInternal 账号服自身故障（签发失败 / 存储异常）。
	KindInternal
)

// Error 账号服的领域错误。
//
// Text 是**面向调用方**的中文文案（进 HTTP body 的 err 字段），Kind 供 server 层映射状态码。
// 两者放在一起，避免「文案在业务层、状态码在传输层」各写一半导致漂移。
type Error struct {
	Kind Kind
	Text string
}

func (e *Error) Error() string { return e.Text }

// errf 构造领域错误。
func errf(kind Kind, text string) *Error { return &Error{Kind: kind, Text: text} }
