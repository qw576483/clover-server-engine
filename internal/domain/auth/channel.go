package auth

import "github.com/qw576483/clover-server-engine/internal/domain/auth/state"

// ChannelVerifier 渠道票据校验器：把第三方票据换成「渠道内唯一账号标识」（如微信 openid）。
//
// 账号体系**唯一需要业务实现**的扩展点。实现约束（票据要报错、标识要稳定、
// 不要在此绑定或建号）见 state.ChannelVerifier 的文档。
//
// 在域根再导出一次，理由：业务注入点（pkg/app.RegisterChannelVerifier）应只依赖
// domain/auth 这一层，不必知道它内部落在哪个子包——与 master 在域根导出 Rank 门面同理。
type ChannelVerifier = state.ChannelVerifier

// RegisterChannelVerifier 注册渠道票据校验器。**须在 app.Run 之前调用**（账号服构造期取用）。
//
// 未注册时 /auth/login 收到 {channel, ticket} 会返回 501 +「账号服未接入该渠道」，
// 而不是含糊的登录失败——便于区分「服务端没接渠道」与「票据真的错了」。
func RegisterChannelVerifier(v ChannelVerifier) {
	state.RegisterChannelVerifier(v)
}
