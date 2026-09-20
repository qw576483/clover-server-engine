// 本文件是渠道登录（微信 / QQ / Steam / Apple / 自建账号中心）的**业务注册入口**。
//
// 为什么注册函数必须在这里：扩展点得放在业务**能导入**的位置。
// 引擎曾提供 `pkg/transport/auth` + `app.RegisterAuthenticator`，但注册函数定义在
// `internal/app`，业务模块（独立 Go module）受 Go internal 规则限制根本调不到——
// 那是「只能实现、无法挂载」的半成品，已删除。这里放在 `pkg/app`（公开门面，
// 业务可导入），内部转发到 `internal/domain/auth`（账号域）。
//
// 本包是**门面包**（见 `结构规则.md` §5.1）：这里只有类型别名 + 声明转发，真身在 internal。
//
// 用法（业务 main，须在 app.Run 之前）：
//
//	app.RegisterChannelVerifier(wechatVerifier{appID: "wx...", secret: "..."})
//
// 之后账号服即可支持：
//
//	POST /auth/login  {"channel":"wechat","ticket":"<客户端拿到的 code>"}
//
// 首次登录会自动建号并绑定（注册即登录）；已绑定则直接签发 token。
// 未注册校验器时该接口返回 501 +「账号服未接入该渠道」。
package app

import (
	iauth "github.com/qw576483/clover-server-engine/internal/domain/auth"
)

// ChannelVerifier 渠道票据校验器：将第三方票据换成「渠道内唯一账号标识」（如微信 openid）。
//
// 这是账号体系唯一需要业务实现的扩展点：
//   - 引擎负责渠道绑定表（account_channel）、建号、签发 token；
//   - 业务只负责回答「这张票据对应渠道里的谁」（调渠道 SDK / 验签 / 换 token 等）。
//
// 实现约束（不满足会导致「每次登录都变成新用户」或「凭空建号」）：
//   - 票据无效 / 过期 / 渠道服务不可用 → 返回 error，不要返回空标识 + nil；
//   - 返回的 channelAccount 必须稳定（同一用户每次一致）；
//   - 不要在这里做绑定或建号，那是账号服的职责。
type ChannelVerifier = iauth.ChannelVerifier

// RegisterChannelVerifier 注册渠道票据校验器。**须在 app.Run 之前调用**（账号服构造期取用）。
//
// 未注册时，/auth/login 收到 {channel, ticket} 会返回 501 +「账号服未接入该渠道」，
// 而不是含糊的登录失败——便于区分「服务端没接渠道」与「票据真的错了」。
var RegisterChannelVerifier = iauth.RegisterChannelVerifier

// ChannelRouter 多渠道路由（门面别名，真身在 internal/domain/auth/router.go）
type ChannelRouter = iauth.ChannelRouter

// NewChannelRouter 新建多渠道路由。业务注册多家渠道后，只需向引擎注册一次：
//
//	r := app.NewChannelRouter()
//	r.Register("wechat", wx)
//	r.Register("douyin", dy)
//	app.RegisterChannelVerifier(r)
var NewChannelRouter = iauth.NewChannelRouter
