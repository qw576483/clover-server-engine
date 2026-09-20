package state

import (
	"context"
	"sync"
)

// ChannelVerifier 渠道票据校验器：将第三方票据换成「渠道内唯一账号标识」（如微信 openid）。
//
// 架构定位：**账号体系的业务扩展点在账号服，不在游戏服**。游戏服的登录链路唯一
// （收到 EMsgLogin{token} → 调账号服 /auth/verify 换 owner），没有任何注入点；
// 而账号服需要一个能力：把「第三方票据」换成「渠道内唯一账号标识」。
// 这一步无法由引擎内置——各渠道校验协议差异极大（有的要回调 HTTP、有的要验签、
// 有的要拿 code 换 access_token），所以引擎把它定义成接口，由业务实现并注册。
//
// 注册入口暴露在 **pkg/app**（业务可导入）：`app.RegisterChannelVerifier`。
//
// 实现约束（不满足会导致「每次登录都变成新用户」或「凭空建号」）：
//   - 票据无效 / 过期 / 渠道服务不可用 → 返回 error，不要返回空标识 + nil；
//   - 返回的 channelAccount 必须**稳定**（同一用户每次一致，如 openid）；
//   - 不要在这里做绑定或建号，那是账号服的职责（本包已实现）。
type ChannelVerifier interface {
	// Verify 校验票据，返回渠道内唯一账号标识。
	Verify(ctx context.Context, channel, ticket string) (channelAccount string, err error)
}

var channelVerifierRegistry struct {
	sync.RWMutex
	v ChannelVerifier
}

// RegisterChannelVerifier 注册渠道票据校验器。
//
// **须在 app.Run 之前调用**（账号服在构造期取用）。
// 业务侧经 pkg/app.RegisterChannelVerifier 调用（本包在 internal，业务直接调不到）。
func RegisterChannelVerifier(v ChannelVerifier) {
	channelVerifierRegistry.Lock()
	channelVerifierRegistry.v = v
	channelVerifierRegistry.Unlock()
}

// currentChannelVerifier 读取已注册的校验器（未注册返回 nil）。
// 刻意不「取用即清空」：账号服可能被多次构造（测试、多角色同进程），清空会让第二次构造失效。
func currentChannelVerifier() ChannelVerifier {
	channelVerifierRegistry.RLock()
	defer channelVerifierRegistry.RUnlock()
	return channelVerifierRegistry.v
}
