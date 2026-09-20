package auth

import (
	"context"
	"sort"
	"sync"

	"clover-server-engine/internal/domain/auth/state"
	"clover-server-engine/pkg/foundation/logger"
)

// ChannelRouter 把「按渠道名分发」收进引擎：业务注册多家，只向引擎注册一次。
//
// 为什么需要它：渠道校验器的注册位是**单值**的（state.RegisterChannelVerifier 后写覆盖先写）。
// 业务同时接两家渠道时，第二次注册会**静默覆盖**第一家 —— 启动期不报错，
// 直到玩家登录才表现为「票据失败」，排查成本极高。本原语把「同名覆盖」改成
// **启动期 panic**（见 Register），并让业务不必自己写 router。
//
// 与既有单值注册的关系：**纯新增、互不影响**。既有 app.RegisterChannelVerifier
// （单值）的行为一字未改；业务接多家渠道时改为注册一个 *ChannelRouter：
//
//	r := app.NewChannelRouter()
//	r.Register("wechat", wx)
//	r.Register("douyin", dy)
//	app.RegisterChannelVerifier(r) // 引擎只被注册一次，多家渠道由 r 分发
//
// 并发安全：注册与分发之间用 RWMutex 保护。**渠道实现本身在锁外调用**
// （见 Verify），否则一家渠道慢会阻塞全部渠道的登录。
type ChannelRouter struct {
	mu sync.RWMutex
	// channels 渠道名 → 校验器。零值不可用，须经 NewChannelRouter 构造
	//（Register 对 nil map 做了惰性初始化兜底）。
	channels map[string]ChannelVerifier
}

// 编译期断言：ChannelRouter 必须能直接作为 ChannelVerifier 注册给引擎。
var _ ChannelVerifier = (*ChannelRouter)(nil)

// NewChannelRouter 构造空路由。
func NewChannelRouter() *ChannelRouter {
	return &ChannelRouter{channels: make(map[string]ChannelVerifier)}
}

// Register 注册一家渠道的校验器。**channel 为空 / v 为 nil / 重复注册同一 channel ⇒ panic**。
//
// 刻意 panic 而非静默覆盖：本原语要消灭的故障正是「覆盖」（见 ChannelRouter 文档）。
// panic 发生在启动期（业务在 app.Run 之前注册），能把配置冲突暴露在启动时，
// 而不是拖到玩家登录时才表现为票据失败。
func (r *ChannelRouter) Register(channel string, v ChannelVerifier) {
	if channel == "" {
		// 空渠道名无法被 Verify 命中，注册了也是死代码 —— 当配置错误处理。
		panic("auth: ChannelRouter.Register: channel 不能为空")
	}
	if v == nil {
		// nil 校验器会让 Verify 在调用处 panic（或静默返回空标识），启动期拒绝更清晰。
		panic("auth: ChannelRouter.Register: verifier 不能为 nil (channel=" + channel + ")")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.channels == nil {
		// 兜底：允许「零值 ChannelRouter + Register」也能工作，避免 nil map 赋值 panic。
		r.channels = make(map[string]ChannelVerifier)
	}
	if _, dup := r.channels[channel]; dup {
		// 覆盖 = 本原语要消灭的故障，一律启动期暴露。
		panic("auth: ChannelRouter.Register: channel " + channel + " 重复注册（覆盖正是本原语要消灭的故障）")
	}
	r.channels[channel] = v
	logger.Infof("auth: channel router registered channel=%q (total=%d)", channel, len(r.channels))
}

// Channels 已注册渠道名，**按字典序排序**（日志与自检要稳定输出）。
func (r *ChannelRouter) Channels() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.channels))
	for name := range r.channels {
		names = append(names, name)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	return names
}

// Verify 满足 ChannelVerifier：按 channel 分发。
//
//   - 已注册 → 调对应 verifier。**读锁内只取指针，渠道调用在锁外**：
//     持锁调用会让一家慢渠道卡死全部渠道的登录（锁粒度必须小到「取一次 map」）。
//   - 未注册的渠道 → 返回 state.Error{KindChannelUnsupported}（与「未注册 verifier」同 Kind）。
//     ⚠️ **但这个 Kind 到不了客户端**：state.ChannelLogin 对 verifier 返回的任何 error 都
//     硬编码成 KindTicketInvalid → HTTP 401「渠道票据校验失败」（见 state.go:297-302）。
//     501「账号服未接入该渠道」只在**根本没注册 verifier** 时出现（state.go:286-290）。
//     所以本 Kind 与 Text 只体现在服务端日志里（state.go:300 会打 %v）。
//     要让「没接渠道」在 HTTP 上真回 501，得改 state.ChannelLogin 把 *state.Error 原样透传（未做）。
//   - ticket 为空 → 返回 KindBadParam 错误；同样会被映射成 401（原因同上）。
//   - verifier 返回 error → 原样透传（票据无效 / 过期 / 渠道不可用都是「凭证错误」；
//     router 不重新分类，因为它不知道原因）。
//   - verifier 返回空标识 → 返回 KindInternal 错误（客户端同样是 401）。
//     ChannelVerifier 契约禁止「空标识 + nil」，此处**绝不放行** —— 放行会凭空造号。
//   - **任何分支都不会返回 ("", nil)**；全部失败路径都是 fail-closed。
func (r *ChannelRouter) Verify(ctx context.Context, channel, ticket string) (channelAccount string, err error) {
	if ticket == "" {
		// 与 state.ChannelLogin 的同名判据保持同一 Kind（KindBadParam）。
		logger.Warnf("auth: channel router verify %q rejected: missing ticket", channel)
		return "", &state.Error{Kind: state.KindBadParam, Text: "缺少 ticket"}
	}

	r.mu.RLock()
	v := r.channels[channel]
	r.mu.RUnlock()

	if v == nil {
		// 未注册渠道：Kind 与「未注册 verifier」一致（便于日志定位）；
		// 客户端实际收到的是 401，501 只在 verifier 完全没注册时才有（见上方 Verify 文档）。
		logger.Warnf("auth: channel router verify %q rejected: channel not registered", channel)
		return "", &state.Error{
			Kind: state.KindChannelUnsupported,
			Text: "账号服未接入该渠道（业务需注册 ChannelVerifier）",
		}
	}

	channelAccount, err = v.Verify(ctx, channel, ticket)
	if err != nil {
		// 票据无效 / 渠道不可用：服务端留日志（含底层原因），对外由引擎回 401 且不回显原因。
		logger.Warnf("auth: channel router channel %q ticket verify failed: %v", channel, err)
		return "", err
	}
	if channelAccount == "" {
		// 校验器返回空标识属于实现错误：宁可失败，也不要凭空造出一个账号。
		logger.Errorf("auth: channel router channel %q verifier returned empty channelAccount (verifier bug)", channel)
		return "", &state.Error{Kind: state.KindInternal, Text: "渠道票据校验异常"}
	}
	return channelAccount, nil
}
