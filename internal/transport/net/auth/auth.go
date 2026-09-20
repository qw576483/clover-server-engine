// Package auth 实现 clover 通用「登录身份层」能力。

// 设计目标（用户强约束）：账号 / 订单等鉴权逻辑完全封装在底层（base），
// 业务服（game）只关心"已登录的对象标识 Owner"，绝不直接接触账号表 / 密码校验。

// 本包提供：
// - ELoginRequest / ELoginReply：通用登录线类型（定义于 base proto，此处复用）。
// - Authenticator 接口：校验登录请求，返回已认证对象标识（owner）。
// - Handler(loginMsgID, auth)：返回一个 event.Handler，
// 即"解开登录请求 → 调 Authenticator → 回 ELoginReply"。登录的全部细节都在 Authenticator。
// - ExtractOwnerID()：返回 gwcore.WithExtractOwnerID 所需的回调，解析 ELoginReply 取 Owner，
// 供网关 generically 绑定会话（业务无需知道登录回包的字段名）。
// - RemoteAuthenticator：引擎唯一的内置实现——调账号服 /auth/verify 换取 owner。
//   （历史上的「本地账号表校验」「本地 JWT 验签」两种实现已随「单一登录模式」收敛删除。）

// 登录实现由 app.newAuthenticator 构造：引擎内置 RemoteAuthenticator
// （internal/domain/auth/client，HTTP 调账号服 /auth/verify 换 owner）；
// 扩展账号体系（微信 / Steam / 自有账号中心）在**账号服**侧做
// （见 internal/domain/auth/state/channel.go 的 ChannelVerifier），
// 而非替换游戏服的 Authenticator——游戏服的登录链路唯一。

// 本包为引擎内部实现（internal/transport/net/auth），不设 pkg 公开门面。
package auth

import (
	"context"
	"encoding/base64"
	"errors"

	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/internal/transport/event"
	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/foundation/logger"
	ujson "clover-server-engine/pkg/shared/json"
)

// 撞库防护（per-account 失败计数与锁定）已随「账号密码式校验」一并移出本包。
//
// 原因：游戏服现在只转发 token（ELoginRequest 已无 account 字段），
// 本层拿不到可用于计数的账号名——按空串计数会让所有玩家共用一个计数器
// （10 次失败即全员被锁），比不做防护更糟。
// **权威防护点在账号服 /auth/login**（见 internal/domain/auth/state/guard.go 的 loginGuard），
// 那里才是真正校验账号密码、能识别"谁在撞库"的地方。

// 业务错误。
var (
	ErrBadCredential = errors.New("auth: bad credential") // 凭证无效（不区分具体原因，避免探测）
	ErrBadParameter  = errors.New("auth: bad parameter")  // 参数错误（空 token 等）
	// ErrAuthUnavailable 账号服不可达（连不上 / 超时 / 响应无法解析）。
	// 与 ErrBadCredential 严格区分：这是服务端故障，不是玩家凭证错误。
	ErrAuthUnavailable = errors.New("auth: account service unavailable")
)

// errMsgAuthFailed 统一鉴权失败消息，不泄露底层 DB/存储错误等内部实现细节。
const errMsgAuthFailed = "authentication failed"

// errMsgAuthUnavailable 账号服不可达时透传给客户端的文案。
// 与「密码错」区分开，玩家与运维才能一眼判断是服务端故障而非自己输错。
const errMsgAuthUnavailable = "账号服务不可用，请稍后重试"

// fallbackErrBody 序列化失败时的静态兜底回包，保证客户端至少收到结构完整的失败响应，
// 而不是空 body（空 body 会让客户端反序列化失败、卡在登录态无任何提示）。
const fallbackErrBody = `{"success":false,"err":"internal error"}`

// marshalReply 序列化回包；失败时记日志并回退静态错误 JSON，避免回空包。
func marshalReply(v any) []byte {
	b, err := ujson.Marshal(v)
	if err != nil {
		logger.Errorf("auth: marshal reply failed: %v", err)
		return []byte(fallbackErrBody)
	}
	return b
}

// Authenticator 校验登录请求，返回已认证对象标识（owner，通常是玩家 UID / 账号名）
// 以及登录成功后下发的 token。
//
// 引擎内只有 RemoteAuthenticator 一个实现（调账号服 /auth/verify）：登录链路唯一，
// 不存在"业务换一个实现接管登录"的平行路径——账号体系的扩展点在账号服，不在游戏服。
type Authenticator interface {
	Authenticate(ctx context.Context, req *proto.ELoginRequest) (owner string, token string, err error)
}

// Handler 返回 logic 事件处理函数：解开 ELoginRequest → 调 Authenticator → 回 ELoginReply。
// 登录的全部细节（token 校验、账号服调用等）封装在 Authenticator 内，
// game 服注册此 handler 即可，自身不感知任何账号逻辑。
//
// 本层**不做**撞库防护：游戏服只转发 token（ELoginRequest 已无 account 字段），
// 拿不到账号名就无法按账号计数，权威防护点在账号服 /auth/login（见本文件顶部说明）。
func Handler(loginMsgID uint32, auth Authenticator) event.Handler {
	return func(c *event.Ctx) error {
		var req proto.ELoginRequest
		if err := c.BindMsg(&req); err != nil {
			b := marshalReply(proto.ELoginReply{Success: false, Err: "bad request"})
			c.MarkReplied(b)
			return nil
		}
		owner, token, err := auth.Authenticate(c.Context(), &req)
		if err != nil {
			// 失败文案按错误类别区分：服务端故障 / 集成错误 / 凭证错误。
			msg := errMsgAuthFailed
			switch {
			case errors.Is(err, ErrAuthUnavailable):
				// 账号服不可达是服务端故障（不是玩家凭证错），文案透传，
				// 让玩家/运维一眼区分「凭证不对」与「账号服没起来」。
				msg = errMsgAuthUnavailable
			case errors.Is(err, ErrBadParameter):
				// 集成类错误（如客户端没带 token），文案透传便于定位。
				msg = err.Error()
			}
			b := marshalReply(proto.ELoginReply{Success: false, Err: msg})
			c.MarkReplied(b)
			return nil
		}
		reply := proto.ELoginReply{Owner: owner, Token: token, Success: true}
		// 通道加密协商：客户端在登录请求里显式声明支持（req.Encrypt）才生成并下发会话密钥。
		// 网关侧 auth.ExtractSessionKey 提取到非空 key 即为本连接启用 AES-GCM；
		// 未声明（老客户端 / robot / msg-client / 网页工具）保持明文，不会被"收到 key 却解不开"踢下线。
		if req.Encrypt {
			key, keyErr := session.NewKey()
			if keyErr != nil {
				// 密钥生成失败（系统熵源不可用）属服务端故障。宁可明确失败，
				// 也不静默降级成明文——否则客户端以为链路已加密，实际是明文，属最坏的静默失效。
				logger.Errorf("auth: generate session key failed: %v", keyErr)
				b := marshalReply(proto.ELoginReply{Success: false, Err: errMsgAuthFailed})
				c.MarkReplied(b)
				return nil
			}
			reply.SessionKey = base64.StdEncoding.EncodeToString(key)
		}
		b := marshalReply(reply)
		c.MarkReplied(b)
		return nil
	}
}

// ExtractSessionKey 返回 gwcore 所需的回调：从登录回包中提取 base64 编码的会话密钥。
// 网关在绑定会话时调用此函数，若返回非空 key 则启用该连接的 AES-GCM 加解密。
// 按 session_key 字段解析，与具体 opcode 解耦。
//
// 已接线：internal/app/bootstrap.go 的 runGateway 注册了本回调；
// key 由本包 Handler 在客户端声明 Encrypt 时生成（见 ELoginRequest.Encrypt）。
func ExtractSessionKey() func(uint32, []byte) ([]byte, bool) {
	return func(_ uint32, body []byte) ([]byte, bool) {
		if len(body) == 0 {
			return nil, false
		}
		var wrapper struct {
			SessionKey string `json:"session_key"`
		}
		// 与 ExtractOwnerID 统一使用 ujson，避免两个解析器对同一 body 行为不一致。
		if err := ujson.Unmarshal(body, &wrapper); err != nil || wrapper.SessionKey == "" {
			return nil, false
		}
		key, err := base64.StdEncoding.DecodeString(wrapper.SessionKey)
		if err != nil || len(key) != 32 {
			return nil, false
		}
		return key, true
	}
}

// ExtractOwnerID 返回 gwcore.WithExtractOwnerID 所需的回调：从登录/注册回包中提取 Owner，
// 供网关 generically 绑定会话。与具体 opcode 及回包结构解耦。

// 只解析 owner JSON 字段：按完整 reply 结构反序列化会与具体结构耦合，
// 任一字段改名都会导致回包无法绑定 owner（登录后的推送/加密链路静默失效）。
func ExtractOwnerID() func(uint32, []byte) (string, bool) {
	return func(_ uint32, body []byte) (string, bool) {
		if len(body) == 0 {
			return "", false
		}
		var wrapper struct {
			Owner string `json:"owner"`
		}
		if err := ujson.Unmarshal(body, &wrapper); err != nil || wrapper.Owner == "" {
			return "", false
		}
		return wrapper.Owner, true
	}
}
