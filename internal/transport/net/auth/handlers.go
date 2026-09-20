// Package auth 引擎内置认证 handler 实现（login / resume session）。
package auth

import (
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/internal/transport/event"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// ValidateSessionFunc 校验 session token（Game.ValidateSessionToken）。
type ValidateSessionFunc func(playerID, token string) bool

// DeleteSessionFunc 删除 session token（Game.DeleteSessionToken）。
type DeleteSessionFunc func(playerID string)

// KickFunc 踢下线回调（Game.KickConn）。
type KickFunc func(connID string) bool

// OnResumedFunc session 恢复成功回调（Game → engine.EmitPlayerSessionResumed）。
type OnResumedFunc func(playerID, account string)

// AccountOfFunc 由 playerID 反查归属对象标识（账号）。可为 nil。
// 恢复会话的场景下连接上的 account 通常为空（新连接没有登录动作），必须靠它反查。
type AccountOfFunc func(playerID string) string

// ResumeSessionHandler 创建 resume session 引擎 handler。
// 校验传入的 session token 是否有效，无效则踢下线。
// onResumed 在 session 校验通过后回调（nil 表示跳过）。
//
// **回包必须带 owner**：网关（gwcore）只在「上游回包能被 auth.ExtractOwnerID 解出非空
// owner」时才把连接绑回 owner。恢复会话走的是**一条新连接**，它没有任何登录动作；
// 若不在这里补上 owner，网关不会绑定它，该连接随即被登录门禁拒掉后续**每一条**业务消息
// （401 unauthenticated）——表现为「ResumeSession 成功，但连接立刻失能」。
// 因此这里用 accountOf 按 playerID 反查账号，并把同一个值交给 onResumed，
// 避免下游事件拿到空 account。
func ResumeSessionHandler(
	resumeMsgID uint32,
	validate ValidateSessionFunc,
	accountOf AccountOfFunc,
	deleteFn DeleteSessionFunc,
	kick KickFunc,
	onResumed OnResumedFunc,
) event.Handler {
	return func(c *event.Ctx) error {
		var req proto.EResumeSessionRequest
		if err := c.BindMsg(&req); err != nil {
			c.MarkReplied(marshalResumeReply(proto.EResumeSessionReply{Success: false, Err: "bad request"}))
			return nil
		}

		if !validate(req.PlayerID, req.SessionToken) {
			// 日志中不打印 playerID，防止敏感信息泄露。
			logger.Infof("resume rejected: token mismatch")
			c.MarkReplied(marshalResumeReply(proto.EResumeSessionReply{PlayerID: req.PlayerID, Success: false, Err: "session expired"}))
			_ = kick(c.ConnID())
			return nil
		}

		// 优先取连接上的 account（同连接重绑场景），否则按 playerID 反查。
		account := c.Account()
		if account == "" && accountOf != nil {
			account = accountOf(req.PlayerID)
		}
		if account == "" {
			// 非预期分支必须留日志：owner 缺失会让网关无法绑定，
			// 连接恢复后即失能（后续业务消息全 401），现场只能靠这行日志定位。
			logger.Warnf("resume accepted but owner unresolved: gateway will reject subsequent messages (player=%s conn=%s)",
				req.PlayerID, c.ConnID())
		}

		c.MarkReplied(marshalResumeReply(proto.EResumeSessionReply{
			PlayerID: req.PlayerID, Success: true, Owner: account,
		}))
		if onResumed != nil {
			onResumed(req.PlayerID, account)
		}
		return nil
	}
}

// marshalResumeReply 序列化恢复会话回包；失败时记录错误并回落最小合法 JSON。
//
// 直接忽略 Marshal 错误会回出空 body：客户端反序列化失败后停在等待状态，
// 且服务端无任何日志可查（固定结构体的 Marshal 失败属极端异常，但必须可观测+可兜底）。
func marshalResumeReply(reply proto.EResumeSessionReply) []byte {
	b, err := ujson.Marshal(reply)
	if err != nil {
		logger.Errorf("resume handler: marshal reply failed: %v", err)
		return []byte(`{"player_id":"","success":false,"err":"internal error"}`)
	}
	return b
}
