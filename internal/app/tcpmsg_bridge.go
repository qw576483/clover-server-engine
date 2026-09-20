package app

import (
	"fmt"
	"sync"

	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/internal/transport/event"
	netpkg "clover-server-engine/internal/transport/net/tcp"
	"clover-server-engine/internal/transport/tcpmsg"
	ujson "clover-server-engine/pkg/shared/json"
)

// tcpMsgBridge 把「裸 TCP 消息通道」桥接进 Core 派发管线，由 MasterGame / LogGame / AuthGame 共用。
//
// 三个非 game 角色都不接客户端长连接，但都要能收 game 节点发来的同步请求
// （CallMaster / CallLog / CallAuth）。桥接语义统一为：
//
//	TCP 帧 → GWLogicPacket → Logic.Dispatch → handler(c event.Ctx) → 回包
//
// srv 尚未就绪时注册的 handler 先缓存，setServer 时统一补注册
// （否则 runXXX 里「先建内核注册业务 handler、后创建 TCP 服务端」的顺序会 panic）。
type tcpMsgBridge struct {
	logic *event.Logic

	mu   sync.Mutex
	srv  *tcpmsg.Server
	pend []pendingTCPHandler
}

// pendingTCPHandler srv 未就绪时缓存的 TCP handler 注册请求。
type pendingTCPHandler struct {
	msgID uint32
	fn    tcpmsg.HandlerFunc
}

// attach 绑定宿主内核（Core.Logic）。须在 OnMsg 之前调用。
func (b *tcpMsgBridge) attach(logic *event.Logic) { b.logic = logic }

// OnMsg 注册业务消息 handler（handler 签名见 event.Handler，与 Game.OnMsg 一致）。
//
// 业务消息号必须 > InternalMsgMax：这是给业务用户的保护（防止业务号撞上引擎内建 handler）。
// 引擎自己内建的消息号（如 master↔game 房间协议 6001..6004）走 InternalOnMsg，不经此守卫。
func (b *tcpMsgBridge) OnMsg(msgID uint32, h event.Handler) {
	if msgID <= proto.InternalMsgMax {
		panic(fmt.Sprintf("app: business message id must be > %d; got %d", proto.InternalMsgMax, msgID))
	}
	b.InternalOnMsg(msgID, h)
}

// InternalOnMsg 注册**引擎内部保留号**（≤ proto.InternalMsgMax）的 handler，供引擎内部包使用
// （例：room 的 master 侧房间协议 EMasterRoomRegister 等）。
//
// 与 OnMsg 完全同一条链路（Logic 派发 + TCP handler 登记），唯一区别是不做业务号下限校验：
// 因此「业务号必须 > InternalMsgMax」这条约束对业务仍然生效，没有被放宽
// （业务经 Game.OnMsg / MasterGame.OnMsg 注册 ≤10000 仍会 panic）。
func (b *tcpMsgBridge) InternalOnMsg(msgID uint32, h event.Handler) {
	b.logic.InternalOnMsg(msgID, h)
	b.register(msgID, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		pkt := &proto.GWLogicPacket{
			RequestID: requestID,
			MsgID:     msgID,
			Body:      body,
			Line:      "tcp",
		}
		if !b.logic.HasHandler(msgID) {
			return nil, fmt.Errorf("no handler for msgID=%d", msgID)
		}
		reply := b.logic.Dispatch(pkt)
		if reply == nil {
			// handler 注册了但未回包（SetNoAutoReply 场景），也算成功。
			return ujson.Marshal(map[string]any{"ok": true})
		}
		return reply.Body, nil
	})
}

// register 向 TCP 服务端注册 handler；srv 尚未就绪时先缓存。
func (b *tcpMsgBridge) register(msgID uint32, fn tcpmsg.HandlerFunc) {
	b.mu.Lock()
	if b.srv == nil {
		b.pend = append(b.pend, pendingTCPHandler{msgID: msgID, fn: fn})
		b.mu.Unlock()
		return
	}
	srv := b.srv
	b.mu.Unlock()
	srv.Register(msgID, fn)
}

// setServer 绑定 TCP 服务端，并把之前缓存的 handler 全部补注册。
func (b *tcpMsgBridge) setServer(srv *tcpmsg.Server) {
	b.mu.Lock()
	b.srv = srv
	pend := b.pend
	b.pend = nil
	b.mu.Unlock()
	for _, p := range pend {
		srv.Register(p.msgID, p.fn)
	}
}

// stop 停止 TCP 服务端（幂等）。
func (b *tcpMsgBridge) stop() {
	b.mu.Lock()
	srv := b.srv
	b.srv = nil
	b.mu.Unlock()
	if srv != nil {
		_ = srv.Stop()
	}
}
