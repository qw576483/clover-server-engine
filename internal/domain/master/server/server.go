// Package server 提供 master 协作服的 TCP 服务端。
// 监听 TCP 端口，把 state.State 挂到 handler，对外暴露节点注册、排行榜、
// 玩家定位与 Session Token 等协调能力。
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
	netpkg "github.com/qw576483/clover-server-engine/internal/transport/net/tcp"
	"github.com/qw576483/clover-server-engine/internal/transport/tcpmsg"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// masterRPCTimeout 单次 master RPC 中后端 IO（Redis）的预算：
// 所有 handler 共享的 context.Background() 不可取消，后端抖动时 handler 会被
// 长期挂住；涉及后端 IO 的 handler 统一派生带超时的子 ctx。
const masterRPCTimeout = 5 * time.Second

// Serve 启动 master TCP 服务端（创建 → 注册内置 handler → 立即监听）。返回 Server 用于 Shutdown。
// 监听失败（端口占用等）同步返回 error，调用方据此决定启动流程是否继续。
// token 非空时启用连接鉴权（首帧必须是 MsgAuth 握手，见 Create）。
func Serve(addr string, st *state.State, token string) (*tcpmsg.Server, error) {
	if st == nil {
		// 与同包 RegisterHealth 的显式判空一致：nil state 时首个请求即空指针 panic。
		return nil, fmt.Errorf("master/tcp: state must not be nil")
	}
	srv := tcpmsg.NewServer(addr)
	installConnAuth(srv, token)
	registerHandlers(srv, st)

	if err := srv.Listen(); err != nil {
		return nil, fmt.Errorf("master/tcp: listen %s: %w", addr, err)
	}
	return srv, nil
}

// Create 创建 master TCP 服务端、注册内置 handler，但不开始监听。
// 调用方应在注册完业务 handler 后调用 Listen 开始接受连接。
// 避免 Serve→runMasterBusinesses 之间连接到达但 handler 未注册。
// st 为 nil 时以明确文案 panic（服务端无法在没有状态的情况下工作，fail-fast 优于首个请求空指针）。
//
// token 非空 ⇒ 安装连接鉴权闸门（见 installConnAuth）。
func Create(addr string, st *state.State, token string) *tcpmsg.Server {
	if st == nil {
		panic("master/server: state must not be nil")
	}
	srv := tcpmsg.NewServer(addr)
	installConnAuth(srv, token)
	registerHandlers(srv, st)
	return srv
}

// authedConnKey conn.Value 上「本连接已通过鉴权」的标记键。
const authedConnKey = "masterAuthed"

// installConnAuth 按需安装连接鉴权闸门。
//
// 为什么 master 内部 RPC 需要它：配置成非回环地址后，这条通道上挂着
// MsgSessionNew / MsgSessionValidate / MsgPlayerRegister / MsgRank* 等
// **无调用方身份校验**的写接口——任何能连到该端口的人都能伪造他人登录态、
// 篡改玩家定位与排行榜。故非回环绑定必须配共享密钥，并在**首帧**完成校验。
// token 为空时（只绑回环的默认部署）不装闸门，行为与加固前完全一致。
func installConnAuth(srv *tcpmsg.Server, token string) {
	if srv == nil || token == "" {
		return
	}
	srv.SetConnAuth(connAuthGate(token))
	logger.Infof("master/tcp: 连接鉴权已启用（首帧必须是 MsgAuth=%d 握手，共享密钥 master_token）", state.MsgAuth)
}

// connAuthGate 构造鉴权钩子：已鉴权连接放行；未鉴权连接只接受 MsgAuth 握手帧，
// 其余一律回 unauthorized 并**关闭连接**。
func connAuthGate(token string) tcpmsg.ConnAuthFunc {
	deny := []byte(`{"ok":false,"error":"unauthorized: master auth required"}`)
	return func(conn *netpkg.Conn, msgID uint32, body []byte) tcpmsg.ConnAuthResult {
		if v, ok := conn.Value(authedConnKey); ok {
			if authed, _ := v.(bool); authed {
				return tcpmsg.ConnAuthResult{Allow: true}
			}
		}
		if msgID != state.MsgAuth {
			// 未鉴权连接调业务消息号：拒绝 + 关连接（非预期分支必须留痕，供运维识别扫描）。
			logger.Warnf("master/tcp: reject unauthenticated conn=%s remote=%s msgID=%d",
				conn.ConnID(), conn.RemoteAddr(), msgID)
			return tcpmsg.ConnAuthResult{Reply: deny, Close: true}
		}
		var req state.AuthReq
		if err := json.Unmarshal(body, &req); err != nil {
			logger.Warnf("master/tcp: bad auth frame conn=%s: %v", conn.ConnID(), err)
			return tcpmsg.ConnAuthResult{Reply: deny, Close: true}
		}
		// 常量时间比较：共享密钥是短字符串，逐字节比较会把「前缀多少位正确」泄露给扫描者。
		if subtle.ConstantTimeCompare([]byte(req.Token), []byte(token)) != 1 {
			logger.Warnf("master/tcp: auth token mismatch conn=%s remote=%s", conn.ConnID(), conn.RemoteAddr())
			return tcpmsg.ConnAuthResult{Reply: deny, Close: true}
		}
		conn.SetValue(authedConnKey, true)
		return tcpmsg.ConnAuthResult{Allow: true, Handled: true, Reply: []byte(`{"ok":true}`)}
	}
}

// Listen 开始监听并接受连接。需在 Create 已创建且所有 handler 已注册后调用。
// 监听失败（端口占用、地址非法）同步返回 error；
// 成功时返回 nil，连接处理在后台进行。
func Listen(srv *tcpmsg.Server) error {
	if err := srv.Listen(); err != nil {
		// 底层 net.Listen 的错误已带地址信息，此处只补上层语义。
		return fmt.Errorf("master/tcp: listen: %w", err)
	}
	return nil
}

func registerHandlers(srv *tcpmsg.Server, st *state.State) {
	ctx := context.Background()
	// newCtx 为一组「会做后端 IO（Redis）」的 handler 派生带超时的独立 ctx：
	// 共享的 Background 不可取消，Redis 抖动会把 handler 线程长期挂住。
	newCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, masterRPCTimeout)
	}

	srv.Register(state.MsgNodesByType, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.NodesByTypeReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		nodeIDs := st.NodeIDsByType(req.Type)
		return json.Marshal(&state.NodesByTypeResp{OK: true, NodeIDs: nodeIDs})
	})

	srv.Register(state.MsgNodesByTag, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.NodesByTagReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		nodeIDs := st.NodeIDsByTag(req.Tag)
		return json.Marshal(&state.NodesByTagResp{OK: true, NodeIDs: nodeIDs})
	})

	srv.Register(state.MsgRegisterNode, func(conn *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RegisterReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.RegisterNode(ctx, req.Node); err != nil {
			return marshalErr(err)
		}
		// 在连接上绑定 nodeID，以便连接断开时触发 RemoveNode。
		conn.SetValue("nodeID", req.Node.ID)
		return marshalOK()
	})

	srv.Register(state.MsgRemoveNode, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RemoveReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.RemoveNode(ctx, req.NodeID); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	// —— 排行榜 ——
	srv.Register(state.MsgRankTop, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankTopReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		entries, err := st.Top(ctx, req.Board, req.N)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankTopResp{OK: true, Entries: entries})
	})

	srv.Register(state.MsgRankAdd, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankAddReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.Add(ctx, req.Board, req.Member, req.Score, req.Extra); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankAddHigher, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankAddHigherReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.AddOnlyUpdateScore(ctx, req.Board, req.Member, req.Score); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankIncr, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankIncrReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		score, err := st.Incr(ctx, req.Board, req.Member, req.Delta)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankIncrResp{OK: true, Score: score})
	})

	srv.Register(state.MsgRankIncrHigher, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankIncrHigherReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		score, err := st.IncrOnlyUpdateScore(ctx, req.Board, req.Member, req.Delta)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankIncrResp{OK: true, Score: score})
	})

	srv.Register(state.MsgRankGet, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankGetReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		entry, found, err := st.GetMember(ctx, req.Board, req.Member)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankGetResp{OK: true, Entry: entry, Found: found})
	})

	srv.Register(state.MsgRankGetRank, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankGetRankReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		n, found, err := st.GetRank(ctx, req.Board, req.Member)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankGetRankResp{OK: true, Rank: n, Found: found})
	})

	srv.Register(state.MsgRankByRankRange, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankByRankReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		entries, err := st.GetByRankRange(ctx, req.Board, req.Start, req.Stop)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankByRankResp{OK: true, Entries: entries})
	})

	srv.Register(state.MsgRankByScoreRange, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankByScoreReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		entries, err := st.GetByScoreRange(ctx, req.Board, req.Min, req.Max)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankByScoreResp{OK: true, Entries: entries})
	})

	srv.Register(state.MsgRankRemove, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankRemoveReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.Remove(ctx, req.Board, req.Member); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankClear, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // 删除备份 key 会做 Redis IO
		defer cancel()
		var req state.RankClearReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.Clear(rctx, req.Board, req.DeleteBackup); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankLen, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankLenReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		total, err := st.Len(ctx, req.Board)
		if err != nil {
			return marshalErr(err)
		}
		return json.Marshal(&state.RankLenResp{OK: true, Total: total})
	})

	srv.Register(state.MsgRankBackupAll, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // 备份 = Redis 写入
		defer cancel()
		if err := st.BackupAll(rctx); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankBackup, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // 备份 = Redis 写入
		defer cancel()
		var req state.RankBackupReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.Backup(rctx, req.Board); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankRestoreAll, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // 恢复 = Redis 读取
		defer cancel()
		if err := st.RestoreAll(rctx); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankRestore, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // 恢复 = Redis 读取
		defer cancel()
		var req state.RankRestoreReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.Restore(rctx, req.Board); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgRankSetThresholds, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.RankSetThresholdsReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.SetRankThresholds(ctx, req.Board, req.Thresholds); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	// —— 玩家定位 ——
	srv.Register(state.MsgPlayerRegister, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.PlayerRegisterReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.RegisterPlayer(ctx, req.UID, req.NodeID); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgPlayerRemove, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.PlayerRemoveReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.RemovePlayer(ctx, req.UID, req.NodeID); err != nil {
			return marshalErr(err)
		}
		return marshalOK()
	})

	srv.Register(state.MsgPlayerLookup, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.PlayerLookupReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		nodeID, found := st.PlayerNode(ctx, req.UID)
		return json.Marshal(&state.PlayerLookupResp{OK: true, NodeID: nodeID, Found: found})
	})

	// —— Session Token ——
	srv.Register(state.MsgSessionNew, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // token 后端（redis）为网络 IO
		defer cancel()
		var req state.SessionNewReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		token, err := st.NewSessionToken(rctx, req.PlayerID)
		if err != nil {
			logger.Errorf("master/session: new token for %s failed: %v", req.PlayerID, err)
			return marshalErr(err)
		}
		return json.Marshal(&state.SessionNewResp{OK: true, Token: token})
	})

	srv.Register(state.MsgSessionValidate, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // token 后端（redis）为网络 IO
		defer cancel()
		var req state.SessionValidateReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		valid, err := st.ValidateSessionTokenResult(rctx, req.PlayerID, req.Token)
		if err != nil {
			// 存储后端故障（仅 redis 后端可能）：返回 OK=false 让客户端感知「未完成校验」，
			// 进而走 game 侧本地 fallback，而不是把后端抖动误当成 token 无效踢人。
			logger.Warnf("master/session: validate %s: backend error: %v (respond OK=false)", req.PlayerID, err)
			return json.Marshal(&state.SessionValidateResp{OK: false, Error: "session backend unavailable"})
		}
		return json.Marshal(&state.SessionValidateResp{OK: true, Valid: valid})
	})

	srv.Register(state.MsgSessionDelete, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // token 后端（redis）为网络 IO
		defer cancel()
		var req state.SessionDeleteReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		// 注意：DeleteSessionToken 无返回值（签名契约），后端删除失败时 state 侧会记日志，
		// 此处无法感知失败（见 s-bug：接口层仍回 OK:true）。
		st.DeleteSessionToken(rctx, req.PlayerID)
		return marshalOK()
	})

	srv.Register(state.MsgSessionCurrent, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // token 后端（redis）为网络 IO
		defer cancel()
		var req state.SessionCurrentReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		token := st.CurrentSessionToken(rctx, req.PlayerID)
		return json.Marshal(&state.SessionCurrentResp{OK: true, Token: token})
	})

	srv.Register(state.MsgSessionRefresh, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		rctx, cancel := newCtx() // token 后端（redis）为网络 IO
		defer cancel()
		var req state.SessionRefreshReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if err := st.RefreshSessionToken(rctx, req.PlayerID, req.Token); err != nil {
			// 后端故障（仅 redis 后端可能）：续期失败不影响在线玩家，返回 OK=false 供客户端感知，
			// 不当作鉴权失败处理。
			logger.Warnf("master/session: refresh %s: backend error: %v (respond OK=false)", req.PlayerID, err)
			return json.Marshal(&state.SessionRefreshResp{OK: false, Error: "session backend unavailable"})
		}
		return json.Marshal(&state.SessionRefreshResp{OK: true})
	})
	// —— 连接断开处理 ——
	// 节点正常关闭时会先发送 MsgRemoveNode，此时 nodeID 已从 state 中移除。
	// 异常断连（kill -9 / 网络中断）不会发送 MsgRemoveNode，由心跳超时 + Detector 判定 Dead 后摘除。
	// 此处仅记录日志，不再直接 RemoveNode，避免正常断连误触发摘除。
	srv.OnDisconnect = func(conn *netpkg.Conn) {
		v, ok := conn.Value("nodeID")
		if !ok {
			return
		}
		nodeID, ok := v.(string)
		if !ok {
			return
		}
		logger.Infof("master: node %s disconnected (will be detected by heartbeat timeout if abnormal)", nodeID)
	}
}

func marshalOK() ([]byte, error) {
	return json.Marshal(&state.Resp{OK: true})
}

func marshalErr(err error) ([]byte, error) {
	return json.Marshal(&state.Resp{OK: false, Error: err.Error()})
}
