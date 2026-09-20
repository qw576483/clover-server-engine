// Package server 提供 log 服的 TCP 服务端。
// 监听 TCP 端口，接收 game 批量上报的日志并落盘。
package server

import (
	"encoding/json"
	"fmt"

	"clover-server-engine/internal/domain/log/state"
	netpkg "clover-server-engine/internal/transport/net/tcp"
	"clover-server-engine/internal/transport/tcpmsg"
	"clover-server-engine/pkg/foundation/logger"
)

// Serve 启动 log 服 TCP 服务端。返回 Server 用于 Shutdown。
// 监听同步完成并把错误返回：此前在 goroutine 里 ListenAndServe 后立即
// return nil error —— 端口被占/绑定失败时调用方会误判「启动成功」。
func Serve(addr string, st state.LogService) (*tcpmsg.Server, error) {
	srv := tcpmsg.NewServer(addr)
	registerHandlers(srv, st)
	if err := srv.Listen(); err != nil {
		return nil, fmt.Errorf("logsvc/tcp: listen %s: %w", addr, err)
	}
	return srv, nil
}

// Create 创建 log 服 TCP 服务端、注册 handler，但不开始监听。
// 调用方应在注册完业务 handler 后调用 Listen 开始接受连接。
func Create(addr string, st state.LogService) *tcpmsg.Server {
	srv := tcpmsg.NewServer(addr)
	registerHandlers(srv, st)
	return srv
}

// Listen 开始监听并接受连接。需在 Create 已创建且所有 handler 已注册后调用。
// 监听同步完成（accept 在底层后台运行），失败记 Errorf；
// 需要感知监听失败请改用 Serve（返回 error）。
func Listen(srv *tcpmsg.Server) {
	if srv == nil {
		return
	}
	if err := srv.Listen(); err != nil {
		logger.Errorf("logsvc/tcp: listen failed: %v", err)
	}
}

func registerHandlers(srv *tcpmsg.Server, st state.LogService) {
	// —— 批量日志上报 ——
	srv.Register(state.MsgLogBatch, func(_ *netpkg.Conn, requestID uint32, body []byte) ([]byte, error) {
		var req state.LogBatchReq
		if err := json.Unmarshal(body, &req); err != nil {
			// 坏帧必须留日志：log 服收到畸形报文此前完全无记录。
			logger.Warnf("logsvc/tcp: decode LogBatchReq failed (len=%d): %v", len(body), err)
			return marshalErr()
		}
		written, err := st.WriteBatch(req.Source, req.Entries)
		if err != nil {
			// 落库失败此前只回包、服务端日志完全不可见——丢日志无从排查。
			logger.Errorf("logsvc/tcp: write batch failed (source=%s entries=%d): %v", req.Source, len(req.Entries), err)
			return marshalErr()
		}
		return json.Marshal(&state.LogBatchResp{OK: true, Written: written})
	})
}

// marshalErr 构造失败回包。不回写底层错误原文（MySQL 错误码/表名/驱动细节
// 会泄露内部实现）：细节已由上面的 logger 记录在服务端。
func marshalErr() ([]byte, error) {
	return json.Marshal(&state.LogBatchResp{OK: false, Error: "log batch rejected"})
}
