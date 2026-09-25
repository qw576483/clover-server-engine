package server

import (
	"encoding/json"
	"errors"

	"github.com/qw576483/clover-server-engine/internal/domain/master/failover"
	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
	netpkg "github.com/qw576483/clover-server-engine/internal/transport/net/tcp"
	"github.com/qw576483/clover-server-engine/internal/transport/tcpmsg"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// RegisterHealth 在已创建的 master TCP 服务端上追加节点健康相关 handler：
// 心跳上报（MsgHeartbeat）与全节点健康视图（MsgNodeHealth）。
//
// 该函数是纯增量的：不改动 registerHandlers 已注册的任何消息号。
// 节点存活探测与集群拓扑无关——单机部署同样生效，因此不随任何高可用开关启停。
func RegisterHealth(srv *tcpmsg.Server, st *state.State, det *failover.Detector) {
	if srv == nil || st == nil || det == nil {
		return
	}

	// —— 心跳上报 ——
	srv.Register(state.MsgHeartbeat, func(_ *netpkg.Conn, _ uint32, body []byte) ([]byte, error) {
		var req state.HeartbeatReq
		if err := json.Unmarshal(body, &req); err != nil {
			return marshalErr(err)
		}
		if req.NodeID == "" {
			return marshalErr(errors.New("master: heartbeat missing node_id"))
		}

		// 先判 state 是否认识该节点，再决定是否喂给探测器：
		// 顺序不能相反——若先喂探测器，它会把未注册节点补登记为 Alive，然后才报 Known:false。
		// 这类节点不在 state 中，判 Dead 后 RemoveNodeWithReason 返回 ErrNodeNotFound、
		// 不触发 Untrack → d.nodes 永久滞留、nodes_dead Gauge 永久虚高；
		// 且 master 一边说「不认识」一边把它计入 Alive 计数，健康视图自相矛盾。
		if _, ok := st.NodeByID(req.NodeID); !ok {
			logger.Warnf("master/health: heartbeat from unregistered node %s ignored (not tracked; node should re-register)", req.NodeID)
			return json.Marshal(&state.HeartbeatResp{
				OK:         true,
				IntervalMS: det.Config().HeartbeatInterval.Milliseconds(),
				Known:      false, // 节点侧收到后会走 onStale 重新注册
			})
		}

		det.Heartbeat(req.NodeID)
		if req.Load >= 0 {
			// 负载为 0 也要刷新：否则负载降 0 时 master 保留旧高负载。
			if err := st.UpdateLoad(req.NodeID, req.Load); err != nil {
				logger.Warnf("master/health: update load for node %s failed: %v", req.NodeID, err)
			}
		}

		return json.Marshal(&state.HeartbeatResp{
			OK:         true,
			IntervalMS: det.Config().HeartbeatInterval.Milliseconds(),
			Known:      true,
		})
	})

	// —— 节点健康视图（只读）——
	srv.Register(state.MsgNodeHealth, func(_ *netpkg.Conn, _ uint32, _ []byte) ([]byte, error) {
		views := det.Snapshot()
		entries := make([]state.NodeHealthEntry, 0, len(views))
		for _, v := range views {
			entries = append(entries, state.NodeHealthEntry{
				NodeID:          v.NodeID,
				Health:          string(v.Health),
				LastHeartbeatMS: v.LastHeartbeat.UnixMilli(),
				SilenceMS:       v.Silence.Milliseconds(),
			})
		}
		return json.Marshal(&state.NodeHealthResp{OK: true, Nodes: entries})
	})

	logger.Infof("master/health: handlers registered (heartbeat=%s suspect=%s dead=%s probe=%s)",
		det.Config().HeartbeatInterval, det.Config().SuspectTimeout,
		det.Config().DeadTimeout, det.Config().ProbeInterval)
}
