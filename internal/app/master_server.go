package app

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"clover-server-engine/internal/domain/data"
	iaccessor "clover-server-engine/internal/domain/data/accessor"
	iredis "clover-server-engine/internal/domain/data/store/redis"
	"clover-server-engine/internal/domain/master"
	"clover-server-engine/internal/domain/master/failover"
	"clover-server-engine/internal/domain/master/metrics"
	"clover-server-engine/internal/domain/master/server"
	"clover-server-engine/internal/domain/master/state"
	"clover-server-engine/internal/domain/room"
	"clover-server-engine/internal/shared/config"
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/internal/transport/event"
	"clover-server-engine/internal/transport/nats"
	"clover-server-engine/internal/transport/tcpmsg"
	apptypes "clover-server-engine/pkg/app/types"
	"clover-server-engine/pkg/foundation/logger"
	ujson "clover-server-engine/pkg/shared/json"
	"clover-server-engine/pkg/shared/timeutil"
)

// MasterGame 嵌入内核，让 master 侧具备游戏逻辑编写能力。
type MasterGame struct {
	*Core
	state *state.State
	rooms *room.MasterRegistry

	// bridge 把裸 TCP 消息通道（game ↔ master RPC）桥进 Core 派发管线。
	bridge tcpMsgBridge

	stopOnce sync.Once
}

// newMasterGame 创建 MasterGame 内核（headless Logic）。
func newMasterGame(cfg *Config, store *data.Store) *MasterGame {
	syncReg := event.NewSyncRegistry()

	masterHTTP := config.DefString(cfg.MasterHTTPListenAddr, defaultMasterHTTPListenAddr)

	core := event.New(event.Config{
		Headless:   true,
		HTTPListen: masterHTTP,
	},
		event.WithStore(store),
		event.WithSyncDownlink(nil, "", syncReg),
	)

	var entityAcc *iaccessor.Accessor
	if store != nil {
		entityAcc = iaccessor.NewAccessor(store)
	}

	mg := &MasterGame{
		Core:  newCore(cfg, store, syncReg),
		state: nil,
		rooms: room.NewMasterRegistry(),
	}
	mg.Core.Logic = core
	mg.Core.entityAcc = entityAcc
	mg.bridge.attach(core)
	timeutil.Init(cfg.Misc.Timezone)
	mg.Core.Timer = NewTimeEvent(timeutil.Location())
	mg.Core.nodeID = "master"
	return mg
}

// Stop 停止 MasterGame 内核，释放全部资源。重复调用只生效一次。
func (mg *MasterGame) Stop() {
	mg.stopOnce.Do(func() {
		// 关闭前先把排行榜内存数据备份到 Redis，避免重启后数据丢失。
		if mg.state != nil {
			if err := mg.state.BackupAll(context.Background()); err != nil {
				logger.Errorf("master stop: rank BackupAll failed: %v", err)
			}
			if rdb := mg.state.RDB(); rdb != nil {
				_ = rdb.Close()
			}
		}
		mg.bridge.stop()
		mg.Logic.Stop()
		mg.closeCoreBackends()
	})
}

// NATS 下行链路
// setupNATSBackends 为 Master 注入 NATS 下行链路（落库广播到 Game 节点）。
func (mg *MasterGame) setupNATSBackends(nc *nats.Client, subject string, store *data.Store) {
	pub := natsRawPublisher{nc}
	subject = config.DefString(subject, proto.NATSSubjectNotify)
	mg.notifyNC = nc
	mg.notifyPub = pub
	mg.notifySubject = subject

	if store != nil {
		mg.entityAcc = iaccessor.NewNotifyingAccessor(store, pub, subject)
	}

	mg.Logic.SetSyncDownlink(pub, subject, mg.syncReg)
	mg.Logic.SetNotifyDownlink(pub, subject)

	// 跨服内存镜像链路
	mirrorSubject := subject + ".mirror"
	mg.Logic.SetMirrorDownlink(pub, mirrorSubject)
	subscribeMirror(nc, mirrorSubject, store)

	logger.Infof("master: NATS backend ready, subject=%s mirror=%s", subject, mirrorSubject)
}

// OnMsg 注册 Master TCP 消息 handler（签名与 Game.OnMsg 一致：func(c event.Ctx) error）。
//
// 内部走共用桥接（tcpMsgBridge）：
//
//	TCP 帧到达 → 封包 GWLogicPacket → Logic.Dispatch → handler(c) → commitEdits → pushBatch → NATS
func (mg *MasterGame) OnMsg(msgID uint32, h event.Handler) { mg.bridge.OnMsg(msgID, h) }

// InternalOnMsg 注册**引擎内部保留号**（≤ proto.InternalMsgMax）的 Master handler。
// 供引擎内部包使用（room 的 master 侧房间协议 6001..6004）；业务注册走 OnMsg，
// 后者「msgID 必须 > InternalMsgMax」的守卫保持不变。
func (mg *MasterGame) InternalOnMsg(msgID uint32, h event.Handler) { mg.bridge.InternalOnMsg(msgID, h) }

// setServer 绑定 TCP 服务端，并把之前缓存的 handler 全部补注册。
func (mg *MasterGame) setServer(srv *tcpmsg.Server) { mg.bridge.setServer(srv) }

// OnEvent 注册 Master 侧领域事件 handler。
func (mg *MasterGame) OnEvent(typ string, h event.Handler) { mg.Logic.OnEvent(typ, h) }

// OnHTTP 注册 Master 侧 HTTP 控制面 handler。
func (mg *MasterGame) OnHTTP(pattern string, h event.HTTPHandler) { mg.Logic.OnHTTP(pattern, h) }

// Reply / ReplyRaw 继承自嵌入的 *Core（见 core.go），四个角色共用一份实现。

// SendToAll 从 Master 向全部 Game 节点广播事件。
func (mg *MasterGame) SendToAll(typ string, payload any) {
	// 事件分发失败不能静默：handler panic / 编码失败等会被 EmitEvent 聚合成错误。
	if err := mg.Logic.EmitEvent(typ, nil, payload); err != nil {
		logger.Warnf("master: emit event %s failed: %v", typ, err)
	}
}

// Bind 从请求 body 反序列化 JSON。
func (mg *MasterGame) Bind(body []byte, v any) error {
	return ujson.Unmarshal(body, v)
}

// NodeID 返回当前节点标识。
func (mg *MasterGame) NodeID() string { return mg.nodeID }

// MasterRegistry 返回 master 侧房间 owner 注册表。
func (mg *MasterGame) MasterRegistry() *room.MasterRegistry { return mg.rooms }

// runMaster 启动 master 协作服，返回 stop 函数。
// 启动顺序：数据存储 → MasterGame 内核 → State 注入 → NATS → TCP Server → 业务挂载。
func runMaster(cfg *Config) (func(), error) {
	// 1. 数据存储（MySQL / 内存模式）
	store, err := data.NewStore(cfg.Data)
	if err != nil {
		return nil, fmt.Errorf("master: create store: %w", err)
	}

	// 2. MasterGame 内核（headless Logic + Timer + store）
	mg := newMasterGame(cfg, store)
	if err := mg.Start(); err != nil {
		// 早退路径必须回收已建的存储：关闭失败也要留痕（否则连接泄漏无人可查）。
		if cerr := store.Close(); cerr != nil {
			logger.Warnf("master: close store after game core start failure: %v", cerr)
		}
		return nil, fmt.Errorf("master: start game core: %w", err)
	}

	// 3. 内存 State + session token 后端（默认内存，配置 redis 时切到 Redis）
	st := state.NewState()
	mg.state = st

	stCfg := cfg.MasterSessionToken.Normalize()
	if stCfg.Backend != master.SessionTokenBackendRedis {
		st.UseMemorySessionToken(stCfg.TTL)
	}

	// 4. Redis（排行榜备份恢复 + 可选 session token Redis 后端）
	if cfg.Data.Redis.Addr != "" {
		rdb, err := iredis.NewClient(cfg.Data.Redis)
		if err != nil {
			mg.Stop()
			return nil, err
		}
		st.SetRedis(rdb)

		if stCfg.Backend == master.SessionTokenBackendRedis {
			st.UseRedisSessionToken(rdb, stCfg.KeyPrefix, stCfg.TTL)
		}

		if err := st.RestoreAll(context.Background()); err != nil {
			logger.Warnf("master: restore rank from redis: %v", err)
		}
	} else if stCfg.Backend == master.SessionTokenBackendRedis {
		logger.Warnf("master: session_token.backend=redis but data.redis.addr is empty, falling back to memory")
	}

	// 5. NATS（数据下发到 Game 节点）
	var nc *nats.Client
	if cfg.NATS.Addr != "" {
		var err error
		nc, err = nats.NewClient(cfg.NATS)
		if err != nil {
			mg.Stop()
			return nil, err
		}
		mg.setupNATSBackends(nc, cfg.NATSSubject, store)
	}

	// 6. TCP 服务端（先创建不监听，等 handler 注册完毕再 Listen）
	listenAddr := config.DefString(cfg.MasterListenAddr, defaultMasterListenAddr)
	// 6.0 传输安全前置校验（fail-fast）：非回环地址必须配共享密钥，否则拒绝启动。
	if err := validateMasterListen(listenAddr, cfg.MasterToken); err != nil {
		// 与后续各早退路径一致：mg.Stop() 负责回收已建的后端连接（此时探测器尚未启动）。
		mg.Stop()
		return nil, err
	}

	// 6.1 节点健康探测：心跳超时判 Dead 并摘除死节点。
	// 与集群拓扑无关（单机部署同样生效），因此不随任何开关启停。
	det, healthStop := setupMasterHealth(cfg, st)

	// 必须先 Create（仅创建+注册内置 handler）再 Listen（注册完业务 handler 后监听）：
	// 提前监听会让 runMasterBusinesses 之前的连接命中未注册 msgID。
	srv := server.Create(listenAddr, st, cfg.MasterToken)
	logger.Infof("master: TCP created on %s (not listening yet, token_auth=%t)", listenAddr, cfg.MasterToken != "")
	mg.setServer(srv)

	// 6.2 注册节点健康 handler（心跳上报 / 健康视图）。
	// 纯增量注册，不影响 server.Create 内置的任何消息号。
	server.RegisterHealth(srv, st, det)

	// 7. 业务挂载
	cfg.MasterGame = mg
	if err := RunMounts(RoleMaster, mg); err != nil {
		healthStop()
		mg.Stop()
		return nil, err
	}

	// 所有 handler 注册完毕，开始监听；监听失败即视作启动失败，交给上层中止流程。
	//
	// 未配置地址 = **不启用**这条通道（默认拒绝姿态）。此前把空地址直接交给
	// net.Listen("tcp", "")：TCP 语义里那是「绑定所有网卡 + 随机端口」——既不可达
	//（端口是随机的，game 无从发现），又把 master 内部 RPC 暴露在所有网卡上，
	// 是「看起来没开、实际全开」的最坏形态。需要跨机时显式配地址（并配 master_token）。
	if listenAddr == "" {
		logger.Warnf("master: master_listen_addr 未配置 —— 内部 RPC 端口**不启用**（需要跨机访问请显式配置地址；" +
			"配置成非回环地址时还必须同时配置 master_token）")
	} else {
		if err := server.Listen(srv); err != nil {
			healthStop()
			mg.Stop()
			return nil, err
		}
		logger.Infof("master: TCP serving on %s", listenAddr)
	}

	// 8. 分片注册：把本分片地址登记到 etcd，供 game 侧按 uid 归属发现。
	// 未配 etcd 时仅告警跳过——game 仍可经静态 master_addr 直连（单分片行为）。
	shard := cfg.MasterShard.Normalize()
	shardEc, closeShardEc := newEtcdClientForRole(cfg, roleMaster)
	unregisterShard := func() {}
	if shardEc != nil {
		stopReg, regErr := registerMasterShard(context.Background(), shardEc, shard.Index, shard.Total, listenAddr)
		if regErr != nil {
			// 注册失败不阻断启动：单分片/静态地址部署仍可工作，但多分片部署必须能查到这条日志。
			logger.Errorf("master: register shard %d/%d failed: %v", shard.Index, shard.Total, regErr)
		} else {
			unregisterShard = stopReg
		}
	} else if shard.Sharded() {
		logger.Warnf("master: shard.total=%d but etcd is not configured; shards cannot be discovered "+
			"(game will fall back to static master_addr)", shard.Total)
	}
	if shard.Sharded() && stCfg.Backend != master.SessionTokenBackendRedis {
		// 多分片 + memory token 后端：**只告警、不阻断**（本期口径，与 master分片-步骤文档.md
		// §七「S5 排行榜 / token 分片」行一致：「memory token 后端在分片下同样正确（按 playerID
		// 路由），多分片仍推荐 redis（重启不丢）」）。
		//
		// 不阻断的依据：session 客户端所有操作都经 Client.ForKey(playerID) 路由到属主分片
		//（见 internal/domain/master/client/session_client.go），每个 player 的 token 恒落
		// 自己的分片、不存在跨分片校验 —— memory 与 redis 的正确性相同，差别仅是属主分片
		// 重启会丢 token、该批玩家需重新登录。
		//
		// ⚠️ 注意文档自相矛盾：master分片-步骤文档.md §四「S5 落点」表写「多分片部署**必须**
		// backend=redis」——那是给运维的部署建议，不是本进程的启动约束。此处刻意不做硬校验，
		// 避免把「重启需重登」这一非致命差异升级为启动失败；两处口径如需统一，应改文档。
		logger.Warnf("master: shard.total=%d with session_token.backend=%s; players on a restarted shard "+
			"must re-login — redis backend is recommended", shard.Total, stCfg.Backend)
	}

	stop := func() {
		unregisterShard()
		closeShardEc()
		healthStop()
		_ = srv.Stop()
		mg.Stop()
	}
	return stop, nil
}

// validateMasterListen 校验 master 内部 RPC 监听地址的安全边界（与 admin 控制面同一口径：
// 「要么只绑回环，要么带共享密钥」，两者同时不满足 ⇒ **拒绝启动**，不是只告警）。
//
// 为什么 master 内部 RPC 必须按同一口径管：这条通道上的
// MsgSessionNew / MsgSessionValidate / MsgPlayerRegister / MsgRank* 等接口
// **没有调用方身份校验**（见 domain/master/server 的说明），能连到该地址者即可
// 伪造他人登录态、篡改玩家定位与排行榜。admin 控制面此前也踩过同一个坑，故统一：
// 默认只绑回环；要绑非回环就必须配 master_token（则连接首帧必须完成 MsgAuth 握手）。
func validateMasterListen(addr, token string) error {
	// 空地址 = 不启用（调用方不会监听），无需校验。
	if addr == "" {
		return nil
	}
	if apptypes.IsLoopbackAddr(addr) {
		// 只绑回环：本机可达范围，与加固前行为一致（默认部署零感知）。
		return nil
	}
	if token == "" {
		logger.Errorf("master: 拒绝启动 —— master_listen_addr=%q 不是回环地址但 master_token 为空；"+
			"请改回回环（127.0.0.1），或同时配置 master_token（内部 RPC 上挂着无身份校验的 "+
			"session/player/rank 写接口，任何能连到该端口的人都能改他人登录态与排行榜）", addr)
		return fmt.Errorf("%w (master_listen_addr=%q)", errMasterNonLoopbackWithoutToken, addr)
	}
	logger.Warnf("master: 内部 RPC 绑在非回环地址 %s，已启用共享密钥握手 —— 仍建议由内网隔离 / 防火墙再兜一层", addr)
	return nil
}

// errMasterNonLoopbackWithoutToken 见 validateMasterListen 的说明（启动期硬错误，不可忽略）。
var errMasterNonLoopbackWithoutToken = errors.New("master: listen_addr is not loopback but master_token is empty")

// setupMasterHealth 组装 master 的节点健康探测：周期扫描节点表，
// 心跳超时判定 Dead 后摘除死节点（跨服事件总线据此停止向死节点路由）。
//
// 该能力与集群拓扑无关：单机部署同样生效，因此不随任何开关启停。
// 返回的 stop 函数负责停止探测并回收后台 goroutine（幂等）。
func setupMasterHealth(cfg *Config, st *state.State) (*failover.Detector, func()) {
	det := failover.NewDetector(cfg.MasterHealth)

	// 节点注册即纳入心跳跟踪，摘除即停止跟踪。
	// 挂在 Detector 而不是 TCP 层，保证不依赖连接（内嵌部署 / 单测）也有完整健康视图。
	st.OnNodeRegistered(func(nodeID string) { det.Track(nodeID) })
	st.OnNodeRemoved(func(nodeID string) { det.Untrack(nodeID) })

	// 心跳超时判 Dead：摘除死节点，同时清掉该节点下的玩家定位与反向索引，
	// 否则跨节点转发会一直投递到已下线的目标。
	det.OnNodeDown(func(nodeID string) {
		if err := st.RemoveNodeWithReason(context.Background(), nodeID, metrics.RemoveReasonDead); err != nil {
			logger.Warnf("master/health: remove dead node %s: %v", nodeID, err)
		}
	})

	stopCh := make(chan struct{})
	det.Start(stopCh)

	// stop 幂等：监听失败路径与正常退出路径都会调用它。
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(stopCh)
			det.Stop()
		})
	}
	return det, stop
}
