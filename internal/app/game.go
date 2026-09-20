package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	authdom "clover-server-engine/internal/domain/auth"
	authclient "clover-server-engine/internal/domain/auth/client"
	"clover-server-engine/internal/domain/data"
	idataaccount "clover-server-engine/internal/domain/data/account"
	idataorder "clover-server-engine/internal/domain/data/order"
	idataplayer "clover-server-engine/internal/domain/data/player"
	"clover-server-engine/internal/domain/log/logbuf"
	masterclient "clover-server-engine/internal/domain/master/client"
	masterstate "clover-server-engine/internal/domain/master/state"
	"clover-server-engine/internal/domain/sessiontoken"
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/internal/transport"
	"clover-server-engine/internal/transport/etcd"
	"clover-server-engine/internal/transport/event"
	engine "clover-server-engine/internal/transport/event/engine"
	"clover-server-engine/internal/transport/nats"
	iauth "clover-server-engine/internal/transport/net/auth"
	"clover-server-engine/internal/transport/net/push"
	"clover-server-engine/internal/transport/pubsub"
	"clover-server-engine/internal/transport/tcpmsg"
	apptypes "clover-server-engine/pkg/app/types"
	"clover-server-engine/pkg/domain/object"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/timeutil"
)

// Game 游戏服（事件驱动）。
// Core 嵌入后自动继承 Data/Store/Push/Monitor/Timer 等全部能力。
type Game struct {
	*Core

	// 认证：引擎唯一实现 RemoteAuthenticator（每次登录调账号服 /auth/verify 换 owner）。
	// 无业务注入点——账号体系的扩展点在账号服，不在游戏服。
	auth iauth.Authenticator

	// 业务 store
	accountStore *idataaccount.Store
	channelStore *idataaccount.ChannelStore
	playerStore  *idataplayer.Store
	orderStore   *idataorder.Store

	// 服务发现
	etcdCli    *etcd.Client
	etcdCancel func()

	// 跨机对象迁移（MMO 切场景）：本节点数字 ID + scene→node 路由表 + 订阅能力。
	// 未配 node_id 或未接 NATS 时分别为 0 / 非 nil（内存后端）/ nil，此时迁移不启用。
	nodeID     uint64
	sceneRoute *transport.RouteStore
	sceneSub   pubsub.Subscriber

	// auth / log 多实例发现：按 etcd 前缀轮询选实例（未配 etcd 时退化为配置里的静态地址）。
	authResolver *serviceResolver
	logResolver  *serviceResolver

	// nodeDir 节点目录（etcd）本地视图：按类型 / tag 查存活节点，取代向 master 查节点表。
	// 未配 etcd 时 Enabled() 为 false，查节点回落到 master TCP 读路径。
	nodeDir *nodeDirectory

	// 引擎事件
	engineEv *engine.Dispatcher

	// 跨服事件
	crossNodeBus *event.CrossNodeEventBus

	// 心跳上报：master 依据心跳判定本节点存活并采集负载。
	heartbeat *masterclient.HeartbeatModule

	// 业务日志缓冲器（攒积→批量上报 log 服；多实例时按片轮询分发）
	logBuf *logbuf.Buffer
	// 缓冲器缺失只告警一次，避免每条业务日志都刷屏。
	logBufWarnOnce sync.Once

	// 连接状态
	connMgr *ConnectionManager

	// 生命周期控制
	stopped  atomic.Bool    // Stop 入口判 stopped，防止回调击穿
	inflight sync.WaitGroup // 在途 handler 计数，Stop 前等待
	// handlerGate 互斥「stopped 检查 + inflight.Add」（见 trackHandler）：
	// 保证 Stop 的 inflight.Wait 开始后不会再有 Add 并发进来
	//（WaitGroup 的 Add 与 Wait 并发误用会 panic）。
	handlerGate sync.Mutex

	// bizHooks 业务侧消息级横切钩子（经 OnBeforeDispatch 注册）。
	// 装配期（RegisterMount / bootstrap）写入，运行期只读，故无需加锁。
	bizHooks []func(*event.Ctx) error

	// 灰度下线（drain）：drainer 惰性创建，drainMu 保护其引用本身。
	// draining 是 drainer.running 的无锁镜像——BeforeDispatch 每帧都要读，
	// 走 atomic 避免给派发主链路加两把锁。
	drainMu  sync.RWMutex
	drainer  *Drainer
	draining atomic.Bool

	// nodeTags 本节点向 master 注册时携带的业务标签，drain 取消后重新注册时复用。
	nodeTags []string

	// 领域子模块（从 app 提取）
	sessionStore *sessiontoken.Store // session token 生命周期管理
	fullSyncer   *FullSyncer         // 玩家全量数据同步器

	// admin 运维控制面：业务通过 OnAdminHTTP 注册的自定义探针路由。
	// 在 runMounts 之后统一挂到 admin server（admin server 在 mount 之后才 Start）。
	adminRoutes []adminRoute

	// objMgr：对象管理器，用于 SendEventToGObject / OnGObjectEvent。
	// 启动时自动创建，业务无需手动注入。
	objMgr *object.Manager
}

// adminRoute 业务注册的 admin 探针路由。
type adminRoute struct {
	pattern string
	h       http.HandlerFunc
}

// natsRawPublisher 把 NATS 客户端适配为 data.Publisher。
type natsRawPublisher struct{ c *nats.Client }

func (p natsRawPublisher) Publish(subject string, data []byte) error {
	return p.c.PublishRaw(subject, data)
}

// GameStores 包装所有结构化基础表存储。
type GameStores struct {
	Store        *data.Store
	AccountStore *idataaccount.Store
	ChannelStore *idataaccount.ChannelStore
	PlayerStore  *idataplayer.Store
	OrderStore   *idataorder.Store
}

// NewGame 构造游戏服并注册默认 handler。
func NewGame(cfg *Config, stores *GameStores) *Game {
	g := &Game{
		auth:         newAuthenticator(cfg),
		accountStore: stores.AccountStore,
		channelStore: stores.ChannelStore,
		playerStore:  stores.PlayerStore,
		orderStore:   stores.OrderStore,
		objMgr:       object.NewManager(),
	}
	grace := cfg.Logic.ReconnectGrace
	if grace <= 0 {
		grace = 30 * time.Second
	}
	g.connMgr = NewConnectionManager(grace)

	syncReg := event.NewSyncRegistry()
	core := event.New(event.Config{
		ListenAddr:     cfg.Logic.ListenAddr,
		HTTPListen:     cfg.Logic.HTTPListen,
		Heartbeat:      cfg.Logic.Heartbeat,
		FrameTimeout:   cfg.Logic.FrameTimeout,
		BeforeDispatch: g.newBeforeDispatchHook(),
	},
		event.WithStore(stores.Store),
		event.WithSyncDownlink(nil, "", syncReg))

	g.Core = newCore(cfg, stores.Store, syncReg)
	g.Core.Logic = core
	timeutil.Init(cfg.Misc.Timezone)
	g.Core.Timer = NewTimeEvent(timeutil.Location())
	g.engineEv = engine.NewDispatcher(engine.AsEmitter(core))
	g.registerEngineDisconnectHandler()

	// 登录 / 恢复会话同样经 trackHandler 计入 inflight：
	// 这两条路径会读写 store / session token，Stop 的等待必须覆盖它们。
	g.Logic.InternalOnMsg(proto.EMsgLogin,
		g.trackHandler(iauth.Handler(proto.EMsgLogin, g.auth)))
	// 不挂注册 handler：注册是账号服的职责（POST {账号服}/auth/signup），
	// 游戏服不该存在可落库的注册入口。原 EMsgSignup=1 号位已作废保留（不再有 handler，
	// 且不在网关免登录白名单里——客户端发过去会先被登录门禁挡下）。
	g.Logic.InternalOnMsg(proto.EMsgResumeSession,
		g.trackHandler(iauth.ResumeSessionHandler(proto.EMsgResumeSession,
			g.ValidateSessionToken, g.AccountOfPlayer, g.DeleteSessionToken, g.KickConn,
			func(playerID, account string) {
				g.engineEv.EmitPlayerSessionResumed(engine.SessionResumedPayload{
					PlayerID: playerID, Account: account,
				})
			})))

	// 业务日志缓冲器的装配在 bootstrap —— 它要接入服务发现做多实例分发，
	// 必须等服务发现 resolver start 之后才能建，故不在此处建。
	return g
}

// newAuthenticator 构造登录认证器。
//
// 引擎只有一种实现：RemoteAuthenticator——每次登录都调账号服 /auth/verify 换取 owner。
// 不设业务注入点：账号体系的扩展点应该在账号服，而不是在游戏服留一个能接管整条登录
// 链路的平行实现——那会让「账号服必须在线」这条不变量出现例外。
func newAuthenticator(cfg *Config) iauth.Authenticator {
	// 地址缺失属配置错误——静默退化会让「账号服必须启动」形同虚设，故启动即失败。
	if cfg.Auth.VerifyAddr == "" {
		panic("app: auth.verify_addr is required (every login is verified against the auth server, e.g. https://127.0.0.1:8051)")
	}
	logger.Infof("app: login verified via auth server %s", cfg.Auth.VerifyAddr)
	// TLS 校验方式由配置决定（自签 / 内网 CA 用 verify_ca_file；默认走系统根证书）。
	// 账号服侧的证书要求见 AuthConfig.ValidateAuthServer（默认要求 TLS）。
	return authclient.NewRemoteAuthenticator(cfg.Auth.VerifyAddr, cfg.Auth.VerifyTimeout,
		authclient.WithCACertFile(cfg.Auth.VerifyCAFile),
		authclient.WithInsecureSkipVerify(cfg.Auth.VerifyInsecureSkipVerify),
	)
}

// newBeforeDispatchHook 返回 BeforeDispatch 回调。
// 按序执行：停机检查 → 连接空值检查 → 灰度下线检查 →
// 重连 bag 迁移 → 绑定 connBag → 恢复 playerID → session token 续期 → 跨服玩家注册。
func (g *Game) newBeforeDispatchHook() func(*event.Ctx) error {
	return func(c *event.Ctx) error {
		if g.stopped.Load() {
			return errors.New("game stopped")
		}
		cid := c.ConnID()
		if cid == "" {
			return nil
		}
		// 灰度下线中：本机不再受理新连接。有迁移目标时就地切走
		// （网关先 dial 新进程再替换上游，客户端 TCP 不断），无目标则直接拒绝。
		// 两种情况都返回错误，客户端重连即落到新版本进程。
		if g.Draining() {
			if tgt := g.drainTarget(); tgt != "" {
				if err := g.SwitchUpstream(cid, tgt); err != nil {
					logger.Warnf("app: drain switch upstream %s -> %s: %v", cid, tgt, err)
				} else {
					g.markMigrated(cid)
				}
			}
			return ErrDraining
		}
		mkey := c.Account()
		if mkey == "" {
			mkey = cid
		}

		g.connMgr.RestoreBag(mkey, cid)
		bag := g.connMgr.EnsureBag(cid)
		c.SetConnBag(bag)
		RestorePlayerID(bag, c.SetPlayerID)
		// 已认证连接（已绑定 playerID）：在统一消息链路中自动续期 session token。
		// Store.Refresh 内部按 refreshInterval 节流，续期失败不影响本次请求；
		// 但失败必须留痕——否则 token 过期只会表现为后续鉴权失败，无从排查。
		if pid := c.PlayerID(); pid != "" {
			if err := g.RefreshSessionToken(pid); err != nil {
				logger.Warnf("app: refresh session token for %s failed: %v", pid, err)
			}
		}
		g.registerCrossNodePlayer(c.Context(), mkey, cid)

		// 业务侧消息级横切钩子（鉴权 / 审计 / 前置条件），按注册顺序依次执行；
		// 任一返回 error 即拒绝本次派发，错误以 EErrorReply 回包（带码见 proto.BizError）。
		// 与网关登录门禁的分工见 OnBeforeDispatch 的注释。
		for _, fn := range g.bizHooks {
			if err := fn(c); err != nil {
				return err
			}
		}
		return nil
	}
}

// OnBeforeDispatch 注册业务侧「消息级横切钩子」。
//
// 引擎内置横切逻辑（停机检查 / 灰度下线迁移 / 连接 KV 与身份恢复 / session token 续期 /
// 跨服玩家注册）执行完毕之后，按注册顺序依次执行本钩子；任一返回 error 即拒绝本次派发，
// 错误以 EErrorReply 回包——返回 *proto.BizError 时携带错误码，客户端可据此做统一处理
// （如 401 回到登录流程），无需匹配错误文案。
//
// 与登录门禁的分工（对齐主流分层）：
//   - 网关负责「你是谁」：连接级登录门禁，未登录连接在门口即被拒
//     （见 gwcore.Config.AuthDisabled / AuthExemptMsgIDs）；
//   - 本钩子负责「你能不能做这个」：消息级业务权限（房间归属、状态前置条件、频控、
//     审计等），这些是业务语义，引擎无从判断。
//
// 本钩子位于每条消息的派发主链路上，实现必须保持轻量（禁止阻塞 I/O）。
// 应在装配阶段（RegisterMount / bootstrap）注册，运行期不再增删。
func (g *Game) OnBeforeDispatch(fn func(c *event.Ctx) error) {
	g.bizHooks = append(g.bizHooks, fn)
}

// registerCrossNodePlayer 向 master 注册本 node 持有该 player（懒注册，幂等）。
func (g *Game) registerCrossNodePlayer(ctx context.Context, mkey, cid string) {
	if g.crossNodeBus == nil || mkey == cid {
		return
	}
	_ = g.crossNodeBus.OnPlayerEnter(ctx, mkey)
	// 标记数据层 "本节点在线"，后续该玩家的 TierSnapshot 读写走内存路径
	g.Data().SetOnline(data.OwnerPlayer, mkey)
}

// trackHandler 给 handler 包一层「停机闸门 + 在途计数」：
//   - Stop 开始后到达的 handler 一律拒绝，不再触碰已关闭资源；
//   - 在途 handler 计入 inflight，Stop 等待其完成后才 closeBackends。
//
// 「检查 stopped + Add」用 handlerGate 做成临界区：否则 handler 可能在
// inflight.Wait 已开始等待、计数恰为 0 的瞬间 Add，命中 WaitGroup
// 「Add called concurrently with Wait」panic；同时保证 Wait 返回后不遗漏在途 handler。
func (g *Game) trackHandler(h event.Handler) event.Handler {
	return func(c *event.Ctx) error {
		g.handlerGate.Lock()
		if g.stopped.Load() {
			g.handlerGate.Unlock()
			return errors.New("game stopped")
		}
		g.inflight.Add(1)
		g.handlerGate.Unlock()
		defer g.inflight.Done()
		return h(c)
	}
}

func (g *Game) OnMsg(msgID uint32, h event.Handler, priority ...int) {
	if msgID <= proto.InternalMsgMax {
		panic(fmt.Sprintf("app: business message id must be > %d; got %d", proto.InternalMsgMax, msgID))
	}
	// 包装后确保 Stop() 前所有在途 handler 执行完毕。
	g.Logic.OnMsg(msgID, g.trackHandler(h), priority...)
}

func (g *Game) OnEvent(typ string, h event.Handler) {
	g.Logic.OnEvent(typ, g.trackHandler(h))
}

func (g *Game) OnHTTP(pattern string, h event.HTTPHandler) { g.Logic.OnHTTP(pattern, h) }

// OnAdminHTTP 注册一个 admin 运维控制面探针端点（health / ready / metrics / debug 等）。
// 风格与 OnHTTP 一致，但挂在独立的 admin HTTP 服务（默认 127.0.0.1:8041，见 AdminConfig.DefaultListenAddr）上，
// 供运维 / 监控系统访问，不参与游戏业务逻辑。
// 必须在 RegisterMount 回调内调用（引擎在 runMounts 之后统一挂载到 admin server）。
func (g *Game) OnAdminHTTP(pattern string, h http.HandlerFunc) {
	if h == nil || pattern == "" {
		return
	}
	g.adminRoutes = append(g.adminRoutes, adminRoute{pattern: pattern, h: h})
}

// SwitchUpstream 通知网关将该连接的客户端迁移到目标节点。
// 业务层不需要直接操作 NATS，一行调用即可完成连接迁移。
func (g *Game) SwitchUpstream(connID, targetAddr string) error {
	return g.Logic.PublishGWControl(proto.GWControlSwitchUpstream{
		ConnID:      connID,
		NewUpstream: targetAddr,
	})
}

func (g *Game) ClearConn(connID string)           { g.connMgr.DeleteBag(connID) }
func (g *Game) ClearConnByAccount(account string) { g.connMgr.ClearByOwner(account) }

// KickConn 踢下线指定连接并清理其连接数据。

// 两条执行路径：
//   - all 模式：ConnectionManager 持有网关注入的 kick 回调（见 SetKicker），本地直接执行；
//   - gateway / game 分离部署：本进程没有网关实例，改发 GWControlKick 控制指令，
//     由网关侧执行踢人。两者对业务完全等价。
func (g *Game) KickConn(connID string) bool {
	if connID == "" {
		return false
	}
	if g.connMgr.Kick(connID) {
		return true
	}
	if err := g.Logic.PublishGWControl(proto.GWControlKick{ConnID: connID, Kick: true, Reason: "kick"}); err != nil {
		logger.Warnf("app: publish GWControlKick %s: %v", connID, err)
		return false
	}
	return true
}

func (g *Game) SetKicker(fn func(connID string) bool) { g.connMgr.SetKicker(fn) }

func (g *Game) sendConnEvent(typ string, payload any) {
	// Stop 后网关回调仍可能触发（回调闭包直接捕获 g、不经 currentGame.Load()）：
	// 此处判 stopped 拦下，避免事件派发走到已 Close 的 store / bus。
	if g.stopped.Load() {
		return
	}
	// 断开事件自动填充 PlayerID（从 connBag 读取），
	// 确保 DeleteSessionToken 使用正确的 playerID 而不是 owner(account)。
	if e, ok := payload.(*ConnDisconnectEvent); ok && e.PlayerID == "" {
		e.PlayerID = g.connMgr.GetPlayerID(e.ConnID)
	}
	var parent *event.Ctx
	if owner := connEventOwner(payload); owner != "" {
		parent = event.NewAccountCtx(context.Background(), owner)
	}
	if err := g.Logic.EmitEvent(typ, parent, payload); err != nil {
		logger.Warnf("app: emit conn event %s failed: %v", typ, err)
	}
}

func connEventOwner(payload any) string {
	switch e := payload.(type) {
	case *ConnDisconnectEvent:
		return e.Owner
	case *ConnConnectEvent:
		return e.Owner
	case *ConnReconnectEvent:
		return e.Owner
	case *ConnKickedEvent:
		return e.Owner
	default:
		return ""
	}
}

// 数据访问句柄
func (g *Game) EngineEmitter() engine.Emitter            { return g.engineEv }
func (g *Game) PlayerStore() *idataplayer.Store          { return g.playerStore }
func (g *Game) AccountStore() *idataaccount.Store        { return g.accountStore }
func (g *Game) ChannelStore() *idataaccount.ChannelStore { return g.channelStore }
func (g *Game) OrderStore() *idataorder.Store            { return g.orderStore }

// GetEtcd 返回 etcd 客户端（未配置 etcd 时为 nil）。
func (g *Game) GetEtcd() *etcd.Client { return g.etcdCli }

// NodeID 返回本节点在集群内的数字标识（跨机对象迁移的路由键）；未配置时为 0。
func (g *Game) NodeID() uint64 { return g.nodeID }

// SceneRoute 返回 scene→node 路由表，供 mmo.WithClusterRoute 注入。
// 未接 NATS 时为 nil（此时跨机迁移整体不启用）。
func (g *Game) SceneRoute() *transport.RouteStore { return g.sceneRoute }

// SceneSubscriber 返回订阅能力，供 mmo.WithRemoteTransferSubscriber 注入；
// 用于接收跨机迁移指令。未接 NATS 时为 nil。
func (g *Game) SceneSubscriber() pubsub.Subscriber { return g.sceneSub }

// AddLog 写入一条业务日志（必填字段 + option 选填），攒积后批量上报 log 服。
// log 服实例由服务发现轮询选择（多实例分发）；发送失败计入 logbuf.Stats 并告警。
// 缓冲器未装配时丢弃并**告警一次**（不再静默），以免业务日志无声消失。
func (g *Game) AddLog(ownerType, ownerID, typ, info string, opts ...logbuf.Option) {
	if g.logBuf == nil {
		g.logBufWarnOnce.Do(func() {
			logger.Warnf("game: business log buffer is not initialized, AddLog calls will be dropped")
		})
		return
	}
	g.logBuf.AddLog(ownerType, ownerID, typ, info, opts...)
}

// MasterCaller 描述游戏服节点调用 master 的最小能力。
// 由 app.Game 实现；room 等子模块通过结构化接口消费，无需反向依赖 app。
type MasterCaller interface {
	CallMaster(msgID uint32, req, resp any) error
	SwitchUpstream(connID, targetAddr string) error
}

// 编译期断言：app.Game 满足 MasterCaller。
var _ MasterCaller = (*Game)(nil)

// CallMaster 向 master 发送一条同步请求，阻塞等待响应。
func (g *Game) CallMaster(msgID uint32, req, resp any) error {
	addr := g.cfg.MasterAddr
	if addr == "" {
		addr = defaultMasterListenAddr
	}
	// master_token 非空时每条连接都要先完成 MsgAuth 握手（master 绑非回环时的前提）。
	var dialOpts []tcpmsg.DialOption
	if g.cfg.MasterToken != "" {
		dialOpts = append(dialOpts, tcpmsg.WithAuth(masterstate.MsgAuth, g.cfg.MasterToken))
	}
	cli, err := tcpmsg.Dial(addr, dialOpts...)
	if err != nil {
		return err
	}
	// 关闭失败只影响连接资源回收，不改变本次调用结果；留日志便于排查连接泄漏。
	defer func() {
		if cerr := cli.Close(); cerr != nil {
			logger.Warnf("app: close master client conn: %v", cerr)
		}
	}()
	return cli.Call(msgID, req, resp)
}

// resolveLogAddr 解析一个可用的 log 实例地址。
// 配了 etcd 时按前缀轮询（多实例横向扩展）；否则回退配置里的静态地址。
func (g *Game) resolveLogAddr() string {
	if g.logResolver != nil {
		if a := g.logResolver.pick(); a != "" {
			return a
		}
	}
	if addr := g.cfg.LogAddr; addr != "" {
		return addr
	}
	return defaultLogListenAddr
}

// resolveAuthAddr 解析一个可用的 auth 实例**消息通道**地址（同上，多实例时轮询）。
func (g *Game) resolveAuthAddr() string {
	if g.authResolver != nil {
		if a := g.authResolver.pick(); a != "" {
			return a
		}
	}
	if addr := g.cfg.Auth.RpcAddr; addr != "" {
		return addr
	}
	return authdom.DefaultRPCListenAddr
}

// CallLog 向 log 服发送一条同步业务请求（自研消息号），阻塞等待响应。
// 与 CallMaster 对称：业务在 log 服侧经 app.Mount(app.RoleLog, ...) + LogGame.OnMsg 注册 handler。
//
// log 服可多实例部署：配了 etcd 时按 `clover/services/log/` 前缀轮询选实例。
func (g *Game) CallLog(msgID uint32, req, resp any) error {
	cli, err := tcpmsg.Dial(g.resolveLogAddr())
	if err != nil {
		return err
	}
	defer func() {
		if cerr := cli.Close(); cerr != nil {
			logger.Warnf("app: close log client conn: %v", cerr)
		}
	}()
	return cli.Call(msgID, req, resp)
}

// CallAuth 向账号服**消息通道**发送一条同步业务请求（自研消息号），阻塞等待响应。
// 与 CallMaster / CallLog 对称：业务在 auth 服侧经 app.Mount(app.RoleAuth, ...) + AuthGame.OnMsg 注册 handler。
//
// 注意：**登录校验不走本通道**——它固定是 HTTP POST {auth.verify_addr}/auth/verify
// （见 RemoteAuthenticator）。本通道是给业务消息用的（账号查询、封号通知等）。
//
// auth 服可多实例部署：配了 etcd 时按 `clover/services/auth/` 前缀轮询选实例。
func (g *Game) CallAuth(msgID uint32, req, resp any) error {
	cli, err := tcpmsg.Dial(g.resolveAuthAddr())
	if err != nil {
		return err
	}
	defer func() {
		if cerr := cli.Close(); cerr != nil {
			logger.Warnf("app: close auth client conn: %v", cerr)
		}
	}()
	return cli.Call(msgID, req, resp)
}

func (g *Game) CreatePlayer(ctx context.Context, owner, name string, serverID uint32) (*idataplayer.EPlayer, error) {
	if g.playerStore == nil {
		return nil, fmt.Errorf("app: player store not available")
	}
	if serverID == 0 {
		return nil, fmt.Errorf("app: server_id must not be 0")
	}
	return g.playerStore.Create(ctx, owner, name, serverID)
}

// Alert 自适应弹窗：根据 ctx 自动选择路由 ID（PlayerID 优先，未登录时回退到 Account）。
// 复用 Core.AlertToPlayer 的唯一定播实现，不在此重复构造 PlayerChannel。
func (g *Game) Alert(c *event.Ctx, a *push.EAlertNotify) error {
	return g.AlertToPlayer(c.TargetID(), a)
}

// SendEventToPlayer 向指定玩家投递领域事件，自动跨节点寻址：本地玩家直投 OnEvent，远程玩家经 Master 定位路由到目标节点。
// c 为调用方 Ctx（原样克隆至 OnEvent handler，playerID/connID/account 等完整保留）。
func (g *Game) SendEventToPlayer(c *event.Ctx, playerID string, typ string, payload any) error {
	if playerID == "" {
		return errors.New("event: empty playerID")
	}
	if g.crossNodeBus == nil {
		if g.Logic != nil {
			return g.Logic.EmitEvent(typ, c, payload)
		}
		return nil
	}
	env := event.Envelope{
		ID:      event.GenID(),
		Type:    typ,
		Payload: payload,
	}
	return g.crossNodeBus.SendToPlayerWithCtx(c, playerID, env)
}

// SendQueueEventToPlayer 向指定玩家投递领域事件，并保证同一玩家的跨节点事件串行 FIFO。
// 语义与 SendEventToPlayer 一致，但远程目标会进入 per-player 队列顺序发送，避免并发等 ACK 导致乱序。
// 本地玩家仍走同步直投，不受队列影响。
func (g *Game) SendQueueEventToPlayer(c *event.Ctx, playerID string, typ string, payload any) error {
	if playerID == "" {
		return errors.New("event: empty playerID")
	}
	if g.crossNodeBus == nil {
		if g.Logic != nil {
			return g.Logic.EmitEvent(typ, c, payload)
		}
		return nil
	}
	env := event.Envelope{
		ID:      event.GenID(),
		Type:    typ,
		Payload: payload,
	}
	return g.crossNodeBus.SendQueueEventToPlayer(c, playerID, env)
}

// SendEventToAll 向全部 game node 广播领域事件（含本节点）：先 NATS 广播再本地派发。

// 跨节点无法序列化 Ctx，故本方法不接收调用方 Ctx——接收端 OnEvent handler 中
// c.PlayerID() / c.Account() / c.ConnID() 为空，业务需自行在 payload 中携带必要身份信息。
// crossNodeBus 未就绪（无 NATS 配置）时降级为本地 EmitEvent，单机部署与集群行为一致。
func (g *Game) SendEventToAll(typ string, payload any) error {
	if typ == "" {
		return errors.New("event: empty type")
	}
	if g.crossNodeBus == nil {
		if g.Logic != nil {
			return g.Logic.EmitEvent(typ, nil, payload)
		}
		return nil
	}
	env := event.Envelope{
		ID:      event.GenID(),
		Type:    typ,
		Payload: payload,
	}
	return g.crossNodeBus.SendToAll(context.Background(), env)
}

// ObjectManager 返回 Game 内置的对象管理器。
func (g *Game) ObjectManager() *object.Manager { return g.objMgr }

// SendEventToGObject 向指定游戏对象投递事件（纯本地内存调用，零网络开销）。
func (g *Game) SendEventToGObject(ctx context.Context, id object.ObjectID, eventType string, payload any) error {
	return notFoundHint(g.objMgr.SendEvent(ctx, id, eventType, payload))
}

// OnGObjectEvent 绑定 (对象类型, 事件名) → 事件处理器。
func (g *Game) OnGObjectEvent(objType uint16, eventType string, h object.EventHandler) {
	g.objMgr.OnEvent(objType, eventType, h)
}

// SendQueueEventToGObject 串行通道：向指定游戏对象投递事件，同一对象上的事件互斥执行。
// 需要原子读-改-写对象状态（怪物掉血、掉落归属、共享计数）时用这个。
func (g *Game) SendQueueEventToGObject(ctx context.Context, id object.ObjectID, eventType string, payload any) error {
	return notFoundHint(g.objMgr.SendQueueEvent(ctx, id, eventType, payload))
}

// notFoundHint 对象未找到时补一句排查提示：场景内对象注册在 SceneManager 的对象表中，
// 建 SceneManager 时若没传 WithObjectManager(g.ObjectManager())，两张表互不可见，
// 调用方会莫名拿到 ErrObjectNotFound 而查不出原因。
func notFoundHint(err error) error {
	if errors.Is(err, object.ErrObjectNotFound) {
		return fmt.Errorf("%w（目标不在 Game 的对象表中；若它是场景内对象，建 SceneManager 时请传 mmo.WithObjectManager(g.ObjectManager())）", err)
	}
	return err
}

// Reply / ReplyRaw 继承自嵌入的 *Core（见 core.go），四个角色共用一份实现。

// RegisterEngineHandler 注册引擎级消息处理器。供外部模块（masterRank 等）使用。
// 把 pkg 层 handler 签名 func(ctx, msgID, data) ([]byte, error) 适配为引擎 event.Handler：
// 取请求体交给 handler，非空返回值按 requestID 配对回包。
func (g *Game) RegisterEngineHandler(msgID uint32, handler func(ctx context.Context, msgID uint32, data []byte) ([]byte, error)) {
	if handler == nil {
		return
	}
	g.Logic.InternalOnMsg(msgID, g.trackHandler(func(ic *event.Ctx) error {
		reply, err := handler(ic.Context(), ic.MsgID(), ic.Body())
		if err != nil {
			return err
		}
		if len(reply) > 0 {
			ic.MarkReplied(reply)
		}
		return nil
	}))
}

// MasterClient 返回跨服 bus 的 master TCP 客户端。供外部模块使用。
func (g *Game) MasterClient() *masterclient.Client {
	if g.crossNodeBus == nil {
		return nil
	}
	return g.crossNodeBus.MasterClient()
}

// NodesByTag 查询包含指定 tag 的存活节点 ID 列表。
// 跨服场景下，业务可据此找到特定角色的 game 实例（如 tag="room" 的房间服）。
//
// 配了 etcd 时读节点目录本地视图（watch 维护，租约到期即摘除）；否则回落 master TCP。
// 两者都不可用时返回 nil。
func (g *Game) NodesByTag(ctx context.Context, tag string) ([]string, error) {
	if g.nodeDir.Enabled() {
		return g.nodeDir.ByTag(tag), nil
	}
	if g.crossNodeBus == nil {
		return nil, nil
	}
	ac := g.crossNodeBus.AuthorityClient()
	if ac == nil {
		return nil, nil
	}
	return ac.NodesByTag(ctx, tag)
}

// NodeDirectory 返回节点目录本地视图（供引擎内部按类型查节点）。
// 未配 etcd 时 Enabled() 为 false。
func (g *Game) NodeDirectory() *nodeDirectory { return g.nodeDir }

// 生命周期
func (g *Game) Start() error { return g.Logic.Start() }

func (g *Game) Addr() string { return g.Logic.Addr() }

// OnlineCount 返回当前活跃连接数（近似在线人数），供心跳上报负载。
func (g *Game) OnlineCount() int { return g.connMgr.CountActive() }

func (g *Game) Stop() {
	// 在闸门内标记 stopped：与 trackHandler 的「检查 + Add」互斥，
	// 保证下面 inflight.Wait 开始后不会再有新的 Add 并发进来
	//（WaitGroup 的 Add/Wait 并发误用会 panic）；此后到达的 handler 一律被拒。
	g.handlerGate.Lock()
	g.stopped.Store(true)
	g.handlerGate.Unlock()
	// 先摘掉全局引用，避免 closeBackends 期间 gateway 回调仍 Load 到已关闭的 Game。
	currentGame.CompareAndSwap(g, nil)
	g.Logic.Stop()
	// 等待在途 handler 全部完成，再关 store/NATS/etcd，
	// 避免 handler 访问已关资源导致 panic 或数据错乱。
	g.inflight.Wait()
	g.closeBackends()
}

func (g *Game) closeBackends() {
	g.closeOnce.Do(func() {
		var firstErr error
		capture := func(err error) {
			if err == nil {
				return
			}
			if firstErr == nil {
				firstErr = err
			}
			// 必须走 logger：落 stdout 的日志不被采集，等于后端关闭失败无人知。
			logger.Warnf("game: close backend: %v", err)
		}
		if g.logBuf != nil {
			g.logBuf.Close()
		}
		if g.heartbeat != nil {
			g.heartbeat.Stop()
		}
		if g.crossNodeBus != nil {
			capture(g.crossNodeBus.Close())
		}
		if g.etcdCancel != nil {
			g.etcdCancel()
		}
		if g.etcdCli != nil {
			// 本实例的注册撤销由 g.etcdCancel 完成（删 key + 回收租约）；这里只关连接。
			capture(g.etcdCli.Close())
		}
		if g.orderStore != nil {
			capture(g.orderStore.Close())
		}
		if g.playerStore != nil {
			capture(g.playerStore.Close())
		}
		if g.channelStore != nil {
			capture(g.channelStore.Close())
		}
		if g.accountStore != nil {
			capture(g.accountStore.Close())
		}
		g.closeCoreBackends() // Timer + notifyNC + store
		if firstErr != nil {
			// 聚合结果必须被消费：列出「本次关闭里有错误」的汇总，便于关停审阅。
			logger.Warnf("game: close backends finished with errors (first: %v)", firstErr)
		}
	})
}

// session token（委托 session.Store）
// NewSessionToken 生成新 session token。
func (g *Game) NewSessionToken(playerID string) (string, error) {
	if g.sessionStore == nil {
		return "", errors.New("session store not available")
	}
	return g.sessionStore.New(context.Background(), playerID)
}

// ValidateSessionToken 验证 session token。
func (g *Game) ValidateSessionToken(playerID, token string) bool {
	if g.sessionStore == nil {
		return false
	}
	ok, _ := g.sessionStore.Validate(context.Background(), playerID, token)
	return ok
}

// AccountOfPlayer 由 playerID 反查归属账号（owner）。
//
// 用途：恢复会话的回包**必须带上 owner**，网关（gwcore.WithExtractOwnerID）才会把
// 重连后的新连接绑回原 owner。恢复会话发生在新连接上（该连接没有登录动作，
// c.Account() 为空），因此只能走存储反查。缺了它，连接过不了网关登录门禁，
// 后续业务消息全部被拒为 401 —— 即「恢复成功但连接失能」。
func (g *Game) AccountOfPlayer(playerID string) string {
	if g.playerStore == nil || playerID == "" {
		return ""
	}
	p, err := g.playerStore.GetByID(context.Background(), playerID)
	if err != nil || p == nil {
		logger.Warnf("account lookup for player %s failed: %v", playerID, err)
		return ""
	}
	return p.Account
}

// DeleteSessionToken 删除 session token。
func (g *Game) DeleteSessionToken(playerID string) {
	if g.sessionStore != nil {
		g.sessionStore.Delete(context.Background(), playerID)
	}
}

// RefreshSessionToken 续期 session token（sliding session，供已认证消息链路自动续期）。
func (g *Game) RefreshSessionToken(playerID string) error {
	if g.sessionStore == nil {
		return nil
	}
	return g.sessionStore.Refresh(context.Background(), playerID)
}

// PushPlayerFullSync 将玩家的全量数据推送至客户端（通知推送，非直连）。
func (g *Game) PushPlayerFullSync(playerID, account string) {
	if g.fullSyncer != nil {
		g.fullSyncer.Push(playerID, account)
	}
}

// ConnDisconnectType 与 ConnDisconnectEvent 的定义位于 internal/transport/event/engine，这里是别名。
const ConnDisconnectType = engine.ConnDisconnectType

type ConnDisconnectEvent = apptypes.ConnDisconnectEvent

const (
	ConnConnectType        = "conn.connect"         // 首次连接（上线/登录成功）
	ConnReconnectType      = "conn.reconnect"       // 重连
	ConnSoftDisconnectType = "conn.soft_disconnect" // 软掉线（断线宽限期内）
	ConnKickedType         = "conn.kicked"          // 被多端互踢
)

type ConnConnectEvent struct {
	ConnID string
	Owner  string
}

type ConnReconnectEvent struct {
	ConnID string
	Owner  string
}

type ConnKickedEvent struct {
	ConnID string
	Owner  string
}

// OnDisconnect 订阅客户端断开连接事件。自动清理对应 owner 的定时任务。
//
// 这里清的是「scope == ev.Owner」这一个 scope，而 ev.Owner 是**连接级 owner**
// （登录回执 owner 字段，并被同时当作 AccountID 使用，见 bootstrap.go），**不是角色 ID**。
// 业务把「离线也要跑」的任务挂在同名 scope 上会被一并清掉，应改用带前缀的独立 scope。
func (g *Game) OnDisconnect(h func(owner string)) {
	g.Logic.OnEvent(ConnDisconnectType, g.trackHandler(func(c *event.Ctx) error {
		if ev, ok := c.Payload().(*ConnDisconnectEvent); ok {
			if ev.Owner != "" {
				g.Timer.StopTimerGroup(ev.Owner)
			}
			h(ev.Owner)
		}
		return nil
	}))
}

// OnSoftDisconnect 订阅软掉线事件（断线宽限期内）。
func (g *Game) OnSoftDisconnect(h func(owner string)) {
	g.Logic.OnEvent(ConnSoftDisconnectType, g.trackHandler(func(c *event.Ctx) error {
		if ev, ok := c.Payload().(*ConnDisconnectEvent); ok {
			h(ev.Owner)
		}
		return nil
	}))
}

// OnHardDisconnect 订阅硬掉线事件（宽限期结束或无宽限）。
// 与 OnDisconnect 相同：清理的是「scope == ev.Owner」这一个 scope（owner 为连接级 owner，不是角色 ID）。
func (g *Game) OnHardDisconnect(h func(owner string)) {
	g.Logic.OnEvent(ConnDisconnectType, g.trackHandler(func(c *event.Ctx) error {
		if ev, ok := c.Payload().(*ConnDisconnectEvent); ok {
			if ev.Owner != "" {
				g.Timer.StopTimerGroup(ev.Owner)
			}
			h(ev.Owner)
		}
		return nil
	}))
}

// OnKick 订阅被互踢下线事件。
func (g *Game) OnKick(h func(connID, owner string)) {
	g.Logic.OnEvent(ConnKickedType, g.trackHandler(func(c *event.Ctx) error {
		if ev, ok := c.Payload().(*ConnKickedEvent); ok {
			h(ev.ConnID, ev.Owner)
		}
		return nil
	}))
}

// OnConnect 订阅首次连接（上线/登录成功）事件。
func (g *Game) OnConnect(h func(owner string)) {
	g.Logic.OnEvent(ConnConnectType, g.trackHandler(func(c *event.Ctx) error {
		if ev, ok := c.Payload().(*ConnConnectEvent); ok && ev.Owner != "" {
			h(ev.Owner)
		}
		return nil
	}))
}

// OnReconnect 订阅重连事件。
func (g *Game) OnReconnect(h func(owner string)) {
	g.Logic.OnEvent(ConnReconnectType, g.trackHandler(func(c *event.Ctx) error {
		if ev, ok := c.Payload().(*ConnReconnectEvent); ok && ev.Owner != "" {
			h(ev.Owner)
		}
		return nil
	}))
}

// registerEngineDisconnectHandler 注册引擎级断线处理。
// 同样经 trackHandler 计数：断线处理会刷脏 / 清跨服映射，Stop 必须等它完成。
func (g *Game) registerEngineDisconnectHandler() {
	g.Logic.OnEvent(ConnDisconnectType, g.trackHandler(func(c *event.Ctx) error {
		ev, ok := c.Payload().(*ConnDisconnectEvent)
		if !ok {
			return nil
		}
		g.sendPlayerDisconnectEvent(ev)
		// 普通网络断开不删除 session token：保留 token 供客户端断线重连后
		// EMsgResumeSession 恢复会话；token 仅由 TTL 过期或显式登出（NewSessionToken 覆盖）失效。
		if ev.ConnID != "" {
			g.parkConnBag(ev)
		}
		g.unregisterCrossNodePlayer(c.Context(), ev.Owner)
		g.ClearMonitorData(ev.Owner)
		return nil
	}))
}

func (g *Game) sendPlayerDisconnectEvent(ev *ConnDisconnectEvent) {
	g.engineEv.EmitPlayerDisconnect(engine.PlayerDisconnectPayload{
		Owner: ev.Owner, AccountID: ev.Owner, Reason: "disconnect",
	})
}

func (g *Game) unregisterCrossNodePlayer(ctx context.Context, owner string) {
	if g.crossNodeBus == nil || owner == "" {
		return
	}
	// 先刷脏数据到 MySQL，避免其他节点 LoadAll 读到过期值。
	// 失败必须留痕：静默吞掉会让其他节点读到过期值而无从排查。
	if err := g.Data().FlushPlayer(ctx, data.OwnerPlayer, owner); err != nil {
		logger.Warnf("game: flush player %s on leave failed: %v", owner, err)
	}
	// 标记数据层 "本节点离线"，后续该玩家的 TierSnapshot 读写走 Redis 路径
	g.Data().SetOffline(data.OwnerPlayer, owner)
	// 清理跨服映射（最后一步，避免竞态）；失败同样不能静默。
	if err := g.crossNodeBus.OnPlayerLeave(ctx, owner); err != nil {
		logger.Warnf("game: cross-node player leave %s failed: %v", owner, err)
	}
}

func (g *Game) parkConnBag(ev *ConnDisconnectEvent) {
	g.connMgr.ParkBag(ev.ConnID, ev.Owner)
}
