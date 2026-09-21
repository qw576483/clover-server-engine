package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	authdom "github.com/qw576483/clover-server-engine/internal/domain/auth"
	"github.com/qw576483/clover-server-engine/internal/domain/data"
	iaccessor "github.com/qw576483/clover-server-engine/internal/domain/data/accessor"
	idataaccount "github.com/qw576483/clover-server-engine/internal/domain/data/account"
	"github.com/qw576483/clover-server-engine/internal/domain/data/migration"
	idataorder "github.com/qw576483/clover-server-engine/internal/domain/data/order"
	idataplayer "github.com/qw576483/clover-server-engine/internal/domain/data/player"
	"github.com/qw576483/clover-server-engine/internal/domain/log/logbuf"
	"github.com/qw576483/clover-server-engine/internal/domain/master"
	masterclient "github.com/qw576483/clover-server-engine/internal/domain/master/client"
	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
	"github.com/qw576483/clover-server-engine/internal/domain/sessiontoken"
	"github.com/qw576483/clover-server-engine/internal/runtime/globalstore"
	"github.com/qw576483/clover-server-engine/internal/shared/config"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/internal/transport"
	"github.com/qw576483/clover-server-engine/internal/transport/etcd"
	"github.com/qw576483/clover-server-engine/internal/transport/event"
	"github.com/qw576483/clover-server-engine/internal/transport/gateway/gwcore"
	"github.com/qw576483/clover-server-engine/internal/transport/nats"
	iauth "github.com/qw576483/clover-server-engine/internal/transport/net/auth"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/runtime/ratelimit"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/qw576483/clover-server-engine/pkg/shared/timeutil"
)

// currentGame 保存本进程已启动的逻辑服实例（all 模式下先起 Game 后起 Gateway），
// 用于把网关的连接级掉线事件回流到 Game 事件总线（业务经 g.OnDisconnect 订阅）。
// 纯 gateway 模式下为 nil（无逻辑服），纯 game 模式下无网关转发，均不影响。
// 使用 atomic.Pointer 防止 Run 多次（测试/多服）覆盖与并发读写。
var currentGame atomic.Pointer[Game]

// currentGateway 保存本进程已启动的网关实例。
// 用途：admin 端点 /admin/gateway/upstream 需要在运行期切换默认上游（灰度重启时
// 把新连接导向新版本进程）。纯 game 模式下为 nil，端点回 503。
var currentGateway atomic.Pointer[gwcore.Gateway]

// RunGame 从配置构造并启动游戏服，自动创建通用数据存储与结构化基础表（含建表）。
func RunGame(ctx context.Context, cfg *Config) (*Game, error) {
	store, err := data.NewStore(cfg.Data)
	if err != nil {
		return nil, err
	}

	// SQL 迁移：若配置了 sqls_dir，扫描目录下的 SQL 文件并按版本顺序执行。
	if cfg.Data.SqlsDir != "" {
		if err := runSQLMigrations(ctx, store, cfg.Data.SqlsDir); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("app: sql migration: %w", err)
		}
	}

	// 结构化基础表（账号 / 账号渠道 / 角色 / 订单）：仅在底层使用 MySQL 时创建。
	accountStore, channelStore, playerStore, orderStore, err := createBaseStores(ctx, cfg)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	g := NewGame(cfg, &GameStores{Store: store, AccountStore: accountStore, ChannelStore: channelStore, PlayerStore: playerStore, OrderStore: orderStore})
	if err := g.Start(); err != nil {
		g.Stop()
		return nil, err
	}
	// 配置 etcd 服务发现时，把本逻辑服监听地址注册到 etcd（gateway 可据此解析上游）。
	// 走引擎统一的服务发现设施（前缀 + 每实例唯一 key）：多逻辑服实例互不覆盖。
	//
	// ★ 必须在 setupNATSBackends **之前**建好：那里有两处读 g.etcdCli 的装配
	//   ——「跨机场景路由后端」（配 etcd 用 etcdStore，否则静默退化为进程内内存表）
	//   与「master 分片路由」（配 etcd 才起分片解析器，否则告警后回落静态单连接）。
	//   顺序写反的后果是**功能静默失效**：etcd 明明配了、服务发现也照常工作，
	//   但分片路由永不启用、跨机对象迁移也查不到别的节点（实测踩过）。
	//   etcd 客户端先建，两者才都拿到真实句柄。
	if len(cfg.Etcd.Endpoints) > 0 {
		ec, err := etcd.NewClient(cfg.Etcd)
		if err != nil {
			g.Stop()
			return nil, err
		}
		g.etcdCli = ec
		cancel, err := registerService(ctx, ec, roleLogic, g.Addr())
		if err != nil {
			g.Stop()
			return nil, err
		}
		// 节点目录（S4）：登记「节点 → 类型 / tags / admin 地址」。
		// type 必须与 master 的 state.Node.Type 同一套取值，否则按类型查节点查不到。
		// admin 地址供运维编排工具定位控制面（见 nodeInfo.Admin）。
		adminAddr, addrErr := adminListenAddr(cfg.Admin)
		if addrErr != nil {
			// 正常流程不可达：同一份配置在 runApp 构造 admin server 时已被拒、进程不会走到这里。
			// 留日志并按「不上报 admin 地址」降级——节点目录本身是非致命能力，不该因它拖垮启动。
			logger.Errorf("app: admin listen addr invalid, node registry will report no admin addr (non-fatal): %v", addrErr)
		}
		cancelNode, nodeErr := registerNode(ctx, ec, g.Addr(), nodeInfo{
			Type:  state.NodeTypeGame,
			Tags:  cfg.Tags,
			Admin: adminAddr,
		})
		if nodeErr != nil {
			logger.Warnf("app: register node registry failed (non-fatal): %v", nodeErr)
			// registerNode 失败时返回 (nil, err)：注销函数兜底成空操作，
			// 否则 closeBackends 里执行 g.etcdCancel() 会调用 nil 函数直接 panic。
			cancelNode = func() {}
		}
		// 两个注销合成一个：closeBackends 只持有 g.etcdCancel。
		g.etcdCancel = func() {
			cancelNode()
			cancel()
		}
	}
	// NATS 可选能力：下行推送、视野同步、跨服事件。连接失败降级不阻断启动。
	if cfg.NATS.Addr != "" {
		nc, err := nats.NewClient(cfg.NATS)
		if err != nil {
			logger.Warnf("app: connect nats failed, push/sync/crossnode disabled: %v", err)
		} else {
			g.setupNATSBackends(nc, cfg, store)
		}
	}

	// 节点目录读取侧（S4b）：存活节点改由 etcd 维护（watch 本地缓存），
	// crossnode 不再每 5s 轮询 master；未配 etcd 时保持原读路径（g.nodeDir 为 nil）。
	if g.etcdCli != nil && g.crossNodeBus != nil {
		g.nodeDir = newNodeDirectory(g.etcdCli)
		g.nodeDir.start(ctx)
		g.crossNodeBus.SetNodeSource(func() []string {
			return g.nodeDir.ByType(state.NodeTypeGame)
		})
		logger.Infof("app: node directory enabled (live nodes from etcd, master node table no longer polled)")
	}

	// auth / log 多实例发现：按 etcd 前缀解析，实例增删自动刷新。
	// 未配 etcd 时解析器只为「静态地址回退」而存在，行为与未接入发现时一致。
	g.authResolver = newServiceResolver(roleAuth, config.DefString(cfg.Auth.RpcAddr, authdom.DefaultRPCListenAddr), g.etcdCli)
	g.logResolver = newServiceResolver(roleLog, config.DefString(cfg.LogAddr, defaultLogListenAddr), g.etcdCli)
	g.authResolver.start(ctx)
	g.logResolver.start(ctx)

	// 业务日志缓冲器：接入服务发现，按实例分发（每实例一条长连接，按片轮询）。
	// 必须在此处建——早于 resolver.start 时实例列表还是空的，会退化成静态地址单点。
	// 建不起来只告警不阻断启动：日志不应拖垮业务。
	if lb, err := logbuf.New(logbuf.Options{
		Addr:   config.DefString(cfg.LogAddr, defaultLogListenAddr),
		Source: cfg.Logic.ListenAddr,
		Dialer: resolverDialer{r: g.logResolver},
	}); err != nil {
		logger.Warnf("app: business log buffer disabled: %v", err)
	} else {
		g.logBuf = lb
		logger.Infof("app: business log buffer ready (discovery=%v, static_fallback=%s)",
			len(cfg.Etcd.Endpoints) > 0, config.DefString(cfg.LogAddr, defaultLogListenAddr))
	}
	return g, nil
}

// dataTierUsesMySQL 判断 Store 的默认 Tier 是否需要 MySQL 作为持久层。
func dataTierUsesMySQL(tier data.StorageTier) bool {
	return tier == data.TierRedisMySQL || tier == data.TierSnapshot
}

// createBaseStores 创建结构化基础表存储（表格驱动，消除重复错误处理）。
func createBaseStores(ctx context.Context, cfg *Config) (*idataaccount.Store, *idataaccount.ChannelStore, *idataplayer.Store, *idataorder.Store, error) {
	if !dataTierUsesMySQL(cfg.Data.Tier) {
		return nil, nil, nil, nil, nil
	}

	type storeInit struct {
		name string
		fn   func() (interface{ Close() error }, error)
		ptr  *interface{ Close() error }
	}
	var a, c, p, o interface{ Close() error }
	specs := []storeInit{
		{"account", func() (interface{ Close() error }, error) { return idataaccount.NewStore(cfg.Data.MySQL) }, &a},
		{"channel", func() (interface{ Close() error }, error) { return idataaccount.NewChannelStore(cfg.Data.MySQL) }, &c},
		{"player", func() (interface{ Close() error }, error) { return idataplayer.NewStore(cfg.Data.MySQL) }, &p},
		{"orders", func() (interface{ Close() error }, error) { return idataorder.NewStore(cfg.Data.MySQL) }, &o},
	}
	closeTo := func(idx int) {
		for i := idx - 1; i >= 0; i-- {
			if s := *specs[i].ptr; s != nil {
				if err := s.Close(); err != nil {
					logger.Warnf("app: close %s store: %v", specs[i].name, err)
				}
			}
		}
	}
	for i, sp := range specs {
		s, e := sp.fn()
		if e != nil {
			closeTo(i)
			return nil, nil, nil, nil, fmt.Errorf("app: create %s store: %w", sp.name, e)
		}
		*sp.ptr = s
	}

	acc, ok1 := a.(*idataaccount.Store)
	chs, ok2 := c.(*idataaccount.ChannelStore)
	pls, ok3 := p.(*idataplayer.Store)
	ods, ok4 := o.(*idataorder.Store)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		closeTo(len(specs))
		return nil, nil, nil, nil, fmt.Errorf("app: store type assertion failed")
	}

	if cfg.Data.AutoCreateTable {
		tables := []struct {
			name string
			fn   func(context.Context) error
		}{
			{"account", acc.CreateTable},
			{"account_channel", chs.CreateTable},
			{"player", pls.CreateTable},
			{"orders", ods.CreateTable},
		}
		for _, t := range tables {
			if err := t.fn(ctx); err != nil {
				for _, sp := range specs {
					if s := *sp.ptr; s != nil {
						if cerr := s.Close(); cerr != nil {
							logger.Warnf("app: close %s store on table error: %v", sp.name, cerr)
						}
					}
				}
				return nil, nil, nil, nil, fmt.Errorf("app: create %s table: %w", t.name, err)
			}
		}
	}
	return acc, chs, pls, ods, nil
}

// setupNATSBackends 装配 NATS 依赖的全部后端能力：下行链路、MMO 场景、跨服事件总线。
// master 走 TCP 直连，NATS 仅用于下行推送 + 视野同步 + 跨服事件广播。
func (g *Game) setupNATSBackends(nc *nats.Client, cfg *Config, store *data.Store) {
	pub := natsRawPublisher{nc}
	subject := config.DefString(cfg.NATSSubject, proto.NATSSubjectNotify)
	g.notifyNC = nc
	g.notifyPub = pub
	g.notifySubject = subject
	g.entityAcc = iaccessor.NewNotifyingAccessor(store, pub, subject)

	// 跨机对象迁移（MMO 切场景）的寻址基础：
	//   - scene→node 路由表建在全局 KV 上：配了 etcd 用 etcd 后端（跨机共享）；
	//     否则退化为内存后端（仅本进程可见，跨机迁移查不到别的节点的场景）；
	//   - 本节点数字 ID 取自配置 node_id（NATS subject 不允许地址里的 ':'，故用数字）。
	// 缺 node_id 时 g.nodeID=0，迁移整体不启用（TransferRemote 返回 ErrNoRoute）。
	if g.etcdCli != nil {
		g.sceneRoute = transport.NewRouteStore(globalstore.NewEtcdStore(g.etcdCli, sceneRoutePrefix))
	} else {
		g.sceneRoute = transport.NewRouteStore(globalstore.NewMemStore(sceneRoutePrefix))
		logger.Warnf("app: scene route uses in-memory backend (no etcd); " +
			"cross-node object transfer cannot locate scenes on other nodes")
	}
	g.sceneSub = sceneSub{c: nc}
	g.nodeID = cfg.NodeID
	if g.nodeID == 0 {
		logger.Infof("app: node_id not set, cross-node object transfer disabled (single-node only)")
	} else {
		logger.Infof("app: cross-node object transfer enabled (node_id=%d)", g.nodeID)
	}

	// 下行链路注入事件内核
	g.Logic.SetSyncDownlink(pub, subject, g.syncReg)            // data-event 自动广播
	g.Logic.SetNotifyDownlink(pub, subject)                     // Game.AlertToPlayer
	g.Logic.SetControlDownlink(pub, proto.NATSSubjectGWControl) // Ctx.SetPlayerID 网关控制指令

	// 跨服内存镜像链路
	mirrorSubject := subject + ".mirror"
	g.Logic.SetMirrorDownlink(pub, mirrorSubject)
	subscribeMirror(nc, mirrorSubject, store)

	// 跨服事件总线：master 走 TCP，跨服事件广播仍走 NATS。
	masterAddr := config.DefString(cfg.MasterAddr, defaultMasterListenAddr)
	// master_token 与 master 侧同值：master 绑非回环时必须握手，否则首个请求即被拒。
	mc, err := masterclient.NewClient(masterAddr, masterclient.WithToken(cfg.MasterToken))
	if err != nil {
		logger.Warnf("app: connect master failed (non-fatal): %v", err)
		// master 连接失败不阻断启动，跨服能力降级。
		// 显式置 nil，避免 typed-nil 指针被包成非 nil 接口。
		mc = nil
	}
	// 可靠投递配置必须从这里传入：NewCrossNodeEventBus 固定用 DefaultReliableConfig()，
	// 那会让 YAML 的 reliable 段（ACK 超时 / 重试 / 死信容量 / enabled=false）整段失效。
	g.crossNodeBus = event.NewCrossNodeEventBusWithConfig(g.Addr(), nc, mc, g.Logic.Bus(), cfg.Reliable)
	// 注入 g.Logic.EmitEvent 桥接：跨节点 SendToPlayer 到达后自动触发 OnEvent 订阅者
	g.crossNodeBus.SetOnEventEmitter(g.Logic.EmitEvent)
	if err := g.crossNodeBus.Start(); err != nil {
		logger.Warnf("app: crossnode start failed (non-fatal): %v", err)
	}

	// master 分片发现：多分片部署时玩家定位按 uid 归属路由（其余能力仍走上面的单连接）。
	// 未配 etcd 或单分片（total=1）时保持静态单连接行为，与未启用分片完全一致。
	if shardCfg := cfg.MasterShard.Normalize(); shardCfg.Sharded() {
		if g.etcdCli == nil {
			// 注意这句话有两种来源，别混为一谈：
			//   ① 确实没配 etcd（正常降级）；
			//   ② 配了 etcd 但句柄还没建——装配顺序错了（etcd 客户端必须早于 setupNATSBackends）。
			// ②曾真实发生过且**完全静默**（服务发现照常工作，只有分片路由永不启用），
			// 所以这里把「配了却为 nil」单独点名，下次再被写反能一眼定位。
			if len(cfg.Etcd.Endpoints) > 0 {
				logger.Errorf("app: master_shard.total=%d and etcd.endpoints is configured, but the etcd client is nil here — "+
					"assembly order bug: the etcd client must be created BEFORE setupNATSBackends (see RunGame). "+
					"player location now stays on static master %s; shard routing is NOT active",
					shardCfg.Total, masterAddr)
			} else {
				logger.Warnf("app: master_shard.total=%d but etcd is not configured; "+
					"player location stays on static master %s", shardCfg.Total, masterAddr)
			}
		} else {
			res := newMasterShardResolver(masterAddr, g.etcdCli)
			// 监听生命周期跟随 Game：g.etcdCli 在 closeBackends 关闭时统一取消 watch，
			// 因此这里用 Background 而不是请求级 ctx（请求结束不该停掉分片监听）。
			res.start(context.Background())
			g.crossNodeBus.UseShardedMaster(res.pick, res.allAddrs)
		}
	}

	// 向 master 注册本节点（携带 tags），使其他节点可按 tag 查询到本实例。
	if ac := g.crossNodeBus.AuthorityClient(); ac != nil {
		// 缓存 tags：drain 取消后重新注册节点时复用同一份标签。
		g.nodeTags = cfg.Tags
		node := state.Node{
			ID:   g.Addr(),
			Addr: g.Addr(),
			Type: state.NodeTypeGame,
			Tags: cfg.Tags,
		}
		if err := ac.RegisterNode(context.Background(), node); err != nil {
			logger.Warnf("app: register node to master failed (non-fatal): %v", err)
		} else {
			logger.Infof("app: registered node to master (addr=%s tags=%v)", g.Addr(), cfg.Tags)
		}
	}

	// 启用心跳上报：master 依据心跳判定本节点存活，未上报会被判死剔除。
	// 初始间隔取 master 侧的默认值（同源常量），之后 master 响应会下发实际间隔自动跟随。
	if mc != nil {
		g.heartbeat = masterclient.NewHeartbeatModule(mc, masterclient.HeartbeatConfig{
			NodeID:   g.Addr(),
			Interval: master.DefaultHeartbeatInterval,
			LoadFn:   g.OnlineCount,
		})
	}

	// 领域子模块初始化（依赖 NATS / crossNodeBus 就绪后）
	//
	// 本地回退 TTL 取 master 侧 `master_session_token.ttl` 的实际值：本地 TTL 是
	// master 不可达时唯一的有效性判据，与远端不一致会「接受 master 已过期的 token」
	// 或「拒绝 master 仍有效的 token」。不再各写死一个 24h 靠巧合对齐。
	g.sessionStore = sessiontoken.NewStoreWithTTL(g.MasterClient(), cfg.MasterSessionToken.Normalize().TTL)
	g.fullSyncer = NewFullSyncer(FullSyncOptions{
		EntityAcc:     g.entityAcc,
		PlayerStore:   g.playerStore,
		AccountStore:  g.accountStore,
		NotifyPub:     g.notifyPub,
		NotifySubject: g.notifySubject,
		SessionToken:  func(pid string) string { t, _ := g.sessionStore.Current(pid); return t },
		MonitoredData: g.monitoredDataLocked,
		MonitorMu:     &g.monitorDataMu,
	})
}

// hostName 返回本机名（用于 etcd 注册 id 生成，避免引入额外依赖）。

//lint:ignore U1000 后续 etcd 注册功能使用
func hostName() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}

// sceneRoutePrefix 是 scene→node 路由表在全局 KV 里的命名空间前缀。
// 独立前缀避免与其他全局键（配置、计数、开关）互相覆盖。
const sceneRoutePrefix = "route/"

// sceneSub 把 NATS 客户端适配成 pubsub.Subscriber。
// 两者的订阅回调签名不同：nats.Client 给 func(*nats.Msg)，Subscriber 要
// func(subject string, payload []byte)。mmo 包内的同名适配器是私有的，跨包需另建一层。
type sceneSub struct{ c *nats.Client }

func (s sceneSub) Subscribe(subject string, handler func(subject string, payload []byte)) error {
	return s.c.Subscribe(subject, func(msg *nats.Msg) { handler(msg.Subject, msg.Data) })
}

// Unsubscribe 让本适配器实现 pubsub.Unsubscriber：
// mmo 模块 Stop 时要能只退自己那几条订阅（否则订阅比持有者活得久）。
func (s sceneSub) Unsubscribe(subject string) error { return s.c.Unsubscribe(subject) }

// subscribeMirror 订阅跨服内存镜像，收到变更后直接写入本进程 Store 内存（不标脏）。
func subscribeMirror(nc *nats.Client, mirrorSubject string, store *data.Store) {
	if nc == nil || store == nil {
		return
	}
	if err := nc.Subscribe(mirrorSubject, func(msg *nats.Msg) {
		var evt event.MirrorEvent
		if err := ujson.Unmarshal(msg.Data, &evt); err != nil {
			logger.Errorf("mirror: unmarshal failed: %v", err)
			return
		}
		key := data.Key{Owner: evt.Owner, ID: evt.ID, Type: evt.Type}
		if err := store.ReceiveMirror(context.Background(), key, evt.Data); err != nil {
			logger.Errorf("mirror: receive %s/%s/%s failed: %v", evt.Owner, evt.ID, evt.Type, err)
		}
	}); err != nil {
		logger.Warnf("mirror: subscribe %s failed (non-fatal): %v", mirrorSubject, err)
		return
	}
	logger.Infof("mirror: subscribed %s", mirrorSubject)
}

// 网关装配（通用内核位于 internal/transport/gateway/gwcore，本层仅做配置填充）
// runGateway 从配置构造并启动网关，返回 *gwcore.Gateway。
// 该函数由 Run 根据 role 自动调用，业务无需关心。
func runGateway(cfg *Config) (*gwcore.Gateway, error) {
	// 与 game / master / log / auth 对称：纯网关模式下同样初始化引擎统一时区，
	// 否则 misc.timezone 在 gateway 角色下配了不生效。
	timeutil.Init(cfg.Misc.Timezone)
	opts := []gwcore.Option{}
	subject := config.DefString(cfg.NATSSubject, proto.NATSSubjectNotify)
	// natsCli 记录本函数创建的 NATS 连接：后续任一步骤失败时须关闭，避免连接泄漏。
	var natsCli *nats.Client
	if cfg.NATS.Addr != "" {
		nc, err := nats.NewClient(cfg.NATS)
		if err != nil {
			return nil, err
		}
		natsCli = nc
		opts = append(opts, gwcore.WithNATS(nc))
		opts = append(opts, gwcore.WithGWControlSubject(proto.NATSSubjectGWControl))
	}
	closeNATSOnErr := func() {
		if natsCli != nil {
			_ = natsCli.Close()
		}
	}
	// 登录回包 → 提取 Owner 绑定会话：完全复用 base auth 解析逻辑。
	opts = append(opts, gwcore.WithExtractOwnerID(iauth.ExtractOwnerID()))
	// 登录回包 → 提取会话密钥启用通道加密（AES-256-GCM）。
	// 只有客户端在 ELoginRequest.Encrypt 声明支持时，登录回包才带 session_key（见 auth.Handler），
	// 因此这条链路对不声明支持的客户端完全无感（明文行为不变）。
	opts = append(opts, gwcore.WithCryptoKeyExtractor(iauth.ExtractSessionKey()))

	// 连接级心跳：若本进程同时承载逻辑服（all 模式），把连接生命周期事件回流到 Game 事件总线，
	// 业务经 g.OnConnect / g.OnReconnect / g.OnDisconnect 一句话订阅，无需自行轮询 lastSeen。
	if g := currentGame.Load(); g != nil {
		// 网关连接生命周期回调 → 逻辑服领域事件。
		// 关键：业务经 g.OnConnect/OnDisconnect/... 注册的 handler 落在 Logic.evHandlers，
		// 只有 EmitEvent（c.SendEvent）能触发；直接 g.Bus().Publish 无订阅者桥接 → 回调永不触发。
		// 因此必须经 g.sendConnEvent 走 EmitEvent，并以「指针」载荷派发
		// （业务 handler 断言 *ConnXxxEvent，值载荷会断言失败）。
		opts = append(opts, gwcore.WithOnSoftDisconnect(func(connID, owner string) {
			// 软掉线：玩家已断线但仍在断线宽限期内，可保留 Scene 上下文。
			g.sendConnEvent(ConnSoftDisconnectType, &ConnDisconnectEvent{ConnID: connID, Owner: owner})
		}))
		opts = append(opts, gwcore.WithOnDisconnect(func(connID, owner string) {
			// 硬掉线（最终清理）：派发 ConnDisconnectType 事件，业务经 OnDisconnect/OnHardDisconnect 订阅。
			g.sendConnEvent(ConnDisconnectType, &ConnDisconnectEvent{ConnID: connID, Owner: owner})
		}))
		opts = append(opts, gwcore.WithOnConnect(func(connID, owner string, isReconnect bool) {
			if isReconnect {
				g.sendConnEvent(ConnReconnectType, &ConnReconnectEvent{ConnID: connID, Owner: owner})
			} else {
				g.sendConnEvent(ConnConnectType, &ConnConnectEvent{ConnID: connID, Owner: owner})
			}
		}))
		opts = append(opts, gwcore.WithOnKick(func(connID, owner string) {
			// 多端登录互踢：旧连接被新登录强制关闭，回流业务侧清理/日志。
			g.sendConnEvent(ConnKickedType, &ConnKickedEvent{ConnID: connID, Owner: owner})
		}))
	}

	// 逻辑服上游地址：优先静态 event.listen_addr；未配置且启用 etcd 时从 etcd 解析。
	// 走统一解析器：列出 `clover/services/logic/` 下全部实例后轮询选一个
	//（旧实现是「单 key 取一次」，多实例会互相覆盖，等于只能发现一个逻辑服）。
	upstream := cfg.Logic.ListenAddr
	if upstream == "" && len(cfg.Etcd.Endpoints) > 0 {
		ec, err := etcd.NewClient(cfg.Etcd)
		if err != nil {
			closeNATSOnErr()
			return nil, fmt.Errorf("gateway: etcd: %w", err)
		}
		res := newServiceResolver(roleLogic, "", ec)
		// 带超时的 etcd 查询，防止永久阻塞。
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res.refresh(ctx)
		cancel()
		upstream = res.pick()
		_ = ec.Close()
		if upstream == "" {
			closeNATSOnErr()
			return nil, fmt.Errorf("gateway: no logic instance found in etcd prefix %s", servicePrefix(roleLogic))
		}
		logger.Infof("gateway: upstream logic resolved from etcd: %s", upstream)
	}

	// 消息级限流：按连接 / 玩家多 key 限速，防御单连接刷包 / CC。
	// 与连接级 MaxConnsPerSec 互补：前者控"连得多不多"，后者控"单个连接发包猛不猛"。
	rlMgr := ratelimit.NewManager(
		ratelimit.WithMaxEntries(1<<20),
		ratelimit.WithIdleTTL(2*time.Minute),
	)
	// 默认策略：单连接 64 帧/秒、突发 128（GCRA）。业务可 RegisterPolicy 细化到登录/聊天等。
	rlMgr.SetDefault(ratelimit.GCRAPolicy(64, 128))
	opts = append(opts, gwcore.WithRateLimitManager(rlMgr, ""))

	var gatewayTLS *tls.Config
	var gatewayWTTLS *tls.Config // WebTransport 专用（短有效期 ECDSA，供证书固定）
	var gatewayWTHash string     // WebTransport 证书哈希（hex），经 /wt-cert-hash 下发
	var wtRot *WTCertRotator     // WebTransport 证书轮换器：运行期间到期自动重签
	if cfg.Gateway.TLSCert != "" || cfg.Gateway.TLSKey != "" {
		if cfg.Gateway.TLSCert == "" || cfg.Gateway.TLSKey == "" {
			closeNATSOnErr()
			return nil, fmt.Errorf("gateway: tls_cert and tls_key must be configured together")
		}
		cert, err := tls.LoadX509KeyPair(cfg.Gateway.TLSCert, cfg.Gateway.TLSKey)
		if err != nil {
			closeNATSOnErr()
			return nil, fmt.Errorf("gateway: load TLS certificate (%s, %s): %w", cfg.Gateway.TLSCert, cfg.Gateway.TLSKey, err)
		}
		gatewayTLS = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
			// 同一份 TLS 配置供 QUIC 与 WebTransport 使用；两者必须分别声明 ALPN。
			NextProtos: []string{"clover-quic", "h3"},
		}
		logger.Infof("gateway: TLS certificate loaded cert=%s key=%s", cfg.Gateway.TLSCert, cfg.Gateway.TLSKey)
	}

	// WebTransport 使用独立证书：浏览器不接受自建根，只能靠 serverCertificateHashes
	// 固定，而该机制要求 ECDSA P-256 + 有效期 < 2 周，与 wss 所需的受信任长期证书冲突。
	//
	// WTPin 未配置时默认 true：本地自签名场景下不固定就完全连不上。
	// 生产环境（公共 CA 证书）须在 yaml 显式设 wt_pin: false 走标准 PKI。
	//
	// 独立于上面的 TLS 块判断：原逻辑嵌套在「配了 tls_cert / tls_key」之内，
	// 只配 wt_cert / wt_key 时整段静默不生效（字段注释承诺「空=自动生成」）。
	// 「任一相关证书配置存在」才进入：什么都没配时保持不生成（与从前行为一致）。
	if cfg.Gateway.EnableWT && (cfg.Gateway.WTPin == nil || *cfg.Gateway.WTPin) &&
		(cfg.Gateway.TLSCert != "" || cfg.Gateway.TLSKey != "" || cfg.Gateway.WTCert != "" || cfg.Gateway.WTKey != "") {
		var werr error
		wtRot, werr = NewWTCertRotator(cfg.Gateway.WTCert, cfg.Gateway.WTKey, cfg.Gateway.TLSCert)
		if werr != nil {
			closeNATSOnErr()
			return nil, werr
		}
		gatewayWTTLS = wtRot.TLSConfig()
		gatewayWTHash = wtRot.Hash()
		// 运行期间自动轮换：证书到期前 3 天起，后台循环每小时检查并重签，
		// 新哈希经 /wt-cert-hash 动态下发，前端每次建连前拉取，全程无需重启。
		wtRot.Start()
		logger.Infof("gateway: WebTransport certificate hash=%s (经 /wt-cert-hash 下发，运行期间自动轮换)", gatewayWTHash)
	}

	// TCP 走明文是「显式降级」：配了证书却把 TCP 留在明文时必须留痕——
	// 否则运维看到 wss / QUIC / WT 都加密，会误以为所有入口都加密了。
	if gatewayTLS != nil && cfg.Gateway.TCPTLSDisabled {
		logger.Warnf("gateway: TCP 接入为明文（tcp_tls_disabled=true，仅 WS/QUIC/WT 走 TLS）—— 生产部署请置 false")
	}

	gc := gwcore.Config{
		TCPListen:     cfg.Gateway.ListenTCP, // 空=不启用 TCP 接入
		WSListen:      cfg.Gateway.ListenWS,  // 空=不启用 WS 接入
		WSPath:        cfg.Gateway.WSPath,
		WSCheckOrigin: wsCheckOrigin(cfg.Gateway.WSAllowAllOrigins),
		UDPListen:     cfg.Gateway.ListenUDP, // 空=不启用 UDP 接入（QUIC + 裸 UDP 共享）
		WTEnabled:     cfg.Gateway.EnableWT,  // 在 WS 端口号上启用 WebTransport（UDP 侧）
		// 网关开了 TLS 后，TCP 与 WS 会共用证书：TCP 转 tls.Listen 会让裸 TCP 的 Unity 客户端
		// 握不上手；而 WS 的 wss 与「经 wss 下发 WT 证书哈希」又必须加密。设 true 让 TCP 保持明文。
		TCPTLSDisabled: cfg.Gateway.TCPTLSDisabled,
		TLSConfig:      gatewayTLS,
		WTTLSConfig:    gatewayWTTLS,
		WTCertHash:     gatewayWTHash,
		// 运行期间证书自动轮换后哈希会变化，/wt-cert-hash 须动态读取最新值。
		WTCertHashFunc: func() string {
			if wtRot != nil {
				return wtRot.Hash()
			}
			return gatewayWTHash
		},
		Upstream:           upstream,
		NATSSubjects:       []string{subject, subject + ".broadcast.>"},
		MaxConns:           cfg.Gateway.MaxConns,
		MaxConnsPerSec:     cfg.Gateway.MaxConnsPerSec, // 连接建立速率限流（0=不限制，gwcore 内部仅在 >0 时启用）
		QueueCap:           cfg.Gateway.QueueCap,
		QueueReleasePerSec: cfg.Gateway.QueueReleasePerSec,
		QueueTimeout:       cfg.Gateway.QueueTimeout,
		ReconnectGrace:     cfg.Gateway.ReconnectGrace,
		DisconnectGrace:    cfg.Gateway.DisconnectGrace,
		MaxFrameSize:       cfg.Gateway.MaxFrameSize, // 上行帧大小硬上限（边缘输入校验）
		// 登录门禁：零值即开启（fail-safe）。未登录连接只放行白名单消息号，
		// 其余在网关侧直接拒绝、不转发逻辑服；业务消息级权限走 g.OnBeforeDispatch。
		AuthDisabled:     cfg.Gateway.AuthDisabled,
		AuthExemptMsgIDs: cfg.Gateway.AuthExemptMsgIDs,
	}
	// closeWTOnErr 回收 WebTransport 证书轮换后台循环（幂等）。
	closeWTOnErr := func() {
		if wtRot != nil {
			wtRot.Stop()
		}
	}
	// 网关停机回调（gwcore 的 onStop 是单槽位，这里合并两类外部资源的清理）：
	//   - WebTransport 证书轮换后台循环；
	//   - 本函数自建的 NATS 连接——错误早退分支走 closeNATSOnErr，
	//     成功路径若不一并挂上，正常关停时该连接永远不会被关闭。
	opts = append(opts, gwcore.WithOnStop(func() {
		closeWTOnErr()
		closeNATSOnErr()
	}))
	g, err := gwcore.New(gc, opts...)
	if err != nil {
		// onStop 只在 Gateway.Stop() 时触发，构造 / 启动失败路径走不到那里：
		// 必须手动回收，否则 wt 轮换 goroutine 泄漏。
		closeWTOnErr()
		closeNATSOnErr()
		return nil, err
	}
	if err := g.Start(); err != nil {
		closeWTOnErr()
		closeNATSOnErr()
		return nil, err
	}
	logger.Infof("clover: gateway running (ws=%s tcp=%s udp=%s)", cfg.Gateway.ListenWS, cfg.Gateway.ListenTCP, cfg.Gateway.ListenUDP)
	return g, nil
}

// printStartupReport 在全部进程启动成功后打印一份启动状态汇总报告，
// 便于开发/运维一眼确认各监听器实际运行状态（含 WT/QUIC 未启用的原因）。
// 整块作为一条消息输出，顶部/底部用分隔线框起；gw 可为 nil（纯 game / master 角色未启动网关）。
// 报告结构：基础依赖信息（[deps]）→ 各服务段（[gateway]/[logic]/[master]/[log]/[auth]/[admin]），
// 未配置的监听项一律打印「未开启」，保证每段常驻、一眼可读。
func printStartupReport(cfg *Config, gw *gwcore.Gateway) {
	const sep = "============================================="
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s\n", sep)
	fmt.Fprintf(&b, "clover engine - %s \n", EngineVersion)
	fmt.Fprintf(&b, " server_type  %s\n", cfg.ServerType)

	// —— 基础依赖信息（紧跟标题，无独立段头） ——
	fmt.Fprintf(&b, "  nats    %s\n", reportAddr(cfg.NATS.Addr))
	mysql := ""
	if cfg.Data.MySQL.Host != "" {
		mysql = fmt.Sprintf("%s:%d/%s", cfg.Data.MySQL.Host, cfg.Data.MySQL.Port, cfg.Data.MySQL.DBName)
	}
	fmt.Fprintf(&b, "  mysql   %s\n", reportAddr(mysql))
	fmt.Fprintf(&b, "  redis   %s\n", reportAddr(cfg.Data.Redis.Addr))
	fmt.Fprintf(&b, "  etcd    %s\n", reportAddr(strings.Join(cfg.Etcd.Endpoints, ",")))
	// 引擎日志输出目标：dir 模式按天落盘；未配置则仅控制台。
	logTarget := ""
	if cfg.Log.Dir != "" {
		logTarget = cfg.Log.Dir + "/" + "{date}-" + cfg.Log.Service + ".log"
	}
	fmt.Fprintf(&b, "  logfile %s\n", reportAddr(logTarget))

	if gw != nil {
		st := gw.Status()
		fmt.Fprintf(&b, "[gateway]\n")
		fmt.Fprintf(&b, "  tcp  %s\n", reportAddr(st.TCP))
		fmt.Fprintf(&b, "  ws   %s\n", reportAddr(st.WS))
		fmt.Fprintf(&b, "  udp  %s\n", reportAddr(st.UDP))
		switch {
		case st.QUIC != "":
			fmt.Fprintf(&b, "  quic %s\n", st.QUIC)
		case st.QUICErr != "":
			fmt.Fprintf(&b, "  quic 未开启 (%s)\n", st.QUICErr)
		default:
			fmt.Fprintf(&b, "  quic 未开启\n")
		}
		switch {
		case st.WT != "":
			fmt.Fprintf(&b, "  wt   %s\n", st.WT)
		case st.WTErr != "":
			fmt.Fprintf(&b, "  wt   未开启 (%s)\n", st.WTErr)
		default:
			fmt.Fprintf(&b, "  wt   未开启\n")
		}
	}
	fmt.Fprintf(&b, "[logic]\n")
	fmt.Fprintf(&b, "  tcp  %s\n", reportAddr(cfg.Logic.ListenAddr))
	fmt.Fprintf(&b, "  http %s\n", reportAddr(cfg.Logic.HTTPListen))

	// master / log / admin 段常驻：未配置打印「未开启」
	fmt.Fprintf(&b, "[master]\n")
	fmt.Fprintf(&b, "  tcp  %s\n", reportAddr(cfg.MasterListenAddr))
	fmt.Fprintf(&b, "  http %s\n", reportAddr(cfg.MasterHTTPListenAddr))

	fmt.Fprintf(&b, "[log]\n")
	fmt.Fprintf(&b, "  tcp  %s\n", reportAddr(cfg.LogListenAddr))
	fmt.Fprintf(&b, "  http %s\n", reportAddr(cfg.LogHTTPListenAddr))

	// 账号服段：listen 是本进程的账号服监听（all / auth 角色），
	// verify 是登录时游戏服要访问的账号服地址（game / gateway / all 角色）。
	// 两项都打印，便于一眼核对「自己监听的」与「实际去连的」是否一致——
	// 不一致会导致自己连不上自己。
	fmt.Fprintf(&b, "[auth]\n")
	fmt.Fprintf(&b, "  listen %s\n", reportAddr(cfg.Auth.Listen))
	fmt.Fprintf(&b, "  verify %s\n", reportAddr(cfg.Auth.VerifyAddr))

	fmt.Fprintf(&b, "[admin]\n")
	if cfg.Admin.Disable {
		fmt.Fprintf(&b, "  http 未开启\n")
	} else {
		fmt.Fprintf(&b, "  http %s\n", reportAddr(cfg.Admin.ListenAddr))
	}

	fmt.Fprintf(&b, "\n%s\n", sep)
	logger.Infof("%s", b.String())
}

// reportAddr 将监听地址转成报告展示串：空=未开启。
func reportAddr(v string) string {
	if v == "" {
		return "未开启"
	}
	return v
}

// wsCheckOrigin 根据配置构造 WS 跨域校验函数。
func wsCheckOrigin(allowAll bool) func(*http.Request) bool {
	if allowAll {
		return func(r *http.Request) bool { return true }
	}
	return nil // gorilla 默认：仅允许同源
}

// player ctx 存取（internal）：框架派发前注入、业务经 PlayerFromContext 取用，不对外暴露字段。
type ctxKeyPlayer struct{}

// PlayerFromContext 从逻辑服派发 ctx 取回已注入的玩家角色档案；未登录 / 尚未创建角色时 ok=false。
func PlayerFromContext(ctx context.Context) (*idataplayer.EPlayer, bool) {
	p, ok := ctx.Value(ctxKeyPlayer{}).(*idataplayer.EPlayer)
	return p, ok
}

// SQL 迁移
// sqlFilePattern 匹配 SQL 迁移文件名：V{版本号}__{描述}.sql
// 示例：V1__create_accounts.sql、V2__add_level_column.sql
var sqlFilePattern = regexp.MustCompile(`^V(\d+)__(.+)\.sql$`)

// runSQLMigrations 扫描指定目录下的 SQL 文件并按版本号顺序执行迁移。
// 文件命名规则：V{版本号}__{描述}.sql，如 V1__init_tables.sql。
func runSQLMigrations(ctx context.Context, store *data.Store, sqlsDir string) error {
	mysqlClient := store.MySQLClient()
	if mysqlClient == nil {
		logger.Warnf("app: sql migration skipped, no MySQL connection (tier=memory?)")
		return nil
	}
	db := mysqlClient.Raw()

	entries, err := os.ReadDir(sqlsDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warnf("app: sql migration skipped, directory not found: %s", sqlsDir)
			return nil
		}
		return fmt.Errorf("read sqls dir %s: %w", sqlsDir, err)
	}

	type sqlFile struct {
		version int
		name    string
		path    string
	}
	var files []sqlFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		matches := sqlFilePattern.FindStringSubmatch(e.Name())
		if matches == nil {
			continue
		}
		ver, convErr := strconv.Atoi(matches[1])
		if convErr != nil {
			// 版本号超 int 范围 / 非法数字：跳过并告警，不能静默落成 0
			//（0 会排到最前并被按错误顺序执行）。
			logger.Warnf("app: sql migration file %s has invalid version %q, skipped", e.Name(), matches[1])
			continue
		}
		desc := strings.ReplaceAll(matches[2], "_", " ")
		files = append(files, sqlFile{
			version: ver,
			name:    desc,
			path:    filepath.Join(sqlsDir, e.Name()),
		})
	}
	if len(files) == 0 {
		logger.Infof("app: no SQL migrations found in %s", sqlsDir)
		return nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })

	// Register 是无去重的全局追加：同进程重跑（如 RunGame 失败后重试）会把
	// 同一版本注册多份，Migrator.Up 的 applied 快照又是循环前读取的——
	// 重复条目会让同一版本被重复执行并撞 _schema_versions 主键。
	// 先查全局注册表，已注册的版本直接跳过。
	registered := make(map[int]bool)
	for _, m := range migration.GetPending() {
		registered[m.Version] = true
	}
	for _, f := range files {
		if registered[f.version] {
			logger.Infof("app: sql migration V%d already registered in this process, skip re-register (%s)", f.version, f.path)
			continue
		}
		content, err := os.ReadFile(f.path)
		if err != nil {
			return fmt.Errorf("read %s: %w", f.path, err)
		}
		migration.Register(migration.Migration{
			Version: f.version,
			Name:    f.name,
			Up:      string(content),
		})
		registered[f.version] = true
	}

	migrator, err := migration.NewMigrator(db, "")
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	if err := migrator.Up(ctx); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	logger.Infof("app: SQL migrations completed, %d file(s) registered from %s", len(files), sqlsDir)
	return nil
}
