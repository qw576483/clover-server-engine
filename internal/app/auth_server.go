// auth 服：账号 / 登录独立进程，**只暴露 HTTP**，且是游戏服登录链路的**必经依赖**。
//
// 存在意义（架构分层）：
//   - 账号 / 密码 / 渠道绑定等鉴权数据与逻辑不该长在游戏服里；
//   - 游戏服只认「token → owner」，不碰账号表、不存密码；
//   - 独立部署后账号体系可单独扩容、单独审计、单独限流。
//
// 接口（全部 JSON；signup / login / verify 为 POST，health 为 GET）：
//
//	POST /auth/signup   {account, password}   → {success, owner, token, exp, err}  客户端注册用
//	POST /auth/login    {account, password}   → {success, owner, token, exp, err}  账号密码登录
//	POST /auth/login    {channel, ticket}     → {success, owner, token, exp, err}  渠道登录
//	POST /auth/verify   {token}               → {valid, owner, exp, err}           游戏服登录校验用
//	GET  /auth/health                         → {ok, service, time}
//
// 完整链路：客户端先 HTTP 换 token，再经游戏长连接发 EMsgLogin{token}；
// 游戏服收到后 POST 本服的 /auth/verify 换取 owner（见 auth.verify_addr）。
// 登录是低频请求-响应，且需与渠道 SDK 服务端、运营后台等 HTTP 世界交互，
// 不适合与游戏消息共用长连接，故整条账号链路都在 HTTP 侧。
//
// 分层（与 master / log 同构）：**本文件只是角色宿主**——Core 接线、消息通道、
// 业务挂载与进程装配；账号域能力全部在 internal/domain/auth：
//
//	domain/auth/state    注册 / 登录 / 验签 / 渠道登录 + 撞库防护 + 线协议契约
//	domain/auth/server   HTTP 路由注册与「领域错误 → 状态码」映射
//	domain/auth/client   调用侧（game 校验登录凭证）
//	domain/auth          域配置（AuthConfig）与渠道票据校验器扩展点
package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	authserver "github.com/qw576483/clover-server-engine/internal/domain/auth/server"
	authstate "github.com/qw576483/clover-server-engine/internal/domain/auth/state"
	idataorder "github.com/qw576483/clover-server-engine/internal/domain/data/order"
	"github.com/qw576483/clover-server-engine/internal/transport/event"
	"github.com/qw576483/clover-server-engine/internal/transport/tcpmsg"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/timeutil"
)

// AuthGame 账号服内核：headless *Core（派发 / 数据 / 定时器 / HTTP）+ 账号域能力。
type AuthGame struct {
	*Core

	// svc 账号域核心：注册 / 登录 / 验签 / 渠道登录 + 撞库防护。
	// 宿主只持有它并接线，不实现任何账号规则。
	svc *authstate.Service

	// orderStore 订单存储（orders 表）。订单归属账号服：渠道支付回调经 OnHTTP
	// 挂在本服 /auth/pay/... 下，建单与状态流转（Create / MarkPaid / MarkDone）在此做。
	// 发货仍由 game 执行（在线角色数据只在 owner 节点内存）。
	orderStore *idataorder.Store

	// bridge 把裸 TCP 消息通道（game → auth 的业务消息）桥进 Core 派发管线，
	// 与 MasterGame / LogGame 共用同一实现（tcpMsgBridge）。
	bridge tcpMsgBridge

	stopOnce sync.Once
}

// newAuthGame 构造账号服内核（不启动监听）。
//
// 校验前置且显式报错：auth 角色缺少监听地址时**启动即失败**，而不是运行期才在请求里
// 暴露问题（静默降级会让整条登录链路难以排查）。密钥 / 账号表 / 渠道表的校验在
// authstate.New 里——那些是**域约束**，不属宿主职责。
//
// 与 MasterGame / LogGame 同构：headless Core（不启逻辑服 TCP 监听），
// HTTP 由 Core 控制面提供；/auth/* 是客户端直连的公开接口，故经 WithHTTPAuth 放行，
// 其余路径维持控制面「默认拒绝」的安全姿态。
func newAuthGame(cfg *Config, store authstate.AccountStore, channel authstate.ChannelStore, orderStore *idataorder.Store) (*AuthGame, error) {
	ac := cfg.Auth.Normalize()
	if ac.Listen == "" {
		return nil, errors.New("app: auth.listen is required when server_type=auth")
	}
	// 传输安全前置校验（fail-fast）：账号服 POST 的是明文口令与 JWT，
	// 默认必须 TLS；只有显式 insecure_plaintext=true 才放行明文（并打 Warn）。
	// 放在构造期而不是首个请求：静默明文上线是「事后无人知情」的失败形态。
	if err := ac.ValidateAuthServer(); err != nil {
		return nil, err
	}

	svc, err := authstate.New(authstate.Config{
		Account: store,
		Channel: channel,
		Secret:  ac.JWTSecret,
		TTL:     ac.TokenTTL,
		Issuer:  ac.Issuer,
	})
	if err != nil {
		return nil, err
	}

	syncReg := event.NewSyncRegistry()
	core := event.New(event.Config{
		Headless:   true,
		HTTPListen: ac.Listen,
		// 证书由 ValidateAuthServer 校验过（要么成对配置，要么显式放行明文）。
		HTTPTLSCertFile: ac.TLS.CertFile,
		HTTPTLSKeyFile:  ac.TLS.KeyFile,
	},
		event.WithStore(nil),
		event.WithSyncDownlink(nil, "", syncReg),
		// /auth/* 是客户端直连的公开接口，必须放行；其余路径默认拒绝。
		event.WithHTTPAuth(func(r *http.Request) bool {
			return strings.HasPrefix(r.URL.Path, "/auth/")
		}),
	)

	ag := &AuthGame{
		Core:       newCore(cfg, nil, syncReg),
		svc:        svc,
		orderStore: orderStore,
	}
	ag.Core.Logic = core
	ag.bridge.attach(core)
	timeutil.Init(cfg.Misc.Timezone)
	ag.Core.Timer = NewTimeEvent(timeutil.Location())
	ag.Core.nodeID = "auth"

	// 四条固定路由由域内 server 包注册（含领域错误 → 状态码映射），宿主只提供控制面。
	// per-IP 限流参数从配置透传（0 由 server 包回落内置默认）。
	authserver.Register(ag, svc, authserver.WithIPRateLimit(ac.RateLimitPerSec, ac.RateLimitBurst))
	return ag, nil
}

// Start 启动 Core（headless：提供 HTTP 控制面 /auth/*，不启逻辑服 TCP 监听）。
func (ag *AuthGame) Start() error { return ag.Logic.Start() }

// Stop 停止账号服：先停消息通道与 Core（含 HTTP），再回收内核资源。重复调用只生效一次。
func (ag *AuthGame) Stop() {
	ag.stopOnce.Do(func() {
		ag.bridge.stop()
		ag.Logic.Stop()
		if ag.orderStore != nil {
			if err := ag.orderStore.Close(); err != nil {
				logger.Warnf("auth: close order store: %v", err)
			}
		}
		ag.closeCoreBackends()
	})
}

// OnMsg 注册账号服业务消息 handler（签名与 Game / MasterGame / LogGame 一致）。
//
// 走共用桥接（tcpMsgBridge）：TCP 帧 → GWLogicPacket → Logic.Dispatch → handler(c)。
// 入站通道由 auth.rpc_listen 开启（与 master / log 对称）；登录校验本身仍走 HTTP /auth/verify。
func (ag *AuthGame) OnMsg(msgID uint32, h event.Handler) { ag.bridge.OnMsg(msgID, h) }

// setServer 绑定消息通道的 TCP 服务端，并把之前缓存的 handler 全部补注册。
func (ag *AuthGame) setServer(srv *tcpmsg.Server) { ag.bridge.setServer(srv) }

// OrderStore 返回订单存储（orders 表）。订单归属账号服：渠道支付回调由业务经 OnHTTP
// 挂到 /auth/pay/... 后用本方法建单 / 流转状态（Create / MarkPaid / MarkDone）。
// 账号服必须接入 MySQL（见 runAuth 的前置校验），因此运行时它不为 nil。
func (ag *AuthGame) OrderStore() *idataorder.Store { return ag.orderStore }

// OnHTTP 注册 HTTP 路由（继承自嵌入的 *Core，见 core.go / transport/event）。
// 账号域的四条固定路由由 domain/auth/server 经此挂载（见 authserver.Registrar）。
//
// Reply / ReplyRaw 同样继承自 *Core，四个角色共用一份实现。

// runAuth 从配置构造并启动账号服（server_type=auth）。
// 返回 stop 函数供进程骨架在收到退出信号时调用。
//
// 启动顺序：基础表 → AuthGame 内核（含域能力与路由）→ 业务挂载 → Core 启动（HTTP 就绪）
// → 消息通道监听 → 服务发现注册。
func runAuth(cfg *Config) (func(), error) {
	// 账号表依赖 MySQL；库与表由引擎启动时自动创建（data.auto_create_table / data.mysql.auto_create），
	// 无需手工建库建表。createBaseStores 会一并建齐基础表（同库部署时幂等）。
	// 第 4 个返回值（orders）一并接住：订单归属账号服，渠道支付回调挂本服 HTTP 后
	// 要在这里建单 / 流转状态。player store 仍不需要，继续丢弃。
	accountStore, chStore, _, orderStore, err := createBaseStores(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	// 必须在**具体类型**上判空：非 MySQL 数据层时 createBaseStores 返回 typed nil，
	// 一旦包成接口就不等于 nil，authstate.New 里的 store==nil 守卫会失效（运行期空指针）。
	if accountStore == nil {
		return nil, errors.New("app: auth requires a MySQL data tier (account store unavailable; check data.tier / data.mysql)")
	}
	// 同理：渠道表也只在非 nil 时包成接口传下去（见 newAuthGame 的说明）。
	// 注意局部变量不能叫 channelStore，否则会遮蔽同名类型。
	var ch authstate.ChannelStore
	if chStore != nil {
		ch = chStore
	}

	ag, err := newAuthGame(cfg, accountStore, ch, orderStore)
	if err != nil {
		// newAuthGame 失败（auth.listen 缺失 / 密钥校验失败等）时必须回收已建连接，
		// 否则 account / channel / orders 三个 MySQL 连接在早退路径泄漏。
		if cerr := accountStore.Close(); cerr != nil {
			logger.Warnf("auth: close account store on init failure: %v", cerr)
		}
		if chStore != nil {
			if cerr := chStore.Close(); cerr != nil {
				logger.Warnf("auth: close channel store on init failure: %v", cerr)
			}
		}
		if orderStore != nil {
			if cerr := orderStore.Close(); cerr != nil {
				logger.Warnf("auth: close order store on init failure: %v", cerr)
			}
		}
		return nil, err
	}

	// 消息通道（与 master / log 对称）：先创建不监听，等业务挂载完再 Listen，
	// 否则挂载前的连接会命中未注册的 msgID。默认地址由配置 Normalize 回落。
	rpcAddr := cfg.Auth.Normalize().RpcListen
	srv := tcpmsg.NewServer(rpcAddr)
	ag.setServer(srv)

	// 业务挂载（与 master / log 对称：Core / 监听启动前统一执行）
	cfg.AuthGame = ag
	if err := RunMounts(RoleAuth, ag); err != nil {
		ag.Stop()
		return nil, err
	}

	if err := ag.Start(); err != nil {
		ag.Stop()
		return nil, err
	}
	logger.Infof("auth: serving /auth/* on %s (issuer=%s ttl=%s)", cfg.Auth.Listen, ag.svc.Issuer(), ag.svc.TTL())

	if err := srv.Listen(); err != nil {
		ag.Stop()
		return nil, fmt.Errorf("auth: rpc listen %s: %w", rpcAddr, err)
	}
	logger.Infof("auth: rpc serving on %s", rpcAddr)

	// 服务发现：注册本实例的消息通道地址，使 game 的 CallAuth 能发现多实例。
	// 未配 etcd 时空操作（退化为配置里的 auth.rpc_addr 静态直连）。
	ec, closeEtcd := newEtcdClientForRole(cfg, roleAuth)
	unregister, err := registerService(context.Background(), ec, roleAuth, rpcAddr)
	if err != nil {
		closeEtcd()
		ag.Stop()
		return nil, err
	}

	stop := func() {
		unregister()
		ag.Stop()
		closeEtcd()
	}
	return stop, nil
}
