// Package gwcore 实现 clover 通用「网关双转发内核」。
//
// 职责（统一网关双转发标准需求）：
// - 同时接受客户端 TCP 与 WebSocket 连接（基于 clover-server-engine 的 net/tcp、net/ws），
// 统一包装为 iconn.Conn，一套下发逻辑兼容两种协议。
// - 每个客户端连接维持一条到逻辑服的专用 TCP（网关 ↔ 逻辑服直连），同步指令原路回包；
// 逻辑服回包经内部信封 GWLogicPacket 路由回来源连接。
// - 订阅消息总线的异步推送（NotifyPush），按 Target 精准下发到目标会话
// （目标语义由 PushRouter 决定：按 UID 单推、按房间群推、按分线/视野群推等）。
//
// 本内核刻意不绑定任何业务：登录回包如何提取目标标识（ExtractOwnerID）、
// 异步推送如何路由（PushRouter）、订阅哪些 subject、上游地址等全部由构造参数注入。
//
// 网关核心（客户端接入 + 逻辑服直连 + NATS 异步推送），仅供引擎 app 层使用，未对 pkg 公开。
package gwcore

import (
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	iconn "github.com/qw576483/clover-server-engine/internal/transport/gateway/conn"
	"github.com/qw576483/clover-server-engine/internal/transport/nats"
	"github.com/qw576483/clover-server-engine/internal/transport/net/demux"
	"github.com/qw576483/clover-server-engine/internal/transport/net/quic"
	"github.com/qw576483/clover-server-engine/internal/transport/net/tcp"
	"github.com/qw576483/clover-server-engine/internal/transport/net/udp"
	"github.com/qw576483/clover-server-engine/internal/transport/net/ws"
	"github.com/qw576483/clover-server-engine/internal/transport/net/wt"
	ratelimt "github.com/qw576483/clover-server-engine/pkg/runtime/ratelimit"

	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/util"
)

// heartbeat 心跳间隔（网关 ↔ 客户端、网关 ↔ 逻辑服共用），**服务端主动探测**语义。
//
// 与客户端心跳（`Game.HeartbeatIntervalMs` = 15s）语义不同、互不依赖，两端默认值不同是
// 有意为之：客户端心跳是「客户端主动上行保活」，本值是「服务端写 ping + 读空闲判活」。
// 判据见 internal/app/config.go 的 LogicConfig.Heartbeat 注释。
const heartbeat = 30 * time.Second

// defaultSessionShards 网关会话表默认分片数（2 的幂）。
//
// 会话表按 connID / 绑定 id 哈希分散到多个分片，每片独立 RWMutex，热点连接之间不再互相阻塞。
const defaultSessionShards = 64

// sessionShard 单分片：按 connID 索引的会话表，持有独立 RWMutex。
type sessionShard struct {
	mu       sync.RWMutex
	sessions map[string]*Session // connID -> 会话
}

// idShard 单分片：按绑定 id 索引的会话表（NATS 推送路由用），持有独立 RWMutex。
// 支持同一 owner 多连接（多端同时在线）：idIndex 为 []*Session。
type idShard struct {
	mu      sync.RWMutex
	idIndex map[string][]*Session // target(id) -> 会话列表
}

// sessionShardOf 返回某 connID 所属会话分片（FNV 哈希，稳定映射）。
func (g *Gateway) sessionShardOf(connID string) *sessionShard {
	h := fnvHash(connID)
	// #nosec G115 -- sessionShards 长度已在 New 中限幅到 <= math.MaxInt32，-1 后在 uint32 范围内。
	return g.sessionShards[h&uint32(len(g.sessionShards)-1)]
}

// idShardOf 返回某绑定 id 所属分片（FNV 哈希，稳定映射）。
func (g *Gateway) idShardOf(id string) *idShard {
	h := fnvHash(id)
	// #nosec G115 -- idShards 长度已在 New 中限幅到 <= math.MaxInt32，-1 后在 uint32 范围内。
	return g.idShards[h&uint32(len(g.idShards)-1)]
}

// fnvHash 对字符串做 FNV-1a 哈希，作为分片路由依据。
func fnvHash(s string) uint32 {
	return util.Fnv32(s)
}

// nextPow2 向上取整到最近的 2 的幂（n <= 1 时返回 1）。
// 分片数必须为 2 的幂，分片路由才能用 h&(len-1) 正确取模。
// 复用 util.NextPow2，避免手写重复实现。
func nextPow2(n int) int {
	return util.NextPow2(n)
}

// cloneTLSForHTTP 为走 HTTP over TCP 的接入（TCP / WS）派生一份 TLS 配置副本，
// 并在 ALPN 列表追加 http/1.1，使浏览器 wss 握手能正确协商出通用应用协议。
// 证书、最低版本等基础参数复用原配置；原配置面向 QUIC/WT 的 ALPN（clover-quic / h3）保留不动。
// 入参为 nil（未启用 TLS）时原样返回 nil。
func cloneTLSForHTTP(base *tls.Config) *tls.Config {
	if base == nil {
		return nil
	}
	next := make([]string, 0, len(base.NextProtos)+1)
	hasHTTP11 := false
	for _, p := range base.NextProtos {
		if p == "http/1.1" {
			hasHTTP11 = true
		}
		next = append(next, p)
	}
	if !hasHTTP11 {
		next = append(next, "http/1.1")
	}
	clone := base.Clone()
	clone.NextProtos = next
	// TLS 下限放宽到 1.2：原始配置的 MinVersion 是 TLS 1.3（QUIC 的硬要求，见 app/bootstrap.go），
	// 但 TCP/WS 接入面对的是浏览器与原生客户端（Unity 的 SslStream），后者普遍只到 TLS 1.2 ——
	// 沿用 1.3 下限会让「TCP 也走 TLS」直接握不上手（现象是连上就断，且服务端只留一条握手失败日志）。
	// 放宽的是**下限**：双方都支持 1.3 时仍协商 1.3，安全强度不降。
	clone.MinVersion = tls.VersionTLS12
	return clone
}

// Config 网关双转发内核配置。
//
// 最小化端口方案：TCP 保底 + UDP（QUIC 与裸 UDP 共享）+ WS（WT 复用同端口号 UDP）。
type Config struct {
	TCPListen     string                     // 客户端 TCP 接入地址；空=不启用 TCP 接入
	WSListen      string                     // 客户端 WebSocket 接入地址（网页端入口；WT 复用同端口号 UDP）；空=不启用 WS 接入
	WSPath        string                     // WebSocket 升级路径
	WSCheckOrigin func(r *http.Request) bool // WS 跨域校验；nil=仅允许同源
	UDPListen     string                     // 客户端 UDP 接入地址（QUIC + 裸 UDP 共享同一 socket）；空=不启用 UDP 接入
	WTEnabled     bool                       // 是否在 WS 端口号上启用 WebTransport（UDP 侧）；默认 true
	Upstream      string                     // 逻辑服 TCP 地址（网关为每个客户端连接拨专用上游）
	Heartbeat     time.Duration              // 心跳间隔；<=0 用默认 30s
	NATSSubjects  []string                   // 订阅的异步推送 subject 列表；空=不订阅
	TLSConfig     *tls.Config                // 可选 TLS 配置；nil=明文（客户端接入启用 wss / 加密 TCP）
	// WTTLSConfig WebTransport 专用 TLS 配置。nil 时回退到 TLSConfig。
	// 独立于 TLSConfig 的原因见 WTCertHash 说明（短有效期 ECDSA vs 受信任长期证书）。
	WTTLSConfig *tls.Config
	// TCPTLSDisabled 让 TCP 接入绕过 TLS（即便 TLSConfig 非空）。
	//
	// 用途：网关一旦配了 tls_cert，gwcore 会把它一并交给 TCP 监听（tls.Listen），
	// 而原生客户端（Unity 等）走的是**裸 TCP**，没有 TLS/SslStream，握手必然失败
	// （症状：连上→秒断→所有 Call 超时）。开启 TLS 又是为了浏览器的 wss 与 WebTransport
	// （WT 的证书哈希必须经已受信任的通道下发）。本开关让两者不再互斥：
	// TCP 保持明文给原生客户端，WS / QUIC / WebTransport 照常加密。
	// 零值（false）= 沿用 TLSConfig，行为与加此字段前完全一致。
	TCPTLSDisabled bool
	// WTCertHash 服务器证书 DER 的 SHA-256（hex）。非空时网关在 WS 端口额外暴露
	// GET /wt-cert-hash，供浏览器以 serverCertificateHashes 建立 WebTransport。
	// 自签名证书场景必须设置，否则浏览器 WebTransport 必然握手失败。
	WTCertHash string
	// WTCertHashFunc 动态读取当前证书哈希。非 nil 时优先于 WTCertHash：
	// 证书运行期间自动轮换（重签）后哈希随之变化，/wt-cert-hash 必须返回最新值，
	// 否则前端拿旧哈希建连必然被证书固定机制拒绝。
	WTCertHashFunc func() string
	ReconnectGrace time.Duration // 重连宽限期：断开后保留 owner 索引的时间；0=立即清理（默认），>0 为宽限
	MaxConnsPerSec int           // 连接建立速率限制（每秒新建连接数上限）；<=0=不限制
	SessionShards  int           // 会话表分片数（2的幂）；<=0=使用默认值 64

	// 连接层：限流 / 排队 / 重连（详见报告 3. 连接层）
	MaxConns           int           // 连接总数上限（活跃会话数）；<=0=不限制（仅受系统资源约束）
	QueueCap           int           // 等候队列容量；>0=启用排队（限流/满载时缓冲而不直接拒绝，并向客户端下发 EMsgQueuePosition 位置通知）；<=0=不排队
	QueueReleasePerSec int           // 排队每秒放行数；>0=节流放行（时间窗口放行）；<=0=尽快放行进队者
	QueueTimeout       time.Duration // 排队最长时间；超时仍未放行则关闭连接；0=不限时
	DisconnectGrace    time.Duration // 断线宽限期：断开后延迟触发 OnDisconnect（软→硬掉线）；0=立即触发（默认）
	MaxFrameSize       int           // 上行客户端帧最大长度（字节）；>0 时超长帧直接丢弃（DoS / 畸形包防护）

	// 多端登录互踢策略。零值=KickOldest。
	KickStrategy KickStrategy

	// —— 登录门禁（对齐主流分层：网关负责「你是谁」）——

	// AuthDisabled 关闭登录门禁。仅开发 / 本地调试使用；生产务必保持 false。
	//
	// 为 false（默认）时：未绑定对象标识的连接只放行 AuthExemptMsgIDs 中的消息号，
	// 其余在网关侧直接拒绝（回 EErrorReply{code:401}），不转发逻辑服。
	//
	// 反向命名的原因与 event.Config.HTTPAuthDisabled 一致：零值 = 开启门禁（fail-safe），
	// 漏配只会挡住功能，不会放行越权。
	AuthDisabled bool
	// AuthExemptMsgIDs 免登录消息号白名单（仅 AuthDisabled=false 时生效）。
	// 为空时取 DefaultAuthExemptMsgIDs（注册 / 登录 / 恢复会话）。
	// 注意是**完全覆盖**而非追加：业务配置后需自行包含登录类消息号。
	AuthExemptMsgIDs []uint32
}

// DefaultAuthExemptMsgIDs 引擎内置的免登录消息号白名单，只覆盖「登录前必须可达」的
// 会话建立类消息：
//   - EMsgLogin：登录本身；
//   - EMsgResumeSession：断线重连恢复会话。
//
// 注册（原 EMsgSignup=1）**不在**白名单里：注册走账号服 HTTP，游戏服不收注册报文，
// 客户端若发该号会被本门禁直接挡在门外——这是预期行为，不是缺配置。
//
// 注意 EMsgBindUDP 不在此列——它由网关在 onUDPFrame 直接处理（凭一次性令牌鉴权），
// 不经过通用转发路径，与本门禁无关。业务若另有登录前可达的消息（公告查询、版本检查等），
// 经 Config.AuthExemptMsgIDs 显式覆盖白名单。
var DefaultAuthExemptMsgIDs = []uint32{
	proto.EMsgLogin,
	proto.EMsgResumeSession,
}

// Status 网关监听器实际运行状态（Start 完成后可用，供启动汇总报告展示）。
// 各地址字段为空表示对应监听未启用或启动失败；失败原因见 WTErr / QUICErr。
type Status struct {
	TCP  string // 客户端 TCP 接入地址；空=未启用
	WS   string // WebSocket 接入地址（含 path）；空=未启用
	UDP  string // UDP 共享端口（QUIC + 裸 UDP）；空=未启用
	WT   string // WebTransport 接入地址（复用 WS 端口号 UDP）；空=未启用或启动失败
	QUIC string // QUIC 接入地址（与 UDP 共享端口）；空=未启用或启动失败

	WTErr   string // WebTransport 启动失败原因；空=正常
	QUICErr string // QUIC 启动失败原因；空=正常
}

// KickStrategy 多端登录互踢策略：同一 owner 已有在线连接时，新连接登录如何处理旧连接。
type KickStrategy int

const (
	// KickOldest 踢掉最早的在线连接（默认，单端在线语义）。
	KickOldest KickStrategy = iota
	// KickNewest 踢掉最近一次登录的在线连接（本次之前的最后一个）。
	KickNewest
	// KickAll 踢掉该 owner 除本次外的所有在线连接。
	KickAll
)

// ExtractOwnerIDFunc 从逻辑服回包中提取"下发目标标识"（如玩家 UID）。
// 返回 ("", false) 表示本条回包不绑定目标（如非登录回包）。
type ExtractOwnerIDFunc func(msgID uint32, body []byte) (string, bool)

// ExtractCryptoKeyFunc 从登录/注册回包中提取 AES-256 会话密钥（32B raw bytes）。
// 返回 (nil, false) 表示本条回包不含密钥；非空则网关为该会话启用 AES-GCM 加解密。
type ExtractCryptoKeyFunc func(msgID uint32, body []byte) ([]byte, bool)

// PushRouterFunc 按下发目标标识解析出应接收推送的连接集合。
// 默认实现按绑定的 owner-id 单推；房间/分线架构可覆盖为群推。
type PushRouterFunc func(target string) []iconn.Conn

// Option 网关内核可选参数。
type Option func(*Gateway)

// WithExtractOwnerID 设置登录回包提取目标标识的回调（不设置则不自动绑定）。
func WithExtractOwnerID(fn ExtractOwnerIDFunc) Option {
	return func(g *Gateway) { g.extract = fn }
}

// WithPushRouter 设置异步推送路由策略（不设置则按绑定 id 单推）。
func WithPushRouter(fn PushRouterFunc) Option {
	return func(g *Gateway) { g.router = fn }
}

// WithNATS 设置 NATS 客户端（用于订阅异步推送；不设置则不订阅）。
func WithNATS(nc *nats.Client) Option {
	return func(g *Gateway) { g.nats = nc }
}

// WithGWControlSubject 设置逻辑服 → 网关控制指令 NATS subject。
// 逻辑服 SetPlayerID 时经此 subject 发送 GWControlBind，
// 网关收到后追加 idIndex[PlayerID]（双 key 架构：account + playerID 共存）。
func WithGWControlSubject(subj string) Option {
	return func(g *Gateway) { g.gwControlSubject = subj }
}

// WithOnDisconnect 设置客户端断开连接时的回调（连接级心跳掉线感知）。
// 回调收到连接标识与已登录玩家标识（未登录为空串），由调用方回流到逻辑服事件总线。
func WithOnDisconnect(fn func(connID, owner string)) Option {
	return func(g *Gateway) { g.onDisconnect = fn }
}

// WithOnConnect 设置客户端「连接 / 上线 / 重连」时的回调。
// 回调收到连接标识、已登录玩家标识（owner）与 isReconnect：
// - isReconnect=false：该 owner 首次绑定（首次上线 / 登录成功）；
// - isReconnect=true：该 owner 曾有一次在线会话（被本次连接替换），即重连。
//
// 触发点为「登录回包提取到 owner 并绑定会话」之时；由调用方回流到逻辑服事件总线。
func WithOnConnect(fn func(connID, owner string, isReconnect bool)) Option {
	return func(g *Gateway) { g.onConnect = fn }
}

// WithRateLimiter 注入业务自定义限流器（pkg/runtime/ratelimit 提供的 GCRA / 令牌桶等）。
// 设置后，每次新连接建立都会先经该限流器 Allow()；超限且未启用排队则拒绝。
// 与内置的 MaxConnsPerSec（每秒新建连接数）叠加生效。
func WithRateLimiter(rl ratelimt.Limiter) Option {
	return func(g *Gateway) { g.rateLimiter = rl }
}

// WithRateLimitManager 注入消息级速率管理器（pkg/runtime/ratelimit.Manager），
// 按连接 / 玩家多 key 限制上行帧频率（每连接每秒上限，防御刷包 / CC）。
// policy 为使用的策略名（空串=管理器默认策略）；管理器未注入对应策略时该 key 一律限流（fail-closed）。
// 与连接级 MaxConnsPerSec 互补：前者控"连得多不多"，后者控"单个连接发包猛不猛"。
func WithRateLimitManager(m *ratelimt.Manager, policy string) Option {
	return func(g *Gateway) { g.rlManager = m; g.rlPolicy = policy }
}

// WithOnSoftDisconnect 设置「软掉线」回调：仅当 DisconnectGrace>0 时，断开连接后
// 立即触发（玩家掉线但仍在宽限期内，可保留 Room/Scene 上下文，等待重连或硬掉线）。
func WithOnSoftDisconnect(fn func(connID, owner string)) Option {
	return func(g *Gateway) { g.onSoftDisconnect = fn }
}

// WithOnHardDisconnect 设置「硬掉线」回调：断开宽限期结束（或宽限期为 0 时立即）触发，
// 等价于 OnDisconnect 的最终清理时机（确认玩家离线后清理 Room/Scene/定时器）。
func WithOnHardDisconnect(fn func(connID, owner string)) Option {
	return func(g *Gateway) { g.onHardDisconnect = fn }
}

// WithOnStop 设置网关停机回调：在 Stop() 完成资源释放后调用，用于清理
// 调用方挂靠在网关生命周期上的外部资源（如 WebTransport 证书轮换器）。
func WithOnStop(fn func()) Option {
	return func(g *Gateway) { g.onStop = fn }
}

// WithOnQueued 设置排队位置变化的观测回调（ahead=前面还有多少人，0=已到队首）。
//
// **下发不由本回调负责**：引擎已内置把位置帧（EMsgQueuePosition）直接写给排队中的连接
// （见 queue_position.go）——排队中的连接尚未建会话，回调只给 connID，业务拿不到可写回的连接。
// 本回调用于观测：埋点统计、日志、容量告警等。
//
// 触发时机：入队时一次，之后按 queuePositionRefreshInterval 检查，位置变化时再触发。
func WithOnQueued(fn func(connID string, ahead int)) Option {
	return func(g *Gateway) { g.onQueued = fn }
}

// WithOnKick 设置「被多端登录互踢下线」回调：同一 owner 已在其他连接在线，
// 本次登录将其旧会话强制关闭时触发，便于业务向旧客户端下发提示或清理上下文。
func WithOnKick(fn func(connID, owner string)) Option {
	return func(g *Gateway) { g.onKick = fn }
}

// WithCryptoKeyExtractor 设置从登录回包中提取会话密钥的回调。
// 网关在绑定会话后调用此函数；若返回非空 key，则为该会话启用 AES-GCM 加解密。
// 使用 auth.ExtractSessionKey() 作为标准实现。
func WithCryptoKeyExtractor(fn ExtractCryptoKeyFunc) Option {
	return func(g *Gateway) { g.extractCrypto = fn }
}

// Gateway 网关双转发内核实例。
type Gateway struct {
	cfg     Config
	nats    *nats.Client
	extract ExtractOwnerIDFunc
	router  PushRouterFunc

	// authExempt 免登录消息号集合（由 cfg.AuthExemptMsgIDs 或内置默认白名单在 New 时固化）。
	// 启动期写入、运行期只读，故无需加锁。
	authExempt map[uint32]struct{}

	// upstream 运行期可切换的默认上游（逻辑服）地址。
	// 与 cfg.Upstream 分离的原因：灰度重启时新连接必须拨到新版本进程，
	// 而 cfg 是启动期快照、不可变。仅影响此后新建的会话，存量会话不受影响
	// （存量会话的迁移走 per-connection 的 GWControlSwitchUpstream）。
	upstream atomic.Value // string

	// onDisconnect 客户端断开连接时的回调（连接级心跳掉线感知）；nil=不回调。
	onDisconnect func(connID, owner string)

	// onConnect 客户端「连接 / 上线 / 重连」时的回调；nil=不回调。
	// isReconnect=true 表示该 owner 此前已有在线会话被本次连接替换（重连）。
	onConnect func(connID, owner string, isReconnect bool)

	// onStop 网关停机回调：Stop() 资源释放完成后调用；nil=不回调。
	onStop func()

	// gwControlSubject 逻辑服 → 网关控制指令 NATS subject（如双 key 索引追加）。
	gwControlSubject string

	tcpSrv  *tcp.Server
	wsSrv   *ws.Server
	udpSrv  *udp.Server
	quicSrv *quic.Server
	wtSrv   *wt.Server
	// udpDemux QUIC + 裸 UDP 共享端口的首字节分发器（UDPListen 配置时创建）。
	udpDemux *demux.PacketConn

	// 会话表分片：sessions 按 connID 哈希分片、idIndex 按绑定 id 哈希分片，
	// 各分片独立 RWMutex，避免海量连接下所有帧处理挤在同一把全局锁上串行。
	sessionShards []*sessionShard // 按 connID 哈希
	idShards      []*idShard      // 按 id 哈希

	// 连接级限流：每秒新建连接数上限（滑动窗口）
	connRateLimiter *ratelimt.SlidingWindow
	// 可选：业务注入的限流器（pkg/runtime/ratelimit 的 GCRA / 令牌桶等）
	rateLimiter ratelimt.Limiter
	// 消息级限流：按连接 / 玩家多 key 的速率管理器（pkg/runtime/ratelimit.Manager）。
	// 非空时每个上行帧在进入派发前经其 Allow() 判定；超限则丢弃该帧（不放行）。
	rlManager *ratelimt.Manager
	rlPolicy  string // 使用的策略名；空串表示使用管理器默认策略
	// 当前活跃会话数（原子计数，供限流 / 排队做负载感知）
	currentConns atomic.Int64
	// 等候队列（限流 / 满载时缓冲，而非直接拒绝）
	queue *connQueue

	// 重连宽限：断开时保留 owner→connID 映射的过期记录，用于 OnReconnect 判定
	reconnectGraces map[string]time.Time // owner → 过期时间
	reconnectMu     sync.Mutex
	reconCh         chan struct{} // 重连清理 goroutine 退出信号

	// 断线宽限：宽限期内待触发的硬掉线（超时或重连取消）
	pendingHardDisconnects map[string]*hardGrace
	dgMu                   sync.Mutex

	// 断线回调（连接级心跳掉线感知）
	onSoftDisconnect func(connID, owner string)     // 软掉线（宽限期内立即触发）
	onHardDisconnect func(connID, owner string)     // 硬掉线（可选额外回调）
	onQueued         func(connID string, ahead int) // 排队位置变化回调（ahead=前面还有多少人）；下发由引擎负责
	// 多端登录互踢回调：同一 owner 已在其他连接在线，本次登录将其旧会话强制关闭时触发，
	// 便于业务向旧客户端下发「账号在别处登录」提示或清理上下文。
	onKick func(connID, owner string)
	// extractCrypto 从登录回包中提取会话密钥的回调；nil=不启用加密
	extractCrypto ExtractCryptoKeyFunc

	// connID→owner 反查表，避免 getOwnerOf O(N) 全表扫描。
	// 每个 idShard 下维护反向索引，由 bind/unbind 路径增删。
	connOwner     map[string]string // connID → owner
	connOwnerLock sync.RWMutex

	// udpEndpoints owner → 常驻裸 UDP 端点集合：TCP/WS 玩家不可靠推送的定位依据。
	// 客户端登录成功后会收到网关下发的绑定令牌（EMsgUDPBindGrant），并凭令牌在
	// EMsgBindUDP 帧中把自己的 UDP socket 来源地址上报给网关；网关校验令牌有效后
	// 把该来源登记为该 owner 的不可靠推送端点（key 为 addr.String()，多端并存）。
	// 会话断开时按令牌撤销登记（delUDPBinding），同一 owner 其他在线端不受影响。
	//
	// ⚠️ 这三张表都是**网关进程本地**的，不跨进程同步。多网关部署时这意味着：
	// 同一玩家的 TCP 连接与其裸 UDP 包**必须落到同一网关进程**（LB 按源 IP 亲和，
	// 或 UDP 地址静态指向固定网关），否则令牌校验必然失败、不可靠推送静默失效。
	// 这是网关多实例**唯一**要求「会话粘性」的地方——可靠通道（TCP / WS / QUIC / WT）
	// 不要求，重连可落任意网关（owner 重绑见 session 的 EMsgResumeSession 处理）。
	udpEndpoints   map[string]map[string]net.Addr // owner -> addrKey -> addr
	udpBindByTok   map[string]udpBind             // token -> 绑定记录（撤销/回收端点用）
	udpTokens      map[string]string              // token -> owner（EMsgBindUDP 校验用）
	udpEndpointsMu sync.RWMutex

	// 追踪所有后台 goroutine，Stop 时等待其全部退出。
	wg sync.WaitGroup

	// lifecycleMu 串行化「会话/监控协程登记（wg.Add）」与「Stop 的 wg.Wait」以消除
	// WaitGroup 的 Add/Wait 竞态：停机时传输层读循环里在途的帧仍可能进入
	// createSession/tryQueue 登记新协程，若 Add 落在 wg.Wait 之后，Wait 会提前返回、
	// 新起的 cleanup/监控协程永不被等待。
	// stopping 为停机门：置位后 createSession/tryQueue 一律拒绝新登记。
	lifecycleMu sync.Mutex
	stopping    atomic.Bool

	// stopOnce 保证 Stop 的清理流程只执行一次：其余组件虽支持重复 Stop，
	// 但 reconCh 的 close 在第二次调用时会 panic，onStop 回调也不应重复触发。
	// sync.Once 的其它调用者会阻塞至首次清理完成，保持「Stop 返回即已停止」的语义。
	stopOnce sync.Once

	// 硬掉线版本号，用于防止 cancel/定时器 竞态导致重复 fire。
	// scheduleHardDisconnect 给每条记录一个单调递增 token；
	// cancelHardDisconnect 标记对应 token 已取消，定时器回调校验 token 仍最新才触发。
	hardDisconnectSeq uint64

	// 运行状态快照（Start 成功后填充），供启动汇总报告 / 运维展示使用。
	status  Status
	wtErr   error // WebTransport 启动失败原因（仅失败时非空）
	quicErr error // QUIC 启动失败原因（仅失败时非空）
}

// udpBind 一条 UDP 绑定令牌的登记记录：令牌所属 owner + 该令牌已登记的端点集合。
// 由 udpBindByTok 按令牌索引，会话断开/换绑时按令牌精确回收，不影响同 owner 其他端。
type udpBind struct {
	owner string              // 令牌所属 owner（account）
	addrs map[string]net.Addr // addrKey -> addr（该令牌登记的常驻裸 UDP 端点）
}

// Session 单条客户端会话：包装后的客户端连接 + 到逻辑服的专用上游连接。
// sessionCrypto 会话级加解密接口。
type sessionCrypto interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

type Session struct {
	bc       iconn.Conn
	upstream *tcp.Conn
	connID   string        // 连接标识（建会话时记录，供互踢回调回流）
	kicked   atomic.Bool   // 被多端登录互踢标记：cleanup 时跳过仍在线 owner 的断线事件
	crypto   sessionCrypto // 会话级加解密器；nil=明文模式
	cryptoMu sync.RWMutex  // 保护 crypto 的并发读写

	// handshakeOK — crypto 已协商但客户端尚未确认（所有解密失败的帧按明文处理）。
	// 首帧解密成功后将 handshakeOK 置 true，此后解密失败则关闭连接。
	handshakeOK atomic.Bool

	// 发送串行化锁，防止并发 NATS 推送/Send 并发写 bc。
	sendMu sync.Mutex

	// 绑定串行化锁。同一会话并发收到两条不同 owner 登录回包时，若不串行化，
	// 对旧/新 idIndex 分片的解绑/追加会交错，导致索引与 bc.PlayerID() 状态不一致。
	// 换绑逻辑（解绑旧 owner → 追加新 owner → BindPlayer）整体持此锁执行。
	// 同时保护 udpToken 的读写（令牌与绑定同生命周期）。
	bindMu sync.Mutex

	// udpToken 本会话当前的 UDP 绑定令牌（EMsgUDPBindGrant 下发的随机串）。
	// 会话绑定 owner 成功时生成并登记 udpTokens；EMsgBindUDP 凭它鉴权登记裸 UDP
	// 端点；会话断开 / 换绑 / 被踢时按它精确撤销端点，不影响同 owner 其他在线端。
	// 由 bindMu 保护。
	udpToken string

	// extraIDs 记录除 account（bc.PlayerID）外的额外 idIndex key（如 playerID）。
	// 断开时统一清理，避免僵尸索引。
	extraIDs []string
}

func (s *Session) setCrypto(c sessionCrypto) {
	s.cryptoMu.Lock()
	s.crypto = c
	s.cryptoMu.Unlock()
	// 刚设置 crypto 时握手未确认，客户端可能仍发送明文帧
	s.handshakeOK.Store(false)
}

func (s *Session) getCrypto() sessionCrypto {
	s.cryptoMu.RLock()
	defer s.cryptoMu.RUnlock()
	return s.crypto
}

// New 构造网关内核（启动监听前调用）。
func New(cfg Config, opts ...Option) (*Gateway, error) {
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = heartbeat
	}
	shards := cfg.SessionShards
	if shards <= 0 {
		shards = defaultSessionShards
	}
	// 先限幅再取整：nextPow2 对接近 int 上限的输入会溢出为负值（make(负长度) 直接 panic），
	// 且超大分片数本身就是资源灾难（几十亿个 shard 结构体直接 OOM）。
	// 65536 片（向下 2 的幂）足以承载百万级连接，超出即视为误配钳制。
	const maxSessionShards = 1 << 16
	if shards > maxSessionShards {
		logger.Warnf("gwcore: config SessionShards too large (%d), clamped to %d",
			cfg.SessionShards, maxSessionShards)
		shards = maxSessionShards
	}
	// sessionShardOf/idShardOf 用 h&(len-1) 取模，len 必须是 2 的幂，
	// 否则掩码运算会漏掉部分分片甚至越界。向上取整到最近的 2 的幂。
	if shards2 := nextPow2(shards); shards2 != shards {
		logger.Warnf("gwcore: config SessionShards %d is not power of 2, adjusted to %d", shards, shards2)
		shards = shards2
	}
	g := &Gateway{
		cfg:                    cfg,
		sessionShards:          make([]*sessionShard, shards),
		idShards:               make([]*idShard, shards),
		reconnectGraces:        make(map[string]time.Time),
		pendingHardDisconnects: make(map[string]*hardGrace),
		connOwner:              make(map[string]string),
		udpEndpoints:           make(map[string]map[string]net.Addr),
		udpBindByTok:           make(map[string]udpBind),
		udpTokens:              make(map[string]string),
	}
	for i := range g.sessionShards {
		g.sessionShards[i] = &sessionShard{sessions: make(map[string]*Session)}
	}
	for i := range g.idShards {
		g.idShards[i] = &idShard{idIndex: make(map[string][]*Session)}
	}
	for _, o := range opts {
		o(g)
	}
	// 连接级限流：仅显式配置 >0 时创建窗口限流器。
	// 不能把 <=0 直接交给 NewSlidingWindow：其内部对 max<=0 回落为 1/s，
	// 会把「不限制」的默认（MaxConnsPerSec 恒为 0）静默变成「全局每秒 1 个新连接」。
	// nil = 不限制（rateAllow 对 nil 直接跳过）。
	if cfg.MaxConnsPerSec > 0 {
		g.connRateLimiter = ratelimt.NewSlidingWindow(cfg.MaxConnsPerSec, time.Second)
	}
	// 免登录白名单固化（放在 opts 之后：Option 可覆盖 cfg.AuthExemptMsgIDs）。
	exempt := g.cfg.AuthExemptMsgIDs
	if len(exempt) == 0 {
		exempt = DefaultAuthExemptMsgIDs
	}
	g.authExempt = make(map[uint32]struct{}, len(exempt))
	for _, id := range exempt {
		g.authExempt[id] = struct{}{}
	}

	// 默认上游取启动期配置；运行期可经 SetUpstream 切换（灰度重启时指向新版本进程）。
	g.upstream.Store(cfg.Upstream)
	if g.router == nil {
		g.router = g.defaultRouter
	}

	// 等候队列：限流 / 满载时缓冲连接（而非直接拒绝），按 QueueReleasePerSec 时间窗口放行；
	// 入队连接由网关直发位置通知（EMsgQueuePosition，见 queue_position.go）。
	if cfg.QueueCap > 0 {
		g.queue = newConnQueue(cfg.QueueCap, cfg.QueueReleasePerSec, g.releasePending, g.notifyQueued)
	} else if cfg.QueueReleasePerSec > 0 || cfg.QueueTimeout > 0 {
		// 排队未启用（QueueCap<=0）时 QueueReleasePerSec / QueueTimeout 一律不生效：
		// 静默忽略会让「配了却没生效」无人可查，故显式告警（不改变任何行为）。
		logger.Warnf("gwcore: QueueReleasePerSec=%d/QueueTimeout=%s configured but QueueCap=%d (queue disabled), ignoring them",
			cfg.QueueReleasePerSec, cfg.QueueTimeout, cfg.QueueCap)
	}

	// TCP / WS 走 HTTP over TCP（tls.Listen），浏览器 wss 握手需要 ALPN 协商 http/1.1；
	// 而 cfg.TLSConfig 的 NextProtos 面向 QUIC/WT（clover-quic / h3），两者不能共用同一份
	// ALPN 列表，否则浏览器 wss 握手因无通用应用协议而失败。此处为 TCP/WS 派生独立副本，
	// 追加 http/1.1，证书与基础参数仍复用原配置。
	wsTLS := cloneTLSForHTTP(cfg.TLSConfig)
	// TCP 单独一份：TCPTLSDisabled 为真时退回明文，让裸 TCP 原生客户端在网关开了 TLS 后仍能连。
	tcpTLS := wsTLS
	if cfg.TCPTLSDisabled {
		tcpTLS = nil
	}
	// 网关的 MaxFrameSize（上行帧上限）必须**同时下传到传输层**：网关边缘校验（session.go
	// 的 frame too large）在传输层之后，传输层自己还有一道单帧上限——不下传时实际生效的是
	// 传输层默认值，与客户端对齐的那个上限根本不生效，且超限是传输层直接断连、网关日志里看不到。
	// 客户端对齐的正是这一个值（见 tcp/config.go / ws/config.go 的 defaultMaxMsgSize 注释）。
	frameLimit := cfg.MaxFrameSize
	// TCP 客户端接入为可选项。
	if cfg.TCPListen != "" {
		g.tcpSrv = tcp.NewServer(tcp.ServerConfig{
			ListenAddr:        cfg.TCPListen,
			HeartbeatInterval: cfg.Heartbeat,
			MaxMsgSize:        frameLimit,
			TLSConfig:         tcpTLS,
		}, g.onTCPFrame)
	}
	// WebSocket 客户端接入为可选项；WebTransport 复用同端口号 UDP（网页端共享端口语义）。
	if cfg.WSListen != "" {
		g.wsSrv = ws.NewServer(ws.ServerConfig{
			Addr:              cfg.WSListen,
			Path:              cfg.WSPath,
			HeartbeatInterval: cfg.Heartbeat,
			MaxMsgSize:        int64(frameLimit),
			// WS 始终跟随 TLSConfig：浏览器的 wss，以及经 wss 下发 WT 证书哈希，都依赖它。
			TLSConfig:      wsTLS,
			CheckOrigin:    cfg.WSCheckOrigin,
			WTCertHash:     cfg.WTCertHash,
			WTCertHashFunc: cfg.WTCertHashFunc,
		}, g.onWSFrame)
		if cfg.WTEnabled {
			wtTLS := cfg.WTTLSConfig
			if wtTLS == nil {
				wtTLS = cfg.TLSConfig
			}
			g.wtSrv = wt.NewServer(wt.ServerConfig{
				ListenAddr:  wsSamePortUDP(cfg.WSListen),
				TLSConfig:   wtTLS,
				CheckOrigin: cfg.WSCheckOrigin,
			}, g.onWTFrame)
		}
	}
	// UDP 客户端接入为可选项：QUIC 与裸 UDP 共享同一端口（按首字节分发）。
	if cfg.UDPListen != "" {
		g.udpSrv = udp.NewServer(udp.ServerConfig{
			ListenAddr: cfg.UDPListen,
		}, g.onUDPFrame)
		g.quicSrv = quic.NewServer(quic.ServerConfig{
			TLSConfig: cfg.TLSConfig,
		}, g.onQUICFrame)
	}

	// NATS 客户端由调用方注入并拥有生命周期（见 WithNATS）。
	// 逐条订阅，某条失败时记录错误并返回，已订阅的 subjects 由调用方决定如何处理。
	if len(cfg.NATSSubjects) > 0 && g.nats != nil {
		for _, subj := range cfg.NATSSubjects {
			if err := g.nats.Subscribe(subj, g.onNotify); err != nil {
				logger.Errorf("gwcore: subscribe NATS subject %s failed: %v (previously subscribed subjects may need cleanup)", subj, err)
				return nil, err
			}
			logger.Infof("gwcore: subscribed NATS subject %s", subj)
		}
	}

	// 控制指令 subject（逻辑服 SetPlayerID 时通知网关追加双 key 索引）。
	if g.gwControlSubject != "" && g.nats != nil {
		if err := g.nats.Subscribe(g.gwControlSubject, g.onGWControl); err != nil {
			logger.Errorf("gwcore: subscribe GW control subject %s failed: %v", g.gwControlSubject, err)
			return nil, err
		}
		logger.Infof("gwcore: subscribed GW control subject %s", g.gwControlSubject)
	}
	return g, nil
}

// Start 启动监听（后台运行，不阻塞）。
func (g *Gateway) Start() error {
	if g.tcpSrv != nil {
		if err := g.tcpSrv.Start(); err != nil {
			return err
		}
	}
	if g.wsSrv != nil {
		if err := g.wsSrv.Start(); err != nil {
			// WS 启动失败时关闭已启动的 TCP。
			if g.tcpSrv != nil {
				_ = g.tcpSrv.Stop()
			}
			return err
		}
	}
	// WebTransport 复用 WS 端口号（UDP 侧）：启动失败仅告警，不影响 WS（网页端仍可用）。
	if g.wtSrv != nil {
		if err := g.wtSrv.Start(); err != nil {
			logger.Errorf("gwcore: wt start failed on %s (ws still serving): %v", g.wtSrv.Addr(), err)
			g.wtErr = err
			g.wtSrv = nil
		} else {
			logger.Infof("gwcore: wt listening on %s", g.wtSrv.Addr())
		}
	}
	// UDP 共享端口（QUIC + 裸 UDP）：同一 socket 按首字节分发。
	if g.udpSrv != nil {
		if err := g.startSharedUDP(); err != nil {
			// UDP 启动失败时关闭已启动的 TCP + WS。
			if g.wsSrv != nil {
				_ = g.wsSrv.Stop()
			}
			if g.tcpSrv != nil {
				_ = g.tcpSrv.Stop()
			}
			return err
		}
	}
	// 重连宽限过期定期清理（每 10s 清理过期记录）
	if g.cfg.ReconnectGrace > 0 {
		g.reconCh = make(chan struct{})
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					g.sweepReconnectGraces()
				case <-g.reconCh:
					return
				}
			}
		}()
	}
	// 等候队列放行循环
	if g.queue != nil {
		g.queue.start()
	}
	tcpAddr, wsAddr, udpAddr, wtAddr := "disabled", "disabled", "disabled", "disabled"
	if g.tcpSrv != nil {
		tcpAddr = g.cfg.TCPListen
	}
	if g.wsSrv != nil {
		wsAddr = g.cfg.WSListen + g.cfg.WSPath
	}
	if g.udpSrv != nil {
		udpAddr = g.cfg.UDPListen + " (quic+udp)"
	}
	if g.wtSrv != nil {
		wtAddr = g.cfg.WSListen + " (wt over udp)"
	}
	logger.Infof("gwcore: started tcp=%s ws=%s udp=%s wt=%s", tcpAddr, wsAddr, udpAddr, wtAddr)
	// 保存实际运行状态快照（地址为空=未启用；WT/QUIC 失败原因单独记录）。
	st := Status{}
	if g.tcpSrv != nil {
		st.TCP = g.cfg.TCPListen
	}
	if g.wsSrv != nil {
		st.WS = g.cfg.WSListen + g.cfg.WSPath
	}
	if g.udpSrv != nil {
		st.UDP = g.cfg.UDPListen
	}
	if g.wtSrv != nil {
		st.WT = g.cfg.WSListen
	}
	if g.quicSrv != nil {
		st.QUIC = g.cfg.UDPListen
	}
	if g.wtErr != nil {
		st.WTErr = g.wtErr.Error()
	}
	if g.quicErr != nil {
		st.QUICErr = g.quicErr.Error()
	}
	g.status = st
	return nil
}

// Status 返回网关监听器实际运行状态快照（Start 完成后调用）。
func (g *Gateway) Status() Status { return g.status }

// SetUpstream 切换默认上游（逻辑服）地址。
//
// 语义边界（灰度重启的关键）：只影响此后**新建**的会话，存量会话仍连在原上游上。
// 存量连接的迁移必须逐条走 GWControlSwitchUpstream（switchUpstream），
// 它会先 dial 新上游再原子替换，客户端 TCP 连接不断开。
func (g *Gateway) SetUpstream(addr string) {
	if addr == "" {
		return
	}
	old := g.Upstream()
	g.upstream.Store(addr)
	logger.Infof("gwcore: upstream switched %s -> %s (affects new sessions only)", old, addr)
}

// Upstream 返回当前默认上游地址。
func (g *Gateway) Upstream() string {
	v, _ := g.upstream.Load().(string)
	return v
}

// startSharedUDP 在一个 UDP socket 上同时启动 QUIC 与裸 UDP（按首字节分发）。
func (g *Gateway) startSharedUDP() error {
	pc, err := net.ListenPacket("udp", g.cfg.UDPListen)
	if err != nil {
		return err
	}
	// 提升 UDP socket 缓冲区，避免 quic-go 在共享端口场景下报 receive buffer 警告。
	// Windows 上设多大受注册表限制，失败只告警，不阻塞启动。
	if uc, ok := pc.(*net.UDPConn); ok {
		if err := uc.SetReadBuffer(2 * 1024 * 1024); err != nil {
			logger.Warnf("gwcore: set udp read buffer failed: %v", err)
		}
		if err := uc.SetWriteBuffer(2 * 1024 * 1024); err != nil {
			logger.Warnf("gwcore: set udp write buffer failed: %v", err)
		}
	}
	dm := demux.New(pc, 1024, 1024)
	g.udpDemux = dm
	dm.Start()
	// 启动 QUIC：QUIC 监听器绑定 demux（作为其 PacketConn）。
	// QUIC 依赖受信任证书才能启动；未配置证书时跳过 QUIC，不拖垮整条共享 UDP，
	// 裸 UDP（首字节 0x55）与 WS/TCP 仍可正常服务。quicCh/rawCh 彼此独立缓冲，
	// QUIC 未消费不会阻塞裸 UDP 分发。
	if g.quicSrv != nil {
		if err := g.quicSrv.StartWithPacketConn(dm); err != nil {
			logger.Errorf("gwcore: quic start failed, skip quic (raw udp still serving): %v", err)
			g.quicErr = err
			g.quicSrv = nil
		}
	}
	// 再启动裸 UDP：消费 demux 分发的非 QUIC 数据报。
	if err := g.udpSrv.StartFrom(dm.RawChan()); err != nil {
		// QUIC 监听器已绑定 dm：必须一并停掉，否则遗留「QUIC 持有的已是关闭的 demux」
		// 的半死状态（监听/协程残留且无法服务）。
		if g.quicSrv != nil {
			_ = g.quicSrv.Stop()
			g.quicSrv = nil
		}
		_ = dm.Close()
		g.udpDemux = nil
		return err
	}
	return nil
}

// wsSamePortUDP 将 WS 的 TCP 监听地址改写为同端口号的 UDP 地址（WebTransport 复用网页端端口）。
func wsSamePortUDP(wsAddr string) string {
	host, port, err := net.SplitHostPort(wsAddr)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, port)
}

// sweepReconnectGraces 清理已过期的重连宽限记录。
func (g *Gateway) sweepReconnectGraces() {
	g.reconnectMu.Lock()
	defer g.reconnectMu.Unlock()
	now := time.Now()
	for owner, expiry := range g.reconnectGraces {
		if now.After(expiry) {
			delete(g.reconnectGraces, owner)
		}
	}
}

// hardGrace 断线宽限期内待触发的硬掉线记录。
// key 按 connID 维度管理（不以 uid 去重），避免同 uid 不同 connID
// 的硬掉线记录被误删（例如玩家同时持有两条连接 A、B，A 断开后 B 的硬掉线被覆盖）。
// ver 版本号在 cancel 时递增，定时器回调校验 ver 一致才触发。
type hardGrace struct {
	connID string
	uid    string
	timer  *time.Timer
	ver    uint64 // 单调递增版本号，cancel 后该记录被覆盖，旧定时器 ver 失配则跳过
}

// scheduleHardDisconnect 安排「硬掉线」（OnDisconnect）在 DisconnectGrace 结束后触发；
// 宽限期内该 owner 重连则经 cancelHardDisconnect 取消（视为未掉线）。
func (g *Gateway) scheduleHardDisconnect(connID, uid string) {
	g.dgMu.Lock()
	defer g.dgMu.Unlock()
	if _, ok := g.pendingHardDisconnects[connID]; ok {
		return // 同一 connID 已有记录，避免重复安排
	}
	seq := atomic.AddUint64(&g.hardDisconnectSeq, 1)
	hg := &hardGrace{connID: connID, uid: uid, ver: seq}
	hg.timer = time.AfterFunc(g.cfg.DisconnectGrace, func() {
		g.dgMu.Lock()
		// 校验 ver 仍为最新（cancel 后 ver 递增，旧定时器失配跳过）
		if cur, ok := g.pendingHardDisconnects[connID]; !ok || cur.ver != hg.ver {
			g.dgMu.Unlock()
			return
		}
		delete(g.pendingHardDisconnects, connID)
		g.dgMu.Unlock()
		g.fireHardDisconnect(hg.connID, hg.uid)
	})
	g.pendingHardDisconnects[connID] = hg
}

// cancelHardDisconnectByUID 取消指定 uid 的所有待触发硬掉线。
// 用于重连时通过 uid 取消；内部转为遍历查找对应 connID。
func (g *Gateway) cancelHardDisconnectByUID(uid string) bool {
	g.dgMu.Lock()
	defer g.dgMu.Unlock()
	cancelled := false
	for connID, hg := range g.pendingHardDisconnects {
		if hg.uid == uid {
			hg.timer.Stop()
			hg.ver = atomic.AddUint64(&g.hardDisconnectSeq, 1)
			delete(g.pendingHardDisconnects, connID)
			cancelled = true
		}
	}
	return cancelled
}

// fireHardDisconnect 触发「硬掉线」（等价于 OnDisconnect 的最终清理回调）。
func (g *Gateway) fireHardDisconnect(connID, uid string) {
	if g.onDisconnect != nil {
		g.onDisconnect(connID, uid)
	}
	if g.onHardDisconnect != nil {
		g.onHardDisconnect(connID, uid)
	}
}

// Kick 强制踢下线指定连接（connID）。返回 true 表示找到并关闭了会话。
// 用于逻辑服主动踢人（GM踢/封禁等）。
func (g *Gateway) Kick(connID string) bool {
	shard := g.sessionShardOf(connID)
	shard.mu.Lock()
	sess, ok := shard.sessions[connID]
	if ok {
		delete(shard.sessions, connID)
	}
	shard.mu.Unlock()
	if !ok {
		return false
	}
	// 本函数已把会话摘出分片表，后续连接关闭触发的 cleanup 会在 !ok 处早返回、
	// 不再执行 -1，故这里必须自行归还活跃计数，否则被踢的连接会永久占用 MaxConns 名额。
	if n := g.currentConns.Add(-1); n < 0 {
		logger.Errorf("gwcore: currentConns went negative (%d) after kick %s — resetting to 0", n, connID)
		g.currentConns.Store(0)
	}
	// 同理：cleanup 不会再执行，会话关闭指标必须在这里记录，
	// 否则 clover_gateway_connections 对被踢下线的连接只增不减。
	metricSessionClosed()
	owner := g.getOwnerOf(connID)
	// 同步清理 idIndex，避免 NATS 推送路由到已踢会话。
	if owner != "" {
		ownerKey := idPrefixAccount + owner
		is := g.idShardOf(ownerKey)
		is.mu.Lock()
		if sessions, exist := is.idIndex[ownerKey]; exist {
			// 从切片中移除匹配的会话（支持多连接索引）。
			for i, cur := range sessions {
				if cur.connID == connID {
					sessions = append(sessions[:i], sessions[i+1:]...)
					sessions[len(sessions)-1] = nil // 清尾：避免底层数组残留被踢会话引用
					is.idIndex[ownerKey] = sessions
					if len(sessions) == 0 {
						delete(is.idIndex, ownerKey)
					}
					break
				}
			}
		}
		is.mu.Unlock()
	}
	// 同步清理额外索引 key（playerID 等）：本函数已把会话摘出分片表，
	// 后续 cleanup 会在 !ok 早返回、不再走到 removeExtraIDs，
	// 故这里必须自行摘除，否则 "p:" 索引残留成僵尸条目。
	g.removeExtraIDs(sess)
	// 同步撤销该会话的 UDP 绑定令牌及其登记端点：cleanup 同样会因 !ok 早返回，
	// 令牌/端点必须在此回收，否则残留继续占用 udpEndpoints 或被攻击者继续复用。
	g.revokeSessionUDP(sess)
	// 清理 connOwner 反查表。
	g.connOwnerLock.Lock()
	delete(g.connOwner, connID)
	g.connOwnerLock.Unlock()
	g.kick(sess, owner)
	return true
}

// kick 强制关闭会话。标记 kicked=true，后续 cleanup 跳过仍在线 owner 的断线/重连事件。
func (g *Gateway) kick(sess *Session, owner string) {
	sess.kicked.Store(true)
	if sess.upstream != nil {
		_ = sess.upstream.Close()
	}
	if err := sess.bc.Close(); err != nil {
		logger.Warnf("gwcore: kick close conn %s: %v", sess.connID, err)
	}
	if g.onKick != nil {
		g.onKick(sess.connID, owner)
	}
}

// getOwnerOf 从 connOwner 反查表 O(1) 查出 session 绑定的 owner。
// 经 connOwner 反查表 O(1) 查询。
func (g *Gateway) getOwnerOf(connID string) string {
	g.connOwnerLock.RLock()
	defer g.connOwnerLock.RUnlock()
	return g.connOwner[connID]
}

// setOwnerOf 记录 connID → owner 反查（bind 路径调用）。
func (g *Gateway) setOwnerOf(connID, owner string) {
	g.connOwnerLock.Lock()
	g.connOwner[connID] = owner
	g.connOwnerLock.Unlock()
}

// delOwnerOf 移除 connID → owner 反查（unbind/cleanup 路径调用）。
func (g *Gateway) delOwnerOf(connID string) {
	g.connOwnerLock.Lock()
	delete(g.connOwner, connID)
	g.connOwnerLock.Unlock()
}

// Stop 停止网关并释放资源（幂等：重复调用直接返回，不重复清理/回调）。
// 等待所有后台 goroutine（cleanup/relay/reconSweep）完成后才退出。
func (g *Gateway) Stop() {
	g.stopOnce.Do(g.stop)
}

// stop Stop 的实际清理主体，仅由 stopOnce 调用一次。
func (g *Gateway) stop() {
	if g.reconCh != nil {
		close(g.reconCh)
	}
	// 停机门先置位：此后 createSession/tryQueue 拒绝登记新协程，
	// 保证下面的 wg.Wait 不会漏等「Wait 开始后才 Add」的协程。
	g.stopping.Store(true)
	// 先停止队列：阻止新连接入队，并取消队中待重入的定时器。
	if g.queue != nil {
		g.queue.stop()
	}
	// 关闭服务器，触发所有活跃连接断开 → 每个 session 的 ClosedCh 可读 →
	// cleanup goroutine 开始执行。必须在 wg.Wait 之前，否则 Wait 将永远阻塞。
	if g.tcpSrv != nil {
		_ = g.tcpSrv.Stop()
	}
	if g.wsSrv != nil {
		_ = g.wsSrv.Stop()
	}
	if g.wtSrv != nil {
		_ = g.wtSrv.Stop()
	}
	// 先停 QUIC（其 listener.Close 会触发共享 demux 关闭），再停裸 UDP 消费，最后兜底关闭 demux。
	if g.quicSrv != nil {
		_ = g.quicSrv.Stop()
	}
	if g.udpSrv != nil {
		_ = g.udpSrv.Stop()
	}
	if g.udpDemux != nil {
		_ = g.udpDemux.Close()
	}
	// 等待所有后台 goroutine 退出（cleanup/relay/reconSweep 均在 wg 内追踪）。
	// 与登记侧（createSession/tryQueue 的 wg.Add）共用同一临界区：
	// Wait 开始后不可能再有新的 Add（否则 Wait 会提前返回、漏等新协程）。
	g.lifecycleMu.Lock()
	g.wg.Wait()
	g.lifecycleMu.Unlock()
	// 取消所有待触发的硬掉线定时器：停机后残留 AfterFunc 仍会向上层（可能已关闭）
	// 触发 OnDisconnect 回调。
	g.dgMu.Lock()
	for connID, hg := range g.pendingHardDisconnects {
		hg.timer.Stop()
		hg.ver = atomic.AddUint64(&g.hardDisconnectSeq, 1)
		delete(g.pendingHardDisconnects, connID)
	}
	g.dgMu.Unlock()
	// 注意：NATS 客户端由调用方通过 WithNATS 注入并拥有生命周期，gwcore 不负责关闭，
	// 否则会误关调用方共享的连接（如逻辑服与网关共用同一 NATS 时）。
	// 最后触发调用方挂靠的停机回调（如 WebTransport 证书轮换器 Stop）。
	if g.onStop != nil {
		g.onStop()
	}
}

// Addr 返回网关实际监听地址：TCP 启用时返回 TCP 地址，否则返回 WS 地址（含系统分配端口）。
// TCP 与 WS 均未启用时返回空串（避免对 nil 服务端解引用）。
func (g *Gateway) Addr() string {
	if g.tcpSrv != nil {
		return g.tcpSrv.Addr()
	}
	if g.wsSrv != nil {
		return g.wsSrv.Addr()
	}
	return ""
}

// QUICAddr 返回 QUIC 监听地址（与裸 UDP 共享 UDP 端口；未启用时返回空串）。
func (g *Gateway) QUICAddr() string {
	if g.udpSrv != nil {
		return g.cfg.UDPListen
	}
	return ""
}

// WTAddr 返回 WebTransport 监听地址（复用 WS 端口号；未启用时返回空串）。
func (g *Gateway) WTAddr() string {
	if g.wtSrv != nil {
		return g.wtSrv.Addr()
	}
	return ""
}
