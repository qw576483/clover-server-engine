// Package event 是 clover 通用「事件 + 事件驱动逻辑服内核」的合集：
// - 事件模型与进程内总线（Envelope / Bus / NewBus / New*Event / With*）；
// - 事件驱动逻辑服内核（Logic / On / OnEvent / OnHTTP / Ctx / New）。
//
// 设计目标：业务逻辑只绑定事件，框架负责派发。这与经典游戏服的
//
// pKernel->AddIntCustomHook("player", MSG_ID, Handler) // 绑定客户端消息
// pKernel->AddEventCallback("role", "OnCreateRole", Handler) // 绑定领域事件
//
// 完全对齐——业务代码不感知网络 / 账号 / 数据库，只写"收到某消息 / 某事件后做什么"。
//
// 本内核提供两条绑定入口：
// - On(msgID, handler) 绑定"客户端消息号 → 处理函数"；
// - OnEvent(name, handler) 绑定"领域事件名 → 处理函数"（如创建角色后触发初始化）。
//
// 处理函数统一签名：func(c *Ctx) error。Ctx 由框架在派发前就绪：
// - c.MsgID() / c.Body() / c.BindMsg(&req)：取请求；
// - g.Reply(c, v)：回包给客户端；
// - g.SendEventToPlayer(ctx, playerID, name, payload)：向指定玩家投递事件（自动跨节点路由）；
// - c.Context()：底层 context（框架已注入玩家对象等）。
//
// 框架在派发前会调用 ContextEnricher（默认：若信封带 Owner 则注入 ctx.Owner），
// 业务借此拿到"已识别的对象标识"；基于此可进一步加载角色并注入 ctx，
// 于是 handler 内 c.Context() 直接携带玩家对象（见 clover-a 装配层）。
//
// 同一 package 内总线层与逻辑层共享若干同名概念，故总线层类型加 Bus 前缀：
// - 总线处理器 BusHandler（func(ctx, Envelope) error）与本内核 Handler（func(*Ctx) error）区分；
// - 总线信封修饰 EventOption（func(*Envelope)）与本内核 Option（func(*Logic)）区分。
package event

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/shared/proto"
	net_http "clover-server-engine/internal/transport/net/http"
	"clover-server-engine/internal/transport/net/tcp"
	"clover-server-engine/pkg/foundation/logger"
	ujson "clover-server-engine/pkg/shared/json"
	"clover-server-engine/pkg/shared/traceid"
)

// 默认错误回包 opcode（与 clover-a 的 EReplyError 一致）；真身在 proto.EMsgError。
const defaultErrorMsgID = proto.EMsgError

// encodeErrorReply 把 handler / 前置钩子返回的 error 编码为 EErrorReply 回包体。
// 返回 *proto.BizError（或其包装）时携带其业务错误码，客户端据此做统一处理
// （如 401 未认证 → 回到登录流程），无需匹配错误文案；普通 error 落 ErrCodeNone。
func encodeErrorReply(err error) []byte {
	return mustEncode(proto.EErrorReply{Err: err.Error(), Code: proto.ErrorCodeOf(err)})
}

// Handler 事件驱动业务处理函数。统一签名：拿到 Ctx 做逻辑，用 Game.Reply 回包，返回 error。
// 返回非 nil 时框架自动以 EErrorReply 回包；返回 nil 且未调用 MarkReplied 则不回包（fire-and-forget）。
type Handler func(c *Ctx) error

// replySlot 单次 dispatch 的回包暂存槽（由 Logic 创建并持有，Ctx 仅握指针引用）。
type replySlot struct {
	mu      sync.Mutex
	id      uint32
	body    []byte
	replied bool
	noAuto  bool // 抑制框架自动回包（handler 报错时 force() 也不覆盖）
	noPush  bool // 关闭数据变更自动推送（commitEdits 不推）
}

// mark 普通回包：按 requestID 配对，不携带业务消息号（id 恒为 0）。
// 带 nil receiver 守卫：Ctx 未经 dispatch 构造（测试/业务自建）时 reply 为 nil。
func (s *replySlot) mark(body []byte) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replied {
		logger.Warnf("logic: replySlot.mark called after already replied (id=%d), ignoring", s.id)
		return
	}
	s.replied = true
	s.body = body
}

func (s *replySlot) force(msgID uint32, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// force 用于错误路径，必须无视 noAuto 确保错误包能回传给客户端；
	// noAuto 仅抑制无回包时的自动补包（mark），不抑制显式错误回包。
	// 错误回包必须覆盖已发的成功包：handler 已 g.Reply 成功后再 commitEdits
	// 落库失败时，若此处因 replied 已置位而返回，客户端会收到成功包却已丢数据。
	// 因此 force 无条件覆盖（错误优先），保证错误包一定能回传。
	s.replied = true
	s.id = msgID
	s.body = body
}

// setNoAuto / getNoAuto / setNoPush / getNoPush 持锁读写标志位，
// 避免 handler 内并发 goroutine 与 mark/force 竞态读写同一字段。
// 均带 nil receiver 守卫：Ctx 未经 dispatch 构造（测试/业务自建）时 reply 为 nil，
// 直接解引用会 panic（与 Ctx.Context() 的 nil 回退风格一致）。
func (s *replySlot) setNoAuto() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.noAuto = true
	s.mu.Unlock()
}

func (s *replySlot) getNoAuto() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.noAuto
}

func (s *replySlot) setNoPush() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.noPush = true
	s.mu.Unlock()
}

func (s *replySlot) getNoPush() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.noPush
}

// snapshot 持锁原子读取回包结果，避免与并发 mark/force 撕裂读（replied/id/body 不一致）。
func (s *replySlot) snapshot() (replied bool, id uint32, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replied, s.id, s.body
}

// dispatchBag 单次 dispatch 的框架基础设施注入（由 Logic 创建，Ctx 仅握指针引用）。
type dispatchBag struct {
	session     *data.Session  // 数据会话（identity map + pending）
	syncPub     data.Publisher // NATS 下行发布器
	syncSubject string         // NATS 下行 subject
	syncReg     *SyncRegistry  // 同步消息号注册表
	ctlPub      data.Publisher // 网关控制指令发布器
	ctlSubject  string         // 网关控制指令 subject
}

// ContextEnricher 派发前改造 ctx 的钩子（注入玩家对象、trace 等）。
// 入参为最小 ctx 与原始信封；返回供 handler 使用的 ctx；返回 nil 则退化为原 ctx。
type ContextEnricher func(baseCtx context.Context, pkt *proto.GWLogicPacket) context.Context

// Option 逻辑服内核可选参数。
type Option func(*Logic)

// WithEnricher 设置派发前 ctx 注入钩子。
func WithEnricher(e ContextEnricher) Option { return func(l *Logic) { l.enricher = e } }

// WithBus 注入事件总线（用于 Push / Emit 的发布）。不传则内部新建进程内同步总线。
func WithBus(b Bus) Option { return func(l *Logic) { l.bus = b } }

// WithStore 注入数据句柄，支撑 Game.LoadStruct / Game.LoadRecord 的「可修改加载 + 自动落库/推送」。
func WithStore(s *data.Store) Option { return func(l *Logic) { l.store = s } }

// WithSyncDownlink 注入系统下行链路，支撑数据加载的「系统自动同步」：
// handler 成功返回后，框架按 key.Type 查 syncReg，自动经 pub/subject 广播变更给本玩家
// （data-event 自动同步），业务无需感知消息号。pub 为 nil 时不广播（如零依赖本地运行）。
func WithSyncDownlink(pub data.Publisher, subject string, reg *SyncRegistry) Option {
	return func(l *Logic) { l.syncPub, l.syncSubject, l.syncReg = pub, subject, reg }
}

// SetSyncDownlink 运行时（如 NATS 连接建立后）更新系统下行链路与注册表。
// 用于 NewGame 时 pub 尚不可用、待 bootstrap 完成再注入的场景。
func (l *Logic) SetSyncDownlink(pub data.Publisher, subject string, reg *SyncRegistry) {
	l.syncPub, l.syncSubject, l.syncReg = pub, subject, reg
}

// SetMirrorDownlink 运行时注入跨服内存镜像链路。
// commitEdits 成功后，框架自动将变更数据的全量 JSON 经 pub/subject 广播，
// 所有订阅节点收到后调用 Store.ReceiveMirror 直接写入进程内存（不标脏）。
func (l *Logic) SetMirrorDownlink(pub data.Publisher, subject string) {
	l.mirrorPub, l.mirrorSubject = pub, subject
}

// SetNotifyDownlink 运行时更新通用通知下行链路（NATS 延迟就绪场景）。
func (l *Logic) SetNotifyDownlink(pub data.Publisher, subject string) {
	l.notifyPub, l.notifySubject = pub, subject
}

// SetControlDownlink 注入网关控制指令下行链路（NATS 延迟就绪场景）。
// 逻辑服 SetPlayerID 时经此 pub/subject 向网关发送 GWControlBind，
// 通知网关追加 idIndex[PlayerID] 索引（双 key 架构：account + playerID 共存）。
func (l *Logic) SetControlDownlink(pub data.Publisher, subject string) {
	l.ctlPub, l.ctlSubject = pub, subject
}

// PublishGWControl 向网关发布控制指令（如 SwitchUpstream 连接迁移）。
// 业务层不需要知道 NATS 存在，Game.SwitchUpstream 封装此方法。
func (l *Logic) PublishGWControl(v any) error {
	if l.ctlPub == nil || l.ctlSubject == "" {
		return fmt.Errorf("event: control downlink not set")
	}
	b, err := ujson.Marshal(v)
	if err != nil {
		return fmt.Errorf("event: marshal gw control: %w", err)
	}
	return l.ctlPub.Publish(l.ctlSubject, b)
}

// WithErrorMsgID 设置错误回包 opcode（默认 0xFFFFFFFF）。
func WithErrorMsgID(id uint32) Option { return func(l *Logic) { l.errMsgID = id } }

// WithHeartbeat 设置心跳间隔（默认 30s）。
func WithHeartbeat(d time.Duration) Option { return func(l *Logic) { l.heartbeat = d } }

// WithHTTPAuth 设置 HTTP 控制面鉴权函数（如 token/API key/IP 白名单）。
// 未设置且 HTTPAuthDisabled=false 时，控制面默认拒绝所有请求（安全优先）。
func WithHTTPAuth(fn func(r *http.Request) bool) Option {
	return func(l *Logic) { l.httpAuth = fn }
}

// Config 逻辑服派发内核配置。
type Config struct {
	ListenAddr   string        // 监听地址（网关拨号此地址，裸 TCP 上行链路）
	HTTPListen   string        // HTTP 控制面监听地址（GMT / 本地后台用，无状态请求-响应式）；空 = 不启用
	Heartbeat    time.Duration // 网关↔逻辑服心跳间隔；0=默认30s
	FrameTimeout time.Duration // 单帧处理上限；0=默认30s
	// HTTPTLSCertFile / HTTPTLSKeyFile HTTP 控制面的证书对；两者都配置时以 HTTPS 提供服务。
	// 本层不做「是否必须 TLS」的策略判断（那是角色配置的事，如账号服要求 TLS）：
	// 只把证书透传给传输层，策略在 internal/app 的角色装配处校验。
	HTTPTLSCertFile string
	HTTPTLSKeyFile  string
	// HTTPAuthFunc HTTP 控制面鉴权函数；返回 true 放行。
	// 安全默认：若未设置且 HTTPAuthDisabled=false，serveHTTP 将拒绝所有请求（401）。
	HTTPAuthFunc func(r *http.Request) bool
	// HTTPAuthDisabled 仅开发/本地调试使用；生产务必保持 false，否则控制面完全无鉴权。
	HTTPAuthDisabled bool
	// BeforeDispatch 消息派发前钩子：每条消息在进入业务 handler 之前执行。
	// 返回 error 拒绝派发，错误信息回包给客户端；nil=放行。未设置时默认放行。
	//
	// 用途是「连接/会话级横切逻辑」。引擎 app 层当前在此按序执行：
	// 停机检查 → 灰度下线时把连接就地迁到新进程 → 恢复连接级 KV 与 playerID
	// → 已认证连接的 session token 续期 → 跨服玩家懒注册。
	//
	// 注意区分职责边界：消息级限流在网关层（gwcore.WithRateLimitManager）完成，
	// 角色权限与消息体格式校验由业务在各 handler 内自行处理，均不在本钩子内。
	//
	// 本钩子每条消息都会执行，实现必须保持轻量（引擎内部刻意走无锁路径）。
	BeforeDispatch func(c *Ctx) error
	// Headless 无头模式：不启动 TCP 监听器，仅保留事件处理（OnMsg/OnEvent/Bus）+ 定时器 + store 能力。
	// 适用于 Master 端嵌入 Logic 内核、单元测试等无需网关直连的场景。
	Headless bool
}

// Logic 事件驱动逻辑服内核实例。
type Logic struct {
	cfg              Config
	errMsgID         uint32
	heartbeat        time.Duration
	frameTimeout     time.Duration
	srv              *tcp.Server
	httpSrv          *net_http.Server
	bus              Bus
	store            *data.Store
	syncPub          data.Publisher // 系统下行发布器（NATS→网关→客户端）；nil=不广播
	syncSubject      string         // 系统下行推送 subject
	syncReg          *SyncRegistry  // 系统级 Type→syncMsgID 注册表
	notifyPub        data.Publisher // 通用通知下行发布器（NATS→网关→客户端），用于 c.Alert/c.Push 等主动推送
	notifySubject    string         // 通用通知下行推送 subject
	ctlPub           data.Publisher // 网关控制指令发布器（NATS→网关），用于 SetPlayerID 双 key 索引追加
	ctlSubject       string         // 网关控制指令推送 subject
	mirrorPub        data.Publisher // 跨服内存镜像发布器（NATS→其他节点），commitEdits 后广播全量数据
	mirrorSubject    string         // 跨服内存镜像 subject
	enricher         ContextEnricher
	msgHandlers      map[uint32][]msgHandlerEntry
	evHandlers       map[string][]Handler
	httpRoutes       []httpRoute
	httpAuth         func(r *http.Request) bool
	httpAuthDisabled bool
	mu               sync.RWMutex
}

// New 构造事件驱动逻辑服内核（尚未注册任何业务 handler）。可传入 Option。
func New(cfg Config, opts ...Option) *Logic {
	hb := cfg.Heartbeat
	if hb <= 0 {
		hb = 30 * time.Second
	}
	ft := cfg.FrameTimeout
	if ft <= 0 {
		ft = 30 * time.Second
	}
	l := &Logic{
		cfg:              cfg,
		errMsgID:         defaultErrorMsgID,
		heartbeat:        hb,
		frameTimeout:     ft,
		bus:              NewBus(),
		msgHandlers:      make(map[uint32][]msgHandlerEntry),
		evHandlers:       make(map[string][]Handler),
		httpRoutes:       []httpRoute{},
		httpAuthDisabled: cfg.HTTPAuthDisabled, // 仅开发/本地调试；生产务必保持 false
	}
	for _, o := range opts {
		o(l)
	}
	if l.bus == nil {
		l.bus = NewBus()
	}
	return l
}

// msgHandlerEntry 带优先级的消息 handler 条目。
type msgHandlerEntry struct {
	h        Handler
	priority int
}

// OnMsg 绑定"客户端消息号 → 处理函数"。同一 msgID 可绑多个 handler，按优先级（降序）依次执行，
// 同优先级保持注册顺序。priority 可选，不传默认 0。
// 同 opcode 同 handler 函数指针重复注册时自动跳过（幂等）。
//
// 仅接受业务消息号（> InternalMsgMax），引擎内部保留号（≤ InternalMsgMax）由 InternalOnMsg 注册。
func (l *Logic) OnMsg(msgID uint32, h Handler, priority ...int) {
	// 业务不能通过 Logic.OnMsg 直接注册引擎保留号（≤ InternalMsgMax），
	// 防止覆盖 EMsgLogin 等内部 handler。
	if msgID <= proto.InternalMsgMax {
		panic(fmt.Sprintf("logic: msgID %d is reserved for engine (≤ %d), use business range (> %d)", msgID, proto.InternalMsgMax, proto.InternalMsgMax))
	}
	l.InternalOnMsg(msgID, h, priority...)
}

// InternalOnMsg 供引擎内部包注册保留消息号的 handler（≤ InternalMsgMax），不检查边界。
// 业务代码必须通过 Game.OnMsg / MasterGame.OnMsg 注册，不应直接调用本方法。
func (l *Logic) InternalOnMsg(msgID uint32, h Handler, priority ...int) {
	p := 0
	if len(priority) > 0 {
		p = priority[0]
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// 检测重复：相同 msgID + handler + priority 已存在则跳过。
	for _, e := range l.msgHandlers[msgID] {
		if handlerEqual(e.h, h) && e.priority == p {
			return
		}
	}
	l.msgHandlers[msgID] = append(l.msgHandlers[msgID], msgHandlerEntry{h: h, priority: p})
	// 注册时按优先级降序预排序，避免每次 dispatch 重复排序。
	sort.SliceStable(l.msgHandlers[msgID], func(i, j int) bool {
		return l.msgHandlers[msgID][i].priority > l.msgHandlers[msgID][j].priority
	})
}

// handlerEqual 函数指针等值比较，用于重复注册检测。
// 使用 reflect.ValueOf 避免 fmt.Sprintf 的字符串分配开销。
func handlerEqual(a, b Handler) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

// OnEvent 绑定"领域事件名 → 处理函数"（如 "player.OnCreateRole"）。
func (l *Logic) OnEvent(typ string, h Handler) {
	l.mu.Lock()
	for _, existing := range l.evHandlers[typ] {
		if handlerEqual(existing, h) {
			l.mu.Unlock()
			return
		}
	}
	l.evHandlers[typ] = append(l.evHandlers[typ], h)
	l.mu.Unlock()
}

// OnHTTP 注册一条「HTTP 事件」路由（无状态请求-响应式，无会话上下文）。
// pattern 支持 ":name" 路径捕获段，如 "/admin/player/:pid" 匹配 "/admin/player/123"
// 且 handler 内 c.Param("pid") == "123"。空 HTTPListen 配置下，HTTP 控制面不启动，
// 此处的注册不会生效（也不报错，便于逻辑服按需启用）。
func (l *Logic) OnHTTP(pattern string, h HTTPHandler) {
	l.mu.Lock()
	l.httpRoutes = append(l.httpRoutes, httpRoute{pattern: pattern, h: h})
	l.mu.Unlock()
}

// Bus 返回内核持有的事件总线（供 app 层转发 / 发布）。
func (l *Logic) Bus() Bus { return l.bus }

// EmitEvent 同步派发领域事件给 OnEvent 订阅者。
// parent 为调用方 Ctx——引擎从 parent 原样克隆 playerID/account/connID/connBag 至每个 handler，
// 确保事件 handler 内 c.PlayerID()/c.Account()/c.ConnID()/c.Push 完整可用。
// parent 为 nil 时构造最小 Ctx（跨节点事件路径）。
// 返回聚合 error：任一 handler 失败都会向上传播。
func (l *Logic) EmitEvent(typ string, parent *Ctx, payload any) error {
	l.mu.RLock()
	hs := append([]Handler(nil), l.evHandlers[typ]...)
	l.mu.RUnlock()

	// 预分配 errs 容量，避免 append 时多次扩容。
	errs := make([]error, 0, len(hs))
	for _, h := range hs {
		cc := l.eventCtx(parent, payload)
		if err := runHandlerRecover(typ, h, cc); err != nil {
			logger.Errorf("logic: event %s handler: %v", typ, err)
			errs = append(errs, fmt.Errorf("event %s: %w", typ, err))
			// handler 失败时不提交该 handler 的 session 修改，避免落脏数据
			continue
		}
		l.commitEdits(cc)
	}
	if len(errs) > 0 {
		return fmt.Errorf("logic: %d event handler(s) failed: %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// runHandlerRecover 执行 handler 并捕获 panic，防止单个订阅者击垮后续 handler。
// panic 被记录（含堆栈）并转为 error 向上传播——不带堆栈的 panic 日志无法定位行号。
func runHandlerRecover(typ string, h Handler, c *Ctx) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("logic: %s handler panic: %v\n%s", typ, r, debug.Stack())
			err = fmt.Errorf("%s: handler panic: %v", typ, r)
		}
	}()
	return h(c)
}

// eventCtx 从 parent Ctx 原样克隆事件 handler 上下文，拷贝 playerID/account/connID/connBag。
// parent 为 nil 时构造最小 Ctx（背景 context + 空标识字段）。
func (l *Logic) eventCtx(parent *Ctx, payload any) *Ctx {
	var sess *data.Session
	if l.store != nil {
		sess = data.NewSession(l.store)
	}
	cc := &Ctx{
		payload: payload,
		reply:   &replySlot{},
		bag: &dispatchBag{
			session:     sess,
			syncPub:     l.syncPub,
			syncSubject: l.syncSubject,
			syncReg:     l.syncReg,
			ctlPub:      l.ctlPub,
			ctlSubject:  l.ctlSubject,
		},
	}
	if parent != nil {
		cc.ctx = parent.ctx
		cc.account = parent.account
		cc.playerID = parent.playerID
		cc.msgID = parent.msgID
		cc.connID = parent.connID
		cc.connBag = parent.connBag
		// 拷贝 requestID：否则事件 handler 内 RequestID() 恒 0，
		// 无法串联到触发它的请求链路。
		cc.requestID = parent.requestID
	} else {
		cc.ctx = context.Background()
	}
	// 标记当前处于同步事件 handler 执行期，供跨节点发送侧检测环形等待。
	cc.ctx = withCrossNodeEventInProgress(cc.ctx)
	return cc
}

// Start 启动监听（后台运行，不阻塞）。同时启动 TCP 逻辑服与 HTTP 控制面（若配置）。
// headless 模式下跳过 TCP 监听，仅启动 HTTP 控制面（若配置）。
func (l *Logic) Start() error {
	if !l.cfg.Headless {
		l.srv = tcp.NewServer(tcp.ServerConfig{
			ListenAddr:        l.cfg.ListenAddr,
			HeartbeatInterval: l.heartbeat,
		}, l.onFrame)
		if err := l.srv.Start(); err != nil {
			return err
		}
	}
	if l.cfg.HTTPListen != "" {
		// HTTP 控制面套 Recovery：业务 OnHTTP panic 时返回 500，不拖垮 HTTP 服务。
		handler := httpRecovery(http.HandlerFunc(l.serveHTTP))
		l.httpSrv = net_http.NewServer(net_http.Config{
			Addr: l.cfg.HTTPListen,
			TLS: net_http.TLSConfig{
				CertFile: l.cfg.HTTPTLSCertFile,
				KeyFile:  l.cfg.HTTPTLSKeyFile,
			},
		}, handler.ServeHTTP)
		if err := l.httpSrv.Start(); err != nil {
			return err
		}
	}
	return nil
}

// Stop 停止逻辑服（TCP + HTTP 控制面）。
func (l *Logic) Stop() {
	if l.srv != nil {
		_ = l.srv.Stop()
	}
	if l.httpSrv != nil {
		_ = l.httpSrv.Stop()
	}
	// 关闭事件总线，置 closed 后 Publish 静默丢弃、清空订阅，
	// 避免异步总线（WithAsync）在关停后仍有在途派发泄漏 goroutine / 触碰已释放资源。
	if l.bus != nil {
		l.bus.Close()
	}
}

// Addr 返回实际监听地址（含系统分配端口）。
func (l *Logic) Addr() string {
	if l.srv != nil {
		return l.srv.Addr()
	}
	return ""
}

// HTTPAddr 返回 HTTP 控制面实际监听地址（未启用则空串）。
func (l *Logic) HTTPAddr() string {
	if l.httpSrv != nil {
		return l.httpSrv.Addr()
	}
	return ""
}

// serveHTTP HTTP 控制面总入口：按路径匹配 OnHTTP 注册的路由，构造 HTTPCtx 派发。
// 无匹配路由返回 404；handler 返回 error 以 500 + {"error":...} 回包；未回包则不回包。
// 内置 /healthz（存活）和 /ready（就绪）仅在没有业务注册路由时才自动响应。
func (l *Logic) serveHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	l.mu.RLock()
	routes := append([]httpRoute(nil), l.httpRoutes...)
	l.mu.RUnlock()
	for _, rt := range routes {
		if params, ok := matchRoute(rt.pattern, path); ok {
			// HTTP 控制面鉴权：默认拒绝无鉴权配置的请求（安全优先）。
			if !l.httpAuthDisabled && l.httpAuth == nil {
				logger.Warnf("logic: HTTP control plane has no auth configured; rejecting %s %s", r.Method, path)
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			if l.httpAuth != nil && !l.httpAuth(r) {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			if l.httpAuthDisabled {
				logger.Warnf("logic: HTTP control plane auth DISABLED - DO NOT use in production")
			}
			cc := &HTTPCtx{w: w, r: r, params: params}
			if err := rt.h(cc); err != nil {
				logger.Errorf("logic: http route %s: %v", rt.pattern, err)
				// 不回内部错误原文（可能泄露路径/实现细节）：对外只回通用错误，细节留在日志。
				cc.ReplyRaw(http.StatusInternalServerError, errorBody("internal error"))
			}
			return
		}
	}

	// 无用户注册路由匹配，回退到内置健康检查端点
	switch path {
	case "/healthz":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	case "/ready":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
		return
	}
	http.NotFound(w, r)
}

// httpRecovery 捕获 panic 并返回 500，防止进程崩溃。
// 日志走项目 logger（而非标准库 log）：否则不进日志系统、无落盘与审计轨迹。
func httpRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Errorf("logic: http control plane panic: %v\n%s", rec, debug.Stack())
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// unknownMsgIDs 已告警过的未知消息号集合（msgID → struct{}）。
//
// 必须按消息号分桶：早先用单一全局计数器时，「第二个及以后出现的新 msgID」的首条告警
// 会被吞掉（要等全局计数恰好撞上 1000 的倍数才留痕），而异常/恶意客户端可以构造任意
// 消息号 —— 排查时表现为「日志里从来没出现过这个号」。
//
// 登记表有容量上限：海量不同 msgID 不得把这张表撑爆。
var (
	unknownMsgIDs     sync.Map // msgID(uint32) → struct{}
	unknownMsgIDKinds atomic.Uint64
	unknownMsgIDLogs  atomic.Uint64 // 种类数超上限后的整体降频计数
)

// unknownMsgIDKindLimit 登记表的容量上限；超过后只做整体降频，不再逐号登记。
const unknownMsgIDKindLimit = 1024

// logUnknownMsgID 未知消息号告警：每个消息号的首次出现必打一条；
// 消息号种类超过上限后转为整体降频（每 1000 次一条），避免逐帧刷屏。
func logUnknownMsgID(msgID uint32) {
	if _, ok := unknownMsgIDs.Load(msgID); !ok {
		// 仅在前 limit 个不同消息号上登记，防止异常客户端用海量 msgID 撑爆这张表。
		if unknownMsgIDKinds.Load() < unknownMsgIDKindLimit {
			unknownMsgIDs.Store(msgID, struct{}{})
			unknownMsgIDKinds.Add(1)
			logger.Warnf("logic: no handler for msgID %d（该消息号首次出现）", msgID)
			return
		}
	}
	if n := unknownMsgIDLogs.Add(1); n%1000 == 0 {
		logger.Warnf("logic: no handler for msgID %d（同类累计 %d 次，已降频输出）", msgID, n)
	}
}

// onFrame 每条网关 → 逻辑服连接的帧入口：解信封、注入 ctx、按 msgID 派发、回包。
func (l *Logic) onFrame(c *tcp.Conn, raw []byte) {
	pkt, err := proto.DecodeGWLogicPacket(raw)
	if err != nil {
		logger.Errorf("logic: decode gw packet: %v", err)
		return
	}

	// 每帧请求使用带超时的 context：与连接生命周期解耦，避免 handler 永久占用连接；
	// 超时后 handler 应经 ctx.Err() 尽快退出（逻辑服回包超时由网关侧兜底）。
	baseCtx, cancel := context.WithTimeout(context.Background(), l.frameTimeout)
	defer cancel()
	if l.enricher != nil {
		if enriched := l.enricher(baseCtx, pkt); enriched != nil {
			baseCtx = enriched
		}
	} else if pkt.Owner != "" {
		baseCtx = proto.WithOwner(baseCtx, pkt.Owner)
	}

	reply := l.dispatch(pkt, baseCtx)
	if reply == nil {
		return
	}
	reply.TraceID = pkt.TraceID // 回包携带同一 TraceID，客户端侧可串联
	buf, err := proto.EncodeGWLogicPacket(reply)
	if err != nil {
		logger.Errorf("logic: encode reply: %v", err)
		return
	}
	if err := c.Send(buf); err != nil {
		logger.Errorf("logic: send reply: %v", err)
	}
}

// Dispatch 解信封并派发（供测试 / 非网络场景直接调用，等价于 onFrame 的后半段）。
// 不带 baseCtx 时以 context.Background() 为底；返回的回包信封为 nil 表示无回包。
func (l *Logic) Dispatch(pkt *proto.GWLogicPacket, baseCtx ...context.Context) *proto.GWLogicPacket {
	ctx := context.Background()
	if len(baseCtx) > 0 && baseCtx[0] != nil {
		ctx = baseCtx[0]
	}
	return l.dispatch(pkt, ctx)
}

// HasHandler 判断是否已为指定 msgID 注册 handler（供 master Dispatch 区分无 handler 错误）。
func (l *Logic) HasHandler(msgID uint32) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.msgHandlers[msgID]) > 0
}

// dispatch 解信封后派发到 On(msgID) handler，返回回包信封（无回包则 nil）。
func (l *Logic) dispatch(pkt *proto.GWLogicPacket, baseCtx context.Context) *proto.GWLogicPacket {
	// 全链路追踪：若信封携带 TraceID（网关生成）且当前 ctx 尚未挂载 span，
	// 则继承该 trace 派生本帧 span。该逻辑对 onFrame（网关上行）与 Dispatch（直调）路径通用。
	if pkt.TraceID != "" && traceid.FromContext(baseCtx) == nil {
		span := traceid.StartNATSSpan(baseCtx, "logic.dispatch", map[string]string{"X-Trace-ID": pkt.TraceID})
		baseCtx = span.WithContext(baseCtx)
		// span 在本次派发结束时收尾：不 End 则耗时指标与上报钩子永不触发。
		defer span.End()
	}

	l.mu.RLock()
	entries := append([]msgHandlerEntry(nil), l.msgHandlers[pkt.MsgID]...)
	l.mu.RUnlock()
	if len(entries) == 0 {
		logUnknownMsgID(pkt.MsgID)
		return nil
	}
	// entries 已在 InternalOnMsg 注册时按优先级降序预排序，此处无需再次排序。

	slot := &replySlot{}
	bag := &dispatchBag{
		session:     data.NewSession(l.store),
		syncPub:     l.syncPub,
		syncSubject: l.syncSubject,
		syncReg:     l.syncReg,
		ctlPub:      l.ctlPub,
		ctlSubject:  l.ctlSubject,
	}
	cc := &Ctx{
		ctx:       baseCtx,
		account:   pkt.Owner,
		requestID: pkt.RequestID,
		msgID:     pkt.MsgID,
		connID:    pkt.ConnID,
		body:      pkt.Body,
		line:      pkt.Line,
		reply:     slot,
		bag:       bag,
	}
	// BeforeDispatch 连接/会话级横切钩子（停机检查 / 灰度下线迁移 / 连接 KV 与身份恢复 / token 续期 / 跨服注册）；
	// 限流在网关层，权限与消息格式校验在各业务 handler 内，均不在此处。
	if l.cfg.BeforeDispatch != nil {
		if err := l.cfg.BeforeDispatch(cc); err != nil {
			logger.Warnf("logic: msgID %d rejected by BeforeDispatch: %v", pkt.MsgID, err)
			// 拒绝回包携带同一 TraceID，避免被拒请求的链路追踪断裂。
			return &proto.GWLogicPacket{ConnID: pkt.ConnID, RequestID: pkt.RequestID, MsgID: l.errMsgID, Body: encodeErrorReply(err), TraceID: pkt.TraceID, Line: pkt.Line}
		}
	}
	var herr error
	for _, e := range entries {
		if err := runHandlerRecover(fmt.Sprintf("msgID %d", pkt.MsgID), e.h, cc); err != nil {
			// 记录首个错误但不 break：同一 msgID 可绑多个 handler（按优先级降序），
			// 遇错中断会跳过后续 handler，与 EmitEvent 的全量执行语义不一致。
			// 首个错误作为本次请求的最终错误回包，其余 handler 仍继续执行。
			if herr == nil {
				herr = err
			}
		}
	}
	// 无论链中某个 handler 是否失败，都提交已累积的可修改加载。
	// commitEdits 仅提交 handler 显式经 Game.LoadStruct / Game.LoadRecord 载入并改动的项，
	// 因此失败 handler 中断后提交剩余 pending 是安全的（尽力持久化）。
	saveFailed := l.commitEdits(cc)
	if herr != nil {
		logger.Warnf("logic: handler msgID %d: %v", pkt.MsgID, herr)
		slot.force(l.errMsgID, encodeErrorReply(herr))
	} else if saveFailed > 0 {
		// handler 已成功执行业务逻辑（返回 nil），但 commitEdits 中部分数据落库失败——
		// 客户端收到成功回包后会认为数据已持久化，但实际上已丢失，造成客户端与服务器状态
		// 认知不一致。此时必须用错误回包替代业务回包，让客户端明确感知并重试。
		logger.Errorf("logic: msgID %d handler succeeded but %d edits failed to persist", pkt.MsgID, saveFailed)
		slot.force(l.errMsgID, mustEncode(proto.EErrorReply{Err: "数据保存失败，请重试", Code: proto.ErrCodeInternal}))
	}
	replied, replyID, replyBody := slot.snapshot()
	if !replied {
		return nil
	}
	// 回包携带同一 TraceID 和 RequestID，客户端侧可串联全链路并按 requestID 匹配。
	return &proto.GWLogicPacket{ConnID: pkt.ConnID, RequestID: pkt.RequestID, MsgID: replyID, Body: replyBody, TraceID: pkt.TraceID, Line: pkt.Line}
}
