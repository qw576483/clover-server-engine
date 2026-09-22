package gwcore

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	iconn "github.com/qw576483/clover-server-engine/internal/transport/gateway/conn"
	"github.com/qw576483/clover-server-engine/internal/transport/nats"
	"github.com/qw576483/clover-server-engine/internal/transport/net/demux"
	"github.com/qw576483/clover-server-engine/internal/transport/net/quic"
	isession "github.com/qw576483/clover-server-engine/internal/transport/net/session"
	"github.com/qw576483/clover-server-engine/internal/transport/net/tcp"
	"github.com/qw576483/clover-server-engine/internal/transport/net/udp"
	"github.com/qw576483/clover-server-engine/internal/transport/net/ws"
	"github.com/qw576483/clover-server-engine/internal/transport/net/wt"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"

	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/foundation/metrics"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
	"github.com/qw576483/clover-server-engine/pkg/shared/traceid"

	"sync/atomic"
)

// unauthRejectLogCount 未鉴权拒绝的**日志降频**计数。
//
// 为什么需要：会话失效/未登录的客户端可能在重连期间持续高频重发业务消息
// （一次服务端重启后客户端仍以 20Hz 重发移动帧，刷出 3262 条同因日志）。
// 这类"同一原因反复发生"的日志必须降频（首次 + 每 1000 次一条），
// 精确次数由 metricRejected(reasonUnauth) 指标承担，不依赖日志计数。
var unauthRejectLogCount atomic.Uint64

// unauthReplyFailLogCount 未鉴权回包发送失败的**日志降频**计数（理由同上：同一原因高频重复）。
var unauthReplyFailLogCount atomic.Uint64

// udpBindRejectLogCount 无效 UDP 绑定令牌的**日志降频**计数（首次 + 每 1000 次）。
// 裸 UDP 无握手、来源可伪造，该分支可被高频触发（含跨网关误发的保活帧）。
var udpBindRejectLogCount atomic.Uint64

// udpBindLimitLogCount UDP 绑定令牌来源数超限的**日志降频**计数（首次 + 每 1000 次）。
var udpBindLimitLogCount atomic.Uint64

// maxUDPEndpointsPerToken 单个 UDP 绑定令牌可登记的来源地址数量上限。
// 裸 UDP 无握手、源地址可伪造：不封顶时持有效令牌者可用大量伪造来源使
// udpBindByTok[token].addrs 与 udpEndpoints[owner] 在会话存续期内无界膨胀。
// 8 个足够覆盖 NAT 换址 / 多网卡切换等正常场景。
const maxUDPEndpointsPerToken = 8

// udpBindTokenFallbackSeq crypto/rand 不可用时降级令牌的进程内单调序号（与纳秒时间拼满 16 字节）。
var udpBindTokenFallbackSeq atomic.Uint64

// idIndex key 前缀：避免 account 与 playerID 值冲突（例如 account="player_123" 与 playerID="player_123"）。
const (
	idPrefixAccount = "a:" // idIndex["a:user1"] → account 维度
	idPrefixPlayer  = "p:" // idIndex["p:player_123"] → playerID 维度
)

// 客户端帧入口（TCP / WS / UDP / QUIC / WebTransport 共用）
func (g *Gateway) onTCPFrame(c *tcp.Conn, data []byte)   { g.handleClient(c, data) }
func (g *Gateway) onWSFrame(c *ws.Conn, data []byte)     { g.handleClient(c, data) }
func (g *Gateway) onQUICFrame(c *quic.Conn, data []byte) { g.handleClient(c, data) }
func (g *Gateway) onWTFrame(c *wt.Conn, data []byte)     { g.handleClient(c, data) }

// onUDPFrame 处理一条裸 UDP 帧。0x55 魔数已在 demux 层剥掉，data 直接是客户端帧
// [4B requestID][4B msgID][body]。先拦截引擎级专用帧 EMsgBindUDP（UDP 端点绑定），
// 其余再进通用客户端帧流程（转发逻辑服）。
func (g *Gateway) onUDPFrame(c *udp.Conn, data []byte) {
	requestID, msgID, body, err := proto.DecodeClientFrame(data)
	if err == nil && msgID == proto.EMsgBindUDP {
		g.onUDPEndpointBind(c, body)
		_ = requestID // 绑定帧无回包，requestID 保留 0
		return
	}
	g.handleClient(c, data)
}

// onUDPEndpointBind 处理 EMsgBindUDP 帧：body 为网关下发的绑定令牌（EMsgUDPBindGrant）。
// 校验令牌有效（udpTokens 命中）后，把【本 UDP 包的来源地址】登记为令牌所属 owner 的
// 不可靠推送端点。同一令牌重绑 = 覆盖/并入对应 addrKey 的最新来源；会话断开 / 换绑
// 令牌被撤销时，按令牌精确回收其登记的全部端点，不影响同 owner 其他在线端。
func (g *Gateway) onUDPEndpointBind(c *udp.Conn, body []byte) {
	token := strings.TrimSpace(string(body))
	if token == "" {
		logger.Warnf("gwcore: udp bind empty token ignored")
		return
	}
	a := c.Addr()
	key := a.String()
	g.udpEndpointsMu.Lock()
	owner, ok := g.udpTokens[token]
	if !ok {
		g.udpEndpointsMu.Unlock()
		// 令牌不在本进程，两种可能：① 已过期 / 被撤销；② 客户端把 UDP 包发到了
		// **另一个网关进程**（令牌表是进程本地的）。多网关部署下后者最常见，
		// 排查方向是 LB 的源亲和配置（同一玩家的 TCP 与 UDP 必须落同一网关）。
		// 不打印 token 本体（鉴权凭证，与有效令牌路径刻意不打 token 保持一致）；只打长度辅助判型。
		// 本分支可被高频触发 → 降频：首次 + 每 1000 次。
		if n := udpBindRejectLogCount.Add(1); n == 1 || n%1000 == 0 {
			logger.Warnf("gwcore: udp bind invalid token ignored (len=%d, from %s)（同类累计 %d 次，已降频输出）", len(token), key, n)
		}
		return
	}
	set := g.udpEndpoints[owner]
	if set == nil {
		set = make(map[string]net.Addr)
		g.udpEndpoints[owner] = set
	}
	// 客户端每 10s 重发绑定帧做 NAT 保活：同一来源地址的重复上报静默返回，
	// 仅在端点首次绑定或地址变化时才打印，避免保活帧刷屏。
	if prev, exists := set[key]; exists && prev.String() == a.String() {
		g.udpEndpointsMu.Unlock()
		return
	}
	// 按令牌记录登记的端点，供会话断开/换绑时精确回收（udpEndpoints[owner] 聚合多个令牌）。
	// 单令牌来源数封顶（见 maxUDPEndpointsPerToken 注释）：同一来源的保活/换址不占新额度。
	bind := g.udpBindByTok[token]
	_, known := bind.addrs[key]
	if !known && len(bind.addrs) >= maxUDPEndpointsPerToken {
		g.udpEndpointsMu.Unlock()
		if n := udpBindLimitLogCount.Add(1); n == 1 || n%1000 == 0 {
			logger.Warnf("gwcore: udp bind: token endpoint limit %d reached, ignoring new endpoint from %s（同类累计 %d 次，已降频输出）", maxUDPEndpointsPerToken, key, n)
		}
		return
	}
	if bind.addrs == nil {
		bind = udpBind{owner: owner, addrs: make(map[string]net.Addr)}
	}
	bind.addrs[key] = a
	g.udpBindByTok[token] = bind
	set[key] = a
	g.udpEndpointsMu.Unlock()
	logger.Infof("gwcore: bound udp endpoint %s -> owner %s (token)", key, owner)
}

// udpEndpointsOf 返回某 owner 绑定的全部裸 UDP 端点（无则返回 nil）。
func (g *Gateway) udpEndpointsOf(owner string) []net.Addr {
	g.udpEndpointsMu.RLock()
	defer g.udpEndpointsMu.RUnlock()
	set := g.udpEndpoints[owner]
	if len(set) == 0 {
		return nil
	}
	out := make([]net.Addr, 0, len(set))
	for _, a := range set {
		out = append(out, a)
	}
	return out
}

// delUDPBinding 撤销一条 UDP 绑定令牌及其登记的全部端点（会话断开 / 换绑 / 被踢时调用）。
// 按令牌精确回收：只删该令牌登记的 addrKey，同一 owner 其他在线端（各持独立令牌）
// 的端点不受影响；令牌随之作废，此后携带该令牌的 EMsgBindUDP 帧一律被拒。
func (g *Gateway) delUDPBinding(token string) {
	g.udpEndpointsMu.Lock()
	defer g.udpEndpointsMu.Unlock()
	if bind, ok := g.udpBindByTok[token]; ok {
		if set := g.udpEndpoints[bind.owner]; len(set) > 0 {
			for key := range bind.addrs {
				delete(set, key)
			}
			if len(set) == 0 {
				delete(g.udpEndpoints, bind.owner)
			}
		}
		delete(g.udpBindByTok, token)
	}
	delete(g.udpTokens, token)
}

// revokeSessionUDP 撤销会话持有的 UDP 绑定令牌及其登记端点（cleanup / Kick 共用）。
// 持 bindMu 读取令牌后置空，再在锁外撤销——避免持 bindMu 期间操作 udpEndpointsMu。
func (g *Gateway) revokeSessionUDP(s *Session) {
	s.bindMu.Lock()
	token := s.udpToken
	s.udpToken = ""
	s.bindMu.Unlock()
	if token != "" {
		g.delUDPBinding(token)
	}
}

// newUDPBindToken 生成会话唯一的随机绑定令牌（16 字节随机数十六进制编码）。
// crypto/rand 失败是极端异常：退化为纳秒时间源序列（仍不可猜测）并记录告警，
// 避免令牌为空导致后续 EMsgBindUDP 鉴权直接失效。
func newUDPBindToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 极端异常（系统熵源不可用）：降级为「纳秒时间 + 进程内单调序号」填满全部 16 字节。
		// 此前用 %024x 拼接再 copy：24 字符串只前 16 字符进 b，前 8 字节恒为 '0' 的 ASCII（0x30），
		// 实际熵仅 ~32bit 且随时间可预测，与「仍不可猜测」的注释不符。
		// 降级值只保证进程内唯一/不可枚举性下降，如实记录并明确「非密码学随机」。
		logger.Errorf("gwcore: crypto/rand failed for udp bind token, fallback to time+seq seed (NOT cryptographically random): %v", err)
		binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
		binary.BigEndian.PutUint64(b[8:], udpBindTokenFallbackSeq.Add(1))
	}
	return hex.EncodeToString(b[:])
}

// grantUDPToken 登记绑定令牌所属 owner，供 EMsgBindUDP 鉴权。调用方需持 bindMu（保护令牌生命周期）。
func (g *Gateway) grantUDPToken(token, owner string) {
	g.udpEndpointsMu.Lock()
	g.udpTokens[token] = owner
	g.udpEndpointsMu.Unlock()
}

// sendUDPBindGrant 向会话下发一次性 UDP 绑定令牌帧（EMsgUDPBindGrant，引擎 S2C 帧，
// requestID=0）。若会话已启用加密则加密后发送（与登录回包后下行一致），保证令牌
// 不被链路嗅探。发送在 bindMu 临界区外进行（不在锁内做网络 I/O）。
// shouldRejectUnauth 判断一条上行消息是否应被登录门禁拒绝。
//
// 抽成独立方法以便单测：forwardFrame 依赖真实连接与上游，无法在单测里构造。
// 以下三条同时成立才拒绝（默认拒绝 / fail-safe）：
//  1. 门禁开启（AuthDisabled=false，即默认）；
//  2. 连接未绑定对象标识（尚未登录）；
//  3. 消息号不在免登录白名单内。
func (g *Gateway) shouldRejectUnauth(bound bool, msgID uint32) bool {
	if g.cfg.AuthDisabled || bound {
		return false
	}
	_, exempt := g.authExempt[msgID]
	return !exempt
}

// replyUnauthenticated 向未登录连接回一条带错误码(401)的错误回包，并丢弃本帧。
//
// 回包按 requestID 配对（与逻辑服错误回包同一约定），客户端会以
// CloverCallException(code=401) 结束该次请求，并触发 Net.OnUnauthorized 事件——
// 于是「回到登录流程」的统一处理在网关侧就完成了，无需业务逐个 handler 判断。
//
// 已协商加密的会话同样加密下发：明文/密文混发会被客户端解密逻辑判为异常。
func (g *Gateway) replyUnauthenticated(s *Session, requestID uint32) {
	body, err := ujson.Marshal(proto.EErrorReply{
		Err:  "unauthenticated: please sign up / log in first",
		Code: proto.ErrCodeUnauthenticated,
	})
	if err != nil {
		logger.Errorf("gwcore: encode unauthenticated reply: %v", err)
		return
	}
	frame := proto.EncodeClientFrame(requestID, proto.EMsgError, body)
	if crypto := s.getCrypto(); crypto != nil {
		enc, encErr := crypto.Encrypt(frame)
		if encErr != nil {
			logger.Errorf("gwcore: encrypt unauthenticated reply for %s: %v", s.connID, encErr)
			return
		}
		frame = enc
	}
	s.sendMu.Lock()
	err = s.bc.Send(frame)
	s.sendMu.Unlock()
	if err != nil {
		// 同样是高频路径：连接已关（或对端已 RST）时，每条未鉴权帧的"回包"都会失败，
		// 不降频会以收帧速率刷日志（一次刷了 747 条）。降频：首次 + 每 1000 次。
		if n := unauthReplyFailLogCount.Add(1); n == 1 || n%1000 == 0 {
			logger.Warnf("gwcore: send unauthenticated reply to %s: %v（同类累计 %d 次，已降频输出）", s.connID, err, n)
		}
	}
}

func (g *Gateway) sendUDPBindGrant(s *Session, token string) {
	frame := proto.EncodeClientFrame(0, proto.EMsgUDPBindGrant, []byte(token))
	if crypto := s.getCrypto(); crypto != nil {
		enc, err := crypto.Encrypt(frame)
		if err != nil {
			logger.Errorf("gwcore: encrypt udp bind grant for %s: %v", s.connID, err)
			return
		}
		frame = enc
	}
	s.sendMu.Lock()
	err := s.bc.Send(frame)
	s.sendMu.Unlock()
	if err != nil {
		logger.Warnf("gwcore: send udp bind grant to %s: %v (token remains valid, client may rebind later)", s.connID, err)
		return
	}
	metricSentFrame(len(frame))
	logger.Infof("gwcore: granted udp bind token for conn %s", s.connID)
}

// handleClient 处理一条来自客户端的帧：先做准入控制（限流 / 排队 / 容量），
// 通过后解客户端信封 → 封装为网关↔逻辑服内部信封 → 转发到该会话的专用上游连接。
func (g *Gateway) handleClient(raw isession.Session, payload []byte) {
	connID := raw.ConnID()

	// 上行帧计数放在最前：统计的是「客户端真实发来多少」，
	// 放到校验之后会漏掉被丢弃的帧，掩盖攻击流量。
	metricRecvFrame(len(payload))
	start := time.Now()
	defer metricFrameDuration(metrics.DirIn, start)

	// 边缘输入校验：超长客户端帧直接丢弃（DoS / 畸形包防护），不进入准入与转发流程。
	if g.cfg.MaxFrameSize > 0 && len(payload) > g.cfg.MaxFrameSize {
		metricRejected(reasonFrameTooLarge)
		logger.Warnf("gwcore: conn %s frame too large (%d > %d) — dropped", connID, len(payload), g.cfg.MaxFrameSize)
		return
	}

	// 排队中：丢弃后续帧（首帧已缓冲，放行时重放），避免重复入队。
	if g.queue != nil && g.queue.contains(connID) {
		metricRejected(reasonQueuedDup)
		logger.Debugf("gwcore: conn %s queued — duplicate frame dropped", connID)
		return
	}

	status, sess := g.admit(connID, raw, payload)
	switch status {
	case admitReject:
		// 超限且未启用排队 / 队列已满：直接拒绝并关闭连接。
		metricRejected(reasonAdmission)
		logger.Warnf("gwcore: admission rejected for %s (rate/queue)", connID)
		_ = raw.Close()
		return
	case admitQueue:
		// 已入等候队列，等待 release 循环放行后重放首帧。
		return
	}
	g.forwardFrame(sess, raw, payload)
}

// admitStatus 准入结果。
type admitStatus int

const (
	admitOK     admitStatus = iota // 通过；已建 / 取回会话
	admitQueue                     // 受限流 / 满载拦截，已入等候队列
	admitReject                    // 拒绝（未启用排队或队列已满）
)

// getExisting 仅在会话表命中时返回已有会话（快路径，不拨号、不计数）。
func (g *Gateway) getExisting(connID string) *Session {
	ss := g.sessionShardOf(connID)
	ss.mu.RLock()
	s, ok := ss.sessions[connID]
	ss.mu.RUnlock()
	if ok {
		return s
	}
	return nil
}

// createSession 惰性拨号到逻辑服专用上游，并新建会话（仅在真正新建时累加 currentConns）。

// 上游拨号（网络 I/O）刻意放在分片读锁之外，避免把"建立连接"的耗时串行化到所有新连接上；
// 拨号完成后再以双检锁（double-checked locking）确认，若并发期间他人已建则弃用本次拨号。
// 会话表已按 connID 分片，每条连接仅锁定其所属分片，热点连接之间不再互相阻塞。
// 返回 isNew 标志。活跃计数由本函数真正落库的分支自增（与 cleanup/Kick 的 -1 严格配对），
// admit / releasePending 只做局部临时预占用于 MaxConns 超限扫描，成功后即归还、不参与计数。
func (g *Gateway) createSession(connID string, raw isession.Session) (*Session, bool) {
	ss := g.sessionShardOf(connID)
	ss.mu.RLock()
	if s, ok := ss.sessions[connID]; ok {
		ss.mu.RUnlock()
		return s, false
	}
	ss.mu.RUnlock()

	// 慢路径：在锁外拨号（网络 I/O 不阻塞其他连接建立）。
	// 读运行期上游而非 cfg.Upstream：灰度重启时 SetUpstream 会把新连接导向新版本进程。
	upAddr := g.Upstream()
	up, err := tcp.Dial(tcp.ClientConfig{
		Address:           upAddr,
		HeartbeatInterval: g.cfg.Heartbeat,
	}, g.makeUpstreamHandler(connID))
	if err != nil {
		metricUpstreamError(upstreamKindDial)
		logger.Errorf("gwcore: dial logic %s: %v", upAddr, err)
		return nil, false
	}

	// 登记 cleanup 协程的额度（wg.Add）与停机门在同一临界区完成（lifecycleMu），
	// 使 Add 与 Stop 的 wg.Wait 串行：否则停机时在途帧的本次登记可能落在 Wait 之后，
	// cleanup 永不被等待（WaitGroup 的 Add/Wait 竞态）。
	// 不持 ss.mu：cleanup 需要取会话分片锁，持锁等待会形成锁序反转。
	g.lifecycleMu.Lock()
	if g.stopping.Load() {
		g.lifecycleMu.Unlock()
		_ = up.Close()
		return nil, false
	}
	g.wg.Add(1)
	g.lifecycleMu.Unlock()

	// 双检锁：拨号/登记期间若他人已建好该会话，则弃用本次拨号，复用已有会话。
	ss.mu.Lock()
	if s, ok := ss.sessions[connID]; ok {
		ss.mu.Unlock()
		_ = up.Close()
		g.wg.Done() // 未启动 cleanup 协程：归还登记，保持 Add/Done 配对
		return s, false
	}
	s := &Session{bc: iconn.Wrap(raw), upstream: up, connID: connID}
	ss.sessions[connID] = s
	ss.mu.Unlock()
	// 活跃计数在当前真正落库的分支自增，与 cleanup/Kick 的递减严格一对一。
	// 注意：admit 层「预占」承担的只是 MaxConns 超限扫描的临时占位（成功即归还），
	// 真正的 +1 只能落在落库处——否则 MaxConns=0（无上限）时预占被跳过、会话漏计，
	// 连接关闭时 cleanup 会把这个计数扣成负数。
	g.currentConns.Add(1)
	metricSessionOpened()

	// 客户端断开后清理会话与上游连接。纳入 wg 追踪：Stop 时先关闭服务器
	// （触发连接断开 → ClosedCh 可读），再 wg.Wait 确保所有 cleanup 执行完毕。
	safe.GoSafe(func() {
		defer g.wg.Done()
		<-s.bc.ClosedCh()
		g.cleanup(connID)
	})
	return s, true
}

// admit 对一条新客户端连接做准入控制：限流（内置滑动窗口 + 可选注入限流器）→ 连接总数上限 →
// 通过则建 / 取会话；否则在启用排队时入队缓冲，否则拒绝。

// 注意：已存在会话（快路径）直接放行，不受限流 / 容量约束（其连接早已建立）。
func (g *Gateway) admit(connID string, raw isession.Session, payload []byte) (admitStatus, *Session) {
	// 快路径：会话已存在（同连接重发帧 / 已建会话）→ 直接转发，不受限流 / 容量约束
	if s := g.getExisting(connID); s != nil {
		return admitOK, s
	}
	// 速率限制（限流）
	if !g.rateAllow() {
		if g.tryQueue(connID, raw, payload) {
			return admitQueue, nil
		}
		return admitReject, nil
	}
	// 连接总数上限（负载感知）。
	// 用 Add(1) 临时预占代替 Load 读后检，消除竞态窗口，确保 MaxConns 不超；
	// 预占仅作超限扫描，无论建会话成败都归还（defer），真实计数由
	// createSession 落库处的 Add(1) 承担，与 cleanup/Kick 的 -1 严格配对。
	if g.cfg.MaxConns > 0 {
		if g.currentConns.Add(1) > int64(g.cfg.MaxConns) {
			g.currentConns.Add(-1) // 超出上限，回滚预占
			if g.tryQueue(connID, raw, payload) {
				return admitQueue, nil
			}
			return admitReject, nil
		}
		defer g.currentConns.Add(-1) // 归还超限判断的临时预占
	}
	s, _ := g.createSession(connID, raw)
	if s == nil {
		// 拨号失败（上游暂不可用）不应直接拒绝；尝试入队重试。
		// 与限流/满载场景一致：队列满或未启用时才拒绝。
		if g.tryQueue(connID, raw, payload) {
			return admitQueue, nil
		}
		return admitReject, nil
	}
	return admitOK, s
}

// rateAllow 综合判定速率限制：内置「每秒新建连接数」窗口 + 可选业务注入限流器（ratelimit 包）。
func (g *Gateway) rateAllow() bool {
	if g.connRateLimiter != nil && !g.connRateLimiter.Allow() {
		return false
	}
	if g.rateLimiter != nil && !g.rateLimiter.Allow() {
		return false
	}
	return true
}

// forwardFrame 把一条客户端帧解包并封装为内部信封，转发到该会话专用上游连接（原路回包）。
func (g *Gateway) forwardFrame(s *Session, raw isession.Session, payload []byte) {
	// 消息级限流：按连接 / 玩家多 key 限速（防御单连接刷包 / CC）。
	if g.rlManager != nil {
		key := raw.ConnID()
		if id, bound := s.bc.PlayerID(); bound {
			key = id
		}
		if !g.rlManager.Allow(key, g.rlPolicy) {
			metricRejected(reasonRateLimit)
			logger.Warnf("gwcore: conn %s rate limited (key=%s) — frame dropped", raw.ConnID(), key)
			return
		}
	}

	// 会话级解密：登录后所有上行帧经 AES-GCM 解密。
	// crypto 刚协商后客户端可能仍发送明文帧（handshake 窗口期）。
	// 此时解密失败不立即断开，尝试按明文处理，待首帧解密成功后 handshakeOK=true。
	if crypto := s.getCrypto(); crypto != nil {
		plain, err := crypto.Decrypt(payload)
		if err != nil {
			if s.handshakeOK.Load() {
				// 握手已完成，解密失败 = 密钥不一致/回放攻击 → 断开
				metricRejected(reasonDecrypt)
				logger.Warnf("gwcore: decrypt frame from %s failed — closing session: %v", raw.ConnID(), err)
				g.cleanup(raw.ConnID())
				return
			}
			// 握手未确认：解密失败按明文处理，不关闭连接
			logger.Debugf("gwcore: handshake plaintext from %s (decrypt skipped)", raw.ConnID())
		} else {
			payload = plain
			// 首帧解密成功 → 标记握手完成
			s.handshakeOK.Store(true)
		}
	}

	requestID, msgID, body, err := proto.DecodeClientFrame(payload)
	if err != nil {
		metricRejected(reasonDecode)
		logger.Warnf("gwcore: decode client frame from %s: %v", raw.ConnID(), err)
		return
	}

	// 丢弃传输层最小保活帧（[requestID][msgID=0][空 body]，如 WT 空闲保活）：
	// 它仅用于刷新服务端 idle 计时，不是业务消息，也不携带权限语义；
	// 若下发逻辑服，业务层未注册 msgID 0 handler 会持续打印 "no handler for msgID 0" 噪音。
	if msgID == 0 && len(body) == 0 {
		metricRejected(reasonKeepalive)
		return
	}

	// 登录门禁（默认拒绝）：未绑定对象标识的连接只放行免登录白名单，其余直接拒绝。
	//
	// 位置在「解码之后、转发之前」：既不让无效流量占用逻辑服资源（挡在门口），
	// 也不影响保活帧与 UDP 绑定帧（后者在 onUDPFrame 层单独处理，到不了这里）。
	//
	// 身份取自会话绑定——登录回包经 WithExtractOwnerID 提取 owner 后绑定到
	// bc.PlayerID，是网关自己维护的状态，客户端无法伪造；未登录连接该值为空。
	if _, bound := s.bc.PlayerID(); g.shouldRejectUnauth(bound, msgID) {
		metricRejected(reasonUnauth)
		if n := unauthRejectLogCount.Add(1); n == 1 || n%1000 == 0 {
			logger.Warnf("gwcore: conn %s msgID %d rejected - unauthenticated (owner unbound)（同类累计 %d 次，已降频输出；精确次数见 gateway rejected 指标）",
				raw.ConnID(), msgID, n)
		}
		g.replyUnauthenticated(s, requestID)
		return
	}

	// 封装为网关 ↔ 逻辑服内部信封，转发到专用上游连接。
	// 全链路追踪：每条上行帧生成 trace span，TraceID 注入信封，逻辑服继承并随回包回流。
	span := traceid.StartSpan(context.Background(), "gateway.recv")
	// span 必须显式结束：不 End 则 endFn 永不触发、Duration 失真（traceid 约定 defer span.End()）。
	defer span.End()

	// 根据连接类型设置线路标识
	line := "unknown"
	switch raw.(type) {
	case *tcp.Conn:
		line = "tcp"
	case *ws.Conn:
		line = "ws"
	case *udp.Conn:
		line = "udp"
	case *quic.Conn:
		line = "quic"
	case *wt.Conn:
		line = "wt"
	}

	pkt := proto.GWLogicPacket{ConnID: raw.ConnID(), RequestID: requestID, MsgID: msgID, Body: body, TraceID: span.TraceID(), Line: line}
	// 若该会话已登录（绑定了对象标识），在信封中携带，便于逻辑服识别对象、
	// 并由派发内核注入 ctx（业务 handler 直接取用，无需自行解析包体）。
	if id, bound := s.bc.PlayerID(); bound {
		pkt.Owner = id
	}
	buf, err := proto.EncodeGWLogicPacket(&pkt)
	if err != nil {
		metricRejected(reasonEncode)
		logger.Errorf("gwcore: encode gw packet: %v", err)
		return
	}
	s.sendMu.Lock()
	err = s.upstream.Send(buf)
	s.sendMu.Unlock()
	if err != nil {
		metricUpstreamError(upstreamKindForward)
		logger.Errorf("gwcore: forward to logic: %v", err)
		g.cleanup(raw.ConnID())
	}
}

// tryQueue 把受限流 / 满载拦截的连接放入等候队列（首帧一并缓冲，放行时重放）。
// 成功入队返回 true；队列未启用或已满返回 false。
// 入队成功后即向该连接下发排队位置（EMsgQueuePosition，「您前面还有 N 人」），
// 此后由队列的刷新循环在位置变化时续发（见 queue.go / queue_position.go）。
func (g *Gateway) tryQueue(connID string, raw isession.Session, payload []byte) bool {
	if g.queue == nil {
		return false
	}
	pc := &pendingConn{raw: raw, payload: append([]byte(nil), payload...), enqueueAt: time.Now()}
	ahead, total, ok := g.queue.offer(pc)
	if !ok {
		return false
	}
	// 客户端在排队期间主动断开 → 移出队列，避免空等超时。
	if cw, ok := raw.(interface{ ClosedCh() <-chan struct{} }); ok {
		// 监控协程的登记与停机门同临界区（理由同 createSession）：
		// 停机中不再登记；该队列条目由 queue.stop() 统一关闭。
		g.lifecycleMu.Lock()
		if g.stopping.Load() {
			g.lifecycleMu.Unlock()
			return true
		}
		g.wg.Add(1)
		g.lifecycleMu.Unlock()
		safe.GoSafe(func() {
			defer g.wg.Done()
			<-cw.ClosedCh()
			if g.queue.remove(pc) {
				// 队列条目已摘除：排队超时定时器必须一并停止，否则它会保持 armed
				// 直到 QueueTimeout 触发（届时 remove 已返回 false，空转），期间还持有 pc/raw 引用。
				if pc.timer != nil {
					pc.timer.Stop()
				}
				logger.Warnf("gwcore: queued conn %s closed by client before release", connID)
			}
		})
	}
	// 排队超时 → 仍未放行则关闭连接。
	if g.cfg.QueueTimeout > 0 {
		pc.timer = time.AfterFunc(g.cfg.QueueTimeout, func() {
			if g.queue.remove(pc) {
				logger.Warnf("gwcore: queued conn %s timed out after %v", connID, g.cfg.QueueTimeout)
				_ = raw.Close()
			}
		})
	}
	// 位置通知放在最后：断开监听与排队超时定时器均已就位，ahead/total 是入队那一刻的快照
	// （offer 在同一临界区内算出），此后由刷新循环按位置变化续发。
	g.notifyQueued(pc, ahead, total)
	return true
}

// releasePending 等候队列放行回调：尝试为挂起连接建会话并重放首帧。
// 仍满载 / 上游暂不可用时放回队首，等待下次 tick（受 QueueReleasePerSec 节流）。
// 放回队首时若 QueueTimeout>0，重新设置超时定时器，避免连接永久滞留队列。
func (g *Gateway) releasePending(pc *pendingConn) {
	if pc.timer != nil {
		pc.timer.Stop()
	}
	// 与 admit 一致：Add(1) 预占仅作 MaxConns 超限扫描，成功后归还（defer）；
	// 真实计数由 createSession 落库处自增，避免队列放行的连接漏占导致计数失真。
	if g.cfg.MaxConns > 0 {
		if g.currentConns.Add(1) > int64(g.cfg.MaxConns) {
			g.currentConns.Add(-1)
			g.requeuePendingWithTimeout(pc)
			return
		}
		defer g.currentConns.Add(-1) // 归还超限判断的临时预占
	}
	s, _ := g.createSession(pc.raw.ConnID(), pc.raw)
	if s == nil {
		g.requeuePendingWithTimeout(pc)
		return
	}
	g.forwardFrame(s, pc.raw, pc.payload)
}

// requeuePendingWithTimeout 把挂起连接放回队首，并重新设置排队超时定时器（若 QueueTimeout>0）。
// releasePending 已停止旧定时器，重新入队后必须重新设时，否则连接可能永久滞留队列。
func (g *Gateway) requeuePendingWithTimeout(pc *pendingConn) {
	g.queue.requeueHead(pc)
	if g.cfg.QueueTimeout > 0 {
		pc.timer = time.AfterFunc(g.cfg.QueueTimeout, func() {
			if g.queue.remove(pc) {
				logger.Warnf("gwcore: requeued conn %s timed out after %v", pc.raw.ConnID(), g.cfg.QueueTimeout)
				_ = pc.raw.Close()
			}
		})
	}
}

// selectKickTargets 按互踢策略从既有会话切片中选出应被踢下线的会话（排除本次会话 self）。
// sessions 为该 owner 当前 idIndex 中的既有连接（不含本次），self 为本次登录会话。
// - KickOldest：踢最早在线的一个（切片头部，单端在线语义，默认）。
// - KickNewest：踢最近一次登录的一个（切片尾部）。
// - KickAll：踢除本次外的所有在线连接。
func selectKickTargets(sessions []*Session, self *Session, strategy KickStrategy) []*Session {
	// 收集除 self 外的既有会话，保持原有顺序（越靠前越早登录）。
	others := make([]*Session, 0, len(sessions))
	for _, cur := range sessions {
		if cur != nil && cur != self {
			others = append(others, cur)
		}
	}
	if len(others) == 0 {
		return nil
	}
	switch strategy {
	case KickAll:
		return others
	case KickNewest:
		return others[len(others)-1:]
	default: // KickOldest
		return others[:1]
	}
}

// makeUpstreamHandler 构造「逻辑服 → 网关」回包处理：把内部信封还原为客户端帧下发，
// 并在登录回包时提取目标标识绑定会话、启用会话加密。
func (g *Gateway) makeUpstreamHandler(connID string) tcp.Handler {
	return func(_ *tcp.Conn, data []byte) {
		pkt, err := proto.DecodeGWLogicPacket(data)
		if err != nil {
			logger.Errorf("gwcore: decode logic packet: %v", err)
			return
		}
		inner := proto.EncodeClientFrame(pkt.RequestID, pkt.MsgID, pkt.Body)

		// 仅本分片读锁查找会话，不阻塞其他分片的帧处理。
		ss := g.sessionShardOf(connID)

		// 会话可能在两段锁之间被 cleanup 摘除并 Close；重检必须重新取出 `s` 并据其发送，
		// 否则仍会向已被清理并替换的旧会话 s.bc.Send。单次 RLock 内取出即用，消除竞态窗口。
		ss.mu.RLock()
		s, ok := ss.sessions[connID]
		ss.mu.RUnlock()
		if !ok {
			return
		}

		// 先提取 crypto key（在发送前读取回包内容，不影响发送）。
		// 不限制 crypto==nil，支持 key 轮转 —— 重复登录回包若携带新 key 仍应更新。
		var cryptoKey []byte
		if g.extractCrypto != nil {
			cryptoKey, _ = g.extractCrypto(pkt.MsgID, pkt.Body)
		}

		// 下发客户端帧：若会话已启用加密则加密后发送。
		// 特殊：携带 crypto key 的登录回包自身以明文发送（客户端凭此 key 解密后续帧）。
		// NATS 多 subject 回调 / 直连 reply 可能并发写 bc，使用 sendMu 串行化。
		frame := inner
		if crypto := s.getCrypto(); crypto != nil {
			enc, err := crypto.Encrypt(inner)
			if err != nil {
				logger.Errorf("gwcore: encrypt reply for %s: %v", connID, err)
				return
			}
			frame = enc
		}
		outStart := time.Now()
		s.sendMu.Lock()
		err = s.bc.Send(frame)
		s.sendMu.Unlock()
		metricFrameDuration(metrics.DirOut, outStart)
		if err == nil {
			// 只统计真正写出去的帧；失败的帧计入 upstream/连接错误而非下行吞吐。
			metricSentFrame(len(frame))
		}
		if err != nil {
			// 区分「连接关闭」（硬错误，应中止）与「发送超时」（软错误，连接仍存活）。
			// 软错误时仍继续绑定操作以免 owner/crypto 丢失导致后续推送路由失败。
			if errors.Is(err, isession.ErrClosed) {
				logger.Errorf("gwcore: push to client %s: conn closed", connID)
				return
			}
			logger.Warnf("gwcore: push to client %s: %v (soft error, continuing bind)", connID, err)
		}

		// 登录回包携带了会话密钥 → 在此之后启用该会话的加解密。
		if cryptoKey != nil {
			if codec, err := isession.NewAESGCMCodec(cryptoKey); err == nil {
				s.setCrypto(codec)
				logger.Infof("gwcore: crypto enabled for conn %s", connID)
			} else {
				logger.Errorf("gwcore: crypto init for conn %s: %v", connID, err)
			}
		}

		// 提取目标标识（如登录回包提取 UID）→ 绑定并建索引供 NATS 推送；
		// 绑定时刻即「上线 / 重连」系统事件触发点。
		if g.extract != nil {
			if id, ok2 := g.extract(pkt.MsgID, pkt.Body); ok2 && id != "" {
				// 换绑期间对同一会话串行化。并发两条不同 owner 登录回包时，
				// 解绑旧 owner → 追加新 owner → BindPlayer 必须整体原子，避免 idIndex 与
				// bc.PlayerID() 交错致状态不一致。
				s.bindMu.Lock()
				old, ob := s.bc.PlayerID()
				if ob && old == id {
					// 同一会话、同一 owner 的重复登录回包：绑定无变化，不触发 connect 事件。
					s.bindMu.Unlock()
					return
				}
				// 解绑旧 uid（若同会话换绑到不同 owner）：单独锁旧 id 分片，避免与新建锁嵌套造成死锁。
				if ob && old != id {
					oldKey := idPrefixAccount + old
					isOld := g.idShardOf(oldKey)
					isOld.mu.Lock()
					if sessions, exist := isOld.idIndex[oldKey]; exist {
						for i, cur := range sessions {
							if cur == s {
								sessions = append(sessions[:i], sessions[i+1:]...)
								sessions[len(sessions)-1] = nil // 清尾：避免底层数组残留已解绑会话引用
								isOld.idIndex[oldKey] = sessions
								if len(sessions) == 0 {
									delete(isOld.idIndex, oldKey)
								}
								break
							}
						}
					}
					isOld.mu.Unlock()
				}
				newKey := idPrefixAccount + id
				isNew := g.idShardOf(newKey)
				isNew.mu.Lock()
				sessions, existed := isNew.idIndex[newKey]
				var kickTargets []*Session
				if existed && len(sessions) > 0 {
					kickTargets = selectKickTargets(sessions, s, g.cfg.KickStrategy)
				}
				// 多端登录互踢与「宽限期重连」是两回事：踢掉其他端的新登录仍是一次全新连接，
				// 不应上报 reconnect=true，否则还会跳过下面真正的宽限期重连判定。
				isReconnect := false
				// 注意：此处 BindPlayer 绑定的是「登录回包提取出的 owner（即 account）」，
				// 不是角色 playerID —— 角色 playerID 由后续 GWControlBind → addExtraID 以 "p:" 维度另行索引。
				s.bc.BindPlayer(id)
				isNew.idIndex[newKey] = append(sessions, s)
				isNew.mu.Unlock()
				// 维护 connID→owner 反查表（Kick/getOwnerOf O(1) 依赖此表；换绑时直接覆盖旧值）。
				g.setOwnerOf(connID, id)

				// UDP 绑定令牌协商：会话每次（重新）绑定 owner 都生成一次性随机令牌并登记
				// （udpTokens[token]=owner），客户端凭令牌（而非自报账号）上报 EMsgBindUDP
				// 端点，防止攻击者谎报他人账号劫持不可靠推送。换绑到不同 owner 时先撤销
				// 旧令牌（连带其登记的端点），本会话再持新令牌。
				revoked := ""
				if s.udpToken != "" {
					revoked = s.udpToken
					s.udpToken = ""
				}
				token := newUDPBindToken()
				s.udpToken = token
				g.grantUDPToken(token, id)
				s.bindMu.Unlock()
				if revoked != "" {
					g.delUDPBinding(revoked)
				}
				// 令牌帧（EMsgUDPBindGrant）在 bindMu 临界区外下发，不在锁内做网络 I/O。
				g.sendUDPBindGrant(s, token)

				// 多端登录互踢：同一 owner 已在其他连接在线，本次登录按策略将其旧会话强制踢下线。
				// （仅当旧会话与本次连接不同——同一连接重绑同一 owner 不算踢）
				for _, prev := range kickTargets {
					g.kick(prev, id)
				}

				// 断线宽限期内重连：取消待触发的硬掉线视为宽限重连（即 isReconnect=true），
				// 返回 true 表示确实取消了待触发硬掉线——此时与 reconnectGraces 判定互斥但语义一致。
				if g.cfg.DisconnectGrace > 0 {
					if g.cancelHardDisconnectByUID(id) {
						isReconnect = true
					}
				}

				// 重连宽限期检测：若该 owner 在宽限期内断开又重连，标记为宽限重连
				if !isReconnect && g.cfg.ReconnectGrace > 0 {
					g.reconnectMu.Lock()
					if graceExpiry, ok := g.reconnectGraces[id]; ok && time.Now().Before(graceExpiry) {
						isReconnect = true
						delete(g.reconnectGraces, id)
					}
					g.reconnectMu.Unlock()
				}

				logger.Infof("gwcore: bound conn %s -> %s (reconnect=%v)", connID, id, isReconnect)
				if g.onConnect != nil {
					g.onConnect(connID, id, isReconnect)
				}
			}
		}
	}
}

// onNotify 处理逻辑服经消息总线下发的异步推送（data-event 广播），按 Target 路由下发。
func (g *Gateway) onNotify(msg *nats.Msg) {
	p, err := proto.DecodeNotifyPush(msg.Data)
	if err != nil {
		logger.Errorf("gwcore: decode notify: %v", err)
		return
	}
	// 全服广播：Target=TargetAll(*) 时向全部在线会话下发（alert 的 AlertToAll 使用）。
	if p.Target == proto.TargetAll {
		g.broadcastAll(p.MsgID, p.Body, p.DeliveryMode)
		return
	}
	inner := proto.EncodeClientFrame(0, p.MsgID, p.Body) // requestID=0 表示推送
	for _, c := range g.router(p.Target) {
		frame := g.encryptFrameForConn(inner, c)
		if err := g.sendByDeliveryMode(c, frame, p.DeliveryMode); err != nil {
			logger.Errorf("gwcore: notify push target %s: %v", p.Target, err)
		}
	}
}

// sendByDeliveryMode 根据 DeliveryMode 选择可靠或不可靠发送。
func (g *Gateway) sendByDeliveryMode(c iconn.Conn, frame []byte, mode proto.DeliveryMode) error {
	switch mode {
	case proto.DeliveryModeReliable:
		return c.Send(frame)
	case proto.DeliveryModeBestEffort:
		// 尽力而为：优先使用不可靠通道，不支持则降级为可靠
		return g.sendUnreliable(c, frame)
	default:
		return c.Send(frame)
	}
}

// sendUnreliable 尽力而为下发一条帧：三级不可靠通道逐级降级。

//  1. 该连接所在玩家若绑定了常驻裸 UDP 端点（EMsgBindUDP 上报）→ 经共享 UDP 端口
//     打 raw UDP（首字节 0x55 魔数）。TCP/WS 玩家由此获得真正的不可靠推送能力：
//     逻辑服推位置等高频数据时不再被迫降级成可靠 TCP。
//  2. 连接自身的不可靠通道（QUIC/WT Datagram）。
//  3. 都不支持（纯 TCP/WS 且未绑定 UDP 端点）→ 降级可靠 Send，保证必达。
func (g *Gateway) sendUnreliable(c iconn.Conn, frame []byte) error {
	// 只在该连接所在会话绑定了 UDP 端点、且网关持有共享 UDP socket 时走裸 UDP。
	if s := g.getExisting(c.ConnID()); s != nil && g.udpDemux != nil {
		if uid, bound := s.bc.PlayerID(); bound {
			if addrs := g.udpEndpointsOf(uid); len(addrs) > 0 {
				payload := make([]byte, 0, 1+len(frame))
				payload = append(payload, demux.RawUDPMagic)
				payload = append(payload, frame...)
				sent := false
				for _, a := range addrs {
					if _, err := g.udpDemux.WriteTo(payload, a); err != nil {
						logger.Warnf("gwcore: udp unreliable push to %s: %v", a, err)
						continue
					}
					sent = true
				}
				if !sent {
					// 全部端点失败：不得上报「已发」、也不得返回成功——否则调用方无法感知推送丢失。
					return errors.New("gwcore: udp unreliable push failed on all endpoints")
				}
				metricSentFrame(len(frame))
				return nil
			}
		}
	}
	// 无 UDP 端点绑定：走连接自身的不可靠通道，不支持则降级可靠。
	if err := c.SendUnreliable(frame); err != nil {
		if errors.Is(err, isession.ErrUnreliableNotSupported) {
			return c.Send(frame)
		}
		return err
	}
	return nil
}

// broadcastAll 向全部在线会话下发一条帧（用于 TargetAll 全服广播）。

// 注意：先在各分片读锁下收集所有连接引用，释放锁后再在锁外逐个 Send（网络 I/O），
// 避免把「向海量连接写数据」的耗时串行化到会话分片锁上；分片锁仅短暂持有用于拷贝引用。
// 使用 goroutine pool 控制并发数，避免瞬间大量并发写入导致内存压力。
func (g *Gateway) broadcastAll(msgID uint32, body []byte, mode proto.DeliveryMode) {
	var conns []iconn.Conn
	for i := range g.sessionShards {
		ss := g.sessionShards[i]
		ss.mu.RLock()
		for _, s := range ss.sessions {
			conns = append(conns, s.bc)
		}
		ss.mu.RUnlock()
	}
	if len(conns) == 0 {
		return
	}
	inner := proto.EncodeClientFrame(0, msgID, body) // requestID=0 表示推送
	// 分批并发发送，每批最多 64 个连接
	const batchSize = 64
	for start := 0; start < len(conns); start += batchSize {
		end := start + batchSize
		if end > len(conns) {
			end = len(conns)
		}
		batch := conns[start:end]
		var wg sync.WaitGroup
		for _, c := range batch {
			wg.Add(1)
			// 走 safe.GoSafe：单个连接的 Send panic（如自定义 Session 实现）不得击垮整个进程。
			safe.GoSafe(func() {
				defer wg.Done()
				frame := g.encryptFrameForConn(inner, c)
				if err := g.sendByDeliveryMode(c, frame, mode); err != nil {
					logger.Errorf("gwcore: broadcast all: %v", err)
				}
			})
		}
		wg.Wait()
	}
}

// encryptFrameForConn 为指定连接加密帧（若该连接所在会话已启用加密）。
func (g *Gateway) encryptFrameForConn(frame []byte, c iconn.Conn) []byte {
	id := c.ConnID()
	if id == "" {
		return frame
	}
	ss := g.sessionShardOf(id)
	ss.mu.RLock()
	s, ok := ss.sessions[id]
	ss.mu.RUnlock()
	if !ok {
		return frame
	}
	cr := s.getCrypto()
	if cr == nil {
		return frame
	}
	enc, err := cr.Encrypt(frame)
	if err != nil {
		logger.Errorf("gwcore: encrypt broadcast for %s: %v", id, err)
		return frame
	}
	return enc
}

// defaultRouter 默认推送路由：按前缀键多推（支持同一 owner 多连接）。
// 依次尝试 playerID 键（"p:"+target）、account 键（"a:"+target），避免两个命名空间值碰撞。
func (g *Gateway) defaultRouter(target string) []iconn.Conn {
	for _, prefix := range []string{idPrefixPlayer, idPrefixAccount} {
		key := prefix + target
		is := g.idShardOf(key)
		is.mu.RLock()
		sessions, ok := is.idIndex[key]
		is.mu.RUnlock()
		if ok && len(sessions) > 0 {
			conns := make([]iconn.Conn, len(sessions))
			for i, s := range sessions {
				conns[i] = s.bc
			}
			return conns
		}
	}
	return nil
}

// onGWControl 处理逻辑服 → 网关控制指令。
// 当前支持两种指令：
//   - GWControlBind：双 key 索引追加（account + playerID 共存）。
//   - GWControlSwitchUpstream：切换指定会话的上游地址（跨节点全量迁移）。

// 逻辑服经 NATSSubjectGWControl 发布，网关按 json 字段自行鉴别指令类型。
func (g *Gateway) onGWControl(msg *nats.Msg) {
	// 先判踢人（kick 判别位）：本结构若不先解析，其 conn_id 会被 GWControlBind
	// 的 Unmarshal 静默接受（未知字段忽略），变成一条 PlayerID 为空的绑定请求。
	var kickReq proto.GWControlKick
	if json.Unmarshal(msg.Data, &kickReq) == nil && kickReq.Kick {
		if kickReq.ConnID == "" {
			logger.Errorf("gwcore: GWControlKick missing conn_id")
			return
		}
		if !g.Kick(kickReq.ConnID) {
			logger.Warnf("gwcore: GWControlKick unknown connID %s", kickReq.ConnID)
		}
		return
	}

	// 再尝试 SwitchUpstream（有 new_upstream 字段即为切换指令）。
	var switchReq proto.GWControlSwitchUpstream
	if json.Unmarshal(msg.Data, &switchReq) == nil && switchReq.NewUpstream != "" {
		if switchReq.ConnID == "" {
			logger.Errorf("gwcore: GWControlSwitchUpstream missing conn_id")
			return
		}
		if err := g.switchUpstream(switchReq.ConnID, switchReq.NewUpstream); err != nil {
			logger.Errorf("gwcore: switchUpstream(%s, %s): %v", switchReq.ConnID, switchReq.NewUpstream, err)
		}
		return
	}

	// 否则按 GWControlBind 解析。
	var bind proto.GWControlBind
	if err := json.Unmarshal(msg.Data, &bind); err != nil {
		logger.Errorf("gwcore: invalid GWControlBind: %v", err)
		return
	}
	if bind.ConnID == "" || bind.PlayerID == "" {
		logger.Errorf("gwcore: GWControlBind missing conn_id or player_id")
		return
	}
	s := g.sessionOf(bind.ConnID)
	if s == nil {
		logger.Warnf("gwcore: GWControlBind unknown connID %s", bind.ConnID)
		return
	}
	// 若该 playerID 已绑定在其他 session 上，则踢掉旧 session（同角色不可双开）。
	g.kickExtraIDConflict(bind.PlayerID, s)
	g.addExtraID(s, bind.PlayerID)
	logger.Debugf("gwcore: added extra idIndex key %s for conn %s", bind.PlayerID, bind.ConnID)
}

// switchUpstream 切换指定会话的上游地址：关闭旧的 TCP 上游连接，拨号到新地址并替换。
// 网关控制指令 GWControlSwitchUpstream 的处理入口，也是跨节点全量迁移的关键步骤。

// 先 dial 新连接，成功后再原子替换新旧 upstream——避免 dial 期间
// s.upstream 为 nil 导致 forwardFrame nil pointer panic，以及 dial 失败后
// 会话永久损坏无法恢复。
func (g *Gateway) switchUpstream(connID string, newAddr string) error {
	s := g.sessionOf(connID)
	if s == nil {
		logger.Warnf("gwcore: switchUpstream unknown connID %s", connID)
		return errors.New("gwcore: session not found")
	}

	// 先拨号新连接，dial 期间 s.upstream 保持旧值 → forwardFrame 不受影响
	newUp, err := tcp.Dial(tcp.ClientConfig{
		Address:           newAddr,
		HeartbeatInterval: g.cfg.Heartbeat,
	}, g.makeUpstreamHandler(connID))
	if err != nil {
		logger.Errorf("gwcore: switchUpstream dial %s for conn %s: %v", newAddr, connID, err)
		return err // dial 失败 → s.upstream 仍是旧值，会话不受影响
	}

	// dial 成功后原子替换：取出旧连接 → 放入新连接 → 关闭旧连接
	s.sendMu.Lock()
	oldUp := s.upstream
	s.upstream = newUp
	s.sendMu.Unlock()

	if oldUp != nil {
		_ = oldUp.Close()
	}

	logger.Infof("gwcore: switched upstream for conn %s → %s", connID, newAddr)
	return nil
}

// sessionOf 按 connID 查找 session；无锁只读，仅内部使用。
func (g *Gateway) sessionOf(connID string) *Session {
	ss := g.sessionShardOf(connID)
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.sessions[connID]
}

// addExtraID 为 session 设置 playerID 额外索引（双 key 架构的 "p:" 维度）。
// 内部自动加 idPrefixPlayer 前缀，避免与 account 键碰撞。

// 一个连接同一时刻只对应一个角色，故采用「替换」语义：若本会话已持有其它/相同的
// playerID 索引，先摘除旧条目再追加新条目——防止同一连接重复 SetPlayerID（或切换
// 角色 A→B）时在 idIndex/extraIDs 残留旧 playerID 僵尸索引。
func (g *Gateway) addExtraID(s *Session, key string) {
	pkey := idPrefixPlayer + key

	// 1) 在持 bindMu 期间收齐旧 playerID 索引 key，并从 extraIDs 剔除（仅保留新 pkey）。
	s.bindMu.Lock()
	var staleKeys []string
	filtered := s.extraIDs[:0]
	for _, ek := range s.extraIDs {
		if strings.HasPrefix(ek, idPrefixPlayer) {
			staleKeys = append(staleKeys, ek) // 旧 playerID 索引，待摘除
			continue
		}
		filtered = append(filtered, ek)
	}
	s.extraIDs = append(filtered, pkey)
	s.bindMu.Unlock()

	// 2) 旧 playerID 索引从 idIndex 分片摘除（不在持锁期间操作分片锁，避免嵌套）。
	for _, old := range staleKeys {
		g.removeFromIDIndex(s, old)
	}

	// 3) 追加新 playerID 索引到 idIndex 分片。
	is := g.idShardOf(pkey)
	is.mu.Lock()
	is.idIndex[pkey] = append(is.idIndex[pkey], s)
	is.mu.Unlock()
}

// removeFromIDIndex 从指定 idIndex 分片摘除本会话的某条索引 key（支持同一 key 多连接切片）。
// 仅移除匹配 connID 的条目；切片清空后删除 key。cleanup / 换绑 / 切换角色多处复用，
// 统一逻辑避免各调用点重复实现切片摘除。
func (g *Gateway) removeFromIDIndex(s *Session, key string) {
	is := g.idShardOf(key)
	is.mu.Lock()
	if sessions, exist := is.idIndex[key]; exist {
		for i, cur := range sessions {
			if cur.connID == s.connID {
				// 同 cleanup 的清尾写法：必须**先在原长度上清尾再截断**，否则
				// 本会话是唯一一条时 len 变 0 ⇒ 对 sessions[-1] 赋值越界 panic。
				// 本函数在 cleanup 的断线事件派发**之前**被调用（removeExtraIDs → 这里），
				// panic 会让派发整段被跳过 ⇒ 与断线回调永不触发是同一个症状。
				n := len(sessions)
				copy(sessions[i:], sessions[i+1:])
				sessions[n-1] = nil // 清尾：避免底层数组残留被摘除会话引用
				sessions = sessions[:n-1]
				is.idIndex[key] = sessions
				if len(sessions) == 0 {
					delete(is.idIndex, key)
				}
				break
			}
		}
	}
	is.mu.Unlock()
}

// kickExtraIDConflict 若同一 playerID 已在其他 session 存在，踢掉旧 session。
func (g *Gateway) kickExtraIDConflict(key string, keep *Session) {
	pkey := idPrefixPlayer + key
	is := g.idShardOf(pkey)
	is.mu.Lock()
	old, exists := is.idIndex[pkey]
	if !exists || len(old) == 0 {
		is.mu.Unlock()
		return
	}
	var toKick []*Session
	for _, s := range old {
		if s.connID != keep.connID {
			toKick = append(toKick, s)
		}
	}
	is.mu.Unlock()
	for _, s := range toKick {
		// 取得旧 session 的真实 owner（account），而非带 "p:" 前缀的索引 key，
		// 否则 onKick 回调收到的 ConnKickedEvent.Owner 会是 "p:player_123" 这种内部值。
		owner := g.getOwnerOf(s.connID)
		logger.Warnf("gwcore: extra idIndex key %s conflict — kicking old conn %s (owner=%s)", pkey, s.connID, owner)
		g.kick(s, owner)
	}
}

// removeExtraIDs 清理 session 上所有额外索引 key（断开 / 切换角色时调用）。
// 遍历 extraIDs 逐个从对应 idIndex 分片摘除本会话条目，避免僵尸索引残留。
func (g *Gateway) removeExtraIDs(s *Session) {
	s.bindMu.Lock()
	// 拷贝一份避免与 addExtraID 的 filtered[:0] 共享底层数组：
	// 两者并发时可能同时读写同一后备数组导致数据竞争。
	keys := append([]string(nil), s.extraIDs...)
	s.extraIDs = nil
	s.bindMu.Unlock()
	for _, key := range keys {
		g.removeFromIDIndex(s, key)
	}
}

// cleanup 移除会话、解除目标索引、关闭连接。

// 注意：先持会话分片锁摘除 sessions 条目并读出绑定 uid，再单独持 idIndex 分片锁解除索引——
// 两段锁分别获取/释放，避免跨分片嵌套加锁（sync.RWMutex 不可重入）造成死锁。
func (g *Gateway) cleanup(connID string) {
	ss := g.sessionShardOf(connID)
	ss.mu.Lock()
	s, ok := ss.sessions[connID]
	if !ok {
		ss.mu.Unlock()
		return
	}
	delete(ss.sessions, connID)
	uid, bound := s.bc.PlayerID()
	ss.mu.Unlock()
	// 放在 !ok 提前返回之后：cleanup 会被重复调用（读循环结束、转发失败、主动踢下线），
	// 只有真正摘除条目的那一次才递减在线数，否则 Gauge 会被减成负数。
	metricSessionClosed()

	// 活跃计数 -1 移出分片锁外，与表删除解耦（对应 admit/createSession 的 +1）。
	// map 删除已保证同一 connID 仅进入本函数体一次（!ok 早返回），故每条会话恰好 -1 一次，
	// 不会重复减。加断言防御未来维护误改成重复减为负 → MaxConns 永久假超限。
	if n := g.currentConns.Add(-1); n < 0 {
		logger.Errorf("gwcore: currentConns went negative (%d) after cleanup %s — resetting to 0", n, connID)
		g.currentConns.Store(0)
	}

	// 仅当索引仍包含本会话时才移除：并发同 UID 重登时，索引可能已被
	// 另一会话覆盖，误删会令新会话静默丢失 NATS 推送。
	// 从切片中移除该会话，若为空则删除 key。
	if bound {
		uidKey := idPrefixAccount + uid
		is := g.idShardOf(uidKey)
		is.mu.Lock()
		if sessions, exist := is.idIndex[uidKey]; exist {
			for i, cur := range sessions {
				if cur == s {
					// 先在**原长度**上清尾再截断：append 截断之后 len 已 -1，
					// 再写 sessions[len-1] 清的是最后一条**存活**会话（多人同 owner 时被误删），
					// 且本会话是唯一一条时 len 变 0 ⇒ 对 sessions[-1] 赋值越界
					// panic: index out of range [-1] —— 该 panic 会让本函数尾部的
					// 断线事件派发（fireHardDisconnect → OnDisconnect）永不执行。
					n := len(sessions)
					copy(sessions[i:], sessions[i+1:])
					sessions[n-1] = nil // 清尾：避免底层数组残留已断开会话引用
					sessions = sessions[:n-1]
					is.idIndex[uidKey] = sessions
					if len(sessions) == 0 {
						delete(is.idIndex, uidKey)
					}
					break
				}
			}
		}
		is.mu.Unlock()
	}
	// 同步清理额外索引 key（playerID 等），避免僵尸索引残留。
	g.removeExtraIDs(s)

	// 同步清理 connID→owner 反查表（未绑定连接无条目，删除幂等）。
	g.delOwnerOf(connID)

	// 同步清理该会话的常驻 UDP 端点绑定：按令牌（udpToken）精确撤销，只回收本会话
	// 登记的端点，同一 owner 其他在线端（各持独立令牌）不受影响；令牌随会话一并作废。
	g.revokeSessionUDP(s)

	// 被多端登录互踢关闭的旧会话：owner 仍由新会话在线，不触发断线/重连事件、
	// 也不保留重连宽限（避免把新登录误判为重连），仅确保连接释放即可。
	if s.kicked.Load() {
		// 读 s.upstream 前先经 sendMu 取快照：switchUpstream 在该锁下替换字段，
		// 裸读与「切换上游」并发构成数据竞争（读到半更新的字段）。
		s.sendMu.Lock()
		up := s.upstream
		s.sendMu.Unlock()
		if up != nil {
			_ = up.Close()
		}
		_ = s.bc.Close()
		return
	}

	// 重连宽限期：断开后保留 owner 记录，若宽限期内再次绑定则为 OnReconnect
	if bound && g.cfg.ReconnectGrace > 0 {
		g.reconnectMu.Lock()
		g.reconnectGraces[uid] = time.Now().Add(g.cfg.ReconnectGrace)
		g.reconnectMu.Unlock()
	}

	// 快照读上游（理由同 kicked 分支）：close 在锁外执行，不持 sendMu 做网络 I/O。
	s.sendMu.Lock()
	up := s.upstream
	s.sendMu.Unlock()
	if up != nil {
		if err := up.Close(); err != nil {
			logger.Warnf("gwcore: close upstream for %s: %v", connID, err)
		}
	}
	if err := s.bc.Close(); err != nil {
		logger.Warnf("gwcore: close connection %s: %v", connID, err)
	}

	// 断线处理（断线宽限期）：
	// - DisconnectGrace>0：立即触发「软掉线」事件（玩家掉线但仍在宽限期内，可保留场景/房间上下文），
	// 「硬掉线」（OnDisconnect）延迟至宽限结束触发；宽限期内该 owner 重连则取消硬掉线。
	// - 否则立即触发「硬掉线」（OnDisconnect）。
	if bound {
		if g.cfg.DisconnectGrace > 0 {
			if g.onSoftDisconnect != nil {
				g.onSoftDisconnect(connID, uid)
			}
			g.scheduleHardDisconnect(connID, uid)
		} else {
			g.fireHardDisconnect(connID, uid)
		}
	}
}
