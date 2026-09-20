// Package event 跨服事件总线：玩家自动寻址 + 跨节点事件投递。
// 本地目标直投 localBus+OnEvent（零网络），远程自动查 master → NATS 路由。
//
// 用法：
//
//	bus := NewCrossNodeEventBus("game-1", nc, masterClient, localBus)
//	bus.Start()
//	bus.OnPlayerEnter(ctx, "player-123")
//	bus.SendToPlayerWithCtx(sourceCtx, "player-456", event.NewInternalServerEvent(...))
//	bus.SendToAll(ctx, event.NewInternalServerEvent(...))
package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"

	"github.com/qw576483/clover-server-engine/internal/domain/master/client"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/internal/transport/nats"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/foundation/trace"
)

// NATS subject 常量。
const (
	// CrossNodeSubjectPrefix 单播事件前缀，完整 subject = {prefix}.{nodeID}
	CrossNodeSubjectPrefix = "game.node"
	// CrossNodeSubjectAll 广播事件 subject，所有 game node 均收到。
	CrossNodeSubjectAll = "game.node.all"

	// defaultKeyQueueCapacity 每个玩家串行发送队列的默认容量。
	defaultKeyQueueCapacity = 1024

	// defaultKeyQueueIdleTimeout 单条 per-uid 串行队列的空闲回收时限。
	// 目标玩家在此时限内没有新的跨节点事件时，回收该队列与其 worker goroutine，
	// 避免 keyQueues / goroutine 数随历史玩家数无上限增长（每条队列还带 1024 容量 channel）。
	defaultKeyQueueIdleTimeout = 60 * time.Second

	// keyQueueStopTimeout 停机时等待所有 per-key worker 收敛的上限。
	// worker 排空队列时每条任务会走完整可靠投递重试退避（单条最长数十秒），
	// 不设上限会把关停拖成分钟级。
	keyQueueStopTimeout = 5 * time.Second

	// maxCrossNodeEventTypeLen 跨节点事件名长度上限（字节）。
	// 事件名是代码里写死的有限枚举，正常远小于该值；超长只可能来自伪造载荷。
	maxCrossNodeEventTypeLen = 256
)

// ctxKeyCrossNodeEvent 是 context 中"处于同步事件 handler"标记的键类型。
type ctxKeyCrossNodeEvent int

const crossNodeEventKey ctxKeyCrossNodeEvent = 0

// ErrNestedCrossNodeEvent 同步事件 handler 内再发跨节点同步事件时返回。
var ErrNestedCrossNodeEvent = errors.New("crossnode: 同步事件 handler 内禁止再发跨节点同步事件")

// withCrossNodeEventInProgress 标记 ctx 处于同步事件 handler 执行期。
func withCrossNodeEventInProgress(ctx context.Context) context.Context {
	return context.WithValue(ctx, crossNodeEventKey, true)
}

// isCrossNodeEventInProgress 判断 ctx 是否处于同步事件 handler 执行期。
func isCrossNodeEventInProgress(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(crossNodeEventKey).(bool)
	return v
}

// CrossNodePayload 跨服事件线格式：携带事件元信息与 JSON 编码载荷。
type CrossNodePayload struct {
	EventType string `json:"event_type"`
	MsgID     uint32 `json:"msg_id"`
	UID       string `json:"uid"`
	TraceID   string `json:"trace_id"`
	Source    string `json:"source"` // 发送方 node ID
	Body      []byte `json:"body"`   // 原始载荷 JSON
}

// 回调签名：桥接事件到 Logic.EmitEvent（typ → 事件类型，c → 源 Ctx 原样透传，payload → 事件载荷）。
// c 可为 nil（跨节点路径无源 Ctx），此时 EmitEvent 构造最小 Ctx。
type eventEmitter func(typ string, c *Ctx, payload any) error

// CrossNodeEventBus 跨服事件总线：玩家→节点自动寻址 + 本地/远程统一投递。
//
// 本地目标短路：localPlayers 命中则直投 localBus+OnEvent，零网络。
// 远程：master 查 owner → NATS 单播到目标节点，对端 onRemoteEvent 拆包投递。
type CrossNodeEventBus struct {
	nodeID   string
	nc       *nats.Client
	master   *client.Client
	localBus Bus

	// 领域 client：从 master（纯 TCP 管道）拆分出的独立能力。
	playerClient    *client.PlayerClient    // 玩家定位
	authorityClient *client.AuthorityClient // 节点查询（按类型/tag）

	// reliable 可靠投递器：ACK + 退避重试 + 死信。为 nil 表示未启用，事件经裸 Publish 发出。
	reliable *reliableSender
	// deduper 接收侧幂等去重器；未启用可靠投递时为 nil。
	deduper Deduper
	// reliableCfg 可靠投递配置快照。
	reliableCfg ReliableConfig

	mu             sync.RWMutex
	localPlayers   map[string]bool // 本节点在线玩家（快捷短路）
	onEventEmitter eventEmitter    // 桥接 Logic.EmitEvent，使事件触发 OnEvent 订阅者

	// liveNodes 为 master 视角的存活节点集合，由 refreshLiveNodes 定时从 master 刷新。
	// 用于拦截向已掉线节点 publish 的静默黑洞：core NATS 对无订阅者主题 Publish 返回
	// nil，调用方会误以为投递成功，实则被丢弃。目标节点不在 liveNodes 中时 sendToNode
	// 直接返回错误，交由调用方重试/补偿，而不是静默吞掉。
	liveNodesMu sync.RWMutex
	liveNodes   map[string]struct{}

	// nodeSource 存活节点来源（etcd 节点目录）。设置后 refreshLiveNodes 不再轮询 master：
	// etcd 租约到期即摘除（无需等心跳判定），且 watch 推送是事件驱动、无 5s 轮询窗口。
	nodeSrcMu  sync.RWMutex
	nodeSource func() []string

	// closeCh 用于通知 refreshLiveNodesLoop goroutine 退出，防止 goroutine 泄漏。
	// liveNodesWg 用来**等它真的退出**：Close 会清空 localPlayers 并关闭 master 客户端，
	// 不等的话那个循环可能正好在两者之间刷新一次状态，用到已释放的资源。
	closeCh     chan struct{}
	liveNodesWg sync.WaitGroup

	// closed 标记总线已关闭；关闭后禁止新建 keyQueue。
	closed atomic.Bool
	// started 标记 Start 已执行，防止重复 Start 覆盖 closeCh 并重复起刷新协程。
	started atomic.Bool

	// keyQueues 每个目标玩家一条的串行发送队列。
	// 仅用于 SendQueueEventToPlayer，保证同一玩家的跨节点事件严格 FIFO。
	keyQueuesMu sync.Mutex
	keyQueues   map[string]*keyQueue
	keyQueueWg  sync.WaitGroup
}

// queuedSend 是 keyQueue 中待处理的跨节点单播任务。
type queuedSend struct {
	sourceCtx *Ctx
	uid       string
	envelope  Envelope
	done      chan error
}

// keyQueue 同一玩家串行发送队列：一个 channel + 一个 worker goroutine。
type keyQueue struct {
	ch   chan queuedSend
	stop chan struct{}
	// drained 在 worker 停机排空（drainKeyQueue）完成后关闭：发送方据此区分
	// 「任务已被排空处理」（job.done 就绪）与「任务入队后再无人消费」（报错），
	// 既不把已成功投递误报为失败，也不会无限等待。
	drained chan struct{}
	// stopOnce 保证 stop 只被关闭一次：Close 与「空闲回收」两条路径可能同时到达同一队列。
	stopOnce sync.Once
}

// closeStop 关闭停止信号（幂等）。
func (q *keyQueue) closeStop() {
	if q == nil {
		return
	}
	q.stopOnce.Do(func() { close(q.stop) })
}

// SetOnEventEmitter 注入事件发射器（通常传 Logic.EmitEvent），使跨节点事件到达后
// 自动触发 OnEvent 订阅者，业务无需关心 Bus 路径。
func (b *CrossNodeEventBus) SetOnEventEmitter(emit eventEmitter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onEventEmitter = emit
}

// MasterClient 返回内嵌的 master 客户端，业务可通过此客户端调用排行榜等
// master 协调服能力（RankService = Top / Add / Incr / Get / GetRank / ...）。
func (b *CrossNodeEventBus) MasterClient() *client.Client {
	return b.master
}

// AuthorityClient 返回内嵌的节点权威客户端，供业务按类型/tag 查询节点。
func (b *CrossNodeEventBus) AuthorityClient() *client.AuthorityClient {
	return b.authorityClient
}

// UseShardedMaster 让内嵌 master 客户端改走「按 key 分片路由」（多 master 部署）。
//
// pick 计算 key 归属分片的地址，all 返回全部分片地址（供排行榜 BackupAll 广播）。
// 启用后：玩家定位按 uid、排行榜按榜名、session token 按 playerID 各自落到属主分片。
// 应在总线创建之后、开始处理业务之前调用；未调用时保持单连接行为（单 master 零感知）。
func (b *CrossNodeEventBus) UseShardedMaster(pick func(key string) string, all func() []string) {
	if b == nil || b.master == nil || pick == nil {
		return
	}
	b.master.UseShardRouting(pick, all)
	logger.Infof("crossnode: master sharding enabled for node %s", b.nodeID)
}

// NewCrossNodeEventBus 创建跨服事件总线（默认启用可靠投递）。
//
// 需要自定义参数时用 NewCrossNodeEventBusWithConfig。
func NewCrossNodeEventBus(nodeID string, nc *nats.Client, masterClient *client.Client, localBus Bus) *CrossNodeEventBus {
	return NewCrossNodeEventBusWithConfig(nodeID, nc, masterClient, localBus, DefaultReliableConfig())
}

// NewCrossNodeEventBusWithConfig 创建跨服事件总线并指定可靠投递配置。
//
// cfg.Enabled=false 时完全退回原「裸 NATS Publish、发出去就不管」的行为。
func NewCrossNodeEventBusWithConfig(nodeID string, nc *nats.Client, masterClient *client.Client, localBus Bus, cfg ReliableConfig) *CrossNodeEventBus {
	b := &CrossNodeEventBus{
		nodeID:       nodeID,
		nc:           nc,
		master:       masterClient,
		localBus:     localBus,
		reliableCfg:  cfg,
		localPlayers: make(map[string]bool),
		liveNodes:    make(map[string]struct{}),
		keyQueues:    make(map[string]*keyQueue),
	}
	if masterClient != nil {
		b.playerClient = client.NewPlayerClient(masterClient)
		b.authorityClient = client.NewAuthorityClient(masterClient)
	}
	if !cfg.Enabled {
		return b
	}
	cfg = cfg.normalize()
	b.reliableCfg = cfg

	// 可靠投递器：底层用 NATS request/reply 等 ACK。
	b.reliable = newReliableSender(cfg, nodeID, b.requestAck, NewMemoryDLQ(cfg.DLQCapacity))
	b.reliable.logf = crossNodeLogf
	// DLQ 的人工重投复用同一条可靠投递链路。
	if mq, ok := b.reliable.DLQ().(*MemoryDLQ); ok {
		mq.SetRetrier(b.retryDeadLetter)
	}

	// 接收侧去重：本地 LRU 兜底（Redis 分布式去重通过 SetRedisDeduper 追加）。
	if cfg.DedupEnabled {
		b.deduper = NewLocalDeduper(cfg.DedupCapacity, cfg.DedupTTL)
	} else {
		b.deduper = NopDeduper()
	}
	return b
}

// crossNodeLogf 将可靠投递层的日志桥接到引擎 logger。
func crossNodeLogf(level, format string, args ...any) {
	switch level {
	case "error":
		logger.Errorf("crossnode: "+format, args...)
	case "warn":
		logger.Warnf("crossnode: "+format, args...)
	default:
		logger.Infof("crossnode: "+format, args...)
	}
}

// SetRedisDeduper 追加 Redis 分布式去重（本地去重仍作为第一级兜底）。
//
// cli 传入实现了 SetNX 的 Redis 客户端（如 *redis.Client）。Redis 不可用时
// RedisDeduper 自动降级放行，由本地去重兜底，不会阻断业务。
func (b *CrossNodeEventBus) SetRedisDeduper(cli RedisSetNX) {
	if b.reliable == nil || cli == nil || !b.reliableCfg.DedupEnabled {
		return
	}
	rd := NewRedisDeduper(cli, b.reliableCfg.RedisDedupPrefix, b.reliableCfg.DedupTTL)
	rd.SetOnError(func(err error) {
		logger.Warnf("crossnode: redis 去重失败，降级为本地去重: %v", err)
	})
	local := NewLocalDeduper(b.reliableCfg.DedupCapacity, b.reliableCfg.DedupTTL)
	// 本地在前（快速拦截），Redis 在后（跨节点兜底）。
	// 必须在 b.mu 内替换：onRemoteEvent 读同一字段，无锁写会构成数据竞争。
	b.mu.Lock()
	b.deduper = NewChainDeduper(local, rd)
	b.mu.Unlock()
}

// SetDeadLetterQueue 替换死信队列实现（如换成 RedisDLQ），并自动接好人工重投器。
func (b *CrossNodeEventBus) SetDeadLetterQueue(q DeadLetterQueue) {
	if b.reliable == nil || q == nil {
		return
	}
	switch v := q.(type) {
	case *MemoryDLQ:
		v.SetRetrier(b.retryDeadLetter)
	case *RedisDLQ:
		v.SetRetrier(b.retryDeadLetter)
	}
	// 经 SetDLQ 写入：toDeadLetter 读同一字段，直写会构成数据竞争。
	b.reliable.SetDLQ(q)
}

// SetDeadLetterHandler 注册死信回调（事件入 DLQ 后触发，用于告警等旁路处理）。
func (b *CrossNodeEventBus) SetDeadLetterHandler(fn DeadLetterHandler) {
	if b.reliable != nil {
		b.reliable.SetDeadLetterHandler(fn)
	}
}

// DLQ 返回死信队列，供人工介入 HTTP 入口使用（未启用可靠投递时返回 nil）。
func (b *CrossNodeEventBus) DLQ() DeadLetterQueue {
	if b.reliable == nil {
		return nil
	}
	return b.reliable.DLQ()
}

// ListPending 返回当前在途（重试中）的投递快照，便于排障。
func (b *CrossNodeEventBus) ListPending() []PendingInfo {
	if b.reliable == nil {
		return nil
	}
	return b.reliable.ListPending()
}

// ReliableStats 返回可靠投递统计快照。
func (b *CrossNodeEventBus) ReliableStats() ReliableStats {
	if b.reliable == nil {
		return ReliableStats{}
	}
	return b.reliable.Stats()
}

// retryDeadLetter 人工重投死信：重新走一遍可靠投递链路。
func (b *CrossNodeEventBus) retryDeadLetter(ctx context.Context, evt *CrossNodeEvent) error {
	if evt == nil {
		return errors.New("crossnode: 死信缺少原事件")
	}
	// 重新生成 MsgID，避免被接收方的幂等去重直接判为重复而丢弃。
	evt.MsgID = b.reliable.idGen.Next()
	evt.Attempt = 0
	// 人工重投用同步模式，便于 HTTP 接口如实返回结果。
	return b.reliable.deliver(ctx, evt)
}

// Start 启动跨服事件总线：订阅本节点 NATS subject 与全局广播 subject，并启动存活节点刷新。
// 重复调用返回错误而非再跑一遍：第二次 Start 会覆盖 closeCh 并再起一个刷新协程，
// 旧的 closeCh 再无人关闭（刷新协程泄漏），Close 也只能通知到后一个。
func (b *CrossNodeEventBus) Start() error {
	if b.closed.Load() {
		return errors.New("crossnode: bus closed")
	}
	if !b.started.CompareAndSwap(false, true) {
		return errors.New("crossnode: bus already started")
	}
	if err := b.nc.Subscribe(b.nodeSubject(b.nodeID), b.onRemoteEvent); err != nil {
		b.started.Store(false) // 订阅失败：允许在修好依赖后重试 Start
		return fmt.Errorf("crossnode: subscribe %s: %w", b.nodeSubject(b.nodeID), err)
	}
	if err := b.nc.Subscribe(CrossNodeSubjectAll, b.onRemoteEvent); err != nil {
		b.started.Store(false)
		return fmt.Errorf("crossnode: subscribe %s: %w", CrossNodeSubjectAll, err)
	}
	// 立即同步一次存活节点，随后按周期刷新。节点掉线由 master 的 RemoveNode 清理，
	// 因此该缓存可反映 master 视角的实时存活状态，用于拦截向死节点 publish 的静默黑洞。
	b.refreshLiveNodes()
	b.closeCh = make(chan struct{})
	b.liveNodesWg.Add(1)
	go func() {
		defer b.liveNodesWg.Done()
		b.refreshLiveNodesLoop()
	}()
	logger.Infof("crossnode: started on %s + %s", b.nodeSubject(b.nodeID), CrossNodeSubjectAll)
	return nil
}

// liveNodesRefreshInterval 存活节点缓存刷新周期。
const liveNodesRefreshInterval = 5 * time.Second

// liveNodesRefreshTimeout 单次从 master 拉取存活节点的超时。
// 必须显著小于刷新周期：一次卡死不能吃掉下一次刷新。
const liveNodesRefreshTimeout = 2 * time.Second

// refreshLiveNodesLoop 周期性从 master 刷新存活节点集合。
func (b *CrossNodeEventBus) refreshLiveNodesLoop() {
	ticker := time.NewTicker(liveNodesRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.closeCh:
			return
		case <-ticker.C:
			b.refreshLiveNodes()
		}
	}
}

// SetNodeSource 注入存活节点来源（如 etcd 节点目录）。
// 设置后 refreshLiveNodes 不再轮询 master 节点表；传 nil 恢复轮询。
func (b *CrossNodeEventBus) SetNodeSource(src func() []string) {
	if b == nil {
		return
	}
	b.nodeSrcMu.Lock()
	b.nodeSource = src
	b.nodeSrcMu.Unlock()
}

// refreshLiveNodes 刷新当前存活的 game 节点缓存。
// 优先用注入的节点来源（etcd 节点目录）；未注入时从 master 拉取。
func (b *CrossNodeEventBus) refreshLiveNodes() {
	b.nodeSrcMu.RLock()
	src := b.nodeSource
	b.nodeSrcMu.RUnlock()

	var nodeIDs []string
	if src != nil {
		nodeIDs = src()
		if len(nodeIDs) == 0 {
			// 目录为空（etcd 抖动 / 首次拉取未完成）时**保留**旧缓存：
			// 把存活集合清空会让 sendToNode 拒绝所有跨节点投递，等于误伤全集群。
			b.liveNodesMu.RLock()
			n := len(b.liveNodes)
			b.liveNodesMu.RUnlock()
			logger.Warnf("crossnode: node directory empty, keep %d cached live nodes", n)
			return
		}
	} else {
		if b.master == nil {
			return
		}
		// 必须带超时：本函数由 5s 周期循环调用，master 慢/半死时用 Background()
		// 会把整个刷新循环一起挂住，存活集合再也不更新（之后所有跨节点投递都被误拦）。
		ctx, cancel := context.WithTimeout(context.Background(), liveNodesRefreshTimeout)
		ids, err := b.authorityClient.NodesByType(ctx, "game")
		cancel()
		if err != nil {
			logger.Warnf("crossnode: refresh live nodes: %v", err)
			return
		}
		nodeIDs = ids
	}

	live := make(map[string]struct{}, len(nodeIDs)+1)
	for _, id := range nodeIDs {
		live[id] = struct{}{}
	}
	// 本节点永远视为存活（避免 NATS 抖动期间把本地直投也误伤）。
	live[b.nodeID] = struct{}{}
	b.liveNodesMu.Lock()
	b.liveNodes = live
	b.liveNodesMu.Unlock()
}

// isNodeLive 判断目标节点是否在 master 存活集合内。
func (b *CrossNodeEventBus) isNodeLive(nodeID string) bool {
	b.liveNodesMu.RLock()
	_, ok := b.liveNodes[nodeID]
	b.liveNodesMu.RUnlock()
	return ok
}

// OnPlayerEnter 玩家上线/进入本节点：向 Master 注册定位，记入本地缓存。
func (b *CrossNodeEventBus) OnPlayerEnter(ctx context.Context, uid string) error {
	// 构造时容许 masterClient 为 nil（无 master 的最小装配）：此时无法注册定位，
	// 必须显式报错而不是解引用 nil 指针 panic。
	if b.playerClient == nil {
		return errors.New("crossnode: master client not configured, cannot register player")
	}
	if err := b.playerClient.RegisterPlayer(ctx, uid, b.nodeID); err != nil {
		return fmt.Errorf("crossnode: register player %s: %w", uid, err)
	}
	b.mu.Lock()
	b.localPlayers[uid] = true
	b.mu.Unlock()
	return nil
}

// OnPlayerLeave 玩家下线/离开本节点：从 Master 移除定位，清本地缓存。
// 若 master.RemovePlayer 失败（抖动/网络错误），master 仍记玩家在本节点，
// 后续 SendToPlayer 会误路由回本节点——此时返回 error 让调用方重试或入补偿队列。
func (b *CrossNodeEventBus) OnPlayerLeave(ctx context.Context, uid string) error {
	if b.playerClient == nil {
		return errors.New("crossnode: master client not configured, cannot remove player")
	}
	// 传本节点 nodeID 作为期望归属，使 master 的版本校验生效：
	// 若玩家已被其他节点接管（existing != b.nodeID），master 会拒绝删除，避免误删新归属。
	err := b.playerClient.RemovePlayer(ctx, uid, b.nodeID)
	if err != nil {
		logger.Errorf("crossnode: remove player %s from master: %v", uid, err)
		return fmt.Errorf("crossnode: remove player %s from master: %w", uid, err)
	}
	b.mu.Lock()
	delete(b.localPlayers, uid)
	b.mu.Unlock()
	return nil
}

// dispatchLocal 将事件走本地短路：localBus.Publish + OnEvent bridge（零网络）。
// sourceCtx 为调用方原始 Ctx（本地路径原样透传至 OnEvent handler），跨节点路径传 nil。
func (b *CrossNodeEventBus) dispatchLocal(p CrossNodePayload, sourceCtx *Ctx) error {
	ctx := context.Background()
	if p.UID != "" {
		ctx = proto.WithOwner(ctx, p.UID)
	}
	envelope := Envelope{
		ID:      GenID(),
		Type:    p.EventType,
		MsgID:   p.MsgID,
		UID:     p.UID,
		TraceID: p.TraceID,
		Payload: &InternalServerPayload{PlayerUID: p.UID, Body: p.Body},
		Ctx:     ctx,
	}
	b.mu.RLock()
	emit := b.onEventEmitter
	b.mu.RUnlock()
	if emit != nil {
		var payload any
		if p.Body != nil {
			payload = json.RawMessage(p.Body)
		}
		if err := emit(p.EventType, sourceCtx, payload); err != nil {
			// emit 失败则不 Publish，避免调用方重投时 Bus 订阅者重复派发。
			return err
		}
	}
	b.localBus.Publish(envelope)
	return nil
}

// SendToPlayerWithCtx 向指定玩家投递事件：本地玩家直投 localBus+OnEvent，远程自动寻址→NATS 路由。
//
// 保留 sourceCtx 原样透传至本地 OnEvent handler，仅本地目标有效（跨节点无法序列化 Ctx）。
// 玩家离线（master 无记录）时本地兜底 dispatch，数据走 MySQL 读写，不影响 handler 逻辑；
// 其他错误（网络/超时等）向上传播，不做兜底以免掩盖故障。
func (b *CrossNodeEventBus) SendToPlayerWithCtx(sourceCtx *Ctx, uid string, envelope Envelope) error {
	if b.isLocal(uid) {
		return b.dispatchLocalPayload(uid, envelope, sourceCtx)
	}
	if b.playerClient == nil {
		// 无 master 客户端时无法查询 owner：不能在这里猜（猜错会把事件投到错误节点），
		// 显式报错交由上层处理；本地玩家已在上面 isLocal 短路。
		return errors.New("crossnode: master client not configured, cannot lookup player node")
	}
	ctx := context.Background()
	if sourceCtx != nil {
		if c := sourceCtx.Context(); c != nil {
			ctx = c
		}
	}
	nodeID, err := b.playerClient.PlayerNode(ctx, uid)
	if err != nil {
		if !errors.Is(err, client.ErrPlayerNotFound) {
			return fmt.Errorf("crossnode: lookup player node for %s: %w", uid, err)
		}
		return b.dispatchLocalPayload(uid, envelope, sourceCtx)
	}
	return b.sendToNode(ctx, nodeID, uid, envelope)
}

// SendToAll 向全部 game node 广播事件（含本节点直接本地分发）。
//
// 先 NATS Publish 再本地 dispatch：若 Publish 失败（NATS 断连），本地尚未执行，
// 调用方可安全重试不会导致本节点 handler 重复执行。
// 回环由 onRemoteEvent 按 Source==nodeID 丢弃。
func (b *CrossNodeEventBus) SendToAll(ctx context.Context, envelope Envelope) error {
	p, err := b.buildPayload("", envelope)
	if err != nil {
		return err
	}
	if err := b.nc.Publish(CrossNodeSubjectAll, p); err != nil {
		return fmt.Errorf("crossnode: broadcast to all: %w", err)
	}
	return b.dispatchLocal(p, nil)
}

// SendQueueEventToPlayer 向指定玩家投递事件，并保证同一玩家的跨节点事件串行 FIFO。
//
// 本地玩家仍走同步直投（零网络、天然有序）；远程玩家进入 per-key 队列，
// 由单一 worker 顺序执行同步 sendToNodeSync，避免多个 goroutine 并发等 ACK 导致乱序。
// 调用方在入队后阻塞等待本事件发送完成，语义与 SendEventToPlayer 一致。
func (b *CrossNodeEventBus) SendQueueEventToPlayer(sourceCtx *Ctx, uid string, envelope Envelope) error {
	if b.closed.Load() {
		return errors.New("crossnode: bus closed")
	}
	if b.isLocal(uid) {
		return b.dispatchLocalPayload(uid, envelope, sourceCtx)
	}
	job := queuedSend{sourceCtx: sourceCtx, uid: uid, envelope: envelope, done: make(chan error, 1)}
	q, err := b.getKeyQueue(uid)
	if err != nil {
		return err
	}
	select {
	case q.ch <- job:
	case <-q.stop:
		return errors.New("crossnode: key queue stopped")
	}
	// 任务已入队：worker 会在正常路径或停机排空（drainKeyQueue）中处理它。
	// 这里**不**再并列 `<-q.stop` —— 那会在队列被回收/关闭时把「已成功投递」误报为失败；
	// 改为等 drained：排空完成后若结果仍未就绪，才是真的没被处理。
	select {
	case err := <-job.done:
		return err
	case <-q.drained:
		select {
		case err := <-job.done:
			return err
		default:
			return errors.New("crossnode: key queue stopped before processing")
		}
	}
}

// getKeyQueue 获取 uid 对应的串行队列；不存在时创建并启动 worker。
func (b *CrossNodeEventBus) getKeyQueue(uid string) (*keyQueue, error) {
	b.keyQueuesMu.Lock()
	defer b.keyQueuesMu.Unlock()
	if b.closed.Load() {
		return nil, errors.New("crossnode: bus closed")
	}
	if q, ok := b.keyQueues[uid]; ok {
		return q, nil
	}
	q := &keyQueue{
		ch:      make(chan queuedSend, defaultKeyQueueCapacity),
		stop:    make(chan struct{}),
		drained: make(chan struct{}),
	}
	b.keyQueues[uid] = q
	b.keyQueueWg.Add(1)
	go b.runKeyQueue(uid, q)
	return q, nil
}

// retireKeyQueue 把空闲队列从 keyQueues 摘除（仅当表里仍是同一条时），
// 使 map 与 worker goroutine 不再随历史玩家数无上限增长。返回是否回收成功。
func (b *CrossNodeEventBus) retireKeyQueue(uid string, q *keyQueue) bool {
	b.keyQueuesMu.Lock()
	defer b.keyQueuesMu.Unlock()
	if b.keyQueues[uid] != q {
		return false // 已被 Close 清表或已被替换：不动别人的队列
	}
	if len(q.ch) > 0 {
		return false // 有新任务刚入队：不回收，继续服务
	}
	delete(b.keyQueues, uid)
	q.closeStop()
	return true
}

// runKeyQueue 单个 key 的串行发送 worker。
// 随总线生命周期运行，直到 Close 触发 q.stop；长时间空闲（defaultKeyQueueIdleTimeout）
// 时自行回收，避免空转的 goroutine 与 channel 无限堆积。
func (b *CrossNodeEventBus) runKeyQueue(uid string, q *keyQueue) {
	defer b.keyQueueWg.Done()
	defer close(q.drained) // 停机/回收排空完成：唤醒所有在等结果的发送方
	idle := time.NewTimer(defaultKeyQueueIdleTimeout)
	defer idle.Stop()
	for {
		select {
		case job := <-q.ch:
			resetTimer(idle, defaultKeyQueueIdleTimeout)
			b.processQueuedSend(job)
		case <-idle.C:
			if b.retireKeyQueue(uid, q) {
				// 摘除成功：把「与摘除竞争、已进入 channel」的任务处理完再退出。
				b.drainKeyQueue(q)
				return
			}
			resetTimer(idle, defaultKeyQueueIdleTimeout)
		case <-q.stop:
			b.drainKeyQueue(q)
			return
		}
	}
}

// resetTimer 重启定时器（先排空已触发的 tick，避免下一轮 select 立刻再次命中）。
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// waitGroupTimeout 等待 wg 归零，最多等待 d；返回是否在时限内完成。
func waitGroupTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// drainKeyQueue 关闭前把队列中剩余任务尽量处理完。
func (b *CrossNodeEventBus) drainKeyQueue(q *keyQueue) {
	for {
		select {
		case job := <-q.ch:
			b.processQueuedSend(job)
		default:
			return
		}
	}
}

// processQueuedSend 执行队列中的一个发送任务。
func (b *CrossNodeEventBus) processQueuedSend(job queuedSend) {
	defer func() {
		if r := recover(); r != nil {
			// 带堆栈：worker 内 panic 若只留 %v，无法定位到具体行号。
			logger.Errorf("crossnode: queue worker panic for player %s: %v\n%s", job.uid, r, debug.Stack())
			job.done <- fmt.Errorf("crossnode: queue worker panic: %v", r)
		}
	}()
	if b.playerClient == nil {
		job.done <- errors.New("crossnode: master client not configured, cannot lookup player node")
		return
	}
	ctx := context.Background()
	if job.sourceCtx != nil {
		if c := job.sourceCtx.Context(); c != nil {
			ctx = c
		}
	}
	nodeID, err := b.playerClient.PlayerNode(ctx, job.uid)
	if err != nil {
		if errors.Is(err, client.ErrPlayerNotFound) {
			job.done <- b.dispatchLocalPayload(job.uid, job.envelope, job.sourceCtx)
		} else {
			job.done <- fmt.Errorf("crossnode: lookup player node for %s: %w", job.uid, err)
		}
		return
	}
	job.done <- b.sendToNodeSync(ctx, nodeID, job.uid, job.envelope)
}

// —— 内部 ——
//
// isLocal 判断玩家是否在本节点（无锁快速判断）。
func (b *CrossNodeEventBus) isLocal(uid string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.localPlayers[uid]
}

func (b *CrossNodeEventBus) sendToNode(ctx context.Context, nodeID, uid string, envelope Envelope) error {
	return b.doSendToNode(ctx, nodeID, uid, envelope, false)
}

// sendToNodeSync 与 sendToNode 相同，但强制走可靠投递同步路径。
// 用于 SendQueueEventToPlayer 的 worker，确保同一 key 的网络发送真正串行。
func (b *CrossNodeEventBus) sendToNodeSync(ctx context.Context, nodeID, uid string, envelope Envelope) error {
	return b.doSendToNode(ctx, nodeID, uid, envelope, true)
}

func (b *CrossNodeEventBus) doSendToNode(ctx context.Context, nodeID, uid string, envelope Envelope, sync bool) error {
	if nodeID == b.nodeID {
		return b.dispatchLocalPayload(uid, envelope, nil)
	}
	// 同步事件 handler 内禁止再发跨节点同步事件，防止 A→B→A 环形等待。
	if isCrossNodeEventInProgress(ctx) {
		return ErrNestedCrossNodeEvent
	}
	// 目标节点不在 master 存活集合内：core NATS 对无订阅者主题 Publish 返回 nil，
	// 若直接发布会被静默丢弃，调用方误以为成功。显式返回错误，
	// 交由上层重试或补偿，避免事件黑洞。
	if !b.isNodeLive(nodeID) {
		metricCrossDropped(dropReasonNodeDown)
		return fmt.Errorf("crossnode: target node %s not alive (no subscriber), drop event %s", nodeID, envelope.Type)
	}
	payload, err := b.buildPayload(uid, envelope)
	if err != nil {
		return err
	}

	// 未启用可靠投递：裸 Publish。
	if b.reliable == nil {
		return b.nc.Publish(b.nodeSubject(nodeID), payload)
	}

	// 可靠投递：request/reply 等 ACK，失败按指数退避重投，耗尽入死信。
	evt := newCrossNodeEvent(payload, nodeID)
	// 链路追踪注入：把 trace 上下文写进与传输无关的 Headers，
	// 接收方 onRemoteEvent 用 ExtractFrom 还原，跨节点链路即可串成一条。
	//
	// 以 payload.TraceID 为准（buildPayload 已从事件信封继承上游 TraceID），
	// 并挂到调用方 ctx 上而非 Background：保持 ctx 的取消/超时与 span 归属，
	// 否则下游从 ctx 里取到的永远是「凭空起的一条链」。
	if payload.TraceID != "" {
		traceCtx := trace.WithTraceID(ctx, payload.TraceID)
		trace.InjectInto(traceCtx, func(k, v string) { evt.SetHeader(k, v) })
	}
	if sync {
		return b.reliable.SendSync(ctx, evt)
	}
	return b.reliable.Send(ctx, evt)
}

// requestAck 通过 NATS request/reply 投递一条事件并等待接收方的 ACK。
//
// 发送侧：接收方在订阅回调里
// 调用 msg.Respond(ack) 回执，NATS 把应答作为 Request 的返回值送回。
func (b *CrossNodeEventBus) requestAck(ctx context.Context, evt *CrossNodeEvent, timeout time.Duration) (Ack, error) {
	if evt == nil {
		return Ack{}, errors.New("crossnode: 待投递事件为空")
	}
	// 投递前再校验一次目标存活：重试期间对端可能已下线。
	if evt.TargetNode != "" && !b.isNodeLive(evt.TargetNode) {
		return Ack{}, fmt.Errorf("crossnode: 目标节点 %s 不在存活集合内", evt.TargetNode)
	}
	evt.stamp()

	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	msg, err := b.nc.RequestCtx(rctx, b.nodeSubject(evt.TargetNode), evt)
	if err != nil {
		// 无应答/超时/断连：统一视为可重试的传输失败。
		// 内层用 %w：调用方仍可 errors.Is/As 到真实传输错误（超时 / 断连）。
		return Ack{}, fmt.Errorf("%w: %w", ErrAckTimeout, err)
	}
	var ack Ack
	if err := msg.GetData(&ack); err != nil {
		return Ack{}, fmt.Errorf("crossnode: 解析 ACK 失败: %w", err)
	}
	if ack.MsgID == "" {
		ack.MsgID = evt.MsgID
	}
	return ack, nil
}

// buildPayload 组装跨节点传输载荷。载荷序列化失败时返回错误：
// 事件必须带着完整 body 上路，否则接收端会解出一个空对象。
func (b *CrossNodeEventBus) buildPayload(uid string, envelope Envelope) (CrossNodePayload, error) {
	body, err := b.encodeBody(envelope.Payload)
	if err != nil {
		return CrossNodePayload{}, err
	}
	return CrossNodePayload{
		EventType: envelope.Type,
		MsgID:     envelope.MsgID,
		UID:       uid,
		TraceID:   envelope.TraceID,
		Source:    b.nodeID,
		Body:      body,
	}, nil
}

// encodeBody 序列化事件载荷；nil 载荷序列化为空 body。
// 序列化失败必须向上冒泡，由调用方中止本次投递。
func (b *CrossNodeEventBus) encodeBody(payload any) ([]byte, error) {
	if payload == nil {
		return nil, nil
	}
	body, err := ujson.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("crossnode: marshal payload: %w", err)
	}
	return body, nil
}

// dispatchLocalPayload 组装载荷后立即本地分发（构造与分发合一，避免调用点漏判序列化错误）。
func (b *CrossNodeEventBus) dispatchLocalPayload(uid string, envelope Envelope, sourceCtx *Ctx) error {
	p, err := b.buildPayload(uid, envelope)
	if err != nil {
		return err
	}
	return b.dispatchLocal(p, sourceCtx)
}

// onRemoteEvent 收到 NATS 远程事件时：幂等去重 → 本地分发 → 回 ACK。
//
// 接收侧：处理完通过 msg.Respond 回执给发送方。广播（SendToAll 走裸 Publish）
// 无 Reply 主题，Respond 返回 ErrNoReply，属预期情况，静默忽略。
func (b *CrossNodeEventBus) onRemoteEvent(msg *nats.Msg) {
	var evt CrossNodeEvent
	if err := msg.GetData(&evt); err != nil {
		logger.Errorf("crossnode: decode event: %v", err)
		b.respondAck(msg, Ack{Status: AckRejected, Node: b.nodeID, Error: "解码失败: " + err.Error()})
		return
	}
	// 丢弃自己发出的广播回环：SendToAll 已在本地直投过，
	// 不过滤会导致本节点同一事件被投递两次（两次 Bus.Publish + 两次 OnEvent）。
	if evt.Source == b.nodeID {
		return
	}
	// 计数放在回环过滤之后：广播回环不是「收到远端事件」，计入会虚高。
	metricEventReceived()

	// 链路追踪续接：从 Headers 还原上游 trace，
	// 回填到 evt.TraceID，使本节点后续日志与上游属于同一条链路。
	if evt.TraceID == "" && len(evt.Headers) > 0 {
		tctx := trace.ExtractFrom(context.Background(), evt.GetHeader)
		evt.TraceID = trace.FromContext(tctx)
	}

	// 输入校验：载荷来自 NATS 主题，必须把明显非法的载荷挡在 dispatch 之前 ——
	// 事件名为空（或异常长）的载荷没有任何订阅者能正确处理，却会把链路与日志拖脏。
	// 回 AckRejected（明确不可重试），不让发送方白白重投。
	if evt.EventType == "" || len(evt.EventType) > maxCrossNodeEventTypeLen {
		logger.Errorf("crossnode: reject remote event with invalid type (source=%s len=%d)", evt.Source, len(evt.EventType))
		metricCrossRejected()
		b.respondAck(msg, Ack{MsgID: evt.MsgID, Status: AckRejected, Node: b.nodeID, Error: "非法事件类型"})
		return
	}

	// 幂等去重：同一 MsgID 重复到达（发送方重投 / NATS 重复投递）且此前已成功
	// 处理过时只处理一次，但仍要回 ACK，否则发送方会一直重投直到进死信。
	//
	// 这里用**只读**的 Contains 判重，登记（Seen）仍放在 dispatch 成功之后：
	// 若在 dispatch 前就用 Seen 占位，处理失败后重投的同一 MsgID 会被误判为
	// AckDuplicate（视为已处理）——事件从未成功处理却报成功，破坏 at-least-once。
	b.mu.RLock()
	deduper := b.deduper
	b.mu.RUnlock()
	dedupCtx := context.Background()
	if evt.MsgID != "" && deduper != nil && deduper.Contains(dedupCtx, evt.MsgID) {
		metricCrossDuplicate()
		b.respondAck(msg, Ack{MsgID: evt.MsgID, Status: AckDuplicate, Node: b.nodeID})
		return
	}

	// 复用 dispatchLocal，确保 Bus.Publish + OnEvent bridge 行为一致。
	if err := b.dispatchLocal(evt.toPayload(), nil); err != nil {
		logger.Errorf("crossnode: dispatch local for remote event %s: %v", evt.EventType, err)
		// 处理失败回 fail，让发送方重投。
		b.respondAck(msg, Ack{MsgID: evt.MsgID, Status: AckFailed, Node: b.nodeID, Error: err.Error()})
		return
	}
	// dispatch 成功后才登记去重，保证失败重投不会被误判为重复。
	if evt.MsgID != "" && deduper != nil {
		deduper.Seen(dedupCtx, evt.MsgID)
	}
	b.respondAck(msg, Ack{MsgID: evt.MsgID, Status: AckOK, Node: b.nodeID})
}

// respondAck 回执 ACK。广播等无 reply 主题的场景 Respond 返回 ErrNoReply，
// 属预期情况，静默忽略。
func (b *CrossNodeEventBus) respondAck(msg *nats.Msg, ack Ack) {
	if msg == nil {
		return
	}
	if err := msg.Respond(ack); err != nil && !errors.Is(err, nats.ErrNoReply) {
		logger.Warnf("crossnode: 回 ACK 失败 msg_id=%s: %v", ack.MsgID, err)
	}
}

func (b *CrossNodeEventBus) nodeSubject(nodeID string) string {
	return CrossNodeSubjectPrefix + "." + nodeID
}

// Close 标记关闭（NATS 订阅由共享 NC 统一回收，此处仅清本地缓存）。
// 置空 map 而非置 nil：关闭后仍可能有在途的 OnPlayerEnter 写入，
// 写 nil map 会直接 panic。
func (b *CrossNodeEventBus) Close() error {
	// 标记关闭，禁止新建 keyQueue。
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	// 通知 refreshLiveNodesLoop goroutine 退出，并**等它退出**再动它可能用到的状态
	//（localPlayers / master 客户端在下面几步被清空与关闭）。
	if b.closeCh != nil {
		close(b.closeCh)
		b.liveNodesWg.Wait()
	}
	// nats.Client.Close() 内部会取消所有订阅，无需外部处理。
	// 先停可靠投递：等待在途重试收敛，避免关闭后仍向已停机的对端重投。
	if b.reliable != nil {
		if err := b.reliable.Close(5 * time.Second); err != nil {
			logger.Warnf("crossnode: %v", err)
		}
	}
	// 停止所有 per-key 发送队列。先把表清空再关 stop：
	// 只置 closed 的话，那些已关闭的 keyQueue 对象会一直挂在 map 上
	//（每条几十字节 × 玩家数），总线实例活多久就留多久。
	b.keyQueuesMu.Lock()
	queues := make([]*keyQueue, 0, len(b.keyQueues))
	for _, q := range b.keyQueues {
		queues = append(queues, q)
	}
	b.keyQueues = make(map[string]*keyQueue)
	b.keyQueuesMu.Unlock()
	for _, q := range queues {
		// closeStop 幂等：空闲回收路径可能已经关过同一条队列。
		q.closeStop()
	}
	// 等待有上限：worker 排空队列时每条任务走完整可靠投递重试退避（单条最长数十秒），
	// 无上限等待会把关停拖成分钟级。超时只告警：剩余任务由各自的重试/死信闭环兜底。
	if !waitGroupTimeout(&b.keyQueueWg, keyQueueStopTimeout) {
		logger.Warnf("crossnode: key queues still draining after %s, continue shutdown", keyQueueStopTimeout)
	}
	b.mu.Lock()
	b.localPlayers = make(map[string]bool)
	master := b.master
	b.master = nil
	b.mu.Unlock()
	// master 客户端（含分片连接池）由总线持有并在此关闭：避免进程退出时连接泄漏。
	if master != nil {
		if err := master.Close(); err != nil {
			logger.Warnf("crossnode: close master client: %v", err)
		}
	}
	logger.Infof("crossnode: closed for node %s", b.nodeID)
	return nil
}
