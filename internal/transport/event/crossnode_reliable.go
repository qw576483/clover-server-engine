package event

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qw576483/clover-server-engine/internal/shared/retry"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// // 跨服事件可靠投递（at-least-once）

//  1. ACK 确认：消费端走 NATS request/reply 模式，处理完用 msg.Respond 回 ACK；
//  2. 超时重试：基于 foundation/retry 的指数退避 + 抖动；
//  3. 幂等去重：接收方按 MsgID 去重（本地 LRU + 可选 Redis），见 dedup.go；
//  4. 死信队列：重试耗尽后入 DLQ 供人工介入，见 deadletter.go。
//
// // 可靠投递默认参数。
const (
	defaultAckTimeout   = 3 * time.Second // 单次投递等待 ACK 的超时
	defaultMaxAttempts  = 5               // 最大投递次数（含首次）
	defaultBaseDelay    = 100 * time.Millisecond
	defaultMaxDelay     = 10 * time.Second
	defaultMultiplier   = 2.0
	defaultJitter       = 0.2
	defaultPendingLimit = 100000 // 在途表容量上限，防止无界增长打爆内存
)

// 可靠投递相关错误。
var (
	// ErrAckTimeout 等待 ACK 超时。
	ErrAckTimeout = errors.New("event: 等待跨服事件 ACK 超时")
	// ErrPendingOverflow 在途表已满，拒绝新的可靠投递。
	ErrPendingOverflow = errors.New("event: 跨服事件在途表已满")
	// ErrDeliveryFailed 投递最终失败（已进入死信）。
	ErrDeliveryFailed = errors.New("event: 跨服事件投递最终失败")
	// ErrAckRejected 接收方明确拒绝且不可重试。
	ErrAckRejected = errors.New("event: 接收方拒绝该事件")
)

// AckStatus 接收方回执状态。
type AckStatus string

const (
	// AckOK 接收方已成功处理。
	AckOK AckStatus = "ok"
	// AckDuplicate 接收方判定为重复消息已丢弃（对发送方而言等同成功）。
	AckDuplicate AckStatus = "dup"
	// AckFailed 接收方处理失败，发送方应重投。
	AckFailed AckStatus = "fail"
	// AckRejected 接收方拒绝且明确不可重试，发送方不应重投。
	AckRejected AckStatus = "reject"
)

// Ack 接收方通过 msg.Respond 回给发送方的确认报文。
type Ack struct {
	MsgID  string    `json:"msg_id"`          // 对应事件的 MsgID
	Status AckStatus `json:"status"`          // 处理结果
	Node   string    `json:"node,omitempty"`  // 处理方节点 ID
	Error  string    `json:"error,omitempty"` // 失败原因（便于排障）
}

// ReliableConfig 可靠投递配置。

// 零值 Enabled=false 表示不启用可靠投递，事件经裸 Publish 发出。
// 通过 DefaultReliableConfig() 获取推荐默认值（默认启用）。
type ReliableConfig struct {
	// 注意：本仓库配置加载走 viper（internal/foundation/config），viper 只认
	// mapstructure tag，不认 yaml tag。带下划线的 key 若缺 mapstructure tag 会
	// 静默绑不上并回落成零值，因此每个字段都必须三种 tag 齐全。

	// Enabled 是否启用可靠投递（request/reply ACK + 重试 + 去重 + 死信）。
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	// AckTimeout 单次投递等待 ACK 的超时，<=0 取 3s。
	AckTimeout time.Duration `json:"ack_timeout" yaml:"ack_timeout" mapstructure:"ack_timeout"`
	// MaxAttempts 最大投递次数（含首次），<=0 取 5。
	MaxAttempts int `json:"max_attempts" yaml:"max_attempts" mapstructure:"max_attempts"`
	// BaseDelay 重投基础退避，<=0 取 100ms。
	BaseDelay time.Duration `json:"base_delay" yaml:"base_delay" mapstructure:"base_delay"`
	// MaxDelay 重投退避上限，<=0 取 10s。
	MaxDelay time.Duration `json:"max_delay" yaml:"max_delay" mapstructure:"max_delay"`
	// Multiplier 退避倍率，<1 取 2。
	Multiplier float64 `json:"multiplier" yaml:"multiplier" mapstructure:"multiplier"`
	// Jitter 抖动比例 [0,1]，默认 0.2。
	Jitter float64 `json:"jitter" yaml:"jitter" mapstructure:"jitter"`
	// PendingLimit 在途表容量上限，<=0 取 100000。
	PendingLimit int `json:"pending_limit" yaml:"pending_limit" mapstructure:"pending_limit"`
	// Async 为 true 时 Send 立即返回、重试在后台 goroutine 进行（推荐，
	// 避免业务线程被退避阻塞）；false 时同步阻塞直到成功或进死信。
	Async bool `json:"async" yaml:"async" mapstructure:"async"`

	// DedupEnabled 是否启用接收侧幂等去重，默认 true。
	DedupEnabled bool `json:"dedup_enabled" yaml:"dedup_enabled" mapstructure:"dedup_enabled"`
	// DedupCapacity 本地去重缓存容量，<=0 取 10000。
	DedupCapacity int `json:"dedup_capacity" yaml:"dedup_capacity" mapstructure:"dedup_capacity"`
	// DedupTTL 去重记录有效期，<=0 取 10 分钟。
	DedupTTL time.Duration `json:"dedup_ttl" yaml:"dedup_ttl" mapstructure:"dedup_ttl"`
	// RedisDedupPrefix Redis 去重 key 前缀，为空取 "clover:evt:dedup:"。
	RedisDedupPrefix string `json:"redis_dedup_prefix" yaml:"redis_dedup_prefix" mapstructure:"redis_dedup_prefix"`

	// DLQCapacity 内存死信队列容量，<=0 取 1000。
	DLQCapacity int `json:"dlq_capacity" yaml:"dlq_capacity" mapstructure:"dlq_capacity"`
}

// DefaultReliableConfig 返回推荐的可靠投递配置（默认启用）。
func DefaultReliableConfig() ReliableConfig {
	return ReliableConfig{
		Enabled:       true,
		AckTimeout:    defaultAckTimeout,
		MaxAttempts:   defaultMaxAttempts,
		BaseDelay:     defaultBaseDelay,
		MaxDelay:      defaultMaxDelay,
		Multiplier:    defaultMultiplier,
		Jitter:        defaultJitter,
		PendingLimit:  defaultPendingLimit,
		Async:         true,
		DedupEnabled:  true,
		DedupCapacity: defaultDedupCapacity,
		DedupTTL:      defaultDedupTTL,
		DLQCapacity:   defaultDLQCapacity,
	}
}

// normalize 归一化非法字段。
func (c ReliableConfig) normalize() ReliableConfig {
	if c.AckTimeout <= 0 {
		c.AckTimeout = defaultAckTimeout
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = defaultBaseDelay
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = defaultMaxDelay
	}
	if c.Multiplier < 1 {
		c.Multiplier = defaultMultiplier
	}
	if c.Jitter < 0 || c.Jitter > 1 {
		c.Jitter = defaultJitter
	}
	if c.PendingLimit <= 0 {
		c.PendingLimit = defaultPendingLimit
	}
	if c.DedupCapacity <= 0 {
		c.DedupCapacity = defaultDedupCapacity
	}
	if c.DedupTTL <= 0 {
		c.DedupTTL = defaultDedupTTL
	}
	if c.DLQCapacity <= 0 {
		c.DLQCapacity = defaultDLQCapacity
	}
	return c
}

// RetryPolicy 将配置转换为通用重试策略。
func (c ReliableConfig) RetryPolicy() retry.Policy {
	n := c.normalize()
	return retry.Policy{
		MaxAttempts: n.MaxAttempts,
		BaseDelay:   n.BaseDelay,
		MaxDelay:    n.MaxDelay,
		Multiplier:  n.Multiplier,
		Jitter:      n.Jitter,
	}
}

// MsgIDGenerator 生成全局唯一的事件消息 ID。

// 组成：节点ID-时间戳(纳秒,36进制)-原子序号(36进制)。
// 同节点内序号单调递增保证唯一，跨节点由节点 ID 区分，重启后由时间戳区分。
type MsgIDGenerator struct {
	nodeID string
	seq    atomic.Uint64
}

// NewMsgIDGenerator 构造消息 ID 生成器。nodeID 为空时用 "unknown"。
func NewMsgIDGenerator(nodeID string) *MsgIDGenerator {
	if nodeID == "" {
		nodeID = "unknown"
	}
	return &MsgIDGenerator{nodeID: nodeID}
}

// Next 生成下一个全局唯一消息 ID，并发安全。
func (g *MsgIDGenerator) Next() string {
	if g == nil {
		return ""
	}
	n := g.seq.Add(1)
	// 用 strconv 拼接，避免 fmt.Sprintf 的反射开销（投递为高频路径）。
	buf := make([]byte, 0, len(g.nodeID)+40)
	buf = append(buf, g.nodeID...)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, time.Now().UnixNano(), 36)
	buf = append(buf, '-')
	buf = strconv.AppendUint(buf, n, 36)
	return string(buf)
}

// 在途（重试）表
// PendingInfo 一条在途投递的可观测快照，供 ListPending 排障使用。
type PendingInfo struct {
	MsgID      string    `json:"msg_id"`
	TargetNode string    `json:"target_node"`
	EventType  string    `json:"event_type"`
	UID        string    `json:"uid,omitempty"`
	Attempt    int       `json:"attempt"`
	FirstAt    time.Time `json:"first_at"`
	LastErr    string    `json:"last_err,omitempty"`
}

// pendingEntry 一条在途投递记录（重试队列成员）。
type pendingEntry struct {
	mu         sync.Mutex
	msgID      string
	targetNode string
	eventType  string
	uid        string
	attempt    int
	firstAt    time.Time
	lastErr    string
}

// snapshot 返回该条在途记录的只读快照。
func (e *pendingEntry) snapshot() PendingInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	return PendingInfo{
		MsgID:      e.msgID,
		TargetNode: e.targetNode,
		EventType:  e.eventType,
		UID:        e.uid,
		Attempt:    e.attempt,
		FirstAt:    e.firstAt,
		LastErr:    e.lastErr,
	}
}

// update 更新尝试次数与最后错误。
func (e *pendingEntry) update(attempt int, err error) {
	e.mu.Lock()
	e.attempt = attempt
	if err != nil {
		e.lastErr = err.Error()
	}
	e.mu.Unlock()
}

// pendingTable 在途表：key=MsgID，记录所有正在投递/重试中的事件。
type pendingTable struct {
	mu    sync.RWMutex
	items map[string]*pendingEntry
	limit int
}

// newPendingTable 构造在途表。
func newPendingTable(limit int) *pendingTable {
	if limit <= 0 {
		limit = defaultPendingLimit
	}
	return &pendingTable{items: make(map[string]*pendingEntry), limit: limit}
}

// add 登记一条在途记录；表满时返回 ErrPendingOverflow。
func (t *pendingTable) add(e *pendingEntry) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.items) >= t.limit {
		return ErrPendingOverflow
	}
	t.items[e.msgID] = e
	return nil
}

// remove 移除在途记录。
func (t *pendingTable) remove(msgID string) {
	t.mu.Lock()
	delete(t.items, msgID)
	t.mu.Unlock()
}

// list 返回全部在途记录快照，按首次投递时间升序（最老的在前，便于排障）。
func (t *pendingTable) list() []PendingInfo {
	t.mu.RLock()
	out := make([]PendingInfo, 0, len(t.items))
	for _, e := range t.items {
		out = append(out, e.snapshot())
	}
	t.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].FirstAt.Before(out[j].FirstAt) })
	return out
}

// len 返回当前在途条数。
func (t *pendingTable) len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.items)
}

// 可靠投递器
// ackRequester 底层「请求-应答」发送函数：把一条事件投到目标节点并等待 ACK。

// 抽象为函数便于测试注入假传输，不依赖真实 NATS。
// 返回的 error 表示传输层失败（连接断开 / 超时无应答）。
type ackRequester func(ctx context.Context, evt *CrossNodeEvent, timeout time.Duration) (Ack, error)

// reliableSender 负责「请求投递 → 收 ACK → 退避重投 → 死信」的完整闭环。
type reliableSender struct {
	cfg      ReliableConfig
	pending  *pendingTable
	idGen    *MsgIDGenerator
	request  ackRequester
	dlq      DeadLetterQueue
	deadLock sync.RWMutex
	deadFn   DeadLetterHandler
	// logf 日志钩子，避免本文件直接耦合具体 logger（由 crossnode.go 注入）。
	logf func(level, format string, args ...any)

	// wg 跟踪异步投递 goroutine，Close 时等待其退出。
	wg sync.WaitGroup
	// sendMu 串行化「异步投递登记（wg.Add）」与「Close 置位」，避免 wg.Add 与 wg.Wait
	// 竞态——Add 在 Wait 已开始后调用是 WaitGroup 的非法用法，会让 Close 漏等该 goroutine。
	sendMu sync.Mutex
	// closed 标记已关闭，关闭后拒绝新投递。
	closed atomic.Bool
	// stopCh 关闭信号，用于中断在途重试的退避等待。
	stopCh chan struct{}

	// 统计计数，便于观测与测试断言。
	statSent       atomic.Uint64
	statAcked      atomic.Uint64
	statRetried    atomic.Uint64
	statDeadLetter atomic.Uint64
}

// newReliableSender 构造可靠投递器。dlq 为 nil 时自动创建内存 DLQ。
func newReliableSender(cfg ReliableConfig, nodeID string, request ackRequester, dlq DeadLetterQueue) *reliableSender {
	cfg = cfg.normalize()
	if dlq == nil {
		dlq = NewMemoryDLQ(cfg.DLQCapacity)
	}
	return &reliableSender{
		cfg:     cfg,
		pending: newPendingTable(cfg.PendingLimit),
		idGen:   NewMsgIDGenerator(nodeID),
		request: request,
		dlq:     dlq,
		stopCh:  make(chan struct{}),
	}
}

// SetDeadLetterHandler 注册死信回调（并发安全，可在运行期替换）。
// 回调在事件入 DLQ 之后调用，用于告警等旁路处理。
func (s *reliableSender) SetDeadLetterHandler(fn DeadLetterHandler) {
	s.deadLock.Lock()
	s.deadFn = fn
	s.deadLock.Unlock()
}

// deadLetterHandler 读取当前死信回调。
func (s *reliableSender) deadLetterHandler() DeadLetterHandler {
	s.deadLock.RLock()
	defer s.deadLock.RUnlock()
	return s.deadFn
}

// DLQ 返回死信队列，供人工介入接口使用。
func (s *reliableSender) DLQ() DeadLetterQueue {
	s.deadLock.RLock()
	defer s.deadLock.RUnlock()
	return s.dlq
}

// SetDLQ 替换死信队列实现（并发安全：toDeadLetter 与 DLQ 读同一字段时持同一把锁）。
func (s *reliableSender) SetDLQ(q DeadLetterQueue) {
	if s == nil || q == nil {
		return
	}
	s.deadLock.Lock()
	s.dlq = q
	s.deadLock.Unlock()
}

// ListPending 返回当前在途（含重试中）的投递快照。
func (s *reliableSender) ListPending() []PendingInfo { return s.pending.list() }

// log 输出日志（未注入 logf 时静默）。
func (s *reliableSender) log(level, format string, args ...any) {
	if s.logf != nil {
		s.logf(level, format, args...)
	}
}

// Send 可靠投递一条事件。

// cfg.Async=true 时立即返回 nil，投递与重试在后台 goroutine 执行（失败进 DLQ）；
// 否则同步阻塞直到成功、失败或 ctx 取消。
func (s *reliableSender) Send(ctx context.Context, evt *CrossNodeEvent) error {
	if evt == nil {
		return errors.New("event: 待投递事件为空")
	}
	if s.closed.Load() {
		return errors.New("event: 可靠投递器已关闭")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// 补齐 MsgID：只在首次生成，重投时保持不变（接收方据此去重）。
	if evt.MsgID == "" {
		evt.MsgID = s.idGen.Next()
	}

	if !s.cfg.Async {
		return s.deliver(ctx, evt)
	}

	// 异步投递：与调用方 ctx 解耦（业务请求 ctx 很快结束，不应中断后台重试），
	// 仅受 Close 信号控制。
	//
	// 登记与 closed 判定必须在同一临界区：否则 Close 可能在 Add 之前就
	// 走完 wg.Wait（此时计数为 0 立即返回），本次 goroutine 在 Close 之后才启动、
	// 且永不被等待（WaitGroup 的 Add/Wait 竞态）。
	s.sendMu.Lock()
	if s.closed.Load() {
		s.sendMu.Unlock()
		return errors.New("event: 可靠投递器已关闭")
	}
	s.wg.Add(1)
	s.sendMu.Unlock()
	go func() {
		defer s.wg.Done()
		bg, cancel := s.backgroundCtx()
		defer cancel()
		if err := s.deliver(bg, evt); err != nil {
			s.log("error", "跨服事件异步投递失败 msg_id=%s target=%s: %v", evt.MsgID, evt.TargetNode, err)
		}
	}()
	return nil
}

// SendSync 同步可靠投递：忽略 cfg.Async，强制阻塞直到收到 ACK、失败或 ctx 取消。
// 用于 per-key 串行队列的 worker，确保同一 key 的实际网络发送严格串行。
func (s *reliableSender) SendSync(ctx context.Context, evt *CrossNodeEvent) error {
	if evt == nil {
		return errors.New("event: 待投递事件为空")
	}
	if s.closed.Load() {
		return errors.New("event: 可靠投递器已关闭")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// 补齐 MsgID：只在首次生成，重投时保持不变（接收方据此去重）。
	if evt.MsgID == "" {
		evt.MsgID = s.idGen.Next()
	}
	return s.deliver(ctx, evt)
}

// backgroundCtx 构造受 Close 控制的后台 ctx。
func (s *reliableSender) backgroundCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-s.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// deliver 执行「请求 → ACK → 重试 → 死信」的核心闭环。
func (s *reliableSender) deliver(ctx context.Context, evt *CrossNodeEvent) error {
	ent := &pendingEntry{
		msgID:      evt.MsgID,
		targetNode: evt.TargetNode,
		eventType:  evt.EventType,
		uid:        evt.UID,
		firstAt:    time.Now(),
	}
	if err := s.pending.add(ent); err != nil {
		metricCrossDropped(dropReasonPending)
		s.log("error", "跨服事件在途表已满，拒绝投递并转入死信 msg_id=%s target=%s", evt.MsgID, evt.TargetNode)
		// 不静默丢弃：与「重试耗尽」同路径入死信，保留证据供人工重投。
		s.toDeadLetter(evt, PendingInfo{
			MsgID:      evt.MsgID,
			TargetNode: evt.TargetNode,
			EventType:  evt.EventType,
			UID:        evt.UID,
			FirstAt:    ent.firstAt,
		}, err)
		return err
	}
	metricPending(s.pending.len())
	defer func() {
		s.pending.remove(evt.MsgID)
		metricPending(s.pending.len())
	}()

	deliverStart := time.Now()
	err := retry.Do(ctx, s.cfg.RetryPolicy(), func(attempt int) error {
		evt.Attempt = attempt
		if attempt > 1 {
			s.statRetried.Add(1)
			metricCrossRetried()
			s.log("warn", "跨服事件重投 msg_id=%s target=%s attempt=%d", evt.MsgID, evt.TargetNode, attempt)
		}
		s.statSent.Add(1)
		metricCrossSent()

		// request/reply：发出去并等待接收方 msg.Respond 回的 ACK。
		ack, rerr := s.request(ctx, evt, s.cfg.AckTimeout)
		if rerr != nil {
			ent.update(attempt, rerr)
			// 传输失败（含超时无应答）均可重试。
			return fmt.Errorf("投递失败: %w", rerr)
		}

		switch ack.Status {
		case AckOK, AckDuplicate:
			// 重复也算成功：说明接收方此前已处理过（at-least-once + 幂等）。
			s.statAcked.Add(1)
			metricCrossAcked()
			return nil
		case AckRejected:
			// 明确不可重试，立即终止且不进死信重试循环。
			metricCrossRejected()
			aerr := fmt.Errorf("%w: %s", ErrAckRejected, ack.Error)
			ent.update(attempt, aerr)
			return retry.Permanent(aerr)
		default:
			aerr := fmt.Errorf("接收方处理失败: %s", ack.Error)
			ent.update(attempt, aerr)
			return aerr
		}
	})

	// 端到端耗时（含全部重试），在所有返回分支之前统一记录一次。
	metricCrossDuration(deliverStart, err)

	if err != nil {
		// ctx 取消/停机不算死信（属于主动中止）。
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		// 接收方明确拒绝：不可重试，也不入死信（业务语义上的正常拒绝）。
		if errors.Is(err, ErrAckRejected) {
			return err
		}
		s.toDeadLetter(evt, ent.snapshot(), err)
		// 内层用 %w：errors.Is/As 可穿透到原始传输错误，便于上层按根因决策。
		return fmt.Errorf("%w: msg_id=%s target=%s: %w", ErrDeliveryFailed, evt.MsgID, evt.TargetNode, err)
	}
	return nil
}

// toDeadLetter 重试耗尽处理：入死信队列 + 打 Error 日志 + 触发回调（回调 panic 不外溢）。

// 刻意不接收调用方 ctx：此时业务 ctx 往往已超时/取消，若复用会导致死信写不进去，
// 内部使用独立的带超时 ctx，保证「重试失败的事件一定留痕」。
func (s *reliableSender) toDeadLetter(evt *CrossNodeEvent, info PendingInfo, reason error) {
	s.statDeadLetter.Add(1)
	metricCrossDeadLetter()
	s.log("error", "跨服事件进入死信 msg_id=%s target=%s type=%s attempt=%d reason=%v",
		evt.MsgID, evt.TargetNode, evt.EventType, evt.Attempt, reason)

	dl := DeadLetter{
		ID:       evt.MsgID,
		MsgID:    evt.MsgID,
		Event:    evt.Clone(),
		Reason:   reason.Error(),
		Attempts: evt.Attempt,
		FirstAt:  info.FirstAt,
		LastAt:   time.Now(),
	}
	// 读 dlq 时持 deadLock：SetDLQ（运行期替换实现）与这里读写同一字段。
	dlq := s.DLQ()
	if dlq != nil {
		// DLQ 写入用独立 ctx：此时业务 ctx 可能已超时，不能因此丢死信。
		dctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := dlq.Push(dctx, dl); err != nil {
			s.log("error", "死信入队失败 msg_id=%s: %v", evt.MsgID, err)
		}
		cancel()
	}

	fn := s.deadLetterHandler()
	if fn == nil {
		return
	}
	safe.SafeRun(func() {
		fn(dl)
	})
}

// Close 停止接收新投递并等待在途投递结束（最多等待 timeout）。
func (s *reliableSender) Close(timeout time.Duration) error {
	// 与 Send 的 wg.Add 互斥：置位后不可能再有新的 Add，wg.Wait 才是可靠的。
	s.sendMu.Lock()
	if s.closed.Swap(true) {
		s.sendMu.Unlock()
		return nil // 幂等
	}
	close(s.stopCh)
	s.sendMu.Unlock()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("event: 关闭可靠投递器超时，仍有 %d 条在途", s.pending.len())
	}
}

// Stats 返回投递统计快照，用于观测与测试断言。
func (s *reliableSender) Stats() ReliableStats {
	return ReliableStats{
		Sent:       s.statSent.Load(),
		Acked:      s.statAcked.Load(),
		Retried:    s.statRetried.Load(),
		DeadLetter: s.statDeadLetter.Load(),
		Pending:    s.pending.len(),
	}
}

// ReliableStats 可靠投递统计快照。
type ReliableStats struct {
	Sent       uint64 `json:"sent"`        // 累计发出次数（含重投）
	Acked      uint64 `json:"acked"`       // 累计收到成功 ACK 次数
	Retried    uint64 `json:"retried"`     // 累计重投次数
	DeadLetter uint64 `json:"dead_letter"` // 累计死信条数
	Pending    int    `json:"pending"`     // 当前在途条数
}
