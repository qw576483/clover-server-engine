package event

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/foundation/metrics"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// busMetrics 是 event Bus 模块的埋点句柄（进程级单例）。
var busMetrics = metrics.ForModule(metrics.ModuleEvent)

// maxPublishMetricKinds 指标 label 的**基数上限**：事件类型最多记这么多种。
const maxPublishMetricKinds = 256

// metricKindOther 超出基数上限后的归并 label 值。
const metricKindOther = "other"

var (
	publishKindsMu   sync.Mutex
	publishKinds     = make(map[string]struct{})
	publishKindsFull bool
)

// metricPublish 本地事件发布计数（按事件类型）。
//
// eventType 直接作为 label 值。调用方（NewEvent 的 typ）本应只用有限枚举（见 NewEvent 说明），
// 但**只靠约定不够**：一旦有人把 playerID / uid / connID 这类动态值拼进事件名，
// 时间序列与指标注册表会无界膨胀（metrics 基数纪律见 metrics/naming.go）。
// 因此总线侧自己做**基数护栏**：已知类型最多登记 maxPublishMetricKinds 种，
// 超出的统一归入 "other"，并只在该护栏首次生效时告警一次。
func metricPublish(eventType string) {
	busMetrics.Count("publish_total", metrics.LabelKind, boundedEventKind(eventType))
}

// boundedEventKind 返回用于 metrics label 的事件类型：超出基数上限时返回 "other"。
func boundedEventKind(eventType string) string {
	publishKindsMu.Lock()
	if _, known := publishKinds[eventType]; known {
		publishKindsMu.Unlock()
		return eventType
	}
	if len(publishKinds) < maxPublishMetricKinds {
		publishKinds[eventType] = struct{}{}
		publishKindsMu.Unlock()
		return eventType
	}
	firstFold := !publishKindsFull
	publishKindsFull = true
	registered := len(publishKinds)
	publishKindsMu.Unlock()
	if firstFold {
		logger.Warnf("event: publish metric label cardinality reached %d, folding further event types into %q — "+
			"事件类型必须是有限枚举（见 NewEvent 与 metrics/naming.go 的基数纪律）",
			registered, metricKindOther)
	}
	return metricKindOther
}

// BusHandler 事件处理器。返回 error 仅用于记录，不影响同一类型下的其它订阅者。
type BusHandler func(ctx context.Context, e Envelope) error

// Bus 事件总线：发布与订阅。当前为进程内实现，未来可桥接 NATS 而不改动业务代码。
type Bus interface {
	// Publish 发布事件，分发给所有订阅该类型的 BusHandler（精确匹配 + 模式匹配）。
	Publish(e Envelope)
	// Subscribe 订阅指定类型的事件，返回取消订阅的函数。
	Subscribe(typ string, h BusHandler) (unsubscribe func())
	// SubscribePattern 按通配符模式订阅事件，返回取消订阅的函数。
	// 模式支持：* 单级通配符、** 多级通配符（类似 MQTT topic）。
	// 示例：SubscribePattern("player.*", h)、SubscribePattern("room.*.join", h)
	SubscribePattern(pattern string, h BusHandler) (unsubscribe func())
	// Close 关闭总线并清空所有订阅。
	Close()
}

// Matcher 模式匹配接口——暴露 MatchPattern 供外部工具使用。
type Matcher interface {
	// MatchPattern 检查模式是否匹配事件类型。
	MatchPattern(pattern, eventType string) bool
}

// BusOption 总线可选配置。
type BusOption func(*inprocBus)

// WithAsync 启用异步派发：每个 BusHandler 在独立 goroutine（safe.GoSafe）中执行。
// 默认同步派发（Publish 的 goroutine 内按顺序执行全部 BusHandler）。
func WithAsync() BusOption {
	return func(b *inprocBus) { b.async = true }
}

// patternEntry 模式订阅条目——一个 pattern 对应一组 handler。
type patternEntry struct {
	handlers map[int64]BusHandler // id → handler
}

type inprocBus struct {
	mu              sync.RWMutex
	handlers        map[string]map[int64]BusHandler // exact subscriptions: type → {id → handler}
	patternHandlers map[string]*patternEntry        // pattern subscriptions: pattern → entry
	async           bool
	nextID          int64
	closed          atomic.Bool
}

// NewBus 构造进程内事件总线。
func NewBus(opts ...BusOption) Bus {
	b := &inprocBus{
		handlers:        make(map[string]map[int64]BusHandler),
		patternHandlers: make(map[string]*patternEntry),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Subscribe 注册订阅，返回取消函数。
func (b *inprocBus) Subscribe(typ string, h BusHandler) func() {
	b.mu.Lock()
	if b.handlers[typ] == nil {
		b.handlers[typ] = make(map[int64]BusHandler)
	}
	id := b.nextID
	b.nextID++
	b.handlers[typ][id] = h
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		// Close 后复用 Subscribe 同 typ 会重建 map，旧闭包若延迟调用需判 nil 防止误删新订阅。
		if m, ok := b.handlers[typ]; ok {
			delete(m, id)
			if len(m) == 0 {
				delete(b.handlers, typ)
			}
		}
		b.mu.Unlock()
	}
}

// SubscribePattern 按通配符模式订阅事件，返回取消函数。
// 模式下发时，精确匹配的处理函数优先执行。
func (b *inprocBus) SubscribePattern(pattern string, h BusHandler) func() {
	b.mu.Lock()
	entry, ok := b.patternHandlers[pattern]
	if !ok {
		entry = &patternEntry{handlers: make(map[int64]BusHandler)}
		b.patternHandlers[pattern] = entry
	}
	id := b.nextID
	b.nextID++
	entry.handlers[id] = h
	b.mu.Unlock()

	return func() {
		b.mu.Lock()
		if entry, ok := b.patternHandlers[pattern]; ok {
			delete(entry.handlers, id)
			if len(entry.handlers) == 0 {
				delete(b.patternHandlers, pattern)
			}
		}
		b.mu.Unlock()
	}
}

// UnsubscribePatternByID 通过订阅 ID 取消模式订阅（O(1)，无 reflect 开销）。
func (b *inprocBus) UnsubscribePatternByID(pattern string, id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry, ok := b.patternHandlers[pattern]
	if !ok {
		return
	}
	delete(entry.handlers, id)
	if len(entry.handlers) == 0 {
		delete(b.patternHandlers, pattern)
	}
}

// entry handler 排序条目（复用类型，避免 Publish 内部重复定义）。
type entry struct {
	id int64
	h  BusHandler
}

// entryPool 复用 entry 切片，减少 Publish 热路径分配。
var entryPool = sync.Pool{
	New: func() any {
		s := make([]entry, 0, 8)
		return &s
	},
}

// Publish 发布事件到所有订阅者。总线关闭后静默丢弃。
// 分发顺序：先精确匹配，后模式匹配（保证精确匹配优先）。
func (b *inprocBus) Publish(e Envelope) {
	b.mu.RLock()
	// closed 检查必须在 RLock 临界区内：否则 Close 可在 Load 通过后、RLock 获取前的
	// 窗口内清空 handlers，导致 dispatch 在已关闭总线上执行触碰已释放资源。
	if b.closed.Load() {
		b.mu.RUnlock()
		return
	}
	// 发布计数：总线已关闭的静默丢弃不计入，保证该指标等于「真实派发的事件数」。
	metricPublish(e.Type)

	// 收集精确匹配的 handler
	m := b.handlers[e.Type]
	esPtr := entryPool.Get().(*[]entry)
	es := (*esPtr)[:0]
	for id, h := range m {
		es = append(es, entry{id: id, h: h})
	}

	// 收集模式匹配的 handler
	var patternES []entry
	for pattern, pe := range b.patternHandlers {
		if MatchPattern(pattern, e.Type) {
			for id, h := range pe.handlers {
				patternES = append(patternES, entry{id: id, h: h})
			}
		}
	}
	b.mu.RUnlock()

	// 保证同步派发时按订阅顺序（id 升序）执行；异步派发的并发顺序仍由调度决定。
	sort.Slice(es, func(i, j int) bool { return es[i].id < es[j].id })
	sort.Slice(patternES, func(i, j int) bool { return patternES[i].id < patternES[j].id })

	// 合并为统一派发顺序：精确匹配 → 模式匹配
	allEntries := make([]entry, 0, len(es)+len(patternES))
	allEntries = append(allEntries, es...)
	allEntries = append(allEntries, patternES...)

	// 归还精确匹配切片到 pool：清空底层数组残留元素，防止 handler 闭包被 GC 延迟回收。
	for i := range es {
		es[i] = entry{}
	}
	*esPtr = es
	entryPool.Put(esPtr)

	for _, ent := range allEntries {
		h := ent.h
		if b.async {
			h := h
			// 异步分发时复制 Envelope，并对全部已知 Payload 类型的字节切片做深拷贝，
			// 避免异步 goroutine 与 Publish 调用方共享底层字节数组。
			eCopy := e
			eCopy.Payload = clonePayload(e.Payload)
			safe.GoSafe(func() { b.dispatch(h, eCopy) })
		} else {
			b.dispatch(h, e)
		}
	}
}

// clonePayload 深拷贝信封载荷中的字节切片（异步派发防共享）。
// InternalServerPayload.Object 为任意业务对象，无法通用深拷贝，保持引用共享，
// 由发布方保证发布后不再修改（见字段注释）。未知类型原样返回。
func clonePayload(p any) any {
	switch cp := p.(type) {
	case *ClientRequestPayload:
		return &ClientRequestPayload{Body: append([]byte(nil), cp.Body...)}
	case *ServerNotifyPayload:
		c := *cp
		c.Body = append([]byte(nil), cp.Body...)
		return &c
	case *InternalServerPayload:
		c := *cp
		c.Body = append([]byte(nil), cp.Body...)
		return &c
	default:
		return p
	}
}

// dispatch 执行单个 BusHandler，错误仅记录不中断链条。
// 同步模式下也需 recover：否则 handler panic 会直接击垮发布者所在 goroutine。
func (b *inprocBus) dispatch(h BusHandler, e Envelope) {
	safe.SafeRun(func() {
		ctx := e.Ctx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := h(ctx, e); err != nil {
			logger.Errorf("event bus handler error type=%s uid=%s: %v", e.Type, e.UID, err)
		}
	})
}

// Close 关闭总线并清空订阅。
func (b *inprocBus) Close() {
	b.closed.Store(true)
	b.mu.Lock()
	b.handlers = make(map[string]map[int64]BusHandler)
	b.patternHandlers = make(map[string]*patternEntry)
	b.mu.Unlock()
}

// MatchPattern 通配符模式匹配。同时实现 Matcher 接口。
// 导出为同一包内可直接使用；外部通过 pkg/transport/event.MatchPattern 调用。
func (b *inprocBus) MatchPattern(pattern, eventType string) bool {
	return MatchPattern(pattern, eventType)
}
