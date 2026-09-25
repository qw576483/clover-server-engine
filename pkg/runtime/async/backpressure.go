// 本文件提供带背压策略的队列（BackpressureQueue）。

// 三种策略：
//   - Block: 队列满时阻塞等待
//   - Discard: 队列满时直接丢弃
//   - Degrade: 队列满时调用降级回调

// 使用示例：

// q := async.NewBackpressureQueue(100)
//
//	q.SetBackpressure(async.BackpressureDegrade, func(item any) {
//	    log.Printf("degraded: %v", item)
//	})
//
// ok := q.PushWithBackpressure(item)
package async

import (
	"sync"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// Backpressure 背压策略枚举。
type Backpressure int

const (
	BackpressureBlock   Backpressure = iota // 阻塞等待
	BackpressureDiscard                     // 直接丢弃
	BackpressureDegrade                     // 调用降级回调
)

// BackpressureQueue 带背压策略的队列。
type BackpressureQueue struct {
	mu           sync.Mutex
	cond         *sync.Cond
	data         []interface{}
	maxLen       int
	strategy     Backpressure
	degradeFn    func(interface{})
	closed       bool
	discardCount int64
	blockCount   int64
}

// NewBackpressureQueue 创建带背压策略的队列。
//
// maxLen 为队列容量上限：负数会使 `make(..., maxLen)` 直接 panic，这里先夹到 0 并留日志；
// 0 容量意味着任何 Push 都不可能成功（见 Push 的 Block 分支，不会无限期阻塞）。
func NewBackpressureQueue(maxLen int) *BackpressureQueue {
	if maxLen < 0 {
		logger.Warnf("async.NewBackpressureQueue: negative maxLen %d clamped to 0", maxLen)
		maxLen = 0
	}
	q := &BackpressureQueue{
		data:   make([]interface{}, 0, maxLen),
		maxLen: maxLen,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// SetBackpressure 设置背压策略和可选的降级回调。
func (q *BackpressureQueue) SetBackpressure(strategy Backpressure, degradeFn func(interface{})) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.strategy = strategy
	q.degradeFn = degradeFn
}

// Push 推入元素。根据背压策略处理队列满的情况。
func (q *BackpressureQueue) Push(v interface{}) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return false
	}

	if len(q.data) < q.maxLen {
		q.data = append(q.data, v)
		q.cond.Signal()
		return true
	}

	// 队列满，根据策略处理
	switch q.strategy {
	case BackpressureBlock:
		if q.maxLen <= 0 {
			// 零容量队列的等待条件永远不成立：等下去就是永久阻塞（默认策略即 Block），
			// 这里显式判失败并留日志，避免调用方静默挂死。
			q.discardCount++
			logger.Warnf("async.BackpressureQueue.Push: zero capacity queue cannot accept items, dropped")
			return false
		}
		q.blockCount++
		for len(q.data) >= q.maxLen && !q.closed {
			q.cond.Wait()
		}
		if q.closed {
			return false
		}
		q.data = append(q.data, v)
		q.cond.Signal()
		return true

	case BackpressureDiscard:
		q.discardCount++
		return false

	case BackpressureDegrade:
		// 在锁内取出回调再于锁外执行：直接读 q.degradeFn 并放进 GoSafe 闭包，
		// 会与 SetBackpressure 的加锁写构成 data race。
		fn := q.degradeFn
		q.discardCount++ // degrade 语义上等同丢弃（元素不入队）
		if fn != nil {
			safe.GoSafe(func() {
				fn(v)
			})
		}
		return false

	default:
		q.discardCount++
		return false
	}
}

// Pop 弹出元素，阻塞直到有数据或关闭。
func (q *BackpressureQueue) Pop() (interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.data) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.data) == 0 {
		return nil, false
	}
	v := q.data[0]
	q.data = q.data[1:]
	q.cond.Signal() // 唤醒可能阻塞的 Push
	return v, true
}

// Len 返回当前队列长度。
func (q *BackpressureQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.data)
}

// Stats 返回背压统计。
func (q *BackpressureQueue) Stats() (discarded, blocked int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.discardCount, q.blockCount
}

// Close 关闭队列。
func (q *BackpressureQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}
