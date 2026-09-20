// 本文件提供优先级队列（PriorityQueue）扩展。

// PriorityQueue 实现四级优先级：Critical > High > Normal > Low。
// 高优先级任务优先出队，同优先级 FIFO。

// 使用示例：

// q := async.NewPriorityQueue()
// q.Push(async.PriorityCritical, criticalTask)
// q.Push(async.PriorityNormal, normalTask)
// task, ok := q.Pop()
package async

import (
	"container/list"
	"sync"

	"clover-server-engine/pkg/foundation/logger"
)

// Priority 任务优先级。
type Priority int

const (
	PriorityCritical Priority = iota // 最高（系统级事件）
	PriorityHigh                     // 高（玩家操作）
	PriorityNormal                   // 普通（定时任务）
	PriorityLow                      // 低（后台统计）
)

// numPriorities 优先级层级数。
const numPriorities = 4

// PriorityQueue 多级优先级队列。
type PriorityQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queues [numPriorities]*list.List
	closed bool
}

// NewPriorityQueue 创建优先级队列。
func NewPriorityQueue() *PriorityQueue {
	q := &PriorityQueue{
		queues: [numPriorities]*list.List{
			list.New(), list.New(), list.New(), list.New(),
		},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push 将任务推入指定优先级队列。
func (q *PriorityQueue) Push(p Priority, v interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	// 越界优先级（负数或 >= numPriorities）直接索引会 index out of range panic；
	// 降级到最低优先级并留日志，保证单次误用不会击穿调用方。
	idx := int(p)
	if idx < 0 || idx >= numPriorities {
		logger.Warnf("async.PriorityQueue.Push: priority %d out of range [0,%d), demoted to PriorityLow", int(p), numPriorities)
		idx = int(PriorityLow)
	}
	q.queues[idx].PushBack(v)
	q.cond.Signal()
}

// Pop 从高到低取任务，阻塞直到有任务或队列关闭。
func (q *PriorityQueue) Pop() (interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for !q.closed {
		for i := 0; i < numPriorities; i++ {
			if e := q.queues[i].Front(); e != nil {
				q.queues[i].Remove(e)
				return e.Value, true
			}
		}
		q.cond.Wait()
	}
	return nil, false
}

// TryPop 非阻塞弹出。
func (q *PriorityQueue) TryPop() (interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i := 0; i < numPriorities; i++ {
		if e := q.queues[i].Front(); e != nil {
			q.queues[i].Remove(e)
			return e.Value, true
		}
	}
	return nil, false
}

// Len 返回队列总长度。
func (q *PriorityQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	total := 0
	for i := 0; i < numPriorities; i++ {
		total += q.queues[i].Len()
	}
	return total
}

// Close 关闭队列，唤醒所有等待者。
func (q *PriorityQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.cond.Broadcast()
}
