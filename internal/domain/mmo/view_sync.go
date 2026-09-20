package mmo

import (
	"context"
	"sync"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	internalaoi "github.com/qw576483/clover-server-engine/internal/domain/mmo/spatial/aoi"
	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
)

// snapshotIOTimeout 进视野快照拉取的兜底超时。
//
// 这条路径在视野事件的关键链路上（Enter 时拉快照），用 context.Background() 意味着
// 一次数据库抖动就能把调用线程永久挂住。给一个上限，超时走降级（不带快照下发）。
const snapshotIOTimeout = 2 * time.Second

// snapshotCtx 生成带超时的快照拉取 context。
func snapshotCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), snapshotIOTimeout)
}

// 视野同步：事件类型 + 批处理池 + 推送管线
// viewPush 是视野推送的内部载体，线上传输一律走 viewproto 二进制编码。
type viewPush struct {
	Target       uint64
	MsgID        uint32
	Body         []byte
	Event        string
	Watcher      uint64
	Object       uint64
	SnapshotBin  map[string][]byte
	DeliveryMode proto.DeliveryMode // 传输模式：0=尽力而为，1=可靠传输，2=持久化
}

type viewBatch struct {
	watchers map[uint64]*watcherBatch
	snaps    map[snapCacheKey]cachedSnap
}

type watcherBatch struct {
	enter map[uint64]data.OwnerType
	leave map[uint64]struct{}
}

type cachedSnap struct {
	snapBin map[string][]byte
	err     error
}

var (
	viewBatchPool     = sync.Pool{New: func() any { return &viewBatch{} }}
	watcherBatchPool  = sync.Pool{New: func() any { return &watcherBatch{} }}
	watchersMapPool   = sync.Pool{New: func() any { return make(map[uint64]*watcherBatch) }}
	enterMapPool      = sync.Pool{New: func() any { return make(map[uint64]data.OwnerType) }}
	leaveMapPool      = sync.Pool{New: func() any { return make(map[uint64]struct{}) }}
	snapMapPool       = sync.Pool{New: func() any { return make(map[snapCacheKey]cachedSnap) }}
	viewPushSlicePool = sync.Pool{New: func() any { b := make([]viewPush, 0, 64); return &b }}
)

// onViewChange AOI 视野变化 → 网络推送。同一 instance 内才有视野。
// viewBatch 的 nil 检查与 map 写入必须在 viewMu 保护下进行，
// 避免与 BeginBatch 的 viewBatch 重置发生数据竞争。
func (s *Scene) onViewChange(instanceID uint32, watcher, target object.ObjectID, ev internalaoi.Event) {
	if s.sm.opts.pub == nil {
		return
	}
	event := "leave"
	if ev == internalaoi.EnterView {
		event = "enter"
	}

	// viewMu 保护 viewBatch 指针与其内部的 watchers map，与 BeginBatch/EndBatch 互斥
	s.viewMu.Lock()
	if s.viewBatch != nil {
		ownerType := data.OwnerPlayer
		if event == "enter" {
			l := s.Instance(instanceID)
			if l != nil {
				l.mu.RLock()
				if k, ok := l.kinds[target.Seq]; ok {
					ownerType = k
				}
				l.mu.RUnlock()
			}
		}
		wb, ok := s.viewBatch.watchers[watcher.Seq]
		if !ok {
			wb = watcherBatchPool.Get().(*watcherBatch)
			wb.enter = enterMapPool.Get().(map[uint64]data.OwnerType)
			wb.leave = leaveMapPool.Get().(map[uint64]struct{})
			s.viewBatch.watchers[watcher.Seq] = wb
		}
		if event == "enter" {
			delete(wb.leave, target.Seq)
			wb.enter[target.Seq] = ownerType
		} else {
			delete(wb.enter, target.Seq)
			wb.leave[target.Seq] = struct{}{}
		}
		s.viewMu.Unlock()
		return
	}
	s.viewMu.Unlock()

	// 视野进出是「不可重建」的状态：漏掉一条，客户端就永远不知道这个实体存在（引擎不重发全量视野）。
	// 因此走可靠传输；只有高频位置增量才适合 BestEffort。
	p := viewPush{Event: event, Watcher: watcher.Seq, Object: target.Seq, DeliveryMode: proto.DeliveryModeReliable}
	if ev == internalaoi.EnterView && s.sm.opts.entityAcc != nil {
		ownerType := s.ownerTypeOf(target.Seq)
		ctx, cancel := snapshotCtx()
		snap, err := s.sm.opts.entityAcc.SnapshotClientBinary(ctx, ownerType, conv.FormatUint(target.Seq))
		cancel()
		if err == nil {
			p.SnapshotBin = snap
		} else {
			// 快照拉取失败属降级路径：客户端会拿到「先进视野、稍后由常规同步补齐」，
			// 不打日志的话「玩家看到空实体」这种现象在线上无从追查。
			viewPushFailf("mmo: scene %d snapshot for obj=%d (kind=%s) failed: %v", s.id, target.Seq, ownerType, err)
		}
	}
	if err := s.sm.opts.pub.Publish(s.sm.opts.viewSubject, encodeViewBatch([]viewPush{p})); err != nil {
		viewPushFailf("mmo: scene %d publish view change failed (watcher=%d obj=%d): %v",
			s.id, watcher.Seq, target.Seq, err)
	}
}

func (s *Scene) beginViewBatch() { s.BeginBatch() }

// endViewBatch 与 beginViewBatch 配对，语义等同 EndBatch（见其注释：AOI 派发期间不能持 viewMu）。
func (s *Scene) endViewBatch() { s.EndBatch() }

// takeViewBatchLocked 在 viewMu 下摘下当前批次（调用方必须持有 viewMu）。
//
// 摘批与「发批」必须分开：发批要做快照拉取（数据库 IO）与 Publish，
// 持 viewMu 做 IO 会把整条视野推送管线钉在 IO 上——所有并发的 Enter/Move/Leave
// 都会卡在拿 viewMu 上，表现为「场景整体卡死几秒」。
func (s *Scene) takeViewBatchLocked() *viewBatch {
	b := s.viewBatch
	s.viewBatch = nil
	return b
}

// flushViewBatch 在**不持任何锁**的前提下发布一个视野批次（持锁部分见 takeViewBatchLocked）。
func (s *Scene) flushViewBatch(b *viewBatch) {
	if b == nil {
		return
	}
	defer func() {
		for _, wb := range b.watchers {
			for k := range wb.enter {
				delete(wb.enter, k)
			}
			for k := range wb.leave {
				delete(wb.leave, k)
			}
			enterMapPool.Put(wb.enter)
			leaveMapPool.Put(wb.leave)
			watcherBatchPool.Put(wb)
		}
		for k := range b.watchers {
			delete(b.watchers, k)
		}
		watchersMapPool.Put(b.watchers)
		for k := range b.snaps {
			delete(b.snaps, k)
		}
		snapMapPool.Put(b.snaps)
		b.watchers = nil
		b.snaps = nil
		viewBatchPool.Put(b)
	}()

	for watcher, wb := range b.watchers {
		if len(wb.enter) == 0 && len(wb.leave) == 0 {
			continue
		}
		events := viewPushSlicePool.Get().(*[]viewPush)
		*events = (*events)[:0]

		for objID, kind := range wb.enter {
			p := viewPush{Event: "enter", Watcher: watcher, Object: objID, DeliveryMode: proto.DeliveryModeReliable}
			if s.sm.opts.entityAcc != nil {
				snapKey := snapKeyOf(objID, kind)
				if cached, ok := b.snaps[snapKey]; ok {
					p.SnapshotBin = cached.snapBin
					if cached.err != nil {
						viewPushFailf("mmo: scene %d snapshot for obj=%d (kind=%s) failed: %v", s.id, objID, kind, cached.err)
					}
				} else {
					ctx, cancel := snapshotCtx()
					snap, err := s.sm.opts.entityAcc.SnapshotClientBinary(ctx, kind, conv.FormatUint(objID))
					cancel()
					sc := cachedSnap{snapBin: snap, err: err}
					b.snaps[snapKey] = sc
					if err == nil {
						p.SnapshotBin = snap
					} else {
						viewPushFailf("mmo: scene %d snapshot for obj=%d (kind=%s) failed: %v", s.id, objID, kind, err)
					}
				}
			}
			*events = append(*events, p)
		}
		for objID := range wb.leave {
			*events = append(*events, viewPush{Event: "leave", Watcher: watcher, Object: objID, DeliveryMode: proto.DeliveryModeReliable})
		}

		if len(*events) > 0 {
			if err := s.sm.opts.pub.Publish(s.sm.opts.viewSubject, encodeViewBatch(*events)); err != nil {
				viewPushFailf("mmo: scene %d publish view batch failed (watcher=%d, %d events): %v",
					s.id, watcher, len(*events), err)
			}
		}
		viewPushSlicePool.Put(events)
	}
}

// snapCacheKey 唯一标识一次快照拉取（对象 + 实体类型）。
// 不能把 kind 压缩成长度参与异或——OwnerType 是开放字符串（player/server/object 同为 6 字符，
// 业务还可自定义），等长 kind 会碰撞成同一个键，导致进视野时把别的实体类型的快照发给客户端。
type snapCacheKey struct {
	objID     uint64
	ownerType data.OwnerType
}

func snapKeyOf(objID uint64, ownerType data.OwnerType) snapCacheKey {
	return snapCacheKey{objID: objID, ownerType: ownerType}
}
