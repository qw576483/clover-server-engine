package event

import (
	"context"
	"encoding/json"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/data/visibility"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// 三阶段自动储存管道：Persist → Sync → Mirror
// commitEdits 提交本请求中所有可修改加载的数据修改：落库 + 同步推送 + 跨服镜像。
// 只会在全部保存成功后才推送同步，避免客户端先收到同步再收到失败回包的矛盾状态。
func (l *Logic) commitEdits(c *Ctx) int {
	if c.bag == nil || c.bag.session == nil {
		return 0
	}
	// Phase 1: Persist —— 落库
	saved, failed := c.bag.session.Commit(c.ctx)
	if failed > 0 {
		logger.Errorf("logic: %d auto-save edits failed, skipping push", failed)
		return failed
	}
	// Phase 2: Sync —— 推送客户端
	if len(saved) > 0 && l.syncPub != nil && l.syncSubject != "" && !c.reply.getNoPush() {
		l.pushBatch(c, saved)
	}
	// Phase 3: Mirror —— 跨服内存镜像
	if len(saved) > 0 && l.mirrorPub != nil && l.mirrorSubject != "" {
		l.publishMirror(c.ctx, saved)
	}
	return 0
}

// Phase 2: Sync（NATS → 网关 → 客户端）
// pushBatch 批量推送所有同步项：按 Owner 路由分发。
func (l *Logic) pushBatch(c *Ctx, edits []data.CommitResult) {
	var items []SyncItem
	bcastByServer := make(map[string][]SyncItem)

	for _, pe := range edits {
		if !visibility.IsClientVisible(pe.Key.Owner, pe.Key.Type) {
			continue
		}
		body := buildSyncBody(pe)
		if body == nil {
			continue
		}
		msgID, hasReg := l.syncReg.SyncMsgID(pe.Key.Type)
		if !hasReg {
			msgID = proto.EPushDataSync
		}
		it := SyncItem{Type: pe.Key.Type, MsgID: msgID, Body: body, HasReg: hasReg}
		if pe.Key.Owner == data.OwnerServer {
			bcastByServer[pe.Key.ID] = append(bcastByServer[pe.Key.ID], it)
		} else {
			items = append(items, it)
		}
	}

	if len(items) > 0 {
		l.pushBatchGroup(c, l.syncSubject, c.TargetID(), items)
	}
	for serverID, serverItems := range bcastByServer {
		subject := l.syncSubject + ".broadcast.server." + serverID
		l.pushBatchGroup(c, subject, proto.TargetAll, serverItems)
	}
}

// pushBatchGroup 对一组 SyncItem 按注册/未注册分发推送。
func (l *Logic) pushBatchGroup(c *Ctx, subject, target string, items []SyncItem) {
	var batch map[string]json.RawMessage
	for _, it := range items {
		if it.HasReg {
			l.pushOne(c, subject, target, it.MsgID, it.Body)
		} else {
			if batch == nil {
				batch = make(map[string]json.RawMessage)
			}
			batch[it.Type] = it.Body
		}
	}
	if len(batch) > 0 {
		batched, err := ujson.Marshal(batch)
		if err != nil {
			logger.Errorf("logic: auto-sync batch marshal failed: %v", err)
			return
		}
		l.pushOne(c, subject, target, proto.EPushDataSync, batched)
	}
}

// pushOne 推送一条 NotifyPush。
// auto-sync 管线默认使用 Reliable（数据同步丢包=客户端状态不一致）。
func (l *Logic) pushOne(c *Ctx, subject, target string, msgID uint32, body []byte) {
	np := proto.NotifyPush{
		Target:       target,
		MsgID:        msgID,
		Body:         body,
		TraceID:      c.TraceID(),
		DeliveryMode: proto.DeliveryModeReliable,
	}
	buf, err := proto.EncodeNotifyPush(&np)
	if err != nil {
		logger.Errorf("logic: encode NotifyPush failed: %v", err)
		return
	}
	if err := l.syncPub.Publish(subject, buf); err != nil {
		logger.Errorf("logic: push to %s failed: %v", subject, err)
	}
}

// publishOne 点对点自动同步（走 syncPub/syncSubject）。

//lint:ignore U1000 P2P同步构建块
func (l *Logic) publishOne(c *Ctx, msgID uint32, body []byte) {
	l.pushOne(c, l.syncSubject, c.TargetID(), msgID, body)
}

// buildSyncBody 构造同步消息体：优先级 CommitDiff → 字段级 diff → 全量兜底。
func buildSyncBody(pe data.CommitResult) []byte {
	if sd, ok := pe.Val.(interface{ CommitDiff() ([]byte, bool) }); ok {
		if body, _ := sd.CommitDiff(); body != nil {
			return body
		}
	}
	// 复用提交期已序列化好的全量 JSON（pe.JSON）：此前这里又 Marshal 一次，
	// 而落库早已序列化过同一份数据。仅在提交期序列化失败（JSON 为 nil）时兜底。
	cur := pe.JSON
	if len(cur) == 0 {
		var err error
		cur, err = ujson.Marshal(pe.Val)
		if err != nil {
			logger.Errorf("logic: auto-sync marshal %v failed: %v", pe.Key, err)
			return nil
		}
	}
	if body := jsonFieldDiff(pe.Snapshot, cur); body != nil {
		return body
	}
	// diff 为空也直接用同一份 cur：原实现这里又 Marshal 了一遍，等于每次同步都序列化两次。
	return cur
}

// jsonFieldDiff 对比两个 JSON 对象，返回仅包含变更字段的 JSON。
func jsonFieldDiff(snap, cur []byte) []byte {
	if len(snap) == 0 || len(cur) == 0 {
		return nil
	}
	var mOld, mNew map[string]json.RawMessage
	if err := ujson.Unmarshal(snap, &mOld); err != nil {
		return nil
	}
	if err := ujson.Unmarshal(cur, &mNew); err != nil {
		return nil
	}
	diff := make(map[string]json.RawMessage, len(mNew))
	for k, v := range mNew {
		old, ok := mOld[k]
		if !ok || string(old) != string(v) {
			diff[k] = v
		}
	}
	if len(diff) == 0 {
		return nil
	}
	b, _ := ujson.Marshal(diff)
	return b
}

// MirrorEvent 跨服内存镜像事件（NATS payload）。
type MirrorEvent struct {
	Owner data.OwnerType `json:"owner"`
	ID    string         `json:"id"`
	Type  string         `json:"type"`
	Data  []byte         `json:"data"` // 全量 JSON
}

// publishMirror 将 commitEdits 变更的全量数据广播到 mirror subject。
// 接收方调用 Store.ReceiveMirror 直接写入进程内存（mmo/memory 模式不标脏）。
func (l *Logic) publishMirror(_ context.Context, edits []data.CommitResult) {
	for _, pe := range edits {
		// 镜像要的是**全量** JSON：优先复用提交期已序列化好的 pe.JSON
		// （buildSyncBody 可能返回的是字段级 diff，不能拿来当镜像体）。
		full := pe.JSON
		if len(full) == 0 {
			var err error
			full, err = ujson.Marshal(pe.Val)
			if err != nil {
				logger.Errorf("logic: mirror marshal %v failed: %v", pe.Key, err)
				continue
			}
		}
		evt := MirrorEvent{
			Owner: pe.Key.Owner,
			ID:    pe.Key.ID,
			Type:  pe.Key.Type,
			Data:  full,
		}
		buf, err := ujson.Marshal(evt)
		if err != nil {
			logger.Errorf("logic: mirror encode %v failed: %v", pe.Key, err)
			continue
		}
		if err := l.mirrorPub.Publish(l.mirrorSubject, buf); err != nil {
			logger.Errorf("logic: mirror publish %v failed: %v", pe.Key, err)
		}
	}
}
