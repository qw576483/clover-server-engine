package event

import "github.com/qw576483/clover-server-engine/pkg/shared/timeutil"

// // 跨服事件线格式（可靠投递版）

// CrossNodeEvent 在线格式之外追加可靠投递所需的元字段：
// MsgID（幂等去重键）、From（发送方节点）、Attempt（第几次投递）、
// TargetNode（目标节点）、Headers（trace 等上下文透传）。

// JSON 字段名与 CrossNodePayload 一致；可靠投递元字段均带 omitempty，
// 缺失时按未启用可靠投递处理（不去重、不等 ACK）。
// // CrossNodeEvent 跨服事件消息体。
type CrossNodeEvent struct {
	// —— 事件元字段（JSON 字段名与 CrossNodePayload 一致）——

	EventType string `json:"event_type"`
	MsgID2    uint32 `json:"msg_id"` // 协议消息号（注意：非幂等键；幂等键为 MsgID）
	UID       string `json:"uid"`
	TraceID   string `json:"trace_id"`
	Source    string `json:"source"` // 发送方 node ID
	Body      []byte `json:"body"`   // 原始载荷 JSON

	// —— 可靠投递元字段 ——

	// MsgID 全局唯一投递 ID（节点ID-时间戳-序号），接收方据此幂等去重。
	// 发送方未启用可靠投递时为空，接收方跳过去重。
	MsgID string `json:"rmid,omitempty"`
	// From 发送方节点 ID（等同 Source，独立字段便于可靠投递层自解释）。
	From string `json:"from,omitempty"`
	// TargetNode 目标节点 ID，仅发送方本地使用，不参与去重语义。
	TargetNode string `json:"target_node,omitempty"`
	// Attempt 第几次投递（从 1 开始）。
	Attempt int `json:"attempt,omitempty"`
	// SentAt 本次投递发出时间（毫秒时间戳），便于排查延迟。
	SentAt int64 `json:"sent_at,omitempty"`

	// Headers 与传输无关的上下文透传，trace 的 Inject(ctx)/Extract(carrier) 原语将写入/读取本字段。
	Headers map[string]string `json:"headers,omitempty"`
}

// Clone 深拷贝事件，避免重投/入死信时与在途投递共享可变状态。
func (e *CrossNodeEvent) Clone() *CrossNodeEvent {
	if e == nil {
		return nil
	}
	cp := *e
	if e.Body != nil {
		cp.Body = make([]byte, len(e.Body))
		copy(cp.Body, e.Body)
	}
	if e.Headers != nil {
		cp.Headers = make(map[string]string, len(e.Headers))
		for k, v := range e.Headers {
			cp.Headers[k] = v
		}
	}
	return &cp
}

// SetHeader 设置一个上下文透传头（nil map 自动初始化）。
func (e *CrossNodeEvent) SetHeader(k, v string) {
	if e == nil || k == "" {
		return
	}
	if e.Headers == nil {
		e.Headers = make(map[string]string, 4)
	}
	e.Headers[k] = v
}

// GetHeader 读取上下文透传头。
func (e *CrossNodeEvent) GetHeader(k string) string {
	if e == nil || e.Headers == nil {
		return ""
	}
	return e.Headers[k]
}

// stamp 填充本次投递的发送时间。
func (e *CrossNodeEvent) stamp() {
	if e != nil {
		e.SentAt = timeutil.NowMS()
	}
}

// toPayload 转换为 CrossNodePayload，用于复用本地分发逻辑。
func (e *CrossNodeEvent) toPayload() CrossNodePayload {
	if e == nil {
		return CrossNodePayload{}
	}
	return CrossNodePayload{
		EventType: e.EventType,
		MsgID:     e.MsgID2,
		UID:       e.UID,
		TraceID:   e.TraceID,
		Source:    e.Source,
		Body:      e.Body,
	}
}

// newCrossNodeEvent 由 payload 构造可靠投递事件。
func newCrossNodeEvent(p CrossNodePayload, targetNode string) *CrossNodeEvent {
	return &CrossNodeEvent{
		EventType:  p.EventType,
		MsgID2:     p.MsgID,
		UID:        p.UID,
		TraceID:    p.TraceID,
		Source:     p.Source,
		Body:       p.Body,
		From:       p.Source,
		TargetNode: targetNode,
	}
}
