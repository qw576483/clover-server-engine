package nats

import (
	"errors"
	"fmt"

	"clover-server-engine/pkg/shared/conv"
	ujson "clover-server-engine/pkg/shared/json"
	"github.com/nats-io/nats.go"
)

// ErrNoReply 表示消息无应答主题，无法执行 Respond。
var ErrNoReply = errors.New("nats: message has no reply subject")

// Msg 统一消息封装，对业务屏蔽底层 nats.Msg 与 JSON 序列化细节。
// 每条消息自动携带公共字段：服务名、时间戳、traceID。
type Msg struct {
	Subject string            // 主题
	Reply   string            // 应答主题（请求-应答场景）
	Data    []byte            // 消息体（JSON）
	Header  map[string]string // 公共/自定义头（取首值）
	// 保留每个 header 的全部值，避免多值 header（如多个 trace/route 标记）被丢弃。
	HeaderValues map[string][]string
	Service      string // 发送方服务名
	Timestamp    int64  // 发送方毫秒时间戳
	TraceID      string // 链路追踪 ID

	raw *nats.Msg // 底层消息，仅用于 Respond
}

// NewMsg 手动构造一条消息，通常用于测试或构造应答。
func NewMsg(subject string, data []byte) *Msg {
	return &Msg{Subject: subject, Data: data, Header: map[string]string{}}
}

// GetData 将消息体反序列化为目标结构体。
func (m *Msg) GetData(out any) error {
	if len(m.Data) == 0 {
		return nil
	}
	return ujson.Unmarshal(m.Data, out)
}

// Respond 向请求方回复应答消息（仅请求-应答场景有效）。
func (m *Msg) Respond(data any) error {
	if m.raw == nil || m.raw.Reply == "" {
		return ErrNoReply
	}
	b, err := ujson.Marshal(data)
	if err != nil {
		return err
	}
	return m.raw.Respond(b)
}

// String 便于日志打印。
func (m *Msg) String() string {
	return fmt.Sprintf("Msg{subject=%s service=%s trace_id=%s len=%d}", m.Subject, m.Service, m.TraceID, len(m.Data))
}

// wrapMsg 将底层 nats.Msg 转换为对外 Msg，提取公共字段。
func (c *Client) wrapMsg(m *nats.Msg) *Msg {
	h := make(map[string]string, len(m.Header))
	// 同时保留全部多值，避免丢弃 header 的第 2+ 个值。
	hv := make(map[string][]string, len(m.Header))
	for k, v := range m.Header {
		if len(v) > 0 {
			h[k] = v[0]
			vals := make([]string, len(v))
			copy(vals, v)
			hv[k] = vals
		}
	}
	return &Msg{
		Subject: m.Subject,
		Reply:   m.Reply,
		// Data 深拷贝底层缓冲：nats.go 的 Msg.Data 切片复用连接读缓冲，
		// 回调把 Msg 留存到返回之后再读会被后续消息串改
		//（同一条规则也适用于发布侧构造新消息时对负载的复用）。
		Data:         append([]byte(nil), m.Data...),
		Header:       h,
		HeaderValues: hv,
		Service:      m.Header.Get("service"),
		Timestamp:    conv.ToInt64(m.Header.Get("timestamp")),
		TraceID:      m.Header.Get("trace_id"),
		raw:          m,
	}
}
