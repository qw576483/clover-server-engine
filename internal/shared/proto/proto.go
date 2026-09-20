// #nosec G115 -- 定长线格式编解码：先判长度再转换，与客户端逐字节对齐。

// Package proto 定义 clover 的三套线上协议帧 / 信封 / 载体。
//
// 本包只描述"传输结构"，不绑定任何游戏业务（无玩家 / 房间 / 战斗结构体）。
//
// 三套协议：
// 1. 客户端 ↔ 网关：4 字节大端 requestID + 4 字节大端 msgID + body，网关对 body 完全透明。
// 2. 网关 ↔ 逻辑服：内部信封 GWLogicPacket，携带连接路由信息，便于逻辑服把回包 / 推送
// 精确路由回来源连接；body 为客户端帧中的业务负载。
// 3. 逻辑服 → 网关的异步 / 广播下发：经消息总线投递 NotifyPush，网关按 target 找到目标会话
// 后封装为客户端帧下发（data-event 自动同步的承载）。
//
// 线协议编码：
// - 客户端帧（EncodeClientFrame）始终为「4B 大端 requestID + 4B 大端 msgID + body」的紧凑二进制。
// - 网关 ↔ 逻辑服信封（GWLogicPacket）与内部推送（NotifyPush）走紧凑二进制
// （长度前缀字段），避免 JSON 反射 + base64 在高频内部链路上的开销；
// 业务 body 以原始字节透传，零额外编码。
//
// 设计遵循仓库规则（§五，2026-09 反转）：消息号等**类型真身在 pkg/shared/proto**，
// 本包 import pkg 复用它们，并只补引擎侧自用的内部信封（GWLogicPacket / NotifyPush 等）。
//
// ───────────────────────────────────────────────────────────────────────────
// 消息号（opcode）全局区间约定
// ───────────────────────────────────────────────────────────────────────────
// 所有消息号按「方向 / 角色」分为三段，业务与引擎互不越界：
//
// [1, …] 引擎 C2S（客户端 → 逻辑服）：EMsgLogin=2、EMsgResumeSession=3、EMsgRankQuery=4、EMsgBindUDP=5、EMsgUDPBindGrant=6、EMsgQueuePosition=7（见 msg.go）
//
//	号位 1 已作废保留（原 EMsgSignup，注册已移到账号服 HTTP）——列表见 pkg/shared/proto/msg.go。
//
// [4001, …] 引擎推送（逻辑服 → 客户端）：EPushPlayerFullSync=4001、EPushAlert=4002、EPushDataSync=4003、EPushRoomTakeover=4004、EPushSceneInfo=4005（见 push.go）
// [10001, …] 业务消息（各 demo def 包定义，C2S/回包/推送统一从此起）：实际取段见 demo 的 def 包，
// 如 MsgGetPlayerList=1000101、MsgCreatePlayer=1000102 …（此两值为约定示例，具体以 demo def 为准）。
//
// 回包约定：回包不占用独立消息号区间，客户端帧按 requestID 配对（回包帧 msgID 恒为 0），
// 客户端按 requestID 对号入座；错误回包保留特殊值 0xFFFFFFFF 供客户端识别。
//
// 约定要点：
// - 引擎占 [1,10000]（C2S 2~7、推送 4001~4005）；
// 引擎未来扩展的内部消息也只会在 [1,10000] 内，不会侵占业务段。
// - 业务消息（C2S / 回包 / 推送）统一从 10001 起，由 demo 的 def 包自行编排。
// - 回包按 requestID 配对，不使用独立回包消息号。
// - 业务消息号必须 >= InternalMsgMax+1（即 >= 10001），该约束由 app.Game.On 在绑定时统一校验。
package proto

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	pproto "clover-server-engine/pkg/shared/proto"
)

// 以下符号是「业务可见协议契约」，真身已上移到 pkg/shared/proto。
// 本包（引擎内部协议实现）反向依赖 pkg 并原样再导出，使引擎内部既有调用点
// 无需改动，且与业务共用同一份定义，杜绝两处常量漂移。
const (
	// RequestIDLen 请求关联 ID 字段长度。
	RequestIDLen = pproto.RequestIDLen
	// MsgIDLen 消息号字段长度。
	MsgIDLen = pproto.MsgIDLen
	// ClientFrameHeaderLen 客户端帧头总长度（requestID + msgID）。
	ClientFrameHeaderLen = pproto.ClientFrameHeaderLen
	// InternalMsgMax 引擎内部保留消息号上界。
	InternalMsgMax = pproto.InternalMsgMax
)

// C2S opcode，真身定义在 pkg/shared/proto/msg.go。
const (
	EMsgLogin         = pproto.EMsgLogin
	EMsgResumeSession = pproto.EMsgResumeSession
	EMsgRankQuery     = pproto.EMsgRankQuery
	EMsgBindUDP       = pproto.EMsgBindUDP
	EMsgUDPBindGrant  = pproto.EMsgUDPBindGrant
	EMsgQueuePosition = pproto.EMsgQueuePosition
)

// 推送 opcode，真身定义在 pkg/shared/proto/push.go。
const (
	EPushPlayerFullSync = pproto.EPushPlayerFullSync
	EPushAlert          = pproto.EPushAlert
	EPushDataSync       = pproto.EPushDataSync
	EPushRoomTakeover   = pproto.EPushRoomTakeover
	EPushSceneInfo      = pproto.EPushSceneInfo
)

// EncodeClientFrame 编码「客户端 ↔ 网关」帧：4B requestID + 4B msgID + body。
//
//	requestID != 0 → 请求/回包（客户端按 requestID 匹配）
//	requestID == 0 → 推送（客户端按 msgID 路由）
func EncodeClientFrame(requestID, msgID uint32, body []byte) []byte {
	buf := make([]byte, ClientFrameHeaderLen+len(body))
	binary.BigEndian.PutUint32(buf[:RequestIDLen], requestID)
	binary.BigEndian.PutUint32(buf[RequestIDLen:RequestIDLen+MsgIDLen], msgID)
	copy(buf[ClientFrameHeaderLen:], body)
	return buf
}

// DecodeClientFrame 解码客户端帧，返回 requestID、msgID 与 body。body 为新分配切片（拷贝）。
func DecodeClientFrame(buf []byte) (requestID, msgID uint32, body []byte, err error) {
	if len(buf) < ClientFrameHeaderLen {
		return 0, 0, nil, errProtoTooShort
	}
	requestID = binary.BigEndian.Uint32(buf[:RequestIDLen])
	msgID = binary.BigEndian.Uint32(buf[RequestIDLen : RequestIDLen+MsgIDLen])
	body = make([]byte, len(buf)-ClientFrameHeaderLen)
	copy(body, buf[ClientFrameHeaderLen:])
	return requestID, msgID, body, nil
}

// errProtoTooShort 协议帧过短（不足 requestID + msgID 长度）的错误。
var errProtoTooShort = errors.New("proto: frame too short")

// GWLogicPacket 网关 ↔ 逻辑服内部信封：携带连接路由信息，便于逻辑服把回包 / 推送
// 精确路由回来源连接。body 即上面的客户端帧。Owner 由网关在转发时填入（已识别的对象标识）。
type GWLogicPacket struct {
	ConnID    string `json:"conn_id"`            // 连接 ID（网关侧会话标识，字符串）
	RequestID uint32 `json:"request_id"`         // 请求关联 ID（客户端生成，回包原样带回；推送为 0）
	MsgID     uint32 `json:"msg_id"`             // 消息号（透明透传）
	Owner     string `json:"owner,omitempty"`    // 已绑定的目标标识（玩家 UID / 房间 ID 等），网关转发时填入
	Body      []byte `json:"body"`               // 客户端帧 body（不含 requestID + msgID）
	TraceID   string `json:"trace_id,omitempty"` // 全链路追踪 ID（网关生成，逻辑服继承并随回包回流）
	Line      string `json:"line,omitempty"`     // 线路标识：tcp/ws/udp/quic/wt
}

// maxFieldLen 单字段最大长度（防止恶意超长字段导致内存膨胀）。
// MMO 内部帧 body 远小于此，取 16MB，恶意超长直接判溢出拒绝。
const maxFieldLen = 16 << 20 // 16MB

// errProtoOverflow 字段长度超出上限（声明长度 > maxFieldLen）。
var errProtoOverflow = errors.New("proto: field length overflow")

// errProtoTruncated 帧被截断：长度前缀声明的字节数多于实际剩余字节（EOF）。
// 与 errProtoOverflow（恶意超长）区分——前者是不完整帧，后者是越界声明。
var errProtoTruncated = errors.New("proto: frame truncated (unexpected EOF)")

// checkFieldLen 编码侧长度上限校验：解码侧 getLenBytes 拒绝 > maxFieldLen 的字段，
// 若编码侧不设限，超长字段会编出一个对端永远解不开的帧（编解码不对称，静默丢消息）。
// 接受 ...string（内部统一转换），消除调用侧重复手写转换的漏字段风险。
func checkFieldLen(fields ...string) error {
	for _, f := range fields {
		if len(f) > maxFieldLen {
			return errProtoOverflow
		}
	}
	return nil
}

// checkByteLen 同 checkFieldLen，但接受 [][]byte，避免热路径上 string(p.Body) 整体复制。
func checkByteLen(fields ...[]byte) error {
	for _, f := range fields {
		if len(f) > maxFieldLen {
			return errProtoOverflow
		}
	}
	return nil
}

// putLenBytes 以 4B 大端写入长度前缀。
func putLenBytes(buf *bytes.Buffer, b []byte) {
	var tmp [4]byte
	// #nosec G115 -- b 的长度已在上游 checkFieldLen/checkByteLen 校验 n <= maxFieldLen (16<<20=16MB)，
	// 非负且在 uint32 范围内。
	binary.BigEndian.PutUint32(tmp[:], uint32(len(b)))
	buf.Write(tmp[:])
	buf.Write(b)
}

// getLenBytes 读取 4B 大端长度前缀并返回后续 length 字节。
// 明确区分三类错误——
// - 读长度前缀时不足 4B / body 不足声明长度：帧被截断（errProtoTruncated，可判 EOF）。
// - 声明长度 > maxFieldLen：恶意/异常超长（errProtoOverflow）。
// - 声明长度 <= 剩余字节但 ReadFull 仍失败：透传底层错误。
func getLenBytes(r *bytes.Reader) ([]byte, error) {
	var tmp [4]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errProtoTruncated
		}
		return nil, err
	}
	n := binary.BigEndian.Uint32(tmp[:])
	if n > maxFieldLen {
		return nil, errProtoOverflow
	}
	// 声明长度超过剩余可读字节 → 截断帧，先行判定避免大分配后再报错。
	if int64(n) > int64(r.Len()) {
		return nil, errProtoTruncated
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errProtoTruncated
		}
		return nil, err
	}
	return b, nil
}

// EncodeGWLogicPacket 序列化网关 ↔ 逻辑服信封为紧凑二进制：
// [connID 长度前缀][4B 大端 requestID][4B 大端 msgID][owner 长度前缀][body 长度前缀][traceID 长度前缀][line 长度前缀]。
func EncodeGWLogicPacket(p *GWLogicPacket) ([]byte, error) {
	if p == nil {
		return nil, errors.New("proto: nil GWLogicPacket")
	}
	if err := checkFieldLen(p.ConnID, p.Owner, p.TraceID, p.Line); err != nil {
		return nil, err
	}
	// 热路径 byte body 用 checkByteLen 避免 string(p.Body) 整份复制。
	if err := checkByteLen(p.Body); err != nil {
		return nil, err
	}
	// 允许 0xFFFFFFFF 作为合法错误回包消息号通过（见 event/logic.go 的 defaultErrorMsgID）。
	// 拦截恶意客户端携带错误回包消息号的逻辑应在路由层（而非编解码层）完成。
	var buf bytes.Buffer
	putLenBytes(&buf, []byte(p.ConnID))
	if err := binary.Write(&buf, binary.BigEndian, p.RequestID); err != nil {
		return nil, fmt.Errorf("proto: encode requestID: %w", err)
	}
	if err := binary.Write(&buf, binary.BigEndian, p.MsgID); err != nil {
		return nil, fmt.Errorf("proto: encode msgID: %w", err)
	}
	putLenBytes(&buf, []byte(p.Owner))
	putLenBytes(&buf, p.Body)
	putLenBytes(&buf, []byte(p.TraceID))
	putLenBytes(&buf, []byte(p.Line))
	return buf.Bytes(), nil
}

// DecodeGWLogicPacket 反序列化网关 ↔ 逻辑服信封（与 EncodeGWLogicPacket 对称）。
func DecodeGWLogicPacket(b []byte) (*GWLogicPacket, error) {
	if len(b) == 0 {
		return nil, errors.New("proto: empty GWLogicPacket")
	}
	r := bytes.NewReader(b)
	connID, err := getLenBytes(r)
	if err != nil {
		return nil, err
	}
	var requestID uint32
	if err := binary.Read(r, binary.BigEndian, &requestID); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errProtoTruncated
		}
		return nil, err
	}
	var msgID uint32
	if err := binary.Read(r, binary.BigEndian, &msgID); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errProtoTruncated
		}
		return nil, err
	}
	owner, err := getLenBytes(r)
	if err != nil {
		return nil, err
	}
	body, err := getLenBytes(r)
	if err != nil {
		return nil, err
	}
	traceID, err := getLenBytes(r)
	if err != nil {
		return nil, err
	}
	// Line 是可选尾字段：帧在 traceID 之后没有剩余字节时按缺省值处理。
	// 一旦仍有剩余字节，就必须是一个完整的长度前缀字段——长度前缀声明的字节数多于实际
	// 剩余（errProtoTruncated）属截断帧，与「帧中不含该字段」的 EOF 语义不同，不可降级。
	var line []byte
	if r.Len() > 0 {
		line, err = getLenBytes(r)
		if err != nil {
			return nil, err
		}
	}
	// 编码侧已对 0xFFFFFFFF 放行，解码侧对称放行。
	return &GWLogicPacket{ConnID: string(connID), RequestID: requestID, MsgID: msgID, Owner: string(owner), Body: body, TraceID: string(traceID), Line: string(line)}, nil
}

// DeliveryMode 推送传输模式，真身定义在 pkg/shared/proto。
type DeliveryMode = pproto.DeliveryMode

// 传输模式取值（再导出 pkg 定义）。
const (
	// DeliveryModeBestEffort 尽力而为（默认）：不保证送达，无 ACK 确认。
	DeliveryModeBestEffort = pproto.DeliveryModeBestEffort
	// DeliveryModeReliable 可靠传输：需要 ACK 确认，支持重试。
	DeliveryModeReliable = pproto.DeliveryModeReliable
	// DeliveryModePersistent 持久化：离线消息暂存，上线后推送。
	DeliveryModePersistent = pproto.DeliveryModePersistent
)

// EAlertNotify 弹窗提示内容，真身定义在 pkg/shared/proto。
type EAlertNotify = pproto.EAlertNotify

// ERoomTakeoverNotify 房间接管恢复推送载体，真身定义在 pkg/shared/proto。
type ERoomTakeoverNotify = pproto.ERoomTakeoverNotify

// ESceneInfoNotify 场景标识推送载体，真身定义在 pkg/shared/proto。
type ESceneInfoNotify = pproto.ESceneInfoNotify

// EQueuePositionNotify 排队位置通知体，真身定义在 pkg/shared/proto。
type EQueuePositionNotify = pproto.EQueuePositionNotify

// ESceneBroadcaster 「可广播的场景」抽象，真身定义在 pkg/shared/proto。
type ESceneBroadcaster = pproto.ESceneBroadcaster

// NotifyPush 逻辑服 → 网关的异步 / 广播载体：经消息总线投递，网关按 Target 找到目标会话后
// 封装为客户端帧下发（data-event 自动同步的承载）。
type NotifyPush struct {
	Target       string       `json:"target"`                  // 路由目标：TargetAll("*")=全服；否则为具体玩家/房间标识（字符串，如 playerID/roomID）
	MsgID        uint32       `json:"msg_id"`                  // 推送消息号（见 push.go，如 EPushPlayerFullSync=4001、EPushAlert=4002）
	Body         []byte       `json:"body"`                    // 推送体（JSON 编码，对应 proto 中各推送载体结构）
	TraceID      string       `json:"trace_id,omitempty"`      // 全链路追踪 ID（随 originate 请求回流，便于串联推送）
	DeliveryMode DeliveryMode `json:"delivery_mode,omitempty"` // 传输模式：0=尽力而为，1=可靠传输，2=持久化
	MessageID    string       `json:"message_id,omitempty"`    // 消息唯一ID（用于去重和ACK跟踪）
}

// EncodeNotifyPush 编码内部推送：
// msgID(4B) + len(target)+target + len(body)+body + len(traceID)+traceID + deliveryMode(1B) + len(messageID)+messageID。
// 与 DecodeNotifyPush 字段顺序一致，网关侧解码可直接复用。
func EncodeNotifyPush(p *NotifyPush) ([]byte, error) {
	if p == nil {
		return nil, errors.New("proto: nil NotifyPush")
	}
	if err := checkFieldLen(p.Target, p.TraceID, p.MessageID); err != nil {
		return nil, err
	}
	// 热路径 byte body 用 checkByteLen 避免 string(p.Body) 整份复制。
	if err := checkByteLen(p.Body); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], p.MsgID)
	buf.Write(tmp[:])
	putLenBytes(&buf, []byte(p.Target))
	putLenBytes(&buf, p.Body)
	putLenBytes(&buf, []byte(p.TraceID))
	buf.WriteByte(byte(p.DeliveryMode))
	putLenBytes(&buf, []byte(p.MessageID))
	return buf.Bytes(), nil
}

// DecodeNotifyPush 解码内部推送。body 为新分配切片。
// 线格式：msgID(4B) + len(target)(4B)+target + len(body)(4B)+body + len(traceID)(4B)+traceID + deliveryMode(1B) + len(messageID)(4B)+messageID。
func DecodeNotifyPush(data []byte) (*NotifyPush, error) {
	// 前置空 payload 判空，与 DecodeGWLogicPacket 风格一致。
	if len(data) == 0 {
		return nil, errors.New("proto: empty NotifyPush")
	}
	r := bytes.NewReader(data)
	var tmp [4]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		// EOF/UnexpectedEOF → errProtoTruncated 统一截断语义，
		// 避免调用方 errors.Is(err, io.EOF) 漏判（与 getLenBytes 风格一致）。
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errProtoTruncated
		}
		return nil, err
	}
	msgID := binary.BigEndian.Uint32(tmp[:])
	targetBytes, err := getLenBytes(r)
	if err != nil {
		return nil, err
	}
	body, err := getLenBytes(r)
	if err != nil {
		return nil, err
	}
	traceBytes, err := getLenBytes(r)
	if err != nil {
		return nil, err
	}
	// deliveryMode 与 messageID 为可选尾字段：有剩余数据时读取，否则取默认值。
	var deliveryMode DeliveryMode
	var messageID string
	if r.Len() > 0 {
		modeByte, err := r.ReadByte()
		if err != nil {
			// EOF/UnexpectedEOF 表示帧中没有 deliveryMode 字段，保持默认值
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, err
			}
		} else {
			deliveryMode = DeliveryMode(modeByte)
			// 编码侧成对写入 deliveryMode 与 messageID：读到 deliveryMode 后若仍有剩余字节，
			// 则 messageID 必然存在且必须完整，长度前缀声明多于实际剩余属截断帧，不可降级。
			if r.Len() > 0 {
				messageIDBytes, err := getLenBytes(r)
				if err != nil {
					return nil, err
				}
				messageID = string(messageIDBytes)
			}
		}
	}
	return &NotifyPush{
		Target:       string(targetBytes),
		MsgID:        msgID,
		Body:         body,
		TraceID:      string(traceBytes),
		DeliveryMode: deliveryMode,
		MessageID:    messageID,
	}, nil
}

// TargetAll 广播目标：全服推送（网关按此值向所有会话下发）。字符串哨兵，
// 与 NotifyPush.Target 同为 string 类型（玩家/房间标识亦为字符串）。
const TargetAll = "*"

// NATSSubjectNotify 内部推送消息总线主题（逻辑服 → 网关的 NotifyPush 投递主题）。
const NATSSubjectNotify = "clover.notify"

// NATSSubjectGWControl 网关控制消息总线主题（逻辑服 → 网关的控制指令，如双 key 索引追加）。
const NATSSubjectGWControl = "clover.gw.control"

// GWControlBind 逻辑服 → 网关控制消息：追加会话索引（双 key 架构，account + playerID 共存）。
// 经 NATSSubjectGWControl 投递，网关收到后追加 idIndex[PlayerID] 指向同一 session。
type GWControlBind struct {
	ConnID   string `json:"conn_id"`   // 已绑定 account 的网关连接标识
	PlayerID string `json:"player_id"` // 需追加索引的业务角色标识
}

// GWControlSwitchUpstream 网关控制指令：切换指定会话的上游地址。
// 逻辑服在 MoveEntity 完成后经 NATSSubjectGWControl 发布，
// 网关收到后关闭旧上游并重连到 new_upstream 地址，实现连接全量迁移。
type GWControlSwitchUpstream struct {
	ConnID      string `json:"conn_id"`      // 需切换上游的网关连接标识
	NewUpstream string `json:"new_upstream"` // 新上游地址（目标游戏服 listen 地址）
}

// GWControlKick 网关控制指令：踢掉指定会话（关闭客户端连接）。
//
// 逻辑服经 NATSSubjectGWControl 发布，用于「逻辑服与网关分离部署」时主动踢人
// （all 模式下逻辑服直接持有网关 Kick 回调，不需要走这条指令）。
//
// Kick 为恒 true 的判别位：网关按字段顺序解析控制指令，若无显式标记，
// 本结构会被 GWControlBind 的 Unmarshal 静默接受（未知字段被忽略），
// 导致 PlayerID 为空的绑定请求。故解析顺序必须先判 kick、再判 new_upstream、最后才 Bind。
type GWControlKick struct {
	ConnID string `json:"conn_id"` // 需踢下线的网关连接标识
	Kick   bool   `json:"kick"`    // 判别位，恒 true
	Reason string `json:"reason,omitempty"`
}

type ctxKeyOwner struct{}

// WithOwner 将 owner 注入 context（逻辑服内部在处理某连接请求时标记当前 owner）。
func WithOwner(ctx context.Context, owner string) context.Context {
	return context.WithValue(ctx, ctxKeyOwner{}, owner)
}

// OwnerFromContext 从 context 取出 owner（无则返回 ("", false)）。
func OwnerFromContext(ctx context.Context) (string, bool) {
	if v, ok := ctx.Value(ctxKeyOwner{}).(string); ok {
		return v, true
	}
	return "", false
}

// 会话密钥（32 字节 AES-256）的生成归 internal/transport/net/session 所有，
// 本包不再提供第二份实现，避免同一能力两处生成、两处维护。
