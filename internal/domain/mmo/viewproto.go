// #nosec G115 -- 线格式定长编解码：转换只发生在按 v2 报文布局判过边界/长度之后。

// viewproto.go 实现 MMO 视野同步的紧凑二进制线协议。
//
// 设计目标：零反射二进制编码，避免每条视野事件都走 JSON 反射 + 序列化，
// 把内部链路（logic→gateway→client）的视野同步开销降低 30%~50%，为主流大型 MMO
// 的每秒数万次视野事件提供可接受的单帧预算。
//
// 本协议仅定义"信封 + 事件实体"，业务 snapshot 仍以原始 JSON 字节透传；
// 因此客户端只需解析外层二进制，内部 snapshot 格式不变。
//
// 线格式：
//
// [1B version=2][1B type][payload...]
//
// type：
//
// 0x01 viewEventBatch [u32 count][events...]
// 0x02 directMessage [u64 target][u32 msgID][1B deliveryMode][u32 bodyLen][body]
//
// event：
//
// [1B event: 1=enter, 2=leave][1B deliveryMode][u64 watcher][u64 object]
// enter 事件可选追加 [u32 snapshotLen][snapshot]
//
// 编解码只走二进制，非本协议 payload 一律报错。
package mmo

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/pkg/foundation/logger"
)

const (
	viewProtoVersion = 2

	viewTypeBatch  byte = 1 // 视野事件批量
	viewTypeDirect byte = 2 // 单条直接消息（SendTo）

	viewEventEnter byte = 1
	viewEventLeave byte = 2
)

var (
	errViewProtoVersion = errors.New("viewproto: unsupported version")
	errViewProtoType    = errors.New("viewproto: unsupported type")
	errViewProtoTrunc   = errors.New("viewproto: truncated")
)

// viewProtoBufPool 复用二进制编码时的临时缓冲区，降低 GC 压力。
// 取出后必须按「容量清零 / 重置长度」后再使用。
var viewProtoBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}

func acquireViewBuf() []byte {
	p := viewProtoBufPool.Get().(*[]byte)
	return (*p)[:0]
}

func releaseViewBuf(b []byte) {
	p := b[:0]
	viewProtoBufPool.Put(&p)
}

// encodeViewBatch 把视野事件批量编码为紧凑二进制。
// 线格式(v2)：[1B version][1B type=1][u32 count][events...]
// event: [1B event][1B deliveryMode][u64 watcher][u64 object][可选 snapshot]
func encodeViewBatch(events []viewPush) []byte {
	// 估算容量：每条事件 18 字节 + 平均快照 64 字节。
	est := len(events) * (1 + 1 + 8 + 8 + 4 + 64)
	buf := acquireViewBuf()
	if cap(buf) < est {
		// 池缓存区不够大，释放旧缓冲区回池再分配新缓冲区，避免泄漏。
		releaseViewBuf(buf)
		buf = make([]byte, 0, est)
	}

	buf = append(buf, viewProtoVersion, viewTypeBatch)
	if len(events) > math.MaxUint32 {
		events = events[:math.MaxUint32]
	}
	// #nosec G115 -- len(events) 已限幅到 [0, math.MaxUint32]。
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(events)))

	for _, e := range events {
		if e.Event == "enter" {
			buf = append(buf, viewEventEnter)
		} else {
			buf = append(buf, viewEventLeave)
		}
		// 编码 DeliveryMode，保留可靠性语义。
		buf = append(buf, byte(e.DeliveryMode))
		buf = binary.BigEndian.AppendUint64(buf, e.Watcher)
		buf = binary.BigEndian.AppendUint64(buf, e.Object)
		if e.Event == "enter" {
			snap := snapshotMarshalBinary(e.SnapshotBin)
			if len(snap) > math.MaxUint32 {
				snap = snap[:math.MaxUint32]
			}
			// #nosec G115 -- len(snap) 已限幅到 [0, math.MaxUint32]。
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(snap)))
			buf = append(buf, snap...)
		}
	}

	// 必须把最终要发布的字节拷贝出来，因为 buf 会被回收到池中。
	out := make([]byte, len(buf))
	copy(out, buf)
	releaseViewBuf(buf)
	return out
}

// snapshotMarshalBinary 把 map[type]binary 编码为紧凑二进制。
//
// 格式：
//
// [u16 count]
// for each type:
// [u8 typeLen][typeLen bytes][u32 dataLen][dataLen bytes]
//
// 内部 data 已经是业务二进制（object.Bag 等）或原 JSON 字节；本函数只负责装帧，零反射。
func snapshotMarshalBinary(m map[string][]byte) []byte {
	if len(m) == 0 {
		return nil
	}
	// 估算容量：count(2) + len(m)*(1+16+4+avg 64) ≈ len(m)*89。
	est := 2 + len(m)*(1+16+4+64)
	buf := acquireViewBuf()
	if cap(buf) < est {
		releaseViewBuf(buf)
		buf = make([]byte, 0, est)
	}
	// 计数写入 u16：超过 65535 会静默回绕，解码侧只取回绕后的前 N 条 → 丢条目且无任何提示。
	// 这里显式限幅并留痕（限幅本身仍是丢数据，但至少可观测，不会变成"随机少几条"）。
	n := len(m)
	if n > math.MaxUint16 {
		logger.Warnf("viewproto: 快照类型数量 %d 超过上限 %d，超出部分被丢弃", n, math.MaxUint16)
		n = math.MaxUint16
	}
	// #nosec G115 -- n 已限幅到 [0, math.MaxUint16]。
	buf = binary.BigEndian.AppendUint16(buf, uint16(n))
	written := 0
	for typ, data := range m {
		if written >= n {
			break
		}
		written++
		if len(typ) > 255 {
			// 静默截断会让往返后的类型名与编码前不一致（解码侧拿到一个不存在于配置里的名字）。
			logger.Warnf("viewproto: 快照类型名长度 %d 超过 255，已截断（原名前缀=%q）", len(typ), typ[:32])
			typ = typ[:255]
		}
		// #nosec G115 -- len(typ) 已限幅到 [0,255]。
		buf = append(buf, byte(len(typ)))
		buf = append(buf, typ...)
		if len(data) > math.MaxUint32 {
			data = data[:math.MaxUint32]
		}
		// #nosec G115 -- len(data) 已限幅到 [0, math.MaxUint32]。
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
		buf = append(buf, data...)
	}
	out := make([]byte, len(buf))
	copy(out, buf)
	releaseViewBuf(buf)
	return out
}

// snapshotUnmarshalBinaryBytes 解码二进制快照为 map[string][]byte（二进制透传）。
func snapshotUnmarshalBinaryBytes(data []byte) map[string][]byte {
	if len(data) < 2 {
		return nil
	}
	count := int(binary.BigEndian.Uint16(data[:2]))
	out := make(map[string][]byte, count)
	off := 2
	for i := 0; i < count; i++ {
		if off >= len(data) {
			break
		}
		typLen := int(data[off])
		off++
		if off+typLen+4 > len(data) {
			break
		}
		typ := string(data[off : off+typLen])
		off += typLen
		dataLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		if off+dataLen > len(data) {
			break
		}
		payload := make([]byte, dataLen)
		copy(payload, data[off:off+dataLen])
		out[typ] = payload
		off += dataLen
	}
	return out
}

// decodeViewPayload 统一解析视野 subject 的 payload，返回内部 viewPush 列表。
// 仅接受二进制协议，版本不符直接报错。
func decodeViewPayload(data []byte) ([]viewPush, error) {
	if len(data) < 2 {
		return nil, errViewProtoTrunc
	}
	if data[0] != viewProtoVersion {
		return nil, errViewProtoVersion
	}
	if data[1] == viewTypeBatch {
		return decodeViewBatch(data)
	}
	if data[1] == viewTypeDirect {
		p, err := decodeDirectMessage(data)
		if err != nil {
			return nil, err
		}
		return []viewPush{p}, nil
	}
	return nil, errViewProtoType
}

// tryDecodeDirect 仅当 payload 是二进制 viewTypeDirect 单发消息时返回 (消息, true, nil)。
// 非 direct（版本不符 / 类型是 batch / 长度不足）返回 ok=false，供 onViewChange 区分单发路由与视野增量。
func tryDecodeDirect(data []byte) (viewPush, bool, error) {
	if len(data) < 2 || data[0] != viewProtoVersion || data[1] != viewTypeDirect {
		return viewPush{}, false, nil
	}
	p, err := decodeDirectMessage(data)
	if err != nil {
		return viewPush{}, false, err
	}
	return p, true, nil
}

// decodeViewBatch 解析二进制视野事件批量。
// v2: 每条事件增加 1B deliveryMode 字段。
func decodeViewBatch(data []byte) ([]viewPush, error) {
	if len(data) < 6 {
		return nil, errViewProtoTrunc
	}
	count := int(binary.BigEndian.Uint32(data[2:6]))
	// count 来自线上不可信数据，直接按其预分配会被恶意 / 损坏包放大成巨量分配导致 OOM。
	// v2: 每条事件最少占 1(ev)+1(dm)+16(watcher+object)=18 字节，故 count 不可能超过剩余字节数 /18；
	// 预分配容量取该上限与 count 的较小值，真实条数仍由后续按需 append 决定。
	if count < 0 {
		return nil, errViewProtoTrunc
	}
	maxPossible := (len(data) - 6) / 18
	capHint := count
	if capHint > maxPossible {
		capHint = maxPossible
	}
	if capHint < 0 {
		capHint = 0
	}
	events := make([]viewPush, 0, capHint)
	off := 6
	for i := 0; i < count; i++ {
		if off >= len(data) {
			return nil, errViewProtoTrunc
		}
		ev := data[off]
		off++
		if off >= len(data) {
			return nil, errViewProtoTrunc
		}
		dm := proto.DeliveryMode(data[off])
		off++
		if off+16 > len(data) {
			return nil, errViewProtoTrunc
		}
		watcher := binary.BigEndian.Uint64(data[off : off+8])
		object := binary.BigEndian.Uint64(data[off+8 : off+16])
		off += 16

		vp := viewPush{Watcher: watcher, Object: object, DeliveryMode: dm}
		switch ev {
		case viewEventEnter:
			vp.Event = "enter"
			if off+4 > len(data) {
				return nil, errViewProtoTrunc
			}
			snapLen := int(binary.BigEndian.Uint32(data[off : off+4]))
			off += 4
			if off+snapLen > len(data) {
				return nil, errViewProtoTrunc
			}
			if snapLen > 0 {
				snapData := data[off : off+snapLen]
				vp.SnapshotBin = snapshotUnmarshalBinaryBytes(snapData)
			}
			off += snapLen
		case viewEventLeave:
			vp.Event = "leave"
		default:
			return nil, errViewProtoType
		}
		events = append(events, vp)
	}
	return events, nil
}

// encodeDirectMessage 把单条直接消息（SendTo）编码为紧凑二进制。
// 线格式：[1B version][1B type][u64 target][u32 msgID][u8 deliveryMode][u32 bodyLen][body]
func encodeDirectMessage(p viewPush) []byte {
	size := 2 + 8 + 4 + 1 + 4 + len(p.Body)
	buf := acquireViewBuf()
	if cap(buf) < size {
		// 与 encodeViewBatch 保持一致——先把池缓冲归还再新分配，避免泄漏。
		releaseViewBuf(buf)
		buf = make([]byte, 0, size)
	}
	buf = append(buf, viewProtoVersion, viewTypeDirect)
	buf = binary.BigEndian.AppendUint64(buf, p.Target)
	buf = binary.BigEndian.AppendUint32(buf, p.MsgID)
	buf = append(buf, byte(p.DeliveryMode))
	if len(p.Body) > math.MaxUint32 {
		p.Body = p.Body[:math.MaxUint32]
	}
	// #nosec G115 -- len(p.Body) 已限幅到 [0, math.MaxUint32]。
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(p.Body)))
	buf = append(buf, p.Body...)
	out := make([]byte, len(buf))
	copy(out, buf)
	releaseViewBuf(buf)
	return out
}

// decodeDirectMessage 解析二进制直接消息。
// 线格式：[1B version][1B type][u64 target][u32 msgID][u8 deliveryMode][u32 bodyLen][body]
func decodeDirectMessage(data []byte) (viewPush, error) {
	if len(data) < 2+8+4+1+4 {
		return viewPush{}, errViewProtoTrunc
	}
	p := viewPush{
		Target:       binary.BigEndian.Uint64(data[2:10]),
		MsgID:        binary.BigEndian.Uint32(data[10:14]),
		DeliveryMode: proto.DeliveryMode(data[14]),
	}
	bodyLen := int(binary.BigEndian.Uint32(data[15:19]))
	if 19+bodyLen > len(data) {
		return viewPush{}, errViewProtoTrunc
	}
	if bodyLen > 0 {
		p.Body = make([]byte, bodyLen)
		copy(p.Body, data[19:19+bodyLen])
	}
	return p, nil
}
