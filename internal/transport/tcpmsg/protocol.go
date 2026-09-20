// Package tcpmsg 提供基于 TCP 的消息编码、请求响应匹配和 handler 分发。
// 复用 clover-server-engine/internal/transport/net/tcp 的帧格式（[1B type][4B len][payload]），
// 上叠 [4B requestID][4B msgID][JSON body] 实现路由与请求-响应匹配。
package tcpmsg

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	pproto "clover-server-engine/internal/shared/proto"
)

// hdrSize 帧头长度 requestID(4) + msgID(4)。
//
// 直接引用 internal/shared/proto 的客户端帧布局常量：两者的**线格式是同一个**
// （网关↔客户端 / 逻辑服↔master 都用它），各自再写一份 4/4 会在改布局时漏改一处。
// 注意 Decode 的语义差异是刻意的：本包零拷贝返回 body 子切片，proto 那版会复制。
const hdrSize = pproto.ClientFrameHeaderLen

// ErrPayloadTooLarge frame payload 超硬上限。
var ErrPayloadTooLarge = errors.New("tcpmsg: payload too large")

// Encode 编码帧：requestID(4) + msgID(4) + body。
func Encode(requestID uint32, msgID uint32, body []byte) []byte {
	if len(body) > math.MaxUint32-hdrSize {
		return nil
	}
	buf := make([]byte, hdrSize+len(body))
	binary.BigEndian.PutUint32(buf[0:4], requestID)
	binary.BigEndian.PutUint32(buf[4:8], msgID)
	copy(buf[8:], body)
	return buf
}

// Decode 解码帧，返回 msgID, requestID, body（body 共享底层，只读）。
func Decode(data []byte) (msgID, requestID uint32, body []byte, err error) {
	if len(data) < hdrSize {
		return 0, 0, nil, fmt.Errorf("tcpmsg: frame too short: %d", len(data))
	}
	requestID = binary.BigEndian.Uint32(data[0:4])
	msgID = binary.BigEndian.Uint32(data[4:8])
	body = data[8:]
	return msgID, requestID, body, nil
}

// WriteFrame 编码并写入帧到 writer。
func WriteFrame(w io.Writer, requestID, msgID uint32, body []byte) error {
	frame := Encode(requestID, msgID, body)
	if frame == nil {
		return ErrPayloadTooLarge
	}
	_, err := w.Write(frame)
	return err
}

// MarshalCall 将请求对象序列化为 JSON + 封装成帧。
func MarshalCall(requestID uint32, msgID uint32, req any) ([]byte, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("tcpmsg: marshal request: %w", err)
	}
	frame := Encode(requestID, msgID, body)
	if frame == nil {
		return nil, ErrPayloadTooLarge
	}
	return frame, nil
}

// MarshalReply 将响应对象序列化为 JSON + 封装成响应帧。
func MarshalReply(requestID uint32, msgID uint32, resp any) ([]byte, error) {
	body, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("tcpmsg: marshal response: %w", err)
	}
	frame := Encode(requestID, msgID, body)
	if frame == nil {
		return nil, ErrPayloadTooLarge
	}
	return frame, nil
}
