package tcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"clover-server-engine/internal/transport/net/session"
)

const lengthFieldSize = 4

// 帧类型：data 为业务数据；ping/pong 为心跳控制帧。
// 引入类型字节可彻底区分 ping 与 pong，避免「零长度帧既当 ping 又当 pong」造成的双向互发风暴。
const (
	frameTypeData byte = 0
	frameTypePing byte = 1
	frameTypePong byte = 2
	// （原 frameTypeMigrate = 3「连接迁移令牌帧」已删除：迁移能力入口
	// session.MigrationManager 全仓零调用、gwcore 也从未传 Migration 配置，收到该帧只会
	// Close()、客户端也从不发送 —— 整条链路不可达。类型 3 现落到 readLoop 的 default 分支
	// 按「未知帧类型」降频留痕。）
)

// writeTypedFrame 写入带类型的帧（控制帧用，如 ping/pong 的空 payload）。
func writeTypedFrame(w io.Writer, typ byte, payload []byte) error {
	// 写侧与读侧对称护栏：超过 hardMaxMsgSize 的帧对端必然判「frame too large」并断连，
	// 本地直接失败，避免「本地写成功、对端必拒」的静默黑洞（10MB 以上帧此前会被正常写出）。
	if len(payload) > hardMaxMsgSize {
		return fmt.Errorf("tcp: payload %d exceeds hard max %d", len(payload), hardMaxMsgSize)
	}
	if len(payload) > math.MaxUint32 {
		return errors.New("tcp: payload too large")
	}
	var hdr [1 + lengthFieldSize]byte
	hdr[0] = typ
	// #nosec G115 -- len(payload) 已限幅到 [0, math.MaxUint32]。
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// hardMaxMsgSize 内建硬上限，即使调用方 maxMsgSize==0（关闭限制）也不得超过此值，
// 防止恶意超大帧导致 OOM。值 = `session.MaxFrameSize`（服务端帧上限唯一来源）：
// 与 tcp/config.go 的 defaultMaxMsgSize、ws/quic 传输层上限、网关 `MaxFrameSize`
// 以及会话加密的密文上限同为 10 MiB。
const hardMaxMsgSize = session.MaxFrameSize

// readFrame 读取一帧，返回类型与 payload。
func readFrame(r io.Reader, maxMsgSize int) (byte, []byte, error) {
	var th [1]byte
	if _, err := io.ReadFull(r, th[:]); err != nil {
		return 0, nil, err
	}
	var lh [lengthFieldSize]byte
	if _, err := io.ReadFull(r, lh[:]); err != nil {
		return 0, nil, err
	}
	// 先按 uint32 读并比较，避免 32 位平台 int 溢出为负后绕过 limit 检查、
	// make([]byte, 负长度) panic。
	n32 := binary.BigEndian.Uint32(lh[:])
	limit := maxMsgSize
	if limit <= 0 || limit > hardMaxMsgSize {
		limit = hardMaxMsgSize
	}
	if n32 > uint32(limit) {
		return 0, nil, errors.New("tcp: frame too large")
	}
	n := int(n32)
	buf := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, nil, err
		}
	}
	return th[0], buf, nil
}
