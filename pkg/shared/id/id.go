// #nosec G115 -- ID 位域拼接：时间戳/序列各占定位，属于按位截断而非数值运算。

// Package id 提供全局唯一短 ID 生成。
package id

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	"clover-server-engine/pkg/shared/timeutil"
	"clover-server-engine/pkg/shared/traceid"
)

// ErrClockBackward 系统时钟回拨幅度超过 uidClockWaitLimit、仍未追平上次时间戳时返回。
var ErrClockBackward = errors.New("id: clock moved backwards beyond the tolerable wait")

// uidClockWaitLimit 是时钟回拨时等待系统时钟追平上次时间戳的最长时长。
// 小幅度回拨（NTP 校时等）等待即可；幅度过大时继续自旋会长时间阻塞调用方，
// 不如尽早返回错误交由上层决定重试还是降级。
const uidClockWaitLimit = time.Second

const uidChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// uidCntBand 是 4 位 36 进制计数器的取值上限（36^4）。计数器值恒定落在 [0, uidCntBand)，
// 保证 encodeBase36(cnt,4) 不会因高位溢出而截断，同一毫秒内可分配 1679616 个不冲突序号。
const uidCntBand = 36 * 36 * 36 * 36 // 1679616

// uidCounter 采用 uint64 单调自增，避免 uint32 在约 49.7 天后回绕导致的 ID 冲突。
var uidCounter uint64

// GenRequestID 生成全局唯一短 ID，用于 NATS 请求应答、消息追踪。
func GenRequestID() string {
	return genID("req")
}

// GenTraceID 生成链路追踪 ID，用于日志与消息透传。
// 格式：trc_<32位十六进制>（由 traceid.NewTraceID → pkg/foundation/trace.NewTraceID 提供随机熵）。
func GenTraceID() string {
	return "trc_" + traceid.NewTraceID()
}

// uidLastMs 记录上一次 GenUID 使用的毫秒时间戳，用于检测时钟回拨。
var uidLastMs uint64

// GenUID 生成引擎数据层唯一标识：固定 16 位全大写英文 + 数字字符串。
//
// 格式：10 位毫秒时间戳（36 进制） + 4 位毫秒内自增计数器（36 进制） + 2 位强随机（36 进制）。
// 单进程内绝对唯一；多进程/多实例之间冲突概率为 36^(-2) ≈ 7.7e-4（由末尾 2 位随机保证），
// 可满足业务唯一性。注意：长度固定 16 位是硬性契约（多张表以 VARCHAR(16) 存放，如 player 的
// id / player_id），切勿改动长度。时间戳字段采用 10 位 36 进制，理论上可用到约公元 118000 年。
func GenUID() (string, error) {
	ms := uint64(timeutil.NowMS())
	// 时钟回拨检测：若当前时间小于上次记录的时间，等待至追上。
	// 等待有上限：超过 uidClockWaitLimit 说明时钟被大幅回调，返回错误而不是无限自旋。
	waitStart := time.Now()
	for {
		last := atomic.LoadUint64(&uidLastMs)
		if ms >= last {
			if atomic.CompareAndSwapUint64(&uidLastMs, last, ms) {
				break
			}
			continue
		}
		if time.Since(waitStart) >= uidClockWaitLimit {
			return "", ErrClockBackward
		}
		time.Sleep(time.Millisecond)
		ms = uint64(timeutil.NowMS())
	}
	// uint64 计数器不回绕；对 uidCntBand 取模使其恒定落在 4 位 36 进制可表达区间内，杜绝截断。
	cnt := atomic.AddUint64(&uidCounter, 1) % uidCntBand
	return encodeBase36(ms, 10) + encodeBase36(cnt, 4) + randomBase36(2), nil
}

func genID(prefix string) string {
	s, err := RandomHex(12)
	if err != nil {
		// rand.Read 在主流平台不会失败，失败时退化为时间戳兜底
		return prefix + "_fallback" + timeutil.NowTime().Format("20060102150405")
	}
	return prefix + "_" + s
}

// RandomHex 生成 n 字节强随机数据并返回小写十六进制字符串（长度 2n）；n<=0 返回空串。
//
// 抽出来是因为「rand.Read → hex 编码」这段在 id / traceid / 账号 token / 渠道占位密码
// 里各写了一遍——其中本包还手写了一张 hex 编码表（与标准库行为一致，但多一份要维护、
// 且容易被误改成有偏实现）。随机源不可用时返回错误，由调用方决定降级策略。
func RandomHex(n int) (string, error) {
	if n <= 0 {
		return "", nil
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// encodeBase36 将 v 编码为固定长度的 36 进制字符串（0-9A-Z），高位补零。
// 超出长度的部分会被截断，调用方需保证 v 不超过 36^length。
func encodeBase36(v uint64, length int) string {
	if length <= 0 {
		return ""
	}
	chars := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		chars[i] = uidChars[v%36]
		v /= 36
	}
	return string(chars)
}

// randomBase36 生成指定长度的强随机 36 进制字符串。
//
// 采用拒绝采样（rejection sampling）消除取模偏置：仅接受 [0,252) 的字节（252=36*7 是 <256
// 的最大 36 的倍数），丢弃 >=252 的取值，使 36 个字符等概率出现。
// rand.Read 失败时退化为纳秒时间戳兜底，且每次余数归零时重新取时钟，避免长度较大时
// 尾部坍缩为全 '0'。
func randomBase36(length int) string {
	if length <= 0 {
		return ""
	}
	out := make([]byte, length)
	const bound = 36 * 7 // 252：<256 的最大 36 倍数，用于无偏拒绝采样
	var one [1]byte
	for i := 0; i < length; {
		if _, err := rand.Read(one[:]); err != nil {
			// 强随机不可用时退化为时间戳兜底（低熵，仅极端情况生效）。
			fillFromClock(out)
			return string(out)
		}
		if one[0] >= bound {
			continue // 拒绝，避免取模偏置
		}
		out[i] = uidChars[one[0]%36]
		i++
	}
	return string(out)
}

// fillFromClock 用纳秒时间戳填充 buf；每当取模余数耗尽（now 归零）时重新读取时钟，
// 保证长度较大时不会坍缩为全 '0'。
func fillFromClock(buf []byte) {
	now := uint64(time.Now().UnixNano())
	for i := range buf {
		if now == 0 {
			now = uint64(time.Now().UnixNano()) | 1 // 或 1 保证非零
		}
		buf[i] = uidChars[now%36]
		now /= 36
	}
}
