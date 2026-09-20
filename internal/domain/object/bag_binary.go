package object

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

// Binary format for Bag:
//
//	[4 bytes field count]
//	for each field (sorted by name for stable output):
//	  [1 byte nameLen][nameLen bytes name][value binary]
//
// MarshalBinary 把 Bag 编码为紧凑二进制（字段名升序，零反射）。
// 并发安全：读锁内完成全部编码。
func (b *Bag) MarshalBinary() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.fields))
	for n := range b.fields {
		names = append(names, n)
	}
	sort.Strings(names)

	buf := make([]byte, 0, 4+len(names)*32)
	if len(names) > math.MaxUint32 {
		return nil, errors.New("object: bag field count exceeds limit")
	}
	// #nosec G115 -- len(names) 已限幅到 [0, math.MaxUint32]。
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(names)))
	for _, n := range names {
		if len(n) > 255 {
			return nil, fmt.Errorf("bag_binary: field name %q exceeds 255 bytes", n[:32])
		}
		// #nosec G115 -- len(n) 已限幅到 [0,255]。
		buf = append(buf, byte(len(n)))
		buf = append(buf, n...)
		vbytes, err := b.fields[n].MarshalBinary()
		if err != nil {
			return nil, err
		}
		buf = append(buf, vbytes...)
	}
	return buf, nil
}

var errBagBinaryTruncated = errors.New("object: binary bag truncated")

// UnmarshalBinary 从紧凑二进制整体载入（完全替换当前字段集，清除脏标记）。
func (b *Bag) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return errBagBinaryTruncated
	}
	// 用 uint64 接收，避免 32 位平台上 uint32→int 溢出为负导致静默丢数据。
	count64 := uint64(binary.BigEndian.Uint32(data[:4]))
	off := 4
	// 每个字段至少占 1 字节 nameLen + 1 字节 value type，据此校验 count 上限，
	// 防止伪造的超大 count 触发超额预分配。加绝对上限 65536 防止攻击载荷耗尽内存。
	// #nosec G115
	if count64 > uint64((len(data)-off)/2) || count64 > 65536 {
		return errBagBinaryTruncated
	}
	count := int(count64)

	// 先解到临时 map，全部成功后再合并，避免中途失败留下半更新的脏状态。
	parsed := make(map[string]Value, count)
	for i := 0; i < count; i++ {
		if off >= len(data) {
			return errBagBinaryTruncated
		}
		nameLen := int(data[off])
		off++
		if off+nameLen > len(data) {
			return errBagBinaryTruncated
		}
		name := string(data[off : off+nameLen])
		off += nameLen

		var v Value
		// 由 Value 自身返回真实消耗字节数，不再靠重新 Marshal 反推长度。
		n, err := v.UnmarshalBinaryN(data[off:])
		if err != nil {
			return err
		}
		parsed[name] = v
		off += n
	}

	b.mu.Lock()
	b.fields = parsed
	b.dirty = make(map[string]struct{})
	b.mu.Unlock()
	return nil
}
