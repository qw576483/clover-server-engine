// Package bitset 动态位集合。
//
// 位级置位/复位/检测 + 位运算，长度任意、可增长的动态位集合（底层按 64 位字分块，
// 零外部依赖、纯标准库、并发不安全需外部保护——若需共享使用请用 sync 保护或 per-goroutine 持有）。
//
// 典型用途：权限/标志位掩码、AOI 视野格子标记、对象集合（在线/离线/屏蔽）等。
// 与 gobject/schema 的 Flag 位、aoi 的格子标记天然互补。
package bitset

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

const wordBits = 64

// ErrInvalidData 表示 FromBytes 收到的数据不是 Bytes() 的合法产物：
// 长度不是 4+8k、自描述位数超出位数据容量，或存在下标超出 n 的残留置位。
var ErrInvalidData = errors.New("bitset: 序列化数据非法")

// BitSet 是动态位集合，索引为非负整数。并发不安全。
type BitSet struct {
	bits []uint64
	n    int // 有效位数（0..len(bits)*64）
}

func wordsFor(n int) int {
	w := n / wordBits
	if n%wordBits != 0 {
		w++
	}
	return w
}

// New 创建至少可容纳 size 位的 BitSet。
func New(size int) *BitSet {
	if size < 0 {
		size = 0
	}
	return &BitSet{bits: make([]uint64, wordsFor(size)), n: size}
}

// Len 返回可容纳的位数。
func (b *BitSet) Len() int { return b.n }

// Set 置位 pos；pos 越界返回 false。
func (b *BitSet) Set(pos int) bool {
	if pos < 0 || pos >= b.n {
		return false
	}
	b.bits[pos/wordBits] |= uint64(1) << uint(pos%wordBits)
	return true
}

// Reset 复位 pos；pos 越界返回 false。
func (b *BitSet) Reset(pos int) bool {
	if pos < 0 || pos >= b.n {
		return false
	}
	b.bits[pos/wordBits] &^= uint64(1) << uint(pos%wordBits)
	return true
}

// Check 检测 pos 是否置位（越界返回 false）。
func (b *BitSet) Check(pos int) bool {
	if pos < 0 || pos >= b.n {
		return false
	}
	return b.bits[pos/wordBits]&(uint64(1)<<uint(pos%wordBits)) != 0
}

// Test 是 Check 的别名。
func (b *BitSet) Test(pos int) bool { return b.Check(pos) }

// Flip 翻转 pos；pos 越界返回 false。
func (b *BitSet) Flip(pos int) bool {
	if pos < 0 || pos >= b.n {
		return false
	}
	b.bits[pos/wordBits] ^= uint64(1) << uint(pos%wordBits)
	return true
}

// Any 报告是否至少有一个位置位。
func (b *BitSet) Any() bool {
	for _, w := range b.bits {
		if w != 0 {
			return true
		}
	}
	return false
}

// None 报告是否没有任何位置位。
func (b *BitSet) None() bool { return !b.Any() }

// All 报告 [0,n) 内是否所有位都已置位。
func (b *BitSet) All() bool {
	full := b.n / wordBits
	for i := 0; i < full; i++ {
		if b.bits[i] != ^uint64(0) {
			return false
		}
	}
	if rem := b.n % wordBits; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		if b.bits[full]&mask != mask {
			return false
		}
	}
	return true
}

// Count 返回置位个数。
func (b *BitSet) Count() int {
	c := 0
	for _, w := range b.bits {
		c += bits.OnesCount64(w)
	}
	return c
}

// FirstSet 返回最低置位的索引，无则 -1。
func (b *BitSet) FirstSet() int {
	for i, w := range b.bits {
		if w != 0 {
			return i*wordBits + bits.TrailingZeros64(w)
		}
	}
	return -1
}

// FirstZero 返回最低未置位的索引，全置位返回 -1。
// 调用方须确保 n<=len(b.bits)*64（由 New/BitSet 初始化保证），否则会越界访问。
func (b *BitSet) FirstZero() int {
	full := b.n / wordBits
	for i := 0; i < full; i++ {
		if b.bits[i] != ^uint64(0) {
			return i*wordBits + bits.TrailingZeros64(^b.bits[i])
		}
	}
	if rem := b.n % wordBits; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		w := b.bits[full] & mask
		if w != mask {
			return full*wordBits + bits.TrailingZeros64(^w&mask)
		}
	}
	return -1
}

// Clear 复位所有位。
func (b *BitSet) Clear() {
	for i := range b.bits {
		b.bits[i] = 0
	}
}

// Clone 返回副本。
func (b *BitSet) Clone() *BitSet {
	nb := make([]uint64, len(b.bits))
	copy(nb, b.bits)
	return &BitSet{bits: nb, n: b.n}
}

// grow 将容量扩展到至少 m 位（超出部分清零）。
func (b *BitSet) grow(m int) {
	if m <= b.n {
		return
	}
	nw := wordsFor(m)
	if nw > len(b.bits) {
		nb := make([]uint64, nw)
		copy(nb, b.bits)
		b.bits = nb
	}
	b.n = m
}

// Or 原地并集（把 other 中置位的位并入 b），并按需扩容。
func (b *BitSet) Or(other *BitSet) *BitSet {
	b.grow(maxInt(b.n, other.n))
	for i := range b.bits {
		if i < len(other.bits) {
			b.bits[i] |= other.bits[i]
		}
	}
	return b
}

// And 原地交集（保留 b 与 other 共有的置位），并按需扩容。
func (b *BitSet) And(other *BitSet) *BitSet {
	b.grow(maxInt(b.n, other.n))
	for i := range b.bits {
		var o uint64
		if i < len(other.bits) {
			o = other.bits[i]
		}
		b.bits[i] &= o
	}
	return b
}

// AndNot 原地差集（清除 b 中也在 other 置位的位），并按需扩容。
func (b *BitSet) AndNot(other *BitSet) *BitSet {
	b.grow(maxInt(b.n, other.n))
	for i := range b.bits {
		var o uint64
		if i < len(other.bits) {
			o = other.bits[i]
		}
		b.bits[i] &^= o
	}
	return b
}

// Xor 原地异或（翻转 b 与 other 不一致的位），并按需扩容。
func (b *BitSet) Xor(other *BitSet) *BitSet {
	b.grow(maxInt(b.n, other.n))
	for i := range b.bits {
		var o uint64
		if i < len(other.bits) {
			o = other.bits[i]
		}
		b.bits[i] ^= o
	}
	return b
}

// Equal 报告两集合是否表示同一组置位（超出各自声明长度的部分视为 0）。
func (b *BitSet) Equal(other *BitSet) bool {
	common := len(b.bits)
	if len(other.bits) < common {
		common = len(other.bits)
	}
	for i := 0; i < common; i++ {
		if b.bits[i] != other.bits[i] {
			return false
		}
	}
	for i := common; i < len(b.bits); i++ {
		if b.bits[i] != 0 {
			return false
		}
	}
	for i := common; i < len(other.bits); i++ {
		if other.bits[i] != 0 {
			return false
		}
	}
	return true
}

// Slice 返回所有置位索引（升序），便于列出所有满足条件的标志。
func (b *BitSet) Slice() []int {
	out := make([]int, 0, b.Count())
	for i, w := range b.bits {
		for w != 0 {
			bit := bits.TrailingZeros64(w)
			out = append(out, i*wordBits+bit)
			w &^= uint64(1) << uint(bit)
		}
	}
	return out
}

// String 返回位串（高位在左，bit n-1 在最左）。
func (b *BitSet) String() string {
	if b.n == 0 {
		return ""
	}
	buf := make([]byte, b.n)
	for i := 0; i < b.n; i++ {
		if b.Check(i) {
			buf[b.n-1-i] = '1'
		} else {
			buf[b.n-1-i] = '0'
		}
	}
	return string(buf)
}

// Bytes 大端序列化（自描述）：4 字节有效位数 n + 各 64 位字，用于持久化 / 跨语言互通。
func (b *BitSet) Bytes() []byte {
	out := make([]byte, 4+len(b.bits)*8)
	// b.n 非负且受 len(b.bits)*64 限制；序列化前限幅到 uint32 范围。
	n := b.n
	if n < 0 {
		n = 0
	}
	if n > math.MaxUint32 {
		n = math.MaxUint32
	}
	// #nosec G115 -- n 已限幅到 [0, math.MaxUint32]。
	binary.BigEndian.PutUint32(out[0:4], uint32(n))
	for i, w := range b.bits {
		binary.BigEndian.PutUint64(out[4+i*8:], w)
	}
	return out
}

// FromBytes 从 Bytes() 的产物还原（读长度前缀，精确还原 n）。
//
// 空数据按空集合处理；
// 其余不合法的输入一律返回 ErrInvalidData，而不是钳制出一个自相矛盾的实例。
func FromBytes(data []byte) (*BitSet, error) {
	if len(data) == 0 {
		return New(0), nil
	}
	// 自描述编码恒为 4 字节长度前缀 + 若干 8 字节字，长度对不上即数据损坏。
	if len(data) < 4 || (len(data)-4)%8 != 0 {
		return nil, ErrInvalidData
	}
	rest := data[4:]
	words := len(rest) / 8
	raw := uint64(binary.BigEndian.Uint32(data[0:4]))
	// 32 位平台上 int 只有 32 位，接近 MaxUint32 的 n 直接转换会溢出成负数，
	// 令 String() 等以负长度 make 而 panic；先按 int 上限拦截。
	if raw > uint64(math.MaxInt) {
		return nil, ErrInvalidData
	}
	n := int(raw)
	if n > words*wordBits {
		return nil, ErrInvalidData
	}
	bits := make([]uint64, words)
	for i := 0; i < words; i++ {
		bits[i] = binary.BigEndian.Uint64(rest[i*8:])
	}
	// 自描述的 n 必须与位数据一致：n 之外若有残留置位，
	// Count/Any/Slice 会返回越界的位，与 Len() 自相矛盾。
	if hasBitsBeyond(bits, n) {
		return nil, ErrInvalidData
	}
	return &BitSet{bits: bits, n: n}, nil
}

// hasBitsBeyond 报告位数组中是否存在下标 >= n 的置位。
func hasBitsBeyond(bits []uint64, n int) bool {
	if rem := n % wordBits; rem != 0 {
		last := n / wordBits
		mask := (uint64(1) << uint(rem)) - 1
		if bits[last]&^mask != 0 {
			return true
		}
	}
	for i := wordsFor(n); i < len(bits); i++ {
		if bits[i] != 0 {
			return true
		}
	}
	return false
}

// ToUint64 当 n<=64 时返回底层整数值，否则返回 (0, false)。
// 当 n<64 时，超出 n 位的高位会被掩码清零，避免泄漏未使用的位。
func (b *BitSet) ToUint64() (uint64, bool) {
	if b.n > wordBits {
		return 0, false
	}
	if len(b.bits) == 0 {
		return 0, true
	}
	if b.n < wordBits {
		mask := (uint64(1) << uint(b.n)) - 1
		return b.bits[0] & mask, true
	}
	return b.bits[0], true
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
