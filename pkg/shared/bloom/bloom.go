// Package bloom 提供基于 FNV 哈希的布隆过滤器。
//
// 布隆过滤器是一种空间高效的概率性数据结构，用于判断一个元素
// "绝对不在集合中"或"可能在集合中"。它允许一定的假阳性率，
// 但绝不会有假阴性。典型用途包括：
//
//   - 缓存穿透防护：快速判断 key 是否可能存在，避免无效的 DB 查询
//   - URL 去重 / 爬虫已访问判定
//   - 黑名单 / 白名单的快速预筛
//
// 实现基于 FNV-1a 双哈希技术生成 k 个哈希函数，使用 uint64 位数组
// 存储位标记。并发不安全，需调用方加锁保护。
//
// 设计要点：
//   - 使用 clover-server-engine/pkg/shared/util 包的 Fnv32/Fnv32Key 作为哈希原语
//   - 通过双哈希（double hashing）从两个基础哈希值派生出 k 个哈希函数
//   - 位数组使用 []uint64，按 bit 操作，内存紧凑
//   - 纯标准库、零外部依赖
package bloom

import (
	"errors"
	"fmt"
	"math"

	cloverHash "clover-server-engine/pkg/shared/util"
)

// maxBits 位数组规模上限（2^34 位 = 2 GiB）。
// 超过该规模的过滤器已无实际意义，且 make 会因长度过大直接失败，
// 故在构造期就拦截并返回 ErrTooLarge。
const maxBits = uint64(1) << 34

// ErrTooLarge 当按入参推导出的位数组规模超过 maxBits 时返回。
// 此时应减小 expectedItems 或放宽 falsePositiveRate。
var ErrTooLarge = errors.New("bloom: 目标位数组规模过大，请减小 expectedItems 或放宽 falsePositiveRate")

// Filter 是一个布隆过滤器。并发不安全，多协程共享需外部加锁。
type Filter struct {
	bits    []uint64 // 位数组，每个 uint64 存储 64 位
	m       uint64   // 位数组总位数
	k       uint64   // 哈希函数个数
	n       uint64   // 已添加元素个数（Add 调用次数，重复添加会重复计数）
	bitSize uint64   // bits 数组长度（uint64 个数）
}

// NewBloomFilter 创建一个布隆过滤器。
//
// expectedItems 是预期要插入的元素数量；
// falsePositiveRate 是预期的假阳性率（例如 0.01 表示 1%）。
//
// 内部根据公式自动计算最优的位数组大小 m 和哈希函数个数 k：
//
//	m = -n * ln(p) / (ln(2))^2
//	k = (m/n) * ln(2)
//
// expectedItems / falsePositiveRate 越界（<=0 或 NaN/Inf）时按默认值兜底；
// 推导出的 m 超过 maxBits 时返回 ErrTooLarge —— 该规模已无法合理分配，
// 静默钳制会让假阳性率与调用方预期严重不符。
func NewBloomFilter(expectedItems int, falsePositiveRate float64) (*Filter, error) {
	if expectedItems <= 0 {
		expectedItems = 1
	}
	// 用「不在 (0,1) 开区间内即兜底」的写法，顺带拦下 NaN/Inf：
	// 它们与任何数的大小比较都为 false，无法用区间判断排除。
	if !(falsePositiveRate > 0 && falsePositiveRate < 1) {
		falsePositiveRate = 0.01
	}
	n := float64(expectedItems)
	p := falsePositiveRate

	// 计算位数组大小 m
	m := math.Ceil(-n * math.Log(p) / (math.Ln2 * math.Ln2))
	// 计算哈希函数个数 k
	k := math.Ceil((m / n) * math.Ln2)

	if m < 1 {
		m = 1
	}
	if k < 1 {
		k = 1
	}
	// float64 → uint64 在值超出 uint64 表示范围时结果未定义，
	// 必须先与上限比较再转换，超限直接报错而不是交给 make 失败。
	// 同时用「非正数也算非法」的写法拦下 m 为 NaN 的情况（NaN 与任何值比较均为 false）。
	if !(m > 0 && m <= float64(maxBits)) {
		return nil, ErrTooLarge
	}

	mu := uint64(m)
	ku := uint64(k)

	// 向上取整到 64 的倍数
	bitSize := (mu + 63) / 64
	if bitSize < 1 {
		bitSize = 1
	}

	return &Filter{
		bits:    make([]uint64, bitSize),
		m:       bitSize * 64,
		k:       ku,
		bitSize: bitSize,
	}, nil
}

// Add 向布隆过滤器中添加一个元素。
func (f *Filter) Add(data []byte) {
	f.n++
	h1, h2 := f.baseHashes(data)
	for i := uint64(0); i < f.k; i++ {
		f.setBit(f.derive(h1, h2, i))
	}
}

// Contains 检查元素是否可能在布隆过滤器中。
// 返回 false 表示元素绝对不在集合中；
// 返回 true 表示元素可能在集合中（存在假阳性可能）。
func (f *Filter) Contains(data []byte) bool {
	h1, h2 := f.baseHashes(data)
	for i := uint64(0); i < f.k; i++ {
		if !f.getBit(f.derive(h1, h2, i)) {
			return false
		}
	}
	return true
}

// EstimatedSize 返回已添加的元素个数（近似值，重复添加会高估）。
func (f *Filter) EstimatedSize() uint64 {
	return f.n
}

// Reset 清空布隆过滤器，移除所有已添加的元素。
func (f *Filter) Reset() {
	for i := range f.bits {
		f.bits[i] = 0
	}
	f.n = 0
}

// BitSize 返回位数组的位数（m）。
func (f *Filter) BitSize() uint64 {
	return f.m
}

// HashCount 返回哈希函数的个数（k）。
func (f *Filter) HashCount() uint64 {
	return f.k
}

// FalsePositiveRate 返回当前假阳性率估计。
func (f *Filter) FalsePositiveRate() float64 {
	return math.Pow(1-math.Exp(-float64(f.k)*float64(f.n)/float64(f.m)), float64(f.k))
}

// baseHashes 用 FNV-1a 为同一元素生成两个相互独立的基础哈希值 h1、h2。
// 每个元素只算一次，k 个哈希位置全部由它们派生（见 derive）。
func (f *Filter) baseHashes(data []byte) (h1, h2 uint64) {
	s := string(data)
	h1 = uint64(cloverHash.Fnv32(s))
	// h2 用独立输入派生，确保与 h1 不相关。
	h2 = uint64(cloverHash.Fnv32Key(s, "bloom"))
	// 步长为 0 会让 k 个派生位置全部重合（等价退化成 k=1），假阳性率陡增，这里保证步长非零。
	if h2 == 0 {
		h2 = 1
	}
	return h1, h2
}

// derive 由两个基础哈希派生第 i 个哈希位置（Kirsch-Mitzenmacher 双哈希）：
//
//	hash_i = (h1 + i * h2) % m
//
// 这样 k 个哈希位置只需两次 FNV 计算，而不是跑 k 次独立哈希。
func (f *Filter) derive(h1, h2, i uint64) uint64 {
	return (h1 + i*h2) % f.m
}

// setBit 设置位数组中指定位置的位为 1。
func (f *Filter) setBit(pos uint64) {
	word := pos / 64
	bit := pos % 64
	f.bits[word] |= 1 << bit
}

// getBit 检查位数组中指定位置的位是否为 1。
func (f *Filter) getBit(pos uint64) bool {
	word := pos / 64
	bit := pos % 64
	return f.bits[word]&(1<<bit) != 0
}

// String 返回布隆过滤器的简要描述。
func (f *Filter) String() string {
	return fmt.Sprintf("BloomFilter(m=%d, k=%d, n=%d, p≈%.4f)",
		f.m, f.k, f.n, f.FalsePositiveRate())
}
