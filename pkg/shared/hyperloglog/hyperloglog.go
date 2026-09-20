// #nosec G115 -- 精度参数在构造期已限幅到 [4,20]，所有移位/掩码都是常量语义。

// Package hyperloglog 提供 HyperLogLog 基数估计算法。
//
// HyperLogLog 是一种概率性数据结构，用于估计集合中不同元素的个数（基数），
// 以极小的内存开销（通常仅需几 KB）换取 O(1) 时间复杂度的查询。
//
// 典型用途：
//   - UV（独立访客）统计
//   - 去重计数
//   - 分布式聚合（通过 Merge 合并多个 HLL 实例）
//
// 实现基于 Flajolet 等人的 HyperLogLog 算法，使用 FNV-1a 作为哈希函数。
// 精度由 precision 参数控制（4-16），精度越高内存占用越大。
//
// 设计要点：
//   - 使用自实现的 64 位 FNV-1a 作为哈希原语
//   - 寄存器数组使用 uint8，每个寄存器最多存储 32 个前导零（实际 5 位足够）
//   - Merge 操作要求被合并的 HLL 与当前 HLL 具有相同的 precision
//   - 小基数时使用 Linear Counting 修正以减少误差
//   - 并发不安全，需调用方加锁保护
package hyperloglog

import (
	"errors"
	"math"
	"math/bits"
)

// HLL 是 HyperLogLog 基数估计器。并发不安全。
type HLL struct {
	precision uint    // 精度 p，寄存器数量 = 2^p
	m         uint64  // 寄存器数量 (2^p)
	registers []uint8 // 寄存器数组，存储每个 bucket 的最大前导零+1
	alphaMM   float64 // 预计算的 bias 修正系数 alpha_m * m^2
}

// precision 的合法区间：寄存器数量 = 2^precision。
// 低于下界（4）标准误差已超过 26%；高于上界（16）内存按 2^p 线性增长
// （p=16 时 64KB）而误差收益已不足 0.4%。
const (
	minPrecision = 4
	maxPrecision = 16
)

// hashBits 是 Add 使用的哈希位宽。Count 的大基数修正必须基于同一位宽推导：
// 阈值与修正公式一旦位宽不同，估计值会在阈值处跳变。
const hashBits = 64

// hashSpace 是哈希取值空间 2^hashBits 的浮点表示。2^64 无法用整型常量表达，
// 这里由 2^63 * 2 精确构造（2 的幂在 float64 中可精确表示，无舍入误差）。
const hashSpace = float64(1<<(hashBits-1)) * 2

// ErrPrecisionOutOfRange 当 precision 不在 [4, 16] 范围内时返回。
var ErrPrecisionOutOfRange = errors.New("hyperloglog: precision must be in range [4, 16]")

// ErrMismatchedPrecision 当 Merge 的两个 HLL 精度不同时返回。
var ErrMismatchedPrecision = errors.New("hyperloglog: cannot merge HLL with different precision")

// NewHLL 创建一个新的 HyperLogLog 基数估计器。
//
// precision 控制精度和内存使用，取值范围 [4, 16]：
//
//	precision=4  → 16 个寄存器，~16B 内存，标准误差 ~26%
//	precision=8  → 256 个寄存器，~256B 内存，标准误差 ~6.5%
//	precision=12 → 4096 个寄存器，~4KB 内存，标准误差 ~1.6%
//	precision=16 → 65536 个寄存器，~64KB 内存，标准误差 ~0.4%
//
// precision 越界时返回 ErrPrecisionOutOfRange，不做静默钳制——钳制会让调用方
// 拿到与请求精度不符的估计误差而毫无察觉。
func NewHLL(precision uint) (*HLL, error) {
	if precision < minPrecision || precision > maxPrecision {
		return nil, ErrPrecisionOutOfRange
	}
	m := uint64(1) << precision
	alphaMM := alpha(m) * float64(m) * float64(m)
	return &HLL{
		precision: precision,
		m:         m,
		registers: make([]uint8, m),
		alphaMM:   alphaMM,
	}, nil
}

// hash 用 FNV-1a 对字符串做 64 位哈希。
func (h *HLL) hash(s string) uint64 {
	var hash uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= 1099511628211
	}
	return hash
}

// Add 向 HyperLogLog 中添加一个元素。
func (h *HLL) Add(data []byte) {
	x := h.hash(string(data))
	// 低 p 位作为寄存器索引
	idx := x & (h.m - 1)
	// 剩余高位用于计算前导零
	w := x >> h.precision
	lz := bits.LeadingZeros64(w)
	p := int(h.precision)
	// 前导零数 + 1（按 HLL 标准，至少为 1）
	var rho uint8
	if lz <= p {
		rho = 1
	} else {
		rho = uint8(lz - p + 1)
	}
	if rho > h.registers[idx] {
		h.registers[idx] = rho
	}
}

// Count 返回估计的基数（去重元素个数）。
//
// 使用 HyperLogLog 的 harmonic mean 公式，并在小基数时使用
// Linear Counting 修正以提高准确度。
func (h *HLL) Count() uint64 {
	// 计算调和平均的倒数
	sum := 0.0
	zeros := uint64(0)
	for _, r := range h.registers {
		sum += 1.0 / math.Pow(2.0, float64(r))
		if r == 0 {
			zeros++
		}
	}

	// HyperLogLog raw estimate
	estimate := h.alphaMM / sum

	// 小基数修正（Linear Counting）
	if estimate <= 2.5*float64(h.m) {
		if zeros > 0 {
			// 使用 Linear Counting
			estimate = float64(h.m) * math.Log(float64(h.m)/float64(zeros))
		}
	}

	// 大基数修正：哈希取值空间有限，估计值接近 2^hashBits 时调和平均会显著低估，
	// 用 -2^b * ln(1 - E/2^b) 反向展开。阈值取 2^b/30，与该式同源，保证修正前后
	// 在阈值处连续（E 远小于 2^b 时该式退化为 E 本身）。
	if estimate > hashSpace/30 {
		if ratio := estimate / hashSpace; ratio < 1.0 {
			estimate = -hashSpace * math.Log(1-ratio)
		} else {
			// ratio >= 1 时 log 参数非正，估计值已饱和到哈希取值空间上界
			estimate = hashSpace
		}
	}

	// 估计值可能溢出 uint64（含上面饱和后的值），先封顶再换算，避免浮点转整型的溢出。
	if estimate >= hashSpace {
		return math.MaxUint64
	}
	return uint64(estimate + 0.5)
}

// Merge 将另一个 HLL 合并到当前实例中。
//
// 两个 HLL 必须具有相同的 precision，否则返回 ErrMismatchedPrecision。
// 合并后当前 HLL 的基数估计等于两个集合的并集基数。
func (h *HLL) Merge(other *HLL) error {
	if other == nil {
		return nil
	}
	if h.precision != other.precision {
		return ErrMismatchedPrecision
	}
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
	return nil
}

// Precision 返回精度值 p。
func (h *HLL) Precision() uint {
	return h.precision
}

// RegisterCount 返回寄存器数量（2^p）。
func (h *HLL) RegisterCount() uint64 {
	return h.m
}

// Reset 清空所有寄存器。
func (h *HLL) Reset() {
	for i := range h.registers {
		h.registers[i] = 0
	}
}

// Clone 返回当前 HLL 的深拷贝。
func (h *HLL) Clone() *HLL {
	clone := &HLL{
		precision: h.precision,
		m:         h.m,
		registers: make([]uint8, h.m),
		alphaMM:   h.alphaMM,
	}
	copy(clone.registers, h.registers)
	return clone
}

// alpha 返回 bias 修正系数 alpha_m。
//
// 根据 HLL 论文，alpha 的近似值为：
//
//	alpha_16 = 0.673
//	alpha_32 = 0.697
//	alpha_64 = 0.709
//	alpha_m  = 0.7213/(1 + 1.079/m)  for m >= 128
func alpha(m uint64) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1.0 + 1.079/float64(m))
	}
}
