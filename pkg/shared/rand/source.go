package rand

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand/v2"
)

// Source 普通随机源，提供 IntN/Shuffle/Float64 等基础随机能力。
// 零值不可用，请用 NewSource 或 NewSeededSource 构造。
type Source struct {
	randInt   func(n int) int
	randFloat func() float64
}

// NewSource 创建一个加密安全的随机源（不需要种子）。
func NewSource() *Source {
	return &Source{
		randInt:   cryptoRandIntn,
		randFloat: cryptoRandFloat64,
	}
}

// NewSeededSource 创建一个确定性随机源（相同种子产出相同序列），用于回放/录像等场景。
//
// 说明（gosec G404 弱随机，**有意保留、不加 #nosec**）：
// 刻意用 math/rand/v2 而非 crypto/rand —— **确定性回放就是设计目标**（同种子必须同序列）。
// 安全无关的随机（战斗、掉落、AI 分支）走本函数；需要不可预测时用 NewSource（crypto/rand）。
func NewSeededSource(seed uint64) *Source {
	r := mrand.New(mrand.NewPCG(seed, seed))
	return &Source{
		randInt:   func(n int) int { return r.IntN(n) },
		randFloat: func() float64 { return r.Float64() },
	}
}

// IntN 返回 [0, n) 范围内的随机整数；n <= 0 时返回 0。
func (s *Source) IntN(n int) int {
	if n <= 0 {
		return 0
	}
	return s.randInt(n)
}

// Float64 返回 [0, 1) 范围内的随机浮点数。
func (s *Source) Float64() float64 {
	return s.randFloat()
}

// Shuffle 将 slice 按 Fisher-Yates 算法随机打乱。
// swap 由调用者提供，典型用法：
//
//	src.Shuffle(len(arr), func(i, j int) { arr[i], arr[j] = arr[j], arr[i] })
func (s *Source) Shuffle(n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		j := s.IntN(i + 1)
		swap(i, j)
	}
}

// Pick 从 slice 中随机选一个元素；slice 为空时返回零值和 false。
func (s *Source) Pick(slice []string) (string, bool) {
	if len(slice) == 0 {
		return "", false
	}
	return slice[s.IntN(len(slice))], true
}

// cryptoRandFloat64 返回 [0, 1) 的加密安全随机浮点数。
// 失败时退化为 math/rand 生成，避免返回固定值 0 导致随机性坍缩。
func cryptoRandFloat64() float64 {
	// 取 53 位随机整数（IEEE 754 双精度尾数精度），除以 2^53 得到 [0,1)
	const maxInt53 = 1 << 53
	v, err := rand.Int(rand.Reader, big.NewInt(maxInt53))
	if err != nil {
		// 退化为 math/rand，保证分布均匀。
		//
		// 说明（gosec G404 弱随机，**有意保留、不加 #nosec**）：
		// 这只是 crypto/rand 读熵失败时的兜底，目标是「不返回恒定 0、保持均匀」，
		// 此时已无密码学质量可言；改成 crypto/rand 重试就等于原地打转（上面刚失败过）。
		return mrand.Float64()
	}
	return float64(v.Int64()) / float64(maxInt53)
}
