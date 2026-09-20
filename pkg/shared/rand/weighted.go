// Package rand 提供加权随机相关原语。
// 当前实现 WeightedPicker：按权重不放回/放回抽样，纯标准库、线程安全、零外部依赖、
// 不依赖仓库内其它包。
package rand

import (
	"crypto/rand"
	"errors"
	"math"
	"math/big"
	mrand "math/rand/v2"
	"sync"
)

// ErrWeightOverflow 当累计权重会超过 int 可表示范围时返回。
var ErrWeightOverflow = errors.New("rand: total weight overflow")

type weightedItem struct {
	item   any
	weight int
}

// WeightedPicker 按权重随机抽样的选择器。零值为不可用，请用 NewWeightedPicker 构造。
type WeightedPicker struct {
	mu      sync.Mutex
	items   []weightedItem
	total   int
	randInt func(n int) int // 随机源：默认 crypto/rand，可通过 NewSeededPicker 切换为确定性 PRNG
}

// NewWeightedPicker 创建一个加权选择器（使用加密安全随机源，不需种子）。
func NewWeightedPicker() *WeightedPicker {
	return &WeightedPicker{randInt: cryptoRandIntn}
}

// NewSeededPicker 创建一个加权选择器（使用确定性随机源，相同种子产出相同序列）。
// 适用于回放/录像等需要可重复随机的场景。
//
// 说明（gosec G404 弱随机，**有意保留、不加 #nosec**）：
// 这里刻意用 math/rand/v2 而不是 crypto/rand —— **确定性回放就是设计目标**：
// 相同种子必须产出相同序列，换 crypto/rand 会直接破坏该能力。
// 本函数不用于任何安全用途（抽奖概率、掉落、AI 随机分支），
// 需要不可预测随机请用 NewWeightedPicker（默认 crypto/rand）。
func NewSeededPicker(seed uint64) *WeightedPicker {
	r := mrand.New(mrand.NewPCG(seed, seed))
	return &WeightedPicker{randInt: func(n int) int { return r.IntN(n) }}
}

// cryptoRandIntn 返回 [0, n) 的加密安全随机整数。
//
// crypto/rand 极少失败：持续重试（有限大循环，避免理论死循环），仅在极端连续失败时
// 回退到 math/rand，取值落在 [0,n) 内且不恒为 0，把分布失真降到最低。
func cryptoRandIntn(n int) int {
	if n <= 0 {
		return 0
	}
	const maxRetries = 1000
	for i := 0; i < maxRetries; i++ {
		v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
		if err == nil {
			return int(v.Int64())
		}
	}
	// 熵源持续不可用的极端兜底：用 math/rand 生成，落在 [0,n)，避免恒定偏向 0。
	// #nosec G404 -- 仅在 crypto/rand 连续 1000 次失败的极端场景生效，非常规路径。
	return mrand.IntN(n)
}

// Add 添加一个带权重的候选项；weight<=0 的项会被忽略。
// 累计权重会超过 int 上限时拒绝该项并返回 ErrWeightOverflow：
// 溢出会让 total 变成非正值，之后每次 Pick 都只能静默失败。
func (p *WeightedPicker) Add(item any, weight int) error {
	if weight <= 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if weight > math.MaxInt-p.total {
		return ErrWeightOverflow
	}
	p.items = append(p.items, weightedItem{item: item, weight: weight})
	p.total += weight
	return nil
}

// TotalWeight 返回当前所有有效项权重之和。
func (p *WeightedPicker) TotalWeight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total
}

// Pick 按权重随机返回一项；无任何有效项时返回 (nil, false)。
func (p *WeightedPicker) Pick() (any, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.total <= 0 {
		return nil, false
	}
	r := p.randInt(p.total) // [0, total)
	acc := 0
	for _, it := range p.items {
		acc += it.weight
		if r < acc {
			return it.item, true
		}
	}
	return p.items[len(p.items)-1].item, true
}

// PickN 不放回抽取 n 项；n 大于总项数时返回全部（顺序随机）。
func (p *WeightedPicker) PickN(n int) []any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n <= 0 {
		return []any{}
	}
	// 先收敛到候选项数、再分配容量：PickN(1e9) 而候选只有几项时，
	// 按入参 n 预分配会先要一块 GB 级底层数组 → OOM/panic。
	if n > len(p.items) {
		n = len(p.items)
	}
	result := make([]any, 0, n)
	// 在副本上做不放回抽样，避免改动原表。
	tmp := make([]weightedItem, len(p.items))
	copy(tmp, p.items)
	total := p.total
	for i := 0; i < n; i++ {
		if total <= 0 {
			break
		}
		r := p.randInt(total)
		acc := 0
		idx := 0
		for j, it := range tmp {
			acc += it.weight
			if r < acc {
				idx = j
				break
			}
		}
		result = append(result, tmp[idx].item)
		total -= tmp[idx].weight
		// 从副本中移除已选项（O(n) 交换删除）。
		tmp[idx] = tmp[len(tmp)-1]
		tmp = tmp[:len(tmp)-1]
	}
	return result
}

// Reset 清空所有候选项。
func (p *WeightedPicker) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = nil
	p.total = 0
}
