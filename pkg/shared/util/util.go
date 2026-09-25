// Package util 提供引擎内常用的工具函数集合。
package util

import (
	"hash/fnv"
	"math/bits"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/cespare/xxhash/v2"
)

// maxRepresentablePow2 int 上可表示的最大 2 的幂（64 位平台为 2^62）：
// 再往上一档 1<<63 会溢出成负数。
const maxRepresentablePow2 = 1 << 62

// NextPow2 返回 >= n 的最小 2 的幂（n <= 1 时返回 1）。
//
// n > 2^62 时不存在可表示的更大 2 的幂：此处钳到 2^62 并留日志。
func NextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	if n&(n-1) == 0 {
		return n
	}
	if n > maxRepresentablePow2 {
		logger.Warnf("util.NextPow2: %d exceeds max representable power of two (2^62); clamped", n)
		return maxRepresentablePow2
	}
	return 1 << uint(bits.Len(uint(n)))
}

// Contains 报告切片 s 是否包含 v。
//
// 泛型版本：各处手写 `for ... if x == v` 小循环时，[]string / []ObjectID /
// 其它元素类型会各写一份同名函数，命名还各不相同（containsStr / containsID / ...）。
func Contains[T comparable](s []T, v T) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// Equal 报告两个切片是否等长且逐元素相等（顺序敏感）。
func Equal[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Fnv32 对字符串做 FNV-1a 32 位哈希，用于分片路由等场景。
func Fnv32(s string) uint32 {
	hf := fnv.New32a()
	_, _ = hf.Write([]byte(s))
	return hf.Sum32()
}

// Fnv32Key 对多个字段拼接后做 FNV-1a 哈希，字段间用 \x00 分隔避免碰撞。
func Fnv32Key(fields ...string) uint32 {
	hf := fnv.New32a()
	for i, f := range fields {
		if i > 0 {
			_, _ = hf.Write([]byte{0})
		}
		_, _ = hf.Write([]byte(f))
	}
	return hf.Sum32()
}

// Xxhash64 对字符串做 xxHash-64 哈希，返回 uint64，适用于分片路由等高频场景。
func Xxhash64(s string) uint64 {
	return xxhash.Sum64String(s)
}

// Xxhash64Key 对多个字段拼接后做 xxHash-64 哈希，字段间用 \x00 分隔避免碰撞。
func Xxhash64Key(fields ...string) uint64 {
	d := xxhash.New()
	for i, f := range fields {
		if i > 0 {
			_, _ = d.Write([]byte{0})
		}
		_, _ = d.Write([]byte(f))
	}
	return d.Sum64()
}
