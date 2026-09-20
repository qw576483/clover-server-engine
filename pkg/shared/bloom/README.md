# bloom 模块

## 模块职责

`bloom` 提供基于 FNV-1a 双哈希的**布隆过滤器**。布隆过滤器是一种空间高效的概率性数据结构，用于判定"绝对不在集合中"或"可能在集合中"。它允许可配置的假阳性率（false positive rate），**绝无假阴性**。

## 规则与约束

1. **并发保护**：`Filter` 非并发安全，多 goroutine 共享同一实例时必须由调用方加锁保护。
2. **元素不可删除**：标准布隆过滤器不支持删除单个元素，`Add` 的结果不可撤销，需要删除能力时必须改用计数型布隆过滤器；`Reset` 只能整体清空。
3. **插入量上限**：`expectedItems` 必须按实际插入总量的上限设定，插入量超过该值后实际假阳性率会显著高于设定值，且 `Contains` 的 `false` 结论仍然可靠、`true` 结论的可信度随之下降。
4. **`EstimatedSize` 的口径**：该函数返回 `Add` 的调用次数而非去重后的元素数，重复插入同一元素会被重复计数，不得用作去重基数或容量水位依据。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `bloom.go` | 198 | 全部内容：`Filter` 结构体、构造（`NewBloomFilter`）、写入与查询（`Add`/`Contains`）、状态观测（`EstimatedSize`/`BitSize`/`HashCount`/`FalsePositiveRate`/`String`）、清空（`Reset`）、内部哈希与位操作（`hash`/`setBit`/`getBit`） |

## 核心类型与接口

### Filter

```go
type Filter struct {
    // 未导出字段
}
```

**并发安全性**：**否**。多协程共享需外部加锁。

## 对外 API

### NewBloomFilter

```go
func NewBloomFilter(expectedItems int, falsePositiveRate float64) (*Filter, error)
```

创建一个布隆过滤器。根据预期元素数量和假阳性率自动计算最优的位数组大小和哈希函数个数；入参越界（<=0 或 NaN/Inf）时按默认值兜底，推导出的位数组规模超过上限时返回 `ErrTooLarge`。

```go
// 预期 100000 个元素，1% 假阳性率
bf, err := bloom.NewBloomFilter(100_000, 0.01)
if err != nil {
    // ErrTooLarge：规模过大，减小 expectedItems 或放宽 falsePositiveRate
}
```

### Add

```go
func (f *Filter) Add(data []byte)
```

向过滤器中添加元素。

### Contains

```go
func (f *Filter) Contains(data []byte) bool
```

检查元素是否可能在集合中。`false` 表示绝对不在；`true` 表示可能在（存在假阳性）。

### Reset

```go
func (f *Filter) Reset()
```

清空所有数据。

### EstimatedSize / BitSize / HashCount / FalsePositiveRate / String

```go
func (f *Filter) EstimatedSize() uint64           // 已 Add 的次数（非去重元素数）
func (f *Filter) BitSize() uint64                 // 位数组总位数 m
func (f *Filter) HashCount() uint64               // 哈希函数个数 k
func (f *Filter) FalsePositiveRate() float64      // 按当前 n 估算的假阳性率
func (f *Filter) String() string                  // 形如 BloomFilter(m=…, k=…, n=…, p≈…)
```

用于观测与调试的辅助方法。

## 算法原理

### 双哈希（Double Hashing）

使用 FNV-1a 生成两个基础哈希值 h1 和 h2，然后通过 Kirsch-Mitzenmacher 公式派生第 i 个哈希：

```
hash_i(data) = (h1(data) + i * h2(data)) % m
```

这避免了为 k 个哈希函数分别计算独立哈希的开销。

### 参数计算

给定预期元素数 n 和目标假阳性率 p：

```
m = ceil(-n * ln(p) / (ln(2))^2)    // 位数组大小
k = ceil((m/n) * ln(2))              // 哈希函数个数
```

## 依赖关系

- **标准库**：`fmt`（`String` 格式化）、`math`（参数计算与假阳性率估计）。
- **引擎内部**：`clover-server-engine/pkg/shared/util`（`Fnv32` / `Fnv32Key`，哈希原语）。
- 零第三方依赖。
