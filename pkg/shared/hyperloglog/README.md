# hyperloglog 模块

## 模块职责

`hyperloglog` 提供 HyperLogLog 基数估计算法，用于以极小内存在 O(1) 时间内估计集合中不同元素的个数。支持多个 HLL 实例的合并（Merge），适合分布式聚合场景。

## 规则与约束

1. **多协程共享 `HLL` 必须由调用方加锁**：本包不提供任何内部同步。
2. **`Merge` 要求双方 `precision` 一致**：精度不一致时返回 `ErrMismatchedPrecision`，且不修改任何寄存器。
3. **`Count` 返回的是估值而非精确值**：结果带有 `±0.4% ~ ±26%` 的标准误差（由 `precision` 决定），不得当作精确去重计数使用。
4. **基数上限由 64 位哈希宽度决定**：`Add` 采用 64 位 FNV-1a 哈希，寄存器值占 `64-precision` 位，超出该量级后估计值饱和。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `hyperloglog.go` | 228 | `HLL` 结构体、`NewHLL`、`Add`、`Count`、`Merge`、`Clone`、bias 修正系数 |

## 核心类型与接口

### HLL

```go
type HLL struct {
    // 未导出字段
}
```

**并发安全性**：**否**。多协程共享需外部加锁。

## 对外 API

### NewHLL

```go
func NewHLL(precision uint) (*HLL, error)
```

创建一个 HyperLogLog 实例。precision 取值范围 [4, 16]，越界返回 `ErrPrecisionOutOfRange`（不做静默钳制）：

| precision | 寄存器数 | 内存   | 标准误差 |
|-----------|---------|--------|---------|
| 4         | 16      | ~16B   | ~26%    |
| 8         | 256     | ~256B  | ~6.5%   |
| 12        | 4096    | ~4KB   | ~1.6%   |
| 14        | 16384   | ~16KB  | ~0.8%   |
| 16        | 65536   | ~64KB  | ~0.4%   |

### Add

```go
func (h *HLL) Add(data []byte)
```

添加一个元素到估计器。

### Count

```go
func (h *HLL) Count() uint64
```

返回估计的去重元素个数。小基数时自动使用 Linear Counting 修正。

### Merge

```go
func (h *HLL) Merge(other *HLL) error
```

合并另一个 HLL 到当前实例（取各寄存器最大值）。

### Clone / Reset

辅助方法。

## 算法原理

1. 对输入 data 计算 FNV-1a 64 位哈希
2. 低 p 位确定寄存器索引，剩余位计算前导零个数
3. 寄存器存储观测到的最大前导零+1
4. Count 使用调和平均公式 + Linear Counting 小基数修正

## 依赖关系

- `pkg/shared/util`：`Fnv32` 哈希原语（当前实现在包内内联 FNV-1a，未直接引用）
- 纯标准库，零外部依赖
