# rand 模块

## 模块职责

`rand` 提供**加权随机相关原语**与通用随机源。当前实现 `WeightedPicker`（按权重做放回抽样 `Pick` 与不放回抽样 `PickN`）与 `Source`（`IntN` / `Float64` / `Shuffle` / `Pick`），典型用于掉落表、随机怪物刷新、抽卡池、随机事件触发等场景。设计上有三个明确取向：**使用 `crypto/rand` 作为随机源**（满足安全扫描 G404 要求，同时避免掉落被预测）、**线程安全**（内部 `sync.Mutex`，可多协程共享同一选择器）、**零外部依赖且不依赖仓库内其它包**（可独立单测）。

> 注意：本包与标准库 `math/rand`、`crypto/rand` 同名，import 时通常需要起别名。

## 规则与约束

1. **导入本包必须起别名**：包名与标准库 `math/rand` / `crypto/rand` 冲突，建议 `urand "clover-server-engine/pkg/shared/rand"`；同一文件内还需 `crypto/rand` 时两者都必须区别命名。
2. **超高频随机判定不得使用本包**：默认随机源为 `crypto/rand`，单次抽样涉及系统调用与 `big.Int` 分配；战斗内每帧数千次的判定必须另建基于 `math/rand` 的快速路径。
3. **高并发场景不得共享同一个 picker**：`Pick` / `PickN` 全程持锁，且随机源在锁内执行，必须为每个协程或每个战斗实例分配独立 picker，或改为「取快照 → 锁外抽样」的模式。
4. **`PickN` 的持锁时长必须由调用方评估**：其复杂度为 O(n·m)（n 为抽取数、m 为候选数）且全程持同一把锁，单次抽取大量项会阻塞其余调用者。
5. **`Add` 只接受正权重**：`weight <= 0` 的项被静默丢弃，既不报错也不计入 `TotalWeight()`；从外部配置构建权重表后必须用 `TotalWeight()` 校验总量是否符合预期。
6. **`Pick` 的返回值必须做安全断言**：返回类型为 `any`，必须使用 `v, ok := x.(T)` 的双返回值形式，且同一池中应只存放同一具体类型。
7. **`Pick` 与 `PickN` 均不改变选择器状态**：`Pick` 为放回抽样，`PickN` 在副本上操作；多次 `PickN(1)` 不等价于一次 `PickN(n)`，需要 n 个不重复结果时必须一次性调用 `PickN(n)`。
8. **`PickN` 的返回顺序为抽取顺序而非配置顺序**：内部采用交换删除且抽取本身是加权的，需要稳定顺序时必须由调用方自行排序。
9. **`PickN(n)` 恒返回 `min(n, 项数)` 项**：`n >= 项数` 时退化为加权随机排列，调用方不得据此推断「结果可能少于 n 项」。
10. **不可依赖降级路径的随机强度**：仅在 `crypto/rand` 连续失败 1000 次后才降级为 `math/rand/v2` 的非加密随机值，该降级无日志与告警；涉及安全或公平性的场景必须在降级发生后停用相关玩法。
11. **可复现的抽样序列必须使用 `NewSeededPicker(seed)` 构造**：`NewWeightedPicker()` 使用 `crypto/rand`，不可播种，无法复现。
12. **空池判定必须按返回类型区分**：`Pick` 返回 `(nil, false)`，`PickN` 返回 `len == 0` 的非 nil 切片，判空必须使用 `len()`。
13. **权重需要动态调整时必须 `Reset()` 后全量重建**：本包不提供 `Remove` / `SetWeight`。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `weighted.go` | 150 | `weightedItem` 内部条目、`WeightedPicker` 选择器主体、`NewWeightedPicker`/`NewSeededPicker` 构造、`cryptoRandIntn` 加密安全随机整数（含重试与降级）、`Add`/`TotalWeight`/`Pick`/`PickN`/`Reset` 五个导出方法、错误值 `ErrWeightOverflow` |
| `source.go` | 77 | `Source` 通用随机源：`NewSource`/`NewSeededSource` 构造、`IntN`/`Float64`/`Shuffle`/`Pick` 四个导出方法，及内部 `cryptoRandFloat64` |

## 核心类型与接口

### `weightedItem`（私有）

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `item` | `any` | 候选项本体，类型不限 |
| `weight` | `int` | 权重，必然 `> 0`（`Add` 已过滤 `<= 0`） |

### `WeightedPicker`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `mu` | `sync.Mutex` | 保护 `items` 与 `total` |
| `items` | `[]weightedItem` | 候选项列表，按 `Add` 顺序排列 |
| `total` | `int` | 所有有效项权重之和，随 `Add` 累加、随 `Reset` 归零 |

**零值不可用**，必须用 `NewWeightedPicker()` 构造（虽然零值实际上也能工作，但这是包注释的明确约定，不应依赖）。

**并发安全性**：**完全并发安全**。所有五个导出方法都在入口 `p.mu.Lock()` / `defer p.mu.Unlock()`。注意 `Pick` 与 `PickN` 在**持锁期间调用 `randIntn`**，而后者可能做 `crypto/rand` 的系统调用——因此高并发下这把锁会成为串行点。

## 算法与实现原理

### 加权随机的核心：累积权重 + 区间落点

给定权重 `[w₁, w₂, ..., wₙ]`，总和 `T`。抽取一个 `r ∈ [0, T)` 的均匀随机整数，然后沿列表累加权重，找到第一个使 `r < acc` 的位置：

```
items:  A(w=5)   B(w=3)   C(w=2)
acc:    5        8        10
区间:   [0,5)    [5,8)    [8,10)
```

`r = 6` 落在 `[5,8)` → 选中 B。每一项被选中的概率恰好是 `wᵢ / T`。

**复杂度**：`Pick` 是 **O(n)** 线性扫描（没有用前缀和 + 二分的 O(log n) 优化，也没有 Alias Method 的 O(1) 优化）。候选项少时完全够用，上千项的掉落表在热路径上会有开销。

**兜底分支**：`Pick` 循环结束后仍有 `return p.items[len(p.items)-1].item, true`。理论上不可达（`r < total` 必然在某处命中），是防御性代码——防止浮点/整数舍入或并发修改导致的边界遗漏。

### 不放回抽样 PickN

```
tmp := items 的副本      // ★ 不改动原表
total := p.total
n = min(n, len(tmp))
重复 n 次:
    r := randIntn(total)
    线性扫描找到落点 idx
    收集 tmp[idx].item
    total -= tmp[idx].weight        // 从总权重中扣除
    tmp[idx] = tmp[len(tmp)-1]      // ★ 交换删除（O(1)）
    tmp = tmp[:len(tmp)-1]
```

关键点：

- **在副本上操作**：原 `items` 与 `total` 不受影响，`PickN` 可反复调用得到不同结果。
- **交换删除（swap-remove）**：把最后一个元素挪到被删位置再截断，**O(1)** 而非 `append(s[:i], s[i+1:]...)` 的 O(n)。代价是**打乱了剩余元素的顺序**——但这不影响概率正确性（每一轮都重新计算累积区间）。
- **同步扣减 total**：保证下一轮的 `randIntn(total)` 落在正确的剩余总权重范围内。
- **n > 项数时返回全部**：先做 `if n > len(tmp) { n = len(tmp) }` 钳制。此时返回的是全部项，**顺序随机**（因为抽取顺序本身就是加权随机的）。
- **总复杂度 O(n·m)**（n 为抽取数，m 为候选数）。

`total <= 0` 时提前 `break`（理论上不可达，因为所有 weight > 0）。

### cryptoRandIntn：加密安全随机整数

```go
func cryptoRandIntn(n int) int {
    if n <= 0 { return 0 }
    const maxRetries = 1000
    for i := 0; i < maxRetries; i++ {
        v, err := rand.Int(rand.Reader, big.NewInt(int64(n))) // crypto/rand
        if err == nil { return int(v.Int64()) }
    }
    // 极端兜底：math/rand/v2 生成，落在 [0,n)，避免恒定偏向 0
    return mrand.IntN(n)
}
```

**为什么用 `crypto/rand.Int`**：它内部已做**拒绝采样**消除取模偏置，保证 `[0, n)` 严格均匀，同时不可预测（防止玩家推算掉落 RNG 状态）。代价是每次调用有系统调用与 `big.Int` 分配开销，比 `math/rand` 慢一到两个数量级。

**最多重试 1000 次**（有限大循环，避免理论上的死循环），只有在熵源连续 1000 次失败的极端情况下才降级——且不采用「回退固定值」（回退 0 会让**权重分布被系统性拉向首个区间**）。

**降级路径**：用 `math/rand/v2` 的 `IntN(n)`。这个值虽然低熵且可预测，但**落在 `[0, n)` 内且不恒为 0**，把分布失真降到最低。`#nosec G404` 注释说明这是非常规路径，已评估过安全影响。

## 对外 API

### 构造

```go
func NewWeightedPicker() *WeightedPicker           // crypto/rand 随机源（不可播种）
func NewSeededPicker(seed uint64) *WeightedPicker  // 确定性随机源，相同种子产出相同序列
```

```go
p := urand.NewWeightedPicker()
```

### `Add`

```go
func (p *WeightedPicker) Add(item any, weight int) error
```

用途：添加一个带权重的候选项。**`weight <= 0` 的项会被静默忽略**（返回 nil、不加入列表、不计入 total）；**累计权重会超过 `int` 上限时拒绝该项并返回 `ErrWeightOverflow`**。

```go
_ = p.Add("传说装备", 1)
_ = p.Add("稀有装备", 9)
_ = p.Add("普通装备", 90)
_ = p.Add("不会出现", 0)   // 被忽略（返回 nil）
_ = p.Add("也不会出现", -5) // 被忽略（返回 nil）
```

### `TotalWeight`

```go
func (p *WeightedPicker) TotalWeight() int
```

用途：返回当前所有有效项权重之和，可用于校验配置或计算单项概率。

```go
total := p.TotalWeight() // 100
// 传说装备概率 = 1 / 100 = 1%
```

### `Pick`（放回抽样）

```go
func (p *WeightedPicker) Pick() (any, bool)
```

用途：按权重随机返回一项。**无任何有效项时返回 `(nil, false)`**。同一项可被反复抽中。

```go
for i := 0; i < 10; i++ {
    v, ok := p.Pick()
    if !ok {
        break // 池为空
    }
    item := v.(string) // 需要类型断言
    fmt.Println(item)
}
```

### `PickN`（不放回抽样）

```go
func (p *WeightedPicker) PickN(n int) []any
```

用途：不放回抽取 n 项。**n 大于总项数时返回全部（顺序随机）**；`n <= 0` 返回空切片（非 nil）。

```go
// 从奖池中抽 3 个不重复的奖品
prizes := p.PickN(3)
for _, v := range prizes {
    fmt.Println(v.(string))
}

// n 超量：返回全部 3 项，顺序随机
all := p.PickN(100) // len(all) == 3

// n<=0：返回空切片（len==0，非 nil）
none := p.PickN(0)
```

### `Reset`

```go
func (p *WeightedPicker) Reset()
```

用途：清空所有候选项（`items = nil`，`total = 0`），选择器可复用。

```go
p.Reset()
_ = p.Add("新一轮的奖品", 1)
```

### `Source`（通用随机源）

```go
func NewSource() *Source                   // crypto/rand 随机源（不可播种）
func NewSeededSource(seed uint64) *Source  // 确定性随机源，相同种子产出相同序列

func (s *Source) IntN(n int) int           // [0, n)，n<=0 返回 0
func (s *Source) Float64() float64         // [0, 1)
func (s *Source) Shuffle(n int, swap func(i, j int)) // Fisher-Yates 洗牌
func (s *Source) Pick(slice []string) (string, bool) // 空切片返回 ("", false)
```

### 完整示例：掉落表

```go
type Drop struct {
    ItemID int
    Count  int
}

func buildDropTable(cfg []DropConfig) *urand.WeightedPicker {
    p := urand.NewWeightedPicker()
    for _, c := range cfg {
        _ = p.Add(Drop{ItemID: c.ItemID, Count: c.Count}, c.Weight) // 仅累计权重溢出时返回 ErrWeightOverflow
    }
    return p
}

func rollDrop(p *urand.WeightedPicker) (Drop, bool) {
    v, ok := p.Pick()
    if !ok {
        return Drop{}, false
    }
    return v.(Drop), true
}
```

## 依赖关系

- **仅依赖 Go 标准库**：`crypto/rand`（加密安全随机源）、`errors`（错误值）、`math`（累计权重溢出判断）、`math/big`（`rand.Int` 的值域参数）、`math/rand/v2`（极端降级路径）、`sync`（互斥锁）。
- 零第三方依赖、**零引擎内部依赖**，可完全独立单测。

