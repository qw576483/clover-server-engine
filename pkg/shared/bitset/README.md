# bitset 模块

## 模块职责

`bitset` 提供**动态位集合**，支持位级的置位 / 复位 / 检测 / 翻转，以及集合级的并、交、差、异或运算。长度任意且可按需增长，底层按 **64 位字（uint64）** 分块存储，纯标准库、零外部依赖。典型用途包括权限与标志位掩码、AOI 视野格子标记、对象集合（在线 / 离线 / 屏蔽名单）等，与 `gobject/schema` 的 Flag 位、`aoi` 的格子标记天然互补。同时提供大端自描述序列化（`Bytes`/`FromBytes`），可持久化与跨语言互通。**并发不安全**，共享使用时需外部加锁或 per-goroutine 持有。

## 规则与约束

1. **并发保护**：`BitSet` 的所有方法均非并发安全，跨 goroutine 共享时必须由调用方使用 `sync.RWMutex` 等外部同步原语保护，或为每个 goroutine 分配独立实例后再合并结果。
2. **集合运算的接收者语义**：`Or`/`And`/`AndNot`/`Xor` 一律就地修改接收者并返回其自身，`other` 保持只读；需要保留原值时必须先 `Clone()`，即写作 `a.Clone().Or(b)`。
3. **集合运算的扩容语义**：四个集合运算在执行前都会把接收者的有效位数扩到 `max(b.n, other.n)`，调用方必须预期 `Len()` 可能因此变大（新增位一律为 0），不得假设运算前后 `Len()` 不变。
4. **越界返回值必须检查**：`Set`/`Reset`/`Check`/`Test`/`Flip` 在 `pos < 0 || pos >= Len()` 时返回 `false` 而不 panic，需要识别下标错误的调用方必须自行检查返回值。
5. **`Equal` 的比较语义**：`Equal` 只比较置位集合，声明位数不同但置位相同即判定相等；需要严格比较长度时必须额外比对 `Len()`。
6. **`ToUint64` 的适用条件**：该函数仅在 `Len() <= 64` 时可用，否则返回 `(0, false)`；`Len() < 64` 时返回值中超出声明位数的高位已被掩码清零。
7. **序列化的位数上限**：`Bytes()` 以 `uint32` 编码有效位数，`Len()` 必须落在 `[0, math.MaxUint32]` 范围内，超出部分在序列化时被截断。
8. **反序列化的数据完整性由调用方保证**：`FromBytes` 仅把空数据按空集合处理，其余不合法输入（长度不是 `4 + 8k`、`n` 超出位数据容量或与位数据不一致等）一律返回 `ErrInvalidData`——**不做宽容截断**；需要检测数据损坏时仍可在传输层附加校验和。
9. **空集合的语义**：`New(0)` 为合法实例，其 `Len()` 为 `0`、`All()` 返回 `true`（空集合平凡全真）、`String()` 返回空串，调用方必须显式处理这些分支。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `bitset.go` | 378 | 全部内容：`BitSet` 类型、单位操作（Set/Reset/Check/Test/Flip）、聚合查询（Any/None/All/Count/FirstSet/FirstZero/Slice）、集合运算（Or/And/AndNot/Xor/Equal）、生命周期（New/Len/Clear/Clone/grow）、序列化（String/Bytes/FromBytes/ToUint64）、内部辅助（wordsFor/maxInt） |

## 核心类型与接口

### `BitSet`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `bits` | `[]uint64` | 底层位存储，每个元素承载 64 位 |
| `n` | `int` | 声明的**有效位数**，取值范围 `0 .. len(bits)*64`。所有边界检查以 `n` 为准，而非 `len(bits)*64` |

### 常量

- `wordBits = 64`：单字位宽。

**并发安全性**：`BitSet` **完全并发不安全**。任何写方法（`Set`/`Reset`/`Flip`/`Clear`/`Or`/`And`/`AndNot`/`Xor`）与其它任意方法并发调用都会产生数据竞争；`Or/And/AndNot/Xor` 还可能触发 `grow` 重新分配底层切片。共享场景请用 `sync.RWMutex` 包裹，或每个 goroutine 持有独立实例后再合并。

## 算法与实现原理

### 位布局与寻址

索引 `pos` 的位落在 `bits[pos/64]` 的第 `pos%64` 位（**低位在前**，即 bit0 是字的最低有效位）：

```go
word := pos / wordBits      // 字下标
mask := uint64(1) << uint(pos % wordBits)  // 字内掩码
```

- **置位**：`bits[w] |= mask`
- **复位**：`bits[w] &^= mask`（Go 的 AND NOT 运算符）
- **检测**：`bits[w] & mask != 0`
- **翻转**：`bits[w] ^= mask`

所有单位操作都是 **O(1)**，越界（`pos < 0 || pos >= n`）时不 panic，而是返回 `false`（`Check` 返回 `false`）。

### 尾字掩码（tail mask）

当 `n` 不是 64 的整数倍时，最后一个字里有 `64 - n%64` 个「无效高位」。`All()` 与 `FirstZero()` 都通过构造尾掩码 `mask = (1<<rem) - 1` 只比较有效低 `rem` 位，避免把未使用的高位误判为 0。`ToUint64()` 同理，在 `n < 64` 时用掩码清零高位，防止泄漏未声明的位。

### 位计数与扫描（硬件加速）

依赖 `math/bits` 内建，通常编译为单条 CPU 指令：

- `Count()`：逐字 `bits.OnesCount64`（POPCNT），O(n/64)。
- `FirstSet()`：找到首个非零字后 `bits.TrailingZeros64`（TZCNT），O(n/64)。
- `FirstZero()`：逐字与 `^uint64(0)` 比较，找到首个非全 1 字后对 `^w` 取 TrailingZeros；尾字额外套掩码，用 `^w & mask` 定位。全置位返回 `-1`。
- `Slice()`：对每个字循环「取最低置位 → 清掉它」（`w &^= 1<<bit`），输出所有置位索引，复杂度 O(置位个数)，而非 O(n)。

### 自动扩容 grow

`grow(m)` 在 `m > n` 时把有效位数扩到 `m`；若所需字数超过当前 `len(bits)` 则重新 `make` 并 `copy`（新增部分天然为 0）。四个集合运算 `Or/And/AndNot/Xor` 在执行前都会 `grow(max(b.n, other.n))`，保证结果长度取两者较大值。注意 **只有接收者 `b` 会扩容并被就地修改，`other` 保持只读**。

对超出 `other.bits` 范围的字，`Or/Xor` 视 `other` 为 0（不变），`And` 视为 0（清零 `b` 的高位字），`AndNot` 视为 0（不变）。

### Equal 的语义

`Equal` 比较「表示同一组置位」而非「长度相同」：先逐字比较公共前缀，再要求各自超出公共部分的字**全为 0**。因此 `New(10)` 与 `New(1000)` 在都未置位时是 `Equal` 的。

### 序列化格式（大端、自描述）

`Bytes()` 输出 `4 字节 n（BigEndian uint32） + len(bits) 个 8 字节字（BigEndian uint64）`，总长 `4 + len(bits)*8`。序列化前对 `n` 做限幅到 `[0, math.MaxUint32]`。`FromBytes` 反向还原：空数据返回 `New(0)`；其余不合法输入（长度不是 `4 + 8k`、`n` 转 `int` 溢出或超过字容量、`n` 之外仍有残留置位）一律返回 `ErrInvalidData`，不再宽容截断。

### String 表示

`String()` 输出**高位在左**的二进制串：`buf[n-1-i]` 对应 bit `i`，即最左字符是 bit `n-1`。`n == 0` 时返回空串。

## 对外 API

### 构造与基本信息

```go
func New(size int) *BitSet    // size<0 视作 0
func (b *BitSet) Len() int    // 返回有效位数 n
func (b *BitSet) Clone() *BitSet
func (b *BitSet) Clear()
```

```go
bs := bitset.New(128)
fmt.Println(bs.Len()) // 128
cp := bs.Clone()      // 深拷贝，互不影响
bs.Clear()            // 全部复位
```

### 单位操作

```go
func (b *BitSet) Set(pos int) bool    // 置位，越界 false
func (b *BitSet) Reset(pos int) bool  // 复位，越界 false
func (b *BitSet) Check(pos int) bool  // 检测，越界 false
func (b *BitSet) Test(pos int) bool   // Check 的别名
func (b *BitSet) Flip(pos int) bool   // 翻转，越界 false
```

```go
bs := bitset.New(64)
bs.Set(3)
fmt.Println(bs.Check(3)) // true
bs.Flip(3)
fmt.Println(bs.Test(3))  // false
fmt.Println(bs.Set(999)) // false（越界，不 panic）
```

### 聚合查询

```go
func (b *BitSet) Any() bool       // 至少一位置位
func (b *BitSet) None() bool      // 一位都没置位
func (b *BitSet) All() bool       // [0,n) 全部置位
func (b *BitSet) Count() int      // 置位个数
func (b *BitSet) FirstSet() int   // 最低置位索引，无则 -1
func (b *BitSet) FirstZero() int  // 最低未置位索引，全置位则 -1
func (b *BitSet) Slice() []int    // 所有置位索引（升序）
```

```go
bs := bitset.New(10)
bs.Set(1); bs.Set(5); bs.Set(9)
fmt.Println(bs.Count())     // 3
fmt.Println(bs.FirstSet())  // 1
fmt.Println(bs.FirstZero()) // 0
fmt.Println(bs.Slice())     // [1 5 9]
fmt.Println(bs.Any(), bs.None(), bs.All()) // true false false
```

### 集合运算（原地修改接收者，链式返回）

```go
func (b *BitSet) Or(other *BitSet) *BitSet      // 并集
func (b *BitSet) And(other *BitSet) *BitSet     // 交集
func (b *BitSet) AndNot(other *BitSet) *BitSet  // 差集 b \ other
func (b *BitSet) Xor(other *BitSet) *BitSet     // 对称差
func (b *BitSet) Equal(other *BitSet) bool
```

```go
a := bitset.New(8); a.Set(0); a.Set(1)
c := bitset.New(8); c.Set(1); c.Set(2)

a.Clone().Or(c).Slice()     // [0 1 2]
a.Clone().And(c).Slice()    // [1]
a.Clone().AndNot(c).Slice() // [0]
a.Clone().Xor(c).Slice()    // [0 2]
```

### 序列化与转换

```go
var ErrInvalidData                        // FromBytes 对损坏数据的错误返回值
func (b *BitSet) String() string          // 高位在左的 0/1 串
func (b *BitSet) Bytes() []byte           // 4B 长度 + N*8B 大端字
func FromBytes(data []byte) (*BitSet, error) // Bytes 的逆操作；非法输入返回 ErrInvalidData
func (b *BitSet) ToUint64() (uint64, bool) // n<=64 时返回底层值
```

```go
bs := bitset.New(5)
bs.Set(0); bs.Set(2)
fmt.Println(bs.String()) // "00101"（bit4..bit0）

raw := bs.Bytes()
back, err := bitset.FromBytes(raw) // 非法输入返回 ErrInvalidData（back 为 nil）
fmt.Println(err == nil && back.Equal(bs)) // true

v, ok := bs.ToUint64()
fmt.Println(v, ok) // 5 true
```

## 依赖关系

- **仅依赖 Go 标准库**：`encoding/binary`（大端序列化）、`math`（`MaxUint32` 限幅）、`math/bits`（`OnesCount64` / `TrailingZeros64`）。
- 零第三方依赖、零引擎内部依赖，可纯内存单测。
- 被上层的标志位掩码、AOI 格子标记等场景使用。
