# memrank 模块

## 模块职责

`memrank` 提供**通用有序榜（排行榜）引擎原语**。核心抽象是 `SortedSet` 接口，把「有序集合」的能力（设分、增分、查排名、取 TopN、按区间查询）标准化，默认给出线程安全的内存实现 `MemSortedSet`；若需要 Redis ZSET 后端，实现同一接口注入即可，业务代码零改动。在此之上提供 `Manager` 多榜注册表（按名字管理多个榜单）与**段位门槛（`master.Thresholds`）**机制——支持「排名区间需达到某最低分才能占位」的常见玩法规则，不满足门槛的名次会被跳空。包内还提供了一组包级便捷函数（`rank.Add` / `rank.Top` 等）直接操作默认 Manager。

> 示例别名说明：本文示例中的 `rank.` 指本包（`import rank "clover-server-engine/pkg/shared/memrank"`，实际包名为 `memrank`）；
> `master.` 指 `clover-server-engine/pkg/domain/master`（`RankMember` / `Threshold` / `Thresholds` 类型真身所在，本包直接复用）。

## 规则与约束

1. **首次上榜必须使用 `Set`**：`Incr` / `AddOnlyUpdateScore` / `IncrOnlyUpdateScore` 对不存在的成员一律返回 `0` / `(0, false)` 且不创建记录，与 Redis `ZINCRBY` 的自动创建行为不同。
2. **`Incr` 的返回值不区分「成员不存在」与「新分数为 0」**：需要区分时必须先调用 `Score` 判断存在性。
3. **排名区间参数统一为 1-based 闭区间**：`SortedSet.GetByRankRange` 与 `Manager.GetByRankRange` 的 `start` / `stop` 语义一致，调用方无需做下标换算。
4. **`RangeOpts` 的边界必须显式开启**：`HasMin` / `HasMax` 为 false 时对应边界视为 ±inf，零值 `RangeOpts{}` 覆盖全部成员。
5. **有段位门槛时不得高频调用 `Manager.GetMember`**：该查询需取出目标成员之前的全部成员并重算门槛，复杂度为 O(N)；无门槛时短路为 O(log n)。
6. **段位门槛的展开结果不缓存**：每次 `Top` / `GetByRankRange` / `All` / `GetMember` 都会重建 gates map，大跨度门槛区间必须控制调用频率。
7. **段位门槛区间按配置顺序覆盖**：`Thresholds` 中重叠的排名区间以后出现的配置为准，配置时必须自行避免冲突。
8. **写入操作复杂度为 O(n)**：每次 `Set` / `Incr` / `Remove` 都会搬移 `order` 数组，万级榜单高频写入场景必须改用 Redis ZSET 后端或批量重建。
9. **`Top` / `All` 对每条记录深拷贝 `Extra`**：只需成员名的场景必须使用 `Members()`。
10. **`Members()` 按排名升序返回 `order` 的副本**：返回顺序即排名顺序。
11. **写入分数前必须校验其为有限值**：`Set` 与 `Incr` 不校验 `NaN` / `Inf`，仅 `IncrOnlyUpdateScore` 拒绝写入；`NaN` 成员会被排到末尾但仍占据名次。
12. **生产路径必须使用 `Get` 或 `GetOrCreate`**：`MustGet` 对未注册的榜名直接 panic。
13. **`GetAll` 不应用段位门槛，`All` 应用段位门槛**：`GetAll` 仅用于备份等内部场景。
14. **`Boards()` 返回浅拷贝**：返回的 map 为新建，其中的 `SortedSet` 为同一批实例。
15. **包级 `Add` 不携带 `Extra`**：需要额外数据必须使用 `Manager.Add` 或直接调用 `SortedSet.Set`。
16. **生产代码必须显式创建 `Manager`**：包级便捷函数共享同一个 `defaultManager`，同名榜会互相干扰。
17. **`GetByRankRange` 的 `stop` 为闭区间**：`start` 越界会被钳制到首位，`stop < 0` 表示取到末尾，`start > stop` 返回 nil。
18. **`GetByScoreRange` 为 O(n) 全扫描**：`Offset` / `Limit` 在收集完所有匹配项之后才生效，深分页不减少扫描量。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `rank.go` | 52 | 包文档、`RangeOpts` 分数区间查询选项、`SortedSet` 后端接口、`New` 构造（`RankMember`/`Threshold`/`Thresholds` 与 `expandGates`/`AdjustMembers` 的真身在 `pkg/domain/master`） |
| `mem.go` | 320 | `MemSortedSet` 内存实现全部内容：`less` 排序谓词、`findIndex` 二分定位、`insert`/`removeFromOrder` 有序数组维护、`setLocked`/`entryLocked` 内部辅助、接口的 15 个方法实现、`inRange` 区间判定 |
| `manager.go` | 232 | `Manager` 多榜注册表（Register/Get/MustGet/GetOrCreate/Unregister/Names/Len/Boards/Add/GetAll）、门槛管理（SetRankThresholds/GetThresholds）、带门槛的查询（Top/GetMember/GetByRankRange/All）、`defaultManager` 与 6 个包级便捷函数 |

## 核心类型与接口

### `RankMember`（类型真身：`pkg/domain/master`）

| 字段 | 类型 | JSON | 含义 |
| --- | --- | --- | --- |
| `Member` | `string` | `member` | 成员标识（如 `"player:1001"`） |
| `Score` | `float64` | `score` | 分数 |
| `Rank` | `int` | `rank` | **1-based** 排名，1 = 分数最高 |
| `Extra` | `json.RawMessage` | `extra,omitempty` | 绑定的额外展示数据（玩家名、等级、头像等） |

### `RangeOpts`

| 字段 | 含义 |
| --- | --- |
| `Min` / `Max` | 分数区间边界 |
| `MinExclusive` / `MaxExclusive` | 边界是否**开区间**（true = 不含边界值） |
| `HasMin` / `HasMax` | **是否显式设置了边界**。false 时对应边界视为 ±inf |
| `Offset` / `Limit` | 分页：跳过前 Offset 个、最多返回 Limit 个（Limit<=0 表示不限） |

**关键**：零值 `RangeOpts{}` 表示 `-inf ~ +inf`（覆盖全部成员），因为 `HasMin`/`HasMax` 都是 false。要精确限定必须**同时**设置 `Min` 和 `HasMin: true`。这个双字段设计是为了区分「Min 就是 0」与「Min 未设置」。

### `SortedSet`（后端接口，15 个方法）

```go
type SortedSet interface {
    Set(member string, score float64, extra json.RawMessage)
    Incr(member string, delta float64) float64
    AddOnlyUpdateScore(member string, score float64, extra json.RawMessage) (float64, bool)
    IncrOnlyUpdateScore(member string, delta float64) (float64, bool)
    Get(member string) (master.RankMember, bool)   // 查分 + 排名（一次调用）
    Score(member string) (float64, bool)    // 仅查分
    Rank(member string) (int, bool)
    Remove(member string) bool
    Len() int
    Top(n int) []master.RankMember
    GetByRankRange(start, stop int) []master.RankMember     // 1-based 闭区间
    GetByScoreRange(opts RangeOpts) []master.RankMember
    Members() []string
    All() []master.RankMember
    Clear()
}
```

### `MemSortedSet`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `mu` | `sync.RWMutex` | 保护以下全部字段 |
| `scores` | `map[string]float64` | member → 分数，O(1) 查分 |
| `extras` | `map[string]json.RawMessage` | member → 额外数据 |
| `order` | `[]string` | **有序数组**，按「分数降序 + 同分 member 字典序升序」排列 |

**并发安全性**：**全部公开方法线程安全**。读方法（Get/Score/Rank/Len/Top/GetByRankRange/GetByScoreRange/Members/All）走 `RLock`，写方法（Set/Incr/AddOnlyUpdateScore/IncrOnlyUpdateScore/Remove/Clear）走 `Lock`。

### `Threshold` / `Thresholds`（类型真身：`pkg/domain/master`）

| 字段 | JSON | 含义 |
| --- | --- | --- |
| `MinRank` | `min_rank` | 排名区间下界（1-based，含） |
| `MaxRank` | `max_rank` | 排名区间上界（含） |
| `MinScore` | `min_score` | 占据该区间名次所需的**最低分数** |

`Thresholds` 是 `[]Threshold`。

### `Manager`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `mu` | `sync.RWMutex` | 保护 boards 与 thresholds |
| `boards` | `map[string]SortedSet` | 榜名 → 榜单实例 |
| `thresholds` | `map[string]master.Thresholds` | 榜名 → 段位门槛配置 |

**并发安全性**：**全部方法线程安全**。注意 `Top`/`GetMember`/`GetByRankRange`/`All` 采用「**先持锁取出 board 与 ts 引用，再释放锁，最后在锁外调用 board 方法**」的模式——避免在持 Manager 锁时调用可能耗时的后端操作，也避免与 board 自身的锁形成嵌套。

### `defaultManager`

包级默认实例，由 `rank.Add` / `rank.Top` / `rank.GetMember` / `rank.GetByRankRange` / `rank.All` / `rank.SetRankThresholds` 六个包级函数操作。

## 算法与实现原理

### 数据结构选型：有序数组 + 双 map（不是跳表）

**注意：本包没有使用跳表（skip list）**，而是「**有序数组 + 哈希表**」的组合：

```
scores: map[string]float64      // O(1) 查分
extras: map[string]RawMessage   // O(1) 查额外数据
order:  []string                // 有序数组，二分定位
```

这是一个**刻意的工程权衡**。对比 Redis ZSET 的跳表实现：

| 操作 | 本包（有序数组） | 跳表 |
| --- | --- | --- |
| 查分 `Score` | **O(1)** | O(1)（配合 dict） |
| 查排名 `Rank` | **O(log n)** 二分 | O(log n) |
| 设分 `Set` | **O(log n) 定位 + O(n) 数组搬移** | O(log n) |
| 删除 `Remove` | **O(log n) + O(n) 搬移** | O(log n) |
| `Top(n)` | **O(n)** 顺序切片 | O(log N + n) |
| `GetByRankRange` | **O(k)** 连续内存 | O(log N + k) |
| 内存 | 紧凑（一个 string 切片） | 多层指针，开销大 |

**代价**：写操作有 O(n) 的 `copy` 搬移；**收益**：读操作（尤其是 TopN 和 `GetByRankRange`，排行榜最高频的操作）在**连续内存**上进行，CPU 缓存友好，实测远快于指针跳转；且实现简单、无额外指针内存开销。对于「读多写少、榜单规模在万级以内」的典型排行榜场景，这是更优选择。

### 排序谓词 less：NaN 的特殊处理

```go
func (s *MemSortedSet) less(x, y string) bool {
    a, b := s.scores[x], s.scores[y]
    na, nb := math.IsNaN(a), math.IsNaN(b)
    if na && nb { return x < y }   // 同为 NaN → 按 member 字典序
    if na        { return false }  // x 是 NaN → x 不小于（排后面）
    if nb        { return true }   // y 是 NaN → x 排前面
    if a != b    { return a > b }  // 分数降序
    return x < y                   // 同分按 member 字典序升序
}
```

**为什么必须特判 NaN**：IEEE 754 规定 NaN 与任何值的比较（`<`、`>`、`==`）都返回 false。若不特判，`less` 会变得**不满足严格弱序**（既不 `less(x,y)` 也不 `less(y,x)`，但也不相等），导致**二分查找定位错误**——`findIndex` 会返回一个错误位置，进而使 `insert` 插到错误的地方、`removeFromOrder` 删不掉元素（残留脏数据）。本包的做法是把所有 NaN **归到末尾**并在 NaN 之间按 member 字典序保持一致性，使 `less` 重新成为合法的全序。

**排序规则**：分数降序（高分在前）+ 同分按 member 字典序升序。第二个规则保证了**确定性**——同分成员的相对顺序稳定，不会因为插入顺序不同而变化，这对分页与快照对比很重要。

### 二分定位 findIndex

```go
lo, hi := 0, len(s.order)
for lo < hi {
    mid := int(uint(lo+hi) >> 1)  // ★ 溢出安全
    if s.less(s.order[mid], member) { lo = mid + 1 } else { hi = mid }
}
return lo
```

标准的**下界二分（lower_bound）**：返回第一个「不小于 member」的位置。

`int(uint(lo+hi) >> 1)` 是 Go 标准库 `sort.Search` 同款的**溢出安全中点计算**：先转 `uint` 再无符号右移，即使 `lo+hi` 溢出 int 也能得到正确结果。

**复杂度 O(log n)**，但每次比较都要走 `less` → 两次 map 查找，常数因子不小。

### 插入 insert（O(n) 搬移）

```go
idx := s.findIndex(member)
s.order = append(s.order, "")       // 扩容一格
copy(s.order[idx+1:], s.order[idx:]) // 后半段整体右移
s.order[idx] = member
```

`copy` 处理重叠切片时行为是良定义的（等价于 memmove），从后往前复制不会覆盖源数据。

### setLocked 的「先删后插」

```go
if _, ok := s.scores[member]; ok {
    s.removeFromOrder(member)   // ★ 必须在改 scores 之前
}
s.scores[member] = score
s.extras[member] = extra
s.insert(member)
```

**顺序至关重要**：`removeFromOrder` 内部调用 `findIndex` → `less` → 读 `s.scores[member]`。如果先更新了分数，二分会按**新分数**去定位，但元素还在**旧分数**对应的位置上，导致定位失败、元素残留在 `order` 中（产生重复项与幽灵成员）。

### Remove 的同样顺序要求

```go
s.removeFromOrder(member)  // ★ 必须先摘除
delete(s.scores, member)
delete(s.extras, member)
```

代码注释明确解释了原因：若先 `delete(s.scores, member)`，`s.scores[member]` 会返回 map 的零值 `0`，`less` 就按分数 0 去二分，定位到错误下标，元素永久残留在 `order` 里。

### Extra 的深拷贝防护

`entryLocked` 在构造返回值时对 `Extra` 做**深拷贝**：

```go
if extra != nil {
    cp := make(json.RawMessage, len(extra))
    copy(cp, extra)
    extra = cp
}
```

`json.RawMessage` 本质是 `[]byte`，若直接返回内部切片，调用方修改返回值的字节会**污染排行榜内部数据**（且是在没有持锁的情况下，造成数据竞争）。深拷贝是必要的安全代价——但也意味着 `Top(10000)` 会产生 10000 次内存分配。

### 段位门槛算法 AdjustMembers

这是本包最有特色的业务能力。规则：某些排名位置有**最低分要求**，达不到就**跳空**该名次。

**第一步 expandGates**：把区间形式的 `Thresholds` 展开成 `map[排名]最低分`：

```go
[{MinRank:1, MaxRank:3, MinScore:1000}]
  ↓ 展开
{1:1000, 2:1000, 3:1000}
```

带 **OOM 防护**：`maxGates = 100000`，展开总数超限（或区间非法：`MinRank < 1`、`MaxRank < MinRank`）时 `Validate()` 返回 error——**校验只返回 error，绝不 panic**；查询路径（`AdjustMembers`）对非法配置按「无门槛」返回原始排名并打印告警。**注意展开是两遍循环**——第一遍算总数并检查上限，第二遍才真正填充。

**第二步逐个分配显示排名**：

```go
displayRank := 1
for _, m := range raw {          // raw 按分数从高到低
    for {
        gate, ok := gates[displayRank]
        if !ok || m.Score >= gate { break }  // 无门槛 或 达标 → 占位
        displayRank++                         // 不达标 → 跳过这个名次
    }
    m.Rank = displayRank
    result = append(result, m)
    displayRank++
}
```

**示例**：门槛「第 1~3 名需 1000 分」，原始榜单：

| 原始排名 | 成员 | 分数 | 调整后排名 | 说明 |
| --- | --- | --- | --- | --- |
| 1 | A | 1500 | **1** | 达标 |
| 2 | B | 900 | **4** | 不足 1000，跳过名次 2、3 |
| 3 | C | 800 | **5** | 名次 4 无门槛，B 已占，C 得 5 |

即：榜单前三名只有 A，第 2、3 名**空缺**（无人达到 1000 分），B 从第 4 名开始。

**复杂度**：外层 O(n)，内层 `displayRank` 只增不减，**总体摊还 O(n + maxGate)**。

**边界**：`len(ts) == 0` 时直接返回 `raw`（零开销短路）。

### Manager.GetMember 的门槛计算

获取单个成员的调整后排名是个难点——**必须知道它前面所有人的分数**才能算出跳过了多少名次。实现：

```go
if rm.Rank > 1 {
    above := b.GetByRankRange(1, rm.Rank-1) // 取出它前面的全部成员
    all := append(above, rm)
    adjusted := ts.AdjustMembers(all)
    return adjusted[len(adjusted)-1], true  // 取最后一个（就是它自己）
}
```

**代价**：查询第 N 名成员需要 **O(N)** 的数据拷贝 + 门槛计算。查排名靠后的成员会非常慢。

### Manager 的锁模式

```go
m.mu.RLock()
b, ok := m.boards[board]
ts := m.thresholds[board]
m.mu.RUnlock()          // ★ 先释放 Manager 锁
if !ok { return nil }
return ts.AdjustMembers(b.Top(n))  // 再调用 board（board 自己有锁）
```

先取引用再释放，避免持有 Manager 锁时调用可能很慢的后端（尤其是 Redis 实现），也避免锁嵌套。

## 对外 API

### 构造

```go
func New() SortedSet                    // 等价于 newMemSortedSet()（非导出）
func NewManager() *Manager
```

```go
board := rank.New()
board.Set("player:1001", 12345, nil)
top := board.Top(100)
```

### SortedSet 写操作

```go
Set(member string, score float64, extra json.RawMessage)
Incr(member string, delta float64) float64
AddOnlyUpdateScore(member string, score float64, extra json.RawMessage) (float64, bool)
IncrOnlyUpdateScore(member string, delta float64) (float64, bool)
Remove(member string) bool
Clear()
```

```go
// 直接设分（会创建成员）
extra, _ := json.Marshal(map[string]any{"name": "小明", "level": 30})
board.Set("player:1001", 5000, extra)

// 增分（成员不存在时返回 0 且不创建）
newScore := board.Incr("player:1001", 100) // 5100

// 只在更高时才写入（成员不存在时返回 (0,false) 且不创建）
final, exists := board.AddOnlyUpdateScore("player:1001", 4000, extra) // (5100, true) 未更新
final, exists = board.AddOnlyUpdateScore("player:1001", 9999, extra)  // (9999, true) 已更新

// 只在增分后更高时才写入
final, exists = board.IncrOnlyUpdateScore("player:1001", -50) // (9999, true) 未更新（会变低）

board.Remove("player:1001")
board.Clear()
```

### SortedSet 读操作

```go
Get(member string) (master.RankMember, bool)   // 分数 + 排名，一次搞定
Score(member string) (float64, bool)
Rank(member string) (int, bool)
Len() int
Top(n int) []master.RankMember                 // n<=0 返回空集（不要任何成员）；n>总数 时返回全部
GetByRankRange(start, stop int) []master.RankMember // 1-based，闭区间
GetByScoreRange(opts RangeOpts) []master.RankMember
Members() []string
All() []master.RankMember
```

```go
// 一次拿到分数和排名（推荐，比分别调 Score+Rank 少一次加锁）
rm, ok := board.Get("player:1001")
fmt.Println(rm.Rank, rm.Score, string(rm.Extra))

// 前 100 名
top := board.Top(100)

// 第 11~20 名（1-based 闭区间）
page := board.GetByRankRange(11, 20)

// 分数在 [1000, 5000] 之间，跳过前 5 个，最多 10 个
list := board.GetByScoreRange(rank.RangeOpts{
    Min: 1000, HasMin: true,
    Max: 5000, HasMax: true,
    Offset: 5, Limit: 10,
})

// 分数 > 1000（开区间），无上界
high := board.GetByScoreRange(rank.RangeOpts{
    Min: 1000, HasMin: true, MinExclusive: true,
})

// 全部成员（零值 RangeOpts = -inf~+inf）
all := board.GetByScoreRange(rank.RangeOpts{})
```

### Manager 榜单管理

```go
func (m *Manager) Register(name string, b SortedSet)
func (m *Manager) Get(name string) (SortedSet, bool)
func (m *Manager) MustGet(name string) SortedSet    // 不存在则 panic
func (m *Manager) GetOrCreate(name string) SortedSet
func (m *Manager) Unregister(name string) bool
func (m *Manager) Names() []string                  // 已排序
func (m *Manager) Len(board string) int             // 榜不存在时返回 0
func (m *Manager) Boards() map[string]SortedSet     // 浅拷贝快照
func (m *Manager) Add(board, member string, score float64, extra json.RawMessage)
func (m *Manager) GetAll(board string) ([]master.RankMember, bool) // 原始排名，不应用门槛
```

```go
mgr := rank.NewManager()

// 注入自定义后端（如 Redis 实现）
mgr.Register("arena", myRedisSortedSet)

// 自动创建内存榜
mgr.Add("power", "player:1001", 8888, nil)

// 遍历所有榜
for _, name := range mgr.Names() {
    b, _ := mgr.Get(name)
    log.Printf("%s: %d 人", name, b.Len())
}
```

### 段位门槛

```go
func (m *Manager) SetRankThresholds(board string, ts master.Thresholds) // 非法配置拒绝写入并告警
func (m *Manager) GetThresholds(board string) master.Thresholds
func (ts master.Thresholds) AdjustMembers(raw []master.RankMember) []master.RankMember
```

```go
// 王者 1~3 名需 3000 分，钻石 4~10 名需 2000 分
mgr.SetRankThresholds("arena", master.Thresholds{
    {MinRank: 1, MaxRank: 3, MinScore: 3000},
    {MinRank: 4, MaxRank: 10, MinScore: 2000},
})

// 之后 Top/GetByRankRange/All/GetMember 自动应用门槛
top := mgr.Top("arena", 10)
```

### Manager 带门槛的查询

```go
func (m *Manager) Top(board string, n int) []master.RankMember
func (m *Manager) GetMember(board, member string) (master.RankMember, bool)
func (m *Manager) GetByRankRange(board string, start, stop int) []master.RankMember // 1-based 闭区间
func (m *Manager) All(board string) []master.RankMember
```

```go
page := mgr.GetByRankRange("arena", 1, 10) // 第 1~10 名
```

### 包级便捷函数（操作 defaultManager）

```go
func Add(board, member string, score float64)   // ★ 无 extra 参数
func Top(board string, n int) []master.RankMember
func GetMember(board, member string) (master.RankMember, bool)
func GetByRankRange(board string, start, stop int) []master.RankMember
func All(board string) []master.RankMember
func SetRankThresholds(board string, ts master.Thresholds)
```

```go
// 最简用法，无需管理 Manager 实例
rank.Add("daily_damage", "player:1001", 99999)
rank.SetRankThresholds("daily_damage", master.Thresholds{{MinRank: 1, MaxRank: 3, MinScore: 50000}})
top3 := rank.Top("daily_damage", 3)
```

## 依赖关系

- **依赖 Go 标准库**：`encoding/json`（`RawMessage`）、`log`（非法门槛配置告警）、`math`（`IsNaN` / `IsInf`）、`sort`（`Names` 排序）、`sync`（读写锁）。
- 零第三方依赖；依赖引擎内 `pkg/domain/master`（`RankMember` / `Threshold` / `Thresholds` 类型真身所在），**不再是零引擎依赖的叶子包**。
- **扩展点**：实现 `SortedSet` 接口即可接入 Redis ZSET 等外部后端，`Manager.Register` 注入。
