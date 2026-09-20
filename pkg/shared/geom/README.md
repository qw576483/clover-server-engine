# geom 模块

## 模块职责

`geom` 提供引擎常用的**数学几何原语**：三维向量 `Vec3`、四元数 `Quaternion`、三角函数查表 `TrigTable`，以及一批标量工具函数（Clamp / Lerp / 弧度角度互转 / Sign）。纯标准库、零外部依赖、不依赖仓库内其它包。设计上所有类型均为**值类型**，方法**返回新值而非原地修改**，便于链式组合与并发只读。归一化对零向量做了安全处理（返回零向量 / 单位四元数），不会除零 panic。

> 注：**二维向量 `Vec2` 不在本包**，已下沉到 `collide` 包（平面坐标统一用 `collide.Vec2`），避免重复定义。

## 规则与约束

1. **平面坐标必须使用 `collide.Vec2`**：本包只提供三维向量 `Vec3`，不提供二维向量。
2. **`Quaternion` 的字段顺序为 `{W, X, Y, Z}`**：位置初始化必须按实部在前的顺序书写，`Quaternion{1, 0, 0, 0}` 才是单位四元数。
3. **`Rotate` 的接收者必须是单位四元数**：优化公式以 `|q| = 1` 为前提，非单位四元数会引入额外缩放。多次 `Mul` 累积后必须先 `Normalize()`。
4. **`FromAxisAngle` 的 `angle` 以弧度为单位**：传入角度值前必须先经 `DegToRad` 转换。
5. **四元数乘法不满足交换律**：`q.Mul(o)` 表示先应用 `o` 再应用 `q`，组合旋转时必须按此顺序书写。
6. **`Vec3.Normalize` 与 `Quaternion.Normalize` 的零值语义不同**：零向量返回零向量，零四元数返回单位四元数 `{W:1}`。使用 `Vec3.Normalize` 前必须先判 `LenSq() > 0`。
7. **`Slerp` 的两个入参必须已归一化**：点积未归一化时 `acos` 的入参会超出 `[-1, 1]` 而产生 NaN。
8. **`Slerp` 的 `t` 由调用方负责钳制**：`t` 超出 `[0, 1]` 时按外插处理，需要限制范围时先自行 `Clamp(t, 0, 1)`。
9. **`NewTrigTable` 的 `n` 表示覆盖的度数范围**：预计算的是 `0..n-1` 度，除明确需求外必须使用 `NewTrigTable(360)` 或 `NewTrigTable(0)`。
10. **`TrigTable` 的角度分辨率为 1 度**：需要平滑连续角度或更高精度时必须改用 `math.Sin` / `math.Cos`。
11. **`Lerp` 存在两个同名入口**：`Vec3.Lerp` 为向量插值方法，包级 `Lerp` 为标量插值函数，调用时必须区分。
12. **`Sign` 将负零按零处理**：`-0.0` 返回 `0`。
13. **`Clamp` 的入参必须满足 `lo <= hi`**：本包不校验边界顺序，违反时返回值无意义。
14. **本包全部类型使用 `float64`**：与 float32 客户端引擎交换坐标时必须显式转换。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `vec.go` | 208 | 包文档、`Vec3` 三维向量及其 10 个方法、`Quaternion` 四元数及 `FromAxisAngle`/`Normalize`/`Len`/`Mul`/`Rotate`/`Slerp`、运动积分 `Integrate`/`IntegrateScalar`、5 个标量工具函数（Clamp/Lerp/RadToDeg/DegToRad/Sign） |
| `trig.go` | 41 | `TrigTable` 离散角度正弦余弦查表：`NewTrigTable` 构造预计算、`Sin`/`Cos` 按度取模查表 |

## 核心类型与接口

### `Vec3`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `X`, `Y`, `Z` | `float64` | 三维分量 |

**值类型**，所有方法都是**值接收者**且返回新 `Vec3`，不修改原值。

### `Quaternion`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `W` | `float64` | **实部**（Hamilton 约定，w 在前） |
| `X`, `Y`, `Z` | `float64` | 虚部（向量部分） |

**值类型**，全部值接收者返回新值。注意字段顺序是 `{W, X, Y, Z}`，用位置初始化时不要写成 `{x,y,z,w}`。

### `TrigTable`

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `n` | `int` | 表的分度数（一圈划分为 n 份），默认 360 |
| `sin` | `[]float64` | 预计算的 `sin(i 度)`，`i ∈ [0, n)` |
| `cos` | `[]float64` | 预计算的 `cos(i 度)`，`i ∈ [0, n)` |

**并发安全性**：

- `Vec3` / `Quaternion` / 标量函数：**纯值语义、无共享状态，完全并发安全**。
- `TrigTable`：构造后 `sin`/`cos` 切片**只读不再修改**，`Sin`/`Cos` 只做查表，因此**并发只读安全**。可安全共享为包级单例。

## 算法与实现原理

### Vec3 基础运算

| 方法 | 公式 |
| --- | --- |
| `Add` / `Sub` | 逐分量加减 |
| `Scale(s)` | 逐分量乘 s |
| `Dot(o)` | `x₁x₂ + y₁y₂ + z₁z₂` |
| `Cross(o)` | `(y₁z₂-z₁y₂, z₁x₂-x₁z₂, x₁y₂-y₁x₂)`，右手法则 |
| `Len()` | `√(x²+y²+z²)` |
| `LenSq()` | `x²+y²+z²`，**省去 sqrt**，比长度用于距离比较 |
| `Distance(o)` | `v.Sub(o).Len()` |
| `Lerp(o,t)` | `v + (o-v)·t`，逐分量 |
| `Normalize()` | 除以模长；**`l == 0` 时返回零向量**（不 panic、不产生 NaN） |

### 四元数与旋转

**Hamilton 乘法**（`Mul`）：

```
w = w₁w₂ - x₁x₂ - y₁y₂ - z₁z₂
x = w₁x₂ + x₁w₂ + y₁z₂ - z₁y₂
y = w₁y₂ - x₁z₂ + y₁w₂ + z₁x₂
z = w₁z₂ + x₁y₂ - y₁x₂ + z₁w₂
```

四元数乘法**不可交换**：`q.Mul(o) != o.Mul(q)`，`q.Mul(o)` 表示「先应用 o 再应用 q」。

**轴角构造**（`FromAxisAngle`）：轴先归一化，然后

```
q = (cos(θ/2), axis · sin(θ/2))
```

`angle` 单位是**弧度**。

**向量旋转**（`Rotate`）：使用 **Fabian Giesen 的优化公式**，避免完整的 `q * v * q⁻¹` 三次四元数乘法：

```
t = 2 · (q.xyz × v)
v' = v + q.w · t + (q.xyz × t)
```

这个版本只需约 15 次乘法 + 15 次加法，远快于朴素展开。**前提是 q 必须是单位四元数**——非单位四元数会同时引入缩放（甚至错误结果）。

**球面线性插值**（`Slerp`）：

1. 计算点积 `cosθ = q·o`。
2. **最短路径修正**：`cosθ < 0` 时把 `o` 取反（`-w,-x,-y,-z`，表示同一旋转），并令 `cosθ = -cosθ`。避免绕远路旋转 >180°。
3. **近平行退化处理**：`cosθ > 0.9995` 时 `sinθ ≈ 0`，除法会数值爆炸，改用**线性插值 (nlerp) + 归一化**近似，误差可忽略。
4. 常规路径：
   ```
   θ = acos(cosθ),  sinθ = √(1 - cos²θ)
   a = sin((1-t)θ) / sinθ
   b = sin(tθ)     / sinθ
   result = q·a + o'·b
   ```

`Normalize()`：模为 0 时返回**单位四元数 `{W:1}`**（而非零四元数），保证结果始终是合法旋转。

### 三角查表 TrigTable

**目的**：用 `O(n)` 的空间换掉热路径上的 `math.Sin`/`math.Cos` 调用（后者是较慢的库函数，通常几十纳秒）。查表是一次数组访问，约 1~2ns。

**构造**：`NewTrigTable(n)` 预计算 `i = 0..n-1` 的 `sin(DegToRad(i))` 与 `cos(DegToRad(i))`。`n <= 0` 时按 360 处理。

**关键**：`DegToRad(float64(i))` 的语义是「把 i 当作**度数**」。因此当 `n != 360` 时，表覆盖的其实是 `0..n-1` **度**而不是一整圈被均分为 n 份。例如 `NewTrigTable(90)` 只覆盖 0~89 度，`Sin(100)` 会取模成 `Sin(10)` 而得到 10 度的值——**这不是 400 分度的圆**。

**取模**：`d := ((deg % n) + n) % n`，标准的**负数安全取模**，保证结果落在 `[0, n)`。因此 `Sin(-90)`（n=360 时）等价于 `Sin(270)`。

**精度**：只能表达**整数度**，分辨率 1°（约 0.017 弧度）。需要更细粒度时应增大 n 并自行换算，或直接用 `math.Sin`。

## 对外 API

### Vec3

```go
type Vec3 struct{ X, Y, Z float64 }

func (v Vec3) Add(o Vec3) Vec3
func (v Vec3) Sub(o Vec3) Vec3
func (v Vec3) Scale(s float64) Vec3
func (v Vec3) Dot(o Vec3) float64
func (v Vec3) Cross(o Vec3) Vec3
func (v Vec3) Len() float64
func (v Vec3) LenSq() float64
func (v Vec3) Normalize() Vec3
func (v Vec3) Distance(o Vec3) float64
func (v Vec3) Lerp(o Vec3, t float64) Vec3
```

```go
a := geom.Vec3{X: 1, Y: 0, Z: 0}
b := geom.Vec3{X: 0, Y: 1, Z: 0}

fmt.Println(a.Add(b))        // {1 1 0}
fmt.Println(a.Dot(b))        // 0（垂直）
fmt.Println(a.Cross(b))      // {0 0 1}
fmt.Println(a.Distance(b))   // 1.4142135623730951
fmt.Println(a.Lerp(b, 0.5))  // {0.5 0.5 0}

// 距离比较用 LenSq 避免开方
if a.Sub(b).LenSq() < radius*radius {
    // 在范围内
}

// 零向量归一化安全
fmt.Println(geom.Vec3{}.Normalize()) // {0 0 0}，不 panic
```

### Quaternion

```go
type Quaternion struct{ W, X, Y, Z float64 }

func FromAxisAngle(axis Vec3, angle float64) Quaternion
func (q Quaternion) Len() float64
func (q Quaternion) Normalize() Quaternion
func (q Quaternion) Mul(o Quaternion) Quaternion
func (q Quaternion) Rotate(v Vec3) Vec3
func (q Quaternion) Slerp(o Quaternion, t float64) Quaternion
```

```go
// 绕 Y 轴转 90 度
q := geom.FromAxisAngle(geom.Vec3{Y: 1}, geom.DegToRad(90))
v := geom.Vec3{X: 1}
fmt.Println(q.Rotate(v)) // 约 {0 0 -1}

// 组合旋转：先 q1 再 q2
q1 := geom.FromAxisAngle(geom.Vec3{X: 1}, geom.DegToRad(30))
q2 := geom.FromAxisAngle(geom.Vec3{Y: 1}, geom.DegToRad(45))
combined := q2.Mul(q1) // 注意顺序

// 平滑朝向过渡
cur := geom.Quaternion{W: 1}
target := q
next := cur.Slerp(target, 0.1) // 每帧插值 10%
```

### TrigTable

```go
type TrigTable struct{ /* 私有 */ }

func NewTrigTable(n int) *TrigTable   // n<=0 视为 360
func (t *TrigTable) Sin(deg int) float64
func (t *TrigTable) Cos(deg int) float64
```

```go
var trig = geom.NewTrigTable(360) // 包级单例，构造一次全局共享

// 热路径：按朝向角度步进
func step(pos geom.Vec3, facingDeg int, speed float64) geom.Vec3 {
    return geom.Vec3{
        X: pos.X + trig.Cos(facingDeg)*speed,
        Y: pos.Y,
        Z: pos.Z + trig.Sin(facingDeg)*speed,
    }
}

fmt.Println(trig.Sin(-90)) // 等价于 Sin(270)，约 -1
fmt.Println(trig.Cos(720)) // 等价于 Cos(0)，1
```

### 标量工具

```go
func Clamp(v, lo, hi float64) float64
func Lerp(a, b, t float64) float64
func RadToDeg(r float64) float64
func DegToRad(d float64) float64
func Sign(f float64) int   // 正 1 / 负 -1 / 零 0
```

```go
hp := geom.Clamp(hp+delta, 0, maxHP)
alpha := geom.Lerp(0, 1, progress)
deg := geom.RadToDeg(math.Pi)     // 180
rad := geom.DegToRad(90)          // π/2
dir := geom.Sign(target.X - self.X) // -1 / 0 / 1
```

## 依赖关系

- **仅依赖 Go 标准库**：`math`（Sqrt / Sin / Cos / Acos / Pi）。
- 零第三方依赖、**零引擎内部依赖**，可完全独立单测。
- 包内部：`trig.go` 依赖 `vec.go` 的 `DegToRad`。
- 相关但不依赖：`collide` 包提供 `Vec2`（二维），两包互补但无 import 关系。
