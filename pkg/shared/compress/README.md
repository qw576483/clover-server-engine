# compress 模块

## 模块职责

`compress` 提供基于标准库 `compress/flate` 的**数据压缩封装**，把 DEFLATE 的 Writer/Reader 样板代码收敛为三个一次性函数。纯标准库、零外部依赖、不依赖仓库内其它包。选用 flate（裸 DEFLATE，无 zlib 头与 Adler-32 校验尾）是为了体积最小、编解码最快，适合引擎内部二进制载荷（大包体网络消息、快照、日志批）的压缩。若业务需要带头部与校验的格式，应改用 `compress/zlib` 或 `compress/gzip` 而不是本包。

## 规则与约束

1. **完整性校验由调用方负责**：输出为裸 DEFLATE 流，不含 Adler-32 / CRC32 校验尾，位翻转一类损坏未必能被检出；有完整性要求的场景必须自行附加 CRC 或哈希，或改用 `compress/zlib` / `compress/gzip`。
2. **格式必须与对端一致**：本包只产出并接受裸 DEFLATE 流，不得与 zlib / gzip 混用；跨语言互通时必须确认对端使用 raw deflate / inflateRaw（如 Node.js 的 `zlib.inflateRawSync`）。
3. **压缩增益必须实测后判定**：DEFLATE 存在块头开销，几十字节的短数据与已压缩数据（图片 / 视频 / 密文）压缩后可能变大；必须在协议中携带压缩标志，并在压缩后未变小时回退原文。
4. **压缩级别取值范围**：`level` 必须落在 `[flate.HuffmanOnly, flate.BestCompression]`（即 `[-2, 9]`），越界时实现回退到 `DefaultCompression` 且不返回错误；级别来自外部配置且需要严格校验时，必须在调用前自行验证范围。
5. **空值的非对称性**：`Compress(nil)` 返回非空的最小 DEFLATE 空流，而 `Decompress(nil)` 返回 `nil`；判定解压结果是否为空必须使用 `len()`，不得与 `nil` 比较。
6. **全量内存模型**：三个函数均为一次性全量内存操作，输入与输出各占一份完整内存；GB 级数据必须使用 `flate.NewWriter` / `flate.NewReader` 做流式处理。
7. **高频调用须自行复用**：每次调用都新建 `bytes.Buffer` 与 `flate.Writer`，超高频小包场景必须在上层引入 `sync.Pool` 并配合 `Writer.Reset` 复用。
8. **不可信输入必须限流**：`Decompress` 不限制输出长度，处理不可信来源的数据时必须先用 `io.LimitReader` 包裹，或在调用前校验来源可信，以防解压炸弹耗尽内存。

## 文件清单

| 文件名 | 行数 | 职责说明 |
| --- | --- | --- |
| `flate.go` | 51 | 全部内容：`CompressLevel`（指定级别压缩，非法级别回退默认）、`Compress`（默认级别便捷函数）、`Decompress`（解压，空输入返回 nil） |

## 核心类型与接口

本包**不定义任何类型**，只导出三个无状态纯函数。

**并发安全性**：三个函数都**无共享状态**，每次调用内部新建 `bytes.Buffer` 与 `flate.Writer`/`flate.Reader`，因此**完全并发安全**，可在任意 goroutine 中直接调用。

## 算法与实现原理

### DEFLATE 算法

DEFLATE（RFC 1951）= **LZ77 滑动窗口字典匹配** + **Huffman 熵编码**：

1. **LZ77**：在 32KB 滑动窗口内查找重复子串，把重复内容替换为 `(距离, 长度)` 二元组。重复度越高压缩率越高。
2. **Huffman**：对字面量 / 长度 / 距离符号做变长前缀编码，高频符号用短码。DEFLATE 支持静态 Huffman 表与动态（随块传输的）Huffman 表两种块类型。

本包使用**裸 DEFLATE 流**：输出不含 zlib 的 2 字节头与 4 字节 Adler-32 校验尾，也不含 gzip 的 10 字节头与 CRC32 尾，因此**比 zlib 少 6 字节、比 gzip 少约 18 字节**，但也**没有内建完整性校验**。

### 压缩级别

`CompressLevel` 的合法区间为 `[flate.HuffmanOnly, flate.BestCompression]`，即 `[-2, 9]`：

| 级别 | 常量 | 含义 |
| --- | --- | --- |
| `-2` | `flate.HuffmanOnly` | 只做 Huffman 编码，**跳过 LZ77 匹配**，最快、压缩率最低 |
| `-1` | `flate.DefaultCompression` | 默认级别（等效约 6），速度与压缩率均衡 |
| `0` | `flate.NoCompression` | 不压缩，仅按 DEFLATE 存储块封装（体积会略微**变大**） |
| `1` | `flate.BestSpeed` | 最快的有效压缩 |
| `9` | `flate.BestCompression` | 最高压缩率、最慢 |

**越界处理**：`level < -2 || level > 9` 时**静默回退**到 `DefaultCompression`，不报错。

### 压缩流程

```
data → flate.NewWriter(&buf, level) → w.Write(data) → w.Close() → buf.Bytes()
```

`w.Close()` 是**必须**的：它负责刷出内部缓冲并写入块结束标记。代码中 `Close` 的错误被显式检查并返回，不会静默丢数据。

### 解压流程

```
data → flate.NewReader(bytes.NewReader(data)) → io.ReadAll → result
```

- **空输入短路**：`len(data) == 0` 直接返回 `(nil, nil)`，不构造 Reader。
- **损坏数据**：非法 DEFLATE 流在 `io.ReadAll` 阶段返回 error（如 `flate: corrupt input`），此时返回 `(nil, err)`。这正是「空输入返回 nil」与「损坏数据返回错误」两种情况得以区分的原因。
- `defer r.Close()` 释放 Reader 资源（`flate.NewReader` 返回 `io.ReadCloser`）。

## 对外 API

### `Compress`

```go
func Compress(data []byte) ([]byte, error)
```

用途：以默认级别压缩，最常用入口。

```go
raw := []byte("...大量重复的 JSON 载荷...")
packed, err := compress.Compress(raw)
if err != nil {
    return err
}
log.Printf("%d -> %d bytes", len(raw), len(packed))
```

### `CompressLevel`

```go
func CompressLevel(data []byte, level int) ([]byte, error)
```

用途：需要在速度与压缩率之间做取舍时指定级别。非法级别自动回退默认。

```go
// 实时消息：优先速度
fast, _ := compress.CompressLevel(payload, flate.BestSpeed)
// 落盘归档：优先体积
small, _ := compress.CompressLevel(archive, flate.BestCompression)
// 传入 42（非法）→ 自动使用 DefaultCompression，不报错
_, _ = compress.CompressLevel(payload, 42)
```

### `Decompress`

```go
func Decompress(data []byte) ([]byte, error)
```

用途：解压由本包（或任何裸 DEFLATE 编码器）产生的数据。

```go
raw, err := compress.Decompress(packed)
if err != nil {
    return fmt.Errorf("数据损坏: %w", err)
}
if raw == nil {
    // 输入为空，不是错误
}
```

### 往返示例

```go
orig := bytes.Repeat([]byte("clover"), 1000)
packed, err := compress.Compress(orig)
if err != nil { panic(err) }

back, err := compress.Decompress(packed)
if err != nil { panic(err) }

fmt.Println(bytes.Equal(orig, back)) // true
```

## 依赖关系

- **仅依赖 Go 标准库**：`bytes`、`compress/flate`、`io`。
- 零第三方依赖、**零引擎内部依赖**，可完全独立单测与复用。
- 使用方若要传 `level` 常量，需自行 `import "compress/flate"`（本包未重导出这些常量）。
