// Package compress 提供基于标准库 compress/flate 的数据压缩封装。纯标准库、零外部依赖、不依赖仓库内其它包。
//
// 说明：flate 是 zlib 的底层 DEFLATE 格式（无 zlib 头/校验尾），体积小、编解码快，
// 适合引擎内部二进制载荷的压缩。若需要带 zlib 头的格式，可改用 compress/zlib。
package compress

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
)

// MaxDecompressedSize 单次解压的输出上限（解压炸弹防护）。
//
// DEFLATE 的压缩率理论上无上限（几 KB 输入可膨胀到 GB 级），对不可信数据
// 用 io.ReadAll 直接收流会把内存打爆。256MiB 远超引擎内任何合法载荷
// （消息合并写 / 对象快照量级都在数 MB），超限即报错而不是继续吃内存。
const MaxDecompressedSize = 256 << 20

// CompressLevel 以指定压缩级别压缩 data；level 非法时回退到 flate.DefaultCompression。
func CompressLevel(data []byte, level int) ([]byte, error) {
	if level < flate.HuffmanOnly || level > flate.BestCompression {
		level = flate.DefaultCompression
	}
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Compress 以默认压缩级别压缩 data（便捷函数）。
func Compress(data []byte) ([]byte, error) {
	return CompressLevel(data, flate.DefaultCompression)
}

// Decompress 解压由 Compress / CompressLevel 生成的 DEFLATE 数据。
// 若输入为空（nil 或空切片）直接返回空；损坏数据经 io.ReadAll 返回 error；
// 解压输出超过 MaxDecompressedSize 返回错误（解压炸弹防护，不再无限收流）。
func Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	r := flate.NewReader(bytes.NewReader(data))
	// flate.Reader 的 Close 在「已读到底」时不返回错误；读取错误已由下方 ReadAll 上报。
	defer func() { _ = r.Close() }()
	// 多读 1 字节用于探测「恰好超限」：LimitReader 会在上限处静默截断，
	// 不探测会把超限数据当成功结果返回（静默截断比报错更危险）。
	result, err := io.ReadAll(io.LimitReader(r, MaxDecompressedSize+1))
	if err != nil {
		return nil, err
	}
	if len(result) > MaxDecompressedSize {
		return nil, fmt.Errorf("compress: decompressed size exceeds limit %d bytes", MaxDecompressedSize)
	}
	// 区分空输入返回 nil 与损坏数据返回错误。
	return result, nil
}
