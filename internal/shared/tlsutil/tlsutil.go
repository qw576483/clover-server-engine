// #nosec G304 -- 证书/私钥/CA 路径来自服务配置（运维输入），不是客户端可控输入；读取失败会返回错误。

// Package tlsutil 提供「按证书 / 私钥 / CA 三个文件路径构建 *tls.Config」的唯一实现。
//
// 为什么单独成包：etcd、nats 各自的 TLSConfig 结构体是**刻意保留**的（为解依赖方向
// 而在本地定义、经适配器转换），但「读文件 → 建证书池 → 装配 tls.Config」这段逻辑
// 没有必要各写一份。此前两边各写一遍，校验细节与错误文案已经不一致
// （一边检查 CA 解析结果，一边只报 failed to parse CA certificate）。
//
// 入参一律用**路径字符串**而非某个包自己的结构体：这样各客户端都能保留自己的配置类型，
// 只在构造时调本包，不必为了复用而强行统一类型。
package tlsutil

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// Build 按路径构建 *tls.Config。
//
//   - caFile 为空：使用系统根证书池（不设置 RootCAs）；
//   - certFile 与 keyFile 同时非空：作为客户端证书加载；
//   - **只配其一**：视为配置错误直接报错。以前静默跳过客户端证书加载（且无日志），
//     漏配私钥会让 mTLS 静默失效、握手报错难以定位——与 gateway.tls_cert/tls_key
//     的成对校验口径一致；
//   - 两者都不配：跳过客户端认证（合法用法）；
//   - MinVersion 一律为 TLS 1.2。
//
// CA 与证书的 sha256 会记入启动日志：证书配错（例如轮换后路径上仍是旧证书）时
// 可直接对比指纹定位，省去现场排查。
func Build(certFile, keyFile, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if caFile != "" {
		caBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("invalid ca file %s: no certificate found", caFile)
		}
		logger.Infof("tls: CA file %s sha256=%x", caFile, sha256.Sum256(caBytes))
		cfg.RootCAs = pool
	}

	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("tls: cert_file 与 key_file 必须成对配置（cert=%q key=%q）", certFile, keyFile)
	}
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load cert/key (%s, %s): %w", certFile, keyFile, err)
		}
		if len(cert.Certificate) > 0 {
			logger.Infof("tls: client cert %s sha256=%x", certFile, sha256.Sum256(cert.Certificate[0]))
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
