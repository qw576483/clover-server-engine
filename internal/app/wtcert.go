package app

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// 本文件处理 WebTransport 在自签名证书场景下的证书签发与校验。
//
// 约束来源：
//   - Chromium 对 WebTransport 的证书校验独立于普通 HTTPS：既不接受本机自建根证书，
//     --ignore-certificate-errors 也对其无效。
//   - 自签名场景下唯一通路是 W3C 规定的 serverCertificateHashes（证书固定）。
//   - 该机制对证书有硬性约束（MDN）：
//       1. 有效期 < 2 周；
//       2. 必须 ECDSA secp256r1(P-256)，明确禁止 RSA。
//
// 注意：mkcert 签出的是 RSA + 825 天证书，两条约束都违反，不能用于本链路。
//
// 由于「被浏览器信任的长期证书」与「符合固定要求的短有效期 ECDSA 证书」不可兼得，
// 这里为 WebTransport 单独维护一张证书，其哈希经已受信任的 wss 通道下发。

const (
	// wtCertValidity 证书有效期。必须 < 2 周，取 10 天留出轮换余量。
	wtCertValidity = 10 * 24 * time.Hour
	// wtCertRenewBefore 剩余有效期不足该值时提前轮换，避免用到一半过期。
	wtCertRenewBefore = 3 * 24 * time.Hour
	// wtCertMaxPeriod 证书固定机制允许的最大有效期（2 周）。
	wtCertMaxPeriod = 14 * 24 * time.Hour
)

// wtCertPaths 解析 WebTransport 证书/密钥路径：未配置时基于 TLS 证书同目录生成默认名，
// 与 prepareWTCert 及运行期轮换器（wtrotator.go）共用同一套路径规则。
func wtCertPaths(wtCertPath, wtKeyPath, tlsCertPath string) (string, string) {
	if wtCertPath == "" {
		dir := filepath.Dir(tlsCertPath)
		if dir == "" || dir == "." {
			dir = "certs"
		}
		wtCertPath = filepath.Join(dir, "wt.pem")
	}
	if wtKeyPath == "" {
		dir := filepath.Dir(wtCertPath)
		wtKeyPath = filepath.Join(dir, "wt-key.pem")
	}
	return wtCertPath, wtKeyPath
}

// prepareWTCert 准备 WebTransport 专用证书，返回其 TLS 配置与证书哈希（hex）。
// tlsCertPath 用于在其同级目录放置自动生成的证书。
func prepareWTCert(wtCertPath, wtKeyPath, tlsCertPath string) (*tls.Config, string, error) {
	wtCertPath, wtKeyPath = wtCertPaths(wtCertPath, wtKeyPath, tlsCertPath)

	cert, err := tls.LoadX509KeyPair(wtCertPath, wtKeyPath)
	switch {
	case err == nil:
		leaf, perr := x509.ParseCertificate(cert.Certificate[0])
		if perr != nil {
			return nil, "", fmt.Errorf("webtransport: parse certificate %s: %w", wtCertPath, perr)
		}
		if wtCertNeedsRotate(leaf, time.Now()) {
			if gerr := generateWTCert(wtCertPath, wtKeyPath); gerr != nil {
				return nil, "", fmt.Errorf("webtransport: rotate certificate: %w", gerr)
			}
			cert, err = tls.LoadX509KeyPair(wtCertPath, wtKeyPath)
			if err != nil {
				return nil, "", fmt.Errorf("webtransport: reload rotated certificate: %w", err)
			}
		}
	case os.IsNotExist(err) || isNotExistAny(wtCertPath, wtKeyPath):
		// 首次启动：签发一张符合要求的证书，保证本地开箱可用。
		if gerr := generateWTCert(wtCertPath, wtKeyPath); gerr != nil {
			return nil, "", gerr
		}
		cert, err = tls.LoadX509KeyPair(wtCertPath, wtKeyPath)
		if err != nil {
			return nil, "", fmt.Errorf("webtransport: reload generated certificate: %w", err)
		}
	default:
		// 证书存在但读不出来（权限/格式/密钥不匹配）：不擅自覆盖用户文件，直接报错。
		return nil, "", fmt.Errorf("webtransport: load certificate (%s, %s): %w", wtCertPath, wtKeyPath, err)
	}

	return newWTTLSConfig(cert), certHashHex(cert), nil
}

// newWTTLSConfig 构造 WebTransport 专用 TLS 配置（TLS 1.3 + h3 ALPN）。
// 生成与轮换两条路径共用此工厂，避免两份几乎相同的配置各自维护导致漂移。
func newWTTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h3"},
	}
}

// wtCertNeedsRotate 判断证书是否需要轮换。
func wtCertNeedsRotate(leaf *x509.Certificate, now time.Time) bool {
	if leaf.PublicKeyAlgorithm != x509.ECDSA {
		return true // RSA 被证书固定机制明确禁止
	}
	ec, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok || ec.Curve != elliptic.P256() {
		return true // 必须是 secp256r1
	}
	if leaf.NotAfter.Sub(leaf.NotBefore) >= wtCertMaxPeriod {
		return true // 有效期达到/超过 2 周
	}
	// 已过期或临近过期
	return !now.Before(leaf.NotAfter.Add(-wtCertRenewBefore))
}

// generateWTCert 签发一张自签名 ECDSA P-256 证书并写入磁盘。
func generateWTCert(certPath, keyPath string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "clover-wt"},
		NotBefore:             now.Add(-time.Hour), // 容忍客户端时钟轻微回拨
		NotAfter:              now.Add(wtCertValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true, // 自签名
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	for _, dir := range []string{filepath.Dir(certPath), filepath.Dir(keyPath)} {
		if dir != "" && dir != "." {
			// 0o750（属主 rwx + 同组 r-x，others 无权限）：证书目录与密钥同处一棵树，
			// 目录可被 others 列出/进入就等于把私钥文件名与内容暴露给同机其他用户
			//（gosec G301 建议 ≤ 0750）。目录只由本进程写入，不需要 others 的写权限。
			if err := os.MkdirAll(dir, 0o750); err != nil {
				return fmt.Errorf("create cert dir %s: %w", dir, err)
			}
		}
	}
	// 两个文件都「先写同目录临时文件、再 rename 覆盖」：
	// 磁盘满 / 中途崩溃发生在写临时文件阶段时，旧的那一对 cert/key 完好无损；
	// 只有两次 rename 之间的微秒级窗口才可能留下不匹配的一对
	//（直接 os.WriteFile 是边写边截断，失败就会留下半个新证书配旧密钥）。
	if err := writeFileAtomic(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return err
	}
	return writeFileAtomic(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
}

// writeFileAtomic 先写同目录临时文件，再 rename 覆盖目标。
// 同目录 rename 在同一文件系统内是原子操作，读者永远看不到「写了一半」的文件。
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}

// certHashHex 证书 DER 的 SHA-256（hex），即 serverCertificateHashes 所需的哈希。
func certHashHex(cert tls.Certificate) string {
	sum := sha256.Sum256(cert.Certificate[0])
	return hex.EncodeToString(sum[:])
}

// isNotExistAny 任一证书/密钥文件不存在即返回 true。
func isNotExistAny(paths ...string) bool {
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil && os.IsNotExist(err) {
			return true
		}
	}
	return false
}
