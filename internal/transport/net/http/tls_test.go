package http

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// genSelfSigned 生成一张仅用于 127.0.0.1 的自签证书，返回文件路径与用于校验的证书池。
func genSelfSigned(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "clover-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	pool = x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatalf("append cert to pool failed")
	}
	return certFile, keyFile, pool
}

// TestServerServesHTTPSWithCertPair 配置证书对后必须真的以 HTTPS 提供服务，
// 且同样的地址上**明文请求必须失败** —— 若只加了配置却没走 TLS，
// 这条断言会失败（这正是「以为加密了、其实明文」这类事故的唯一可查证据）。
func TestServerServesHTTPSWithCertPair(t *testing.T) {
	certFile, keyFile, pool := genSelfSigned(t)
	srv := NewServer(Config{
		Addr: "127.0.0.1:0",
		TLS:  TLSConfig{CertFile: certFile, KeyFile: keyFile},
	}, func(w stdhttp.ResponseWriter, _ *stdhttp.Request) { _, _ = w.Write([]byte("hello")) })
	if err := srv.Start(); err != nil {
		t.Fatalf("Start with TLS: %v", err)
	}
	defer func() { _ = srv.Stop() }()
	addr := srv.Addr()

	tlsClient := &stdhttp.Client{
		Timeout: 3 * time.Second,
		Transport: &stdhttp.Transport{TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		}},
	}
	resp, err := tlsClient.Get("https://" + addr + "/")
	if err != nil {
		t.Fatalf("HTTPS 请求失败（说明证书没被真正用起来）: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "hello" {
		t.Errorf("body = %q, want hello", body)
	}

	// 明文请求打到 TLS 端口**不得被业务 handler 服务**：标准库会直接回 400
	//（"client sent an HTTP request to an HTTPS server"，并打一条 TLS handshake error 日志）。
	// 判据是「拿不到业务响应」，不是「连接报错」——两种形态都说明 TLS 生效了。
	plain := &stdhttp.Client{Timeout: 3 * time.Second}
	presp, perr := plain.Get("http://" + addr + "/")
	if perr == nil {
		defer func() { _ = presp.Body.Close() }()
		pbody, _ := io.ReadAll(presp.Body)
		if presp.StatusCode == stdhttp.StatusOK && string(pbody) == "hello" {
			t.Errorf("明文 HTTP 请求被业务 handler 服务了（status=%d body=%q）—— TLS 未生效", presp.StatusCode, pbody)
		}
	}
}

// TestServerRejectsHalfConfiguredTLS 只配一半（有证书无私钥 / 反之）必须启动即失败：
// 静默回落明文是「运维以为已加密、实际裸奔」的最坏失败方式。
func TestServerRejectsHalfConfiguredTLS(t *testing.T) {
	for _, cfg := range []TLSConfig{
		{CertFile: "cert.pem"},
		{KeyFile: "key.pem"},
	} {
		srv := NewServer(Config{Addr: "127.0.0.1:0", TLS: cfg}, nil)
		if err := srv.Start(); err == nil {
			_ = srv.Stop()
			t.Errorf("TLS=%+v 半配置应启动失败", cfg)
		}
	}
}

// TestServerPlaintextStillWorks 不配 TLS 时保持明文行为（可配置回退）：
// 是否允许明文由上层角色策略决定（账号服默认要求 TLS），传输层不做一刀切。
func TestServerPlaintextStillWorks(t *testing.T) {
	srv := NewServer(Config{Addr: "127.0.0.1:0"}, func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		_, _ = w.Write([]byte("plain"))
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("Start plaintext: %v", err)
	}
	defer func() { _ = srv.Stop() }()
	resp, err := (&stdhttp.Client{Timeout: 3 * time.Second}).Get("http://" + srv.Addr() + "/")
	if err != nil {
		t.Fatalf("明文请求失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
}
