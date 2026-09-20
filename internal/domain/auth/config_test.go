package auth

import "testing"

// TestValidateAuthServerRequiresTLSByDefault 「默认要求 TLS」是本次加固的核心口径：
// 账号服 POST 的是明文口令与 JWT，未配证书又没显式放行明文时必须**拒绝启动**。
func TestValidateAuthServerRequiresTLSByDefault(t *testing.T) {
	c := AuthConfig{Listen: "127.0.0.1:8051", JWTSecret: "0123456789abcdef0123456789abcdef"}
	if err := c.ValidateAuthServer(); err == nil {
		t.Fatalf("未配 TLS 且未显式放行明文时应当报错（默认要求 TLS）")
	}
}

// TestValidateAuthServerAllowsExplicitPlaintext 「可配置回退」的口：
// 显式 insecure_plaintext=true 才放行明文（并打 Warn），不能把现有部署一棍打死。
func TestValidateAuthServerAllowsExplicitPlaintext(t *testing.T) {
	c := AuthConfig{Listen: "127.0.0.1:8051", InsecurePlaintext: true}
	if err := c.ValidateAuthServer(); err != nil {
		t.Fatalf("显式 insecure_plaintext=true 应放行明文，got err=%v", err)
	}
}

// TestValidateAuthServerAcceptsCertPair 配齐证书对则放行（并记录一条启用 TLS 的日志）。
func TestValidateAuthServerAcceptsCertPair(t *testing.T) {
	c := AuthConfig{TLS: TLSServerConfig{CertFile: "cert.pem", KeyFile: "key.pem"}}
	if err := c.ValidateAuthServer(); err != nil {
		t.Fatalf("证书成对配置应放行，got err=%v", err)
	}
}

// TestValidateAuthServerRejectsHalfConfiguredTLS 只配一半必须报错：
// 静默回落明文是最坏的失败方式（运维以为已加密，实际裸奔）。
func TestValidateAuthServerRejectsHalfConfiguredTLS(t *testing.T) {
	for _, cfg := range []TLSServerConfig{
		{CertFile: "cert.pem"},
		{KeyFile: "key.pem"},
	} {
		c := AuthConfig{TLS: cfg, InsecurePlaintext: true}
		// 即便显式放行了明文，半配置也是配置错误（写错了就该看见，而不是被放行掩盖）。
		if err := c.ValidateAuthServer(); err == nil {
			t.Errorf("TLS=%+v 半配置应报错", cfg)
		}
	}
}
