package session

import (
	"bytes"
	"errors"
	"testing"
)

// 本文件守住「会话通道加密」的线格式契约：客户端（Unity，AES-GCM）按同一格式实现，
// 一旦这里改了布局或 tag 长度，必须同步改客户端 SessionCrypto，否则登录后所有帧解密失败。

// TestNewKey 密钥必须是 32B 且每次不同（相同即熵源复用，等于所有会话共用一把钥匙）。
func TestNewKey(t *testing.T) {
	a, err := NewKey()
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	if len(a) != KeySize {
		t.Fatalf("密钥长度 %d，期望 %d", len(a), KeySize)
	}
	b, err := NewKey()
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("两次生成的密钥相同——熵源异常")
	}
}

// TestEncryptDecryptRoundTrip 往返一致，且线格式为 [nonce(12)][ciphertext+tag(16)]。
func TestEncryptDecryptRoundTrip(t *testing.T) {
	key, err := NewKey()
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	codec, err := NewAESGCMCodec(key)
	if err != nil {
		t.Fatalf("NewAESGCMCodec: %v", err)
	}

	plain := []byte(`{"token":"abc"}`)
	enc, err := codec.Encrypt(plain)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if want := NonceSize + len(plain) + 16; len(enc) != want {
		t.Errorf("密文长度 %d，期望 %d（nonce + 明文 + tag）", len(enc), want)
	}

	got, err := codec.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("解密结果 %q，期望 %q", got, plain)
	}
}

// TestLimitsAlignWithFrameLimit 守住「加密上限 = 帧上限」这条口径：
// 会话加密的密文上限必须与 MaxFrameSize（服务端帧上限唯一来源）同值，不再各写一个数；
// 明文上限 = 密文上限 - nonce - tag（密文 = 明文 + nonce + tag，见 crypto.go 常量注释），
// 且**帧上限内的明文必须真的能加密并解回** —— 旧值 1<<20 会让 >1MiB 的合法帧「发不出/
// 被按篡改断连」，而这对两侧（Encrypt 护栏 / Decrypt 校验）是同一把尺子。
func TestLimitsAlignWithFrameLimit(t *testing.T) {
	if maxDecryptSize != MaxFrameSize {
		t.Fatalf("maxDecryptSize=%d，期望与 MaxFrameSize=%d 同值（单一来源）", maxDecryptSize, MaxFrameSize)
	}
	if MaxFrameSize != 10<<20 {
		t.Fatalf("MaxFrameSize=%d，期望 10 MiB（与客户端帧上限常量同值）", MaxFrameSize)
	}

	key, _ := NewKey()
	codec, _ := NewAESGCMCodec(key)
	limit := maxDecryptSize - NonceSize - codec.aead.Overhead()

	// 明文上限处必须能加密成功，且密文长度正好 = maxDecryptSize（nonce + 明文 + tag）。
	enc, err := codec.Encrypt(make([]byte, limit))
	if err != nil {
		t.Fatalf("Encrypt(%d 字节明文): %v —— 帧上限内的明文必须能加密", limit, err)
	}
	if len(enc) != maxDecryptSize {
		t.Errorf("密文长度 %d，期望 %d（明文 + nonce + tag = 密文上限）", len(enc), maxDecryptSize)
	}
	if _, err := codec.Decrypt(enc); err != nil {
		t.Fatalf("Decrypt(处于上限的密文): %v —— 加密出来的帧必须解得开", err)
	}

	// 再超一个字节即被拦（返回哨兵错误，不是「加密成功却解不开」）。
	if _, err := codec.Encrypt(make([]byte, limit+1)); !errors.Is(err, ErrPlaintextTooLarge) {
		t.Fatalf("超限明文的 err = %v，期望 ErrPlaintextTooLarge", err)
	}
}

// TestEncryptNonceIsRandom 同一明文两次加密必须得到不同密文（nonce 随机，防重放与密文比对）。
func TestEncryptNonceIsRandom(t *testing.T) {
	key, _ := NewKey()
	codec, _ := NewAESGCMCodec(key)
	plain := []byte("same-plaintext")

	first, err := codec.Encrypt(plain)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	second, err := codec.Encrypt(plain)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("两次加密结果相同——nonce 未随机化")
	}
}

// TestDecryptWrongKey 换一把密钥必须解密失败，且不被当成明文放行。
func TestDecryptWrongKey(t *testing.T) {
	keyA, _ := NewKey()
	keyB, _ := NewKey()
	codecA, _ := NewAESGCMCodec(keyA)
	codecB, _ := NewAESGCMCodec(keyB)

	enc, _ := codecA.Encrypt([]byte("secret"))
	if _, err := codecB.Decrypt(enc); err == nil {
		t.Fatal("错误密钥解密居然成功")
	}

	// 被篡改的密文同样必须失败（GCM 认证标签的作用）。
	tampered := append([]byte(nil), enc...)
	tampered[len(tampered)-1] ^= 0xFF
	if _, err := codecA.Decrypt(tampered); err == nil {
		t.Fatal("被篡改的密文解密居然成功")
	}
}
