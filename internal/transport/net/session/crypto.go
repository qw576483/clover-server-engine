// Package session 提供会话级 AES-256-GCM 加解密能力。
//
// 设计原则：
//   - 会话级密钥：登录成功后由 auth handler 生成随机 32B key（NewKey），
//     通过 ELoginReply 明文下发客户端；**仅当客户端声明支持时**才生成（协商制）
//   - 网关透明：客户端 ↔ 网关通信在此层加解密，上层 proto / gwcore 无感知
//   - 首次登录回包不加密（ELoginReply 自身携带 key），之后全量加密
//   - 线格式：[12B nonce][ciphertext||tag(16B)]，整帧（含 8B 帧头）一起加密

package session

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

const (
	// KeySize AES-256 key size in bytes.
	KeySize = 32
	// NonceSize AES-GCM nonce size in bytes (standard 12 bytes).
	NonceSize = 12

	// MaxFrameSize 单帧上限（字节，10 MiB）——**服务端帧上限的唯一来源**。
	//
	// 口径 = 「客户端整帧」= 8B 帧头 + body（差 body 8B，业务按 10 MiB 组帧时注意）。
	// 引用它的地方（改这里 = 改跨端契约，必须与客户端常量同步）：
	//   - 网关配置默认值 `internal/app/config.go` 的 `GatewayConfig.MaxFrameSize`；
	//   - 各传输层单帧上限：`tcp/config.go`(defaultMaxMsgSize)、`tcp/codec.go`(hardMaxMsgSize)、
	//     `ws/config.go`(defaultMaxMsgSize)、`quic/config.go`(maxQUICFrameSize)；
	//   - 本包会话加密的密文上限 maxDecryptSize（见下）。
	// 客户端侧的独立副本：`Runtime/Network/Connection.cs`(ClientFrame.MaxBodySize /
	// TcpConnection.MaxFramePayload)、`WebSocketConnection.cs`(MaxMsgPayload)、
	// `Quic/QuicConnection.cs`(MaxFrameSize)、`Runtime/Network/SessionCrypto.cs`(MaxCiphertextBytes)。
	MaxFrameSize = 10 << 20

	// maxDecryptSize Decrypt 接受的**密文**最大长度 = MaxFrameSize（10 MiB），
	// 与帧上限同值同源 —— 不再另立一个更小口径（旧值 1<<20 与 10 MiB 的帧上限自相矛盾：
	// >1 MiB 的合法帧「加密得出来却永远解不开」，对端按密钥不符/被篡改**断连**，
	// 现场表现为连接静默失效而非报错）。
	//
	// 与帧上限的换算关系（下文 Encrypt/Decrypt 两处护栏共用同一把尺子）：
	//   **密文 = 明文 + NonceSize(12B) + tag(16B)**（线格式见包注释），
	//   故 Encrypt 放行的明文上限 = maxDecryptSize - NonceSize - aead.Overhead()
	//   = 10 MiB - 28B，即比网关放行的帧上限少 28B 的 GCM 开销。
	maxDecryptSize = MaxFrameSize
)

// ErrDecryptFailed indicates ciphertext tampering or incorrect key.
var ErrDecryptFailed = errors.New("session: decrypt failed")

// ErrPlaintextTooLarge 明文超过「可被对端解密」的长度上限（= maxDecryptSize - nonce - tag）：
// 加密成功但密文超出 Decrypt 的 maxDecryptSize，对端必然拒绝（帧静默失效）。
var ErrPlaintextTooLarge = errors.New("session: plaintext too large")

// AESGCMCodec 基于 AES-256-GCM 的会话级加密器。
type AESGCMCodec struct {
	aead cipher.AEAD
}

// NewKey 生成一个随机会话密钥（KeySize 字节）。
//
// 调用方是登录 handler：客户端在 ELoginRequest 里声明支持通道加密时才生成，
// 随 ELoginReply.SessionKey（base64）明文回给客户端，网关凭同一份 key 启用加解密。
func NewKey() ([]byte, error) {
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	return key, nil
}

// NewAESGCMCodec creates a new AES-256-GCM codec from a raw key.
// key must be exactly KeySize (32) bytes.
func NewAESGCMCodec(key []byte) (*AESGCMCodec, error) {
	if len(key) != KeySize {
		return nil, errors.New("session: key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &AESGCMCodec{aead: aead}, nil
}

// Encrypt encrypts plaintext with a random nonce.
// Returns [nonce(12B)][ciphertext+tag] so the nonce is self-contained.
func (c *AESGCMCodec) Encrypt(plaintext []byte) ([]byte, error) {
	// 与 Decrypt 的 maxDecryptSize 对称的长度护栏：明文上限 = maxDecryptSize - NonceSize - tag，
	// 即 = MaxFrameSize - 28B（帧上限那把尺子，见常量注释）。
	// 不设则长度介于两者之间的帧「能加密成功却永远解不开」（对端一律按坏包丢弃），
	// 现场表现为连接静默失效，而非报错。
	if limit := maxDecryptSize - NonceSize - c.aead.Overhead(); len(plaintext) > limit {
		return nil, ErrPlaintextTooLarge
	}
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, NonceSize+c.aead.Overhead()+len(plaintext))
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext that was produced by Encrypt.
// Expects [nonce(12B)][ciphertext+tag] format.
func (c *AESGCMCodec) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < NonceSize {
		return nil, ErrDecryptFailed
	}
	// 限制密文最大长度（= MaxFrameSize，与帧上限同源；传输层已按帧上限挡过一次，
	// 这里是本层自己的兜底），防止恶意巨长包导致 aead.Open 分配 OOM。
	if len(ciphertext) > maxDecryptSize {
		return nil, ErrDecryptFailed
	}
	nonce := ciphertext[:NonceSize]
	enc := ciphertext[NonceSize:]
	plain, err := c.aead.Open(nil, nonce, enc, nil)
	if err != nil {
		return nil, ErrDecryptFailed
	}
	return plain, nil
}
