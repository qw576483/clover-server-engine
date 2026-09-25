// Package jwt 提供最小实现的 HS256 JWT 签发与验签，仅依赖标准库。
//
// 为什么自己实现而不用第三方库：登录 token 只需要 HS256（对称签名），
// 实现不足百行，且**游戏服验签侧**因此无需引入新依赖、更容易审计。
// 若将来需要 RS256（账号服持私钥签、游戏服持公钥验），再引入库不迟。
//
// 典型用法：
//
//	// 账号服（签发）
//	token, _ := jwt.Sign(jwt.Claims{Sub: owner, Exp: time.Now().Add(time.Hour).Unix()}, secret)
//	// 游戏服（验签）
//	claims, err := jwt.Verify(token, secret)
//
// 安全要点：验签**强制校验 alg 字段必须为 HS256**，否则可被 "alg=none"
// 伪造型攻击绕过签名（把 alg 改成 none 并去掉签名段即可伪造任意身份）。
package jwt

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// 验签错误。调用方应区分「token 无效」（拒绝登录）与「内部错误」。
var (
	// ErrMalformed token 结构不合法（段数不对 / base64 解码失败 / JSON 非法）。
	ErrMalformed = errors.New("jwt: malformed token")
	// ErrAlgUnsupported header.alg 不是 HS256（含 alg=none 伪造）。
	ErrAlgUnsupported = errors.New("jwt: unsupported alg (only HS256)")
	// ErrSignature 签名不匹配（密钥不对或内容被篡改）。
	ErrSignature = errors.New("jwt: signature mismatch")
	// ErrExpired token 已过期。
	ErrExpired = errors.New("jwt: token expired")
	// ErrNotYetValid token 尚未生效（当前时间早于 nbf）。
	ErrNotYetValid = errors.New("jwt: token not yet valid")
	// ErrIssuedInFuture 签发时间 iat 位于未来（超出允许时钟偏移），疑为伪造或时钟错乱。
	ErrIssuedInFuture = errors.New("jwt: iat in the future")
	// ErrAudience 受众不匹配（多环境/多服隔离）。
	ErrAudience = errors.New("jwt: audience mismatch")
	// ErrWeakSecret 密钥长度不足：HS256 的密钥不得短于 SHA-256 输出长度（32 字节），
	// 否则 HMAC 安全强度被密钥熵而非哈希长度决定，短密钥可被离线爆破。
	ErrWeakSecret = errors.New("jwt: secret shorter than MinSecretLen")
)

// MinSecretLen HMAC-SHA256 密钥的最小长度。
//
// 取值与账号服侧 `internal/domain/auth/state` 的 minJWTSecretLen 对齐（同为 16）：
// 本包是引擎公开门面，若取比配置校验更严的值（如 RFC 7518 建议的 32），
// 会通过配置校验、却在签发/验签期才失败的密钥变成运行期故障。
const MinSecretLen = 16

// maxIatSkewSec iat 允许的未来偏移（容忍签发方与验签方的时钟差）。
const maxIatSkewSec = 60

// Claims JWT 载荷：只保留引擎实际需要的字段，避免载荷膨胀。
type Claims struct {
	// Sub 对象标识（owner）——账号服签发时是账号名或玩家 UID。
	Sub string `json:"sub"`
	// Iss 签发者标识（可选，便于多环境排查）。
	// 注：Verify 不强制校验该字段（校验需要「期望的 iss」这一部署决策）；
	// 需要多环境隔离时改用 VerifyAudience 校验 Aud，或由业务侧自行比对。
	Iss string `json:"iss,omitempty"`
	// Aud 受众标识（可选）。Verify 不校验；VerifyAudience 强制校验。
	Aud string `json:"aud,omitempty"`
	// Nbf 生效时间（Unix 秒）。0 表示不限制；非 0 时 Verify 在该时刻之前一律拒绝。
	Nbf int64 `json:"nbf,omitempty"`
	// Iat 签发时间（Unix 秒）。Verify 拒绝超出允许时钟偏移的未来值。
	Iat int64 `json:"iat"`
	// Exp 过期时间（Unix 秒）。Verify 强制校验，0 视为立即过期。
	Exp int64 `json:"exp"`
}

// headerHS256 固定头部。alg 恒为 HS256 / typ 恒为 JWT，无需每次构造。
var headerHS256 = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// signingAlg 验签时唯一接受的 alg 取值（HS256）。
const signingAlg = "HS256"

// Sign 用 secret 签发 HS256 token。exp 为 0 时由调用方自行保证不签发永久 token
// （本函数不禁止，但 Verify 会把 exp==0 视为已过期）。
func Sign(c Claims, secret []byte) (string, error) {
	if c.Sub == "" {
		return "", errors.New("jwt: sub must not be empty")
	}
	if err := checkSecret(secret); err != nil {
		return "", err
	}
	if c.Iat == 0 {
		c.Iat = time.Now().Unix()
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("jwt: marshal claims: %w", err)
	}
	signingInput := headerHS256 + "." + base64.RawURLEncoding.EncodeToString(payload)
	return signingInput + "." + sign(signingInput, secret), nil
}

// Verify 验签并返回载荷。
//
// 校验顺序：结构 → alg → 签名 → 过期。alg 必须先于签名校验，
// 否则攻击者可把 alg 改成 none 并清空签名段绕过签名检查。
func Verify(token string, secret []byte) (*Claims, error) {
	if err := checkSecret(secret); err != nil {
		return nil, err
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrMalformed
	}
	// alg 白名单：只接受 HS256。拒绝 none / RS256 等一切其它取值，
	// 防止「算法混淆」类攻击（如把 alg 改为 none 或改为 RS256 让验签方用公钥当密钥）。
	headerRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrMalformed
	}
	var hdr struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerRaw, &hdr); err != nil {
		return nil, ErrMalformed
	}
	if hdr.Alg != signingAlg {
		return nil, ErrAlgUnsupported
	}

	// 签名用常量时间比较，避免按字节短路造成的时序侧信道。
	expected := sign(parts[0]+"."+parts[1], secret)
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return nil, ErrSignature
	}

	payloadRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrMalformed
	}
	var c Claims
	if err := json.Unmarshal(payloadRaw, &c); err != nil {
		return nil, ErrMalformed
	}
	if c.Sub == "" {
		return nil, ErrMalformed
	}
	now := time.Now().Unix()
	// exp 为 0（未设置）一律视为过期：宁可拒绝，也不接受不过期的 token。
	if c.Exp == 0 || now >= c.Exp {
		return nil, ErrExpired
	}
	// nbf：载荷可自带生效时间，未到点必须拒绝。
	if c.Nbf > 0 && now < c.Nbf {
		return nil, ErrNotYetValid
	}
	// iat：超出允许偏移的未来签发时间视为伪造/时钟错乱。
	if c.Iat > 0 && c.Iat > now+maxIatSkewSec {
		return nil, ErrIssuedInFuture
	}
	return &c, nil
}

// VerifyAudience 在 Verify 之外额外校验 aud 字段。
//
// 用途：多环境（测试/正式）或多服共用同一 jwt_secret 时，仅靠签名无法区分签发来源；
// 签发侧写入 Aud、验签侧指定期望 audience，即可拒绝「别处签发的合法 token」。
// audience 为空串表示不校验（等价于 Verify）。
func VerifyAudience(token string, secret []byte, audience string) (*Claims, error) {
	c, err := Verify(token, secret)
	if err != nil {
		return nil, err
	}
	if audience != "" && c.Aud != audience {
		return nil, ErrAudience
	}
	return c, nil
}

// checkSecret 密钥校验：非空 + 不小于 MinSecretLen。
// 只判空会让「1 字节密钥」这种配置错误一路通过到生产，HS256 的强度完全由密钥熵决定。
func checkSecret(secret []byte) error {
	if len(secret) == 0 {
		return errors.New("jwt: secret must not be empty")
	}
	if len(secret) < MinSecretLen {
		return ErrWeakSecret
	}
	return nil
}

// sign 计算 signingInput 的 HMAC-SHA256 签名并做 base64url（无 padding）编码。
func sign(signingInput string, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
