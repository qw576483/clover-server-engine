// Package client 提供账号服的调用侧：游戏服登录时调 /auth/verify 换 owner。
//
// 与 domain/{master,log}/client 同构（那两个是 game 调 master / log），
// 差别仍是传输形态：这里是 HTTP JSON，不是自有 TCP 报文。
//
// 依赖方向说明：本包实现 transport/net/auth 定义的 Authenticator 接口，
// 并复用它的错误值 —— 即「业务域通过接口依赖 transport」，方向合规。
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"clover-server-engine/internal/domain/auth/state"
	"clover-server-engine/internal/shared/proto"
	iauth "clover-server-engine/internal/transport/net/auth"
	"clover-server-engine/pkg/foundation/logger"
)

const (
	// defaultVerifyTimeout /auth/verify 单次调用默认超时。
	// 登录是交互式操作，超过这个时间玩家已经在等了。
	defaultVerifyTimeout = 3 * time.Second
	// maxVerifyRespBytes 响应体读取上限（防异常服务端返回超大 body 打爆内存）。
	maxVerifyRespBytes = 64 << 10
)

// RemoteAuthenticator 登录时向账号服换取 owner，**完全不碰账号表**。
//
// 这是引擎唯一的内置登录实现：账号体系外置到账号服（server_type=auth），
// 游戏服只负责把客户端带来的 token 递过去、把 owner 拿回来。
//
// 取舍：登录强依赖账号服可用。这是刻意选择——换来的是「封号即时生效」与
// 「账号表不出账号服」；代价是账号服故障期间无人能登录（不会静默放行）。
type RemoteAuthenticator struct {
	verifyURL string
	client    *http.Client
}

// options NewRemoteAuthenticator 的可选参数。
type options struct {
	caFile             string
	insecureSkipVerify bool
}

// Option 构造 RemoteAuthenticator 的可选项。
type Option func(*options)

// WithCACertFile 用指定的 CA 证书包校验账号服证书（自签 / 内网 CA 场景）。
// 读不到该文件时**不静默降级**：记 Error 并保持系统根证书校验（宁可连不上，
// 也不要悄悄把证书校验关掉——那正是中间人最想要的形态）。
func WithCACertFile(path string) Option {
	return func(o *options) { o.caFile = path }
}

// WithInsecureSkipVerify 跳过账号服证书校验（仅内网自签临时使用，会打 Warn）。
func WithInsecureSkipVerify(skip bool) Option {
	return func(o *options) { o.insecureSkipVerify = skip }
}

// NewRemoteAuthenticator 用账号服地址构造（形如 https://127.0.0.1:8051）。
// timeout <= 0 时使用 defaultVerifyTimeout。
//
// TLS：账号服默认要求 TLS（见 domain/auth 的 AuthConfig.ValidateAuthServer），
// 因此地址通常写 https://；自签 / 内网 CA 用 WithCACertFile。
// 地址是 http:// 时打一条 Warn —— 明文链路上 game↔auth 之间传输的是 token，
// 这种部署只应出现在本机联调（账号服侧同样需要显式 insecure_plaintext=true 才能起）。
func NewRemoteAuthenticator(verifyAddr string, timeout time.Duration, opts ...Option) *RemoteAuthenticator {
	if timeout <= 0 {
		timeout = defaultVerifyTimeout
	}
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	verifyURL := strings.TrimRight(strings.TrimSpace(verifyAddr), "/") + state.PathVerify
	client := &http.Client{Timeout: timeout}
	if o.caFile != "" || o.insecureSkipVerify {
		// 克隆默认 Transport：直接 new 一个空 Transport 会丢掉标准库的连接池与
		// 超时默认值（表现为每次调用都新建连接）。
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: o.insecureSkipVerify} // #nosec G402 -- 由显式配置开关决定
		if o.caFile != "" {
			pem, err := os.ReadFile(o.caFile)
			if err != nil {
				logger.Errorf("auth: read verify_ca_file %s failed: %v (fallback to system roots)", o.caFile, err)
			} else {
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(pem) {
					logger.Errorf("auth: verify_ca_file %s contains no valid PEM certificate (fallback to system roots)", o.caFile)
				} else {
					tlsCfg.RootCAs = pool
				}
			}
		}
		if o.insecureSkipVerify {
			logger.Warnf("auth: 账号服证书校验已被显式关闭（verify_insecure_skip_verify=true）——" +
				"仅防被动嗅探、不防中间人")
		}
		tr.TLSClientConfig = tlsCfg
		client.Transport = tr
	}
	if strings.HasPrefix(strings.ToLower(verifyURL), "http://") {
		logger.Warnf("auth: auth.verify_addr 是明文 HTTP (%s) —— game↔auth 之间的 token 会被明文传输，"+
			"生产环境请改用 https:// 并配置账号服证书", verifyAddr)
	}
	return &RemoteAuthenticator{verifyURL: verifyURL, client: client}
}

// Authenticate 调账号服 /auth/verify 校验 token 并换取 owner。
//
// 错误分类刻意区分「凭证不对」与「账号服不可达」：
// 前者是玩家问题（应累计失败次数、防暴力破解），后者是服务端故障
// （不该锁玩家，且文案要能一眼看出是账号服的问题）。
func (a *RemoteAuthenticator) Authenticate(ctx context.Context, req *proto.ELoginRequest) (string, string, error) {
	if req.Token == "" {
		// 归为参数错误：Handler 会把这类文案透传给客户端，
		// 便于定位「客户端没带 token」这种集成问题。
		return "", "", fmt.Errorf("%w: token 不能为空", iauth.ErrBadParameter)
	}

	body, err := json.Marshal(map[string]string{"token": req.Token})
	if err != nil {
		logger.Errorf("auth: verify marshal request failed: %v", err)
		return "", "", iauth.ErrAuthUnavailable
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.verifyURL, bytes.NewReader(body))
	if err != nil {
		logger.Errorf("auth: verify build request failed (url=%s): %v", a.verifyURL, err)
		return "", "", iauth.ErrAuthUnavailable
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		// 最常见的故障：账号服没启动 / 地址填错 / 网络不通。必须打日志，
		// 否则玩家只会看到"登录失败"，运维无从下手。
		logger.Errorf("auth: verify unavailable (url=%s): %v", a.verifyURL, err)
		return "", "", iauth.ErrAuthUnavailable
	}
	// body 已整体读完（下方 LimitReader 读到底），关闭失败无补救动作；显式忽略。
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxVerifyRespBytes))
	if err != nil {
		logger.Errorf("auth: verify read response failed (url=%s): %v", a.verifyURL, err)
		return "", "", iauth.ErrAuthUnavailable
	}
	var r state.VerifyResp
	if err := json.Unmarshal(raw, &r); err != nil {
		logger.Errorf("auth: verify response invalid (url=%s http=%d): %v", a.verifyURL, resp.StatusCode, err)
		return "", "", iauth.ErrAuthUnavailable
	}
	if !r.Valid {
		// 不透传账号服的具体失败原因（过期 / 签名不符 / 伪造），避免被用来探测。
		logger.Warnf("auth: verify rejected token (http=%d err=%s)", resp.StatusCode, r.Err)
		return "", "", iauth.ErrBadCredential
	}
	if r.Owner == "" {
		// 纵深防御：账号服契约保证 valid=true 时 owner 非空，违反即视为服务端异常。
		logger.Errorf("auth: verify returned valid=true but empty owner (url=%s)", a.verifyURL)
		return "", "", iauth.ErrAuthUnavailable
	}
	// token 原样回传：仍是账号服签发的那一个。游戏服不续期、也无法续期。
	return r.Owner, req.Token, nil
}
