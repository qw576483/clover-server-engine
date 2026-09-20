package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	idataaccount "github.com/qw576483/clover-server-engine/internal/domain/data/account"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/id"
	"github.com/qw576483/clover-server-engine/pkg/shared/jwt"
)

// 默认值：零值自动回落到这里。
const (
	// DefaultTokenTTL JWT 默认有效期。
	DefaultTokenTTL = 2 * time.Hour
	// DefaultIssuer JWT 默认签发者（iss 声明）。
	DefaultIssuer = "clover-auth"
	// minJWTSecretLen HS256 对称密钥的最小长度。过短的密钥可被离线爆破后
	// 伪造任意 owner 的 token（登录态全灭），启动期即拒绝。
	minJWTSecretLen = 16
	// maxTicketLen 渠道票据长度上限：票据是短凭证，超长输入直接拒绝，
	// 避免透传 verifier/日志造成内存与日志放大。
	maxTicketLen = 8 << 10
)

// AccountStore 账号服真正用到的账号存储能力（*idataaccount.Store 满足该接口）。
// 抽成接口的唯一目的是让传输层可单测——否则每个用例都要起一个 MySQL。
type AccountStore interface {
	Register(ctx context.Context, account, password string) error
	Load(ctx context.Context, account string) (*idataaccount.EAccount, error)
}

// ChannelStore 账号服真正用到的渠道绑定能力（*idataaccount.ChannelStore 满足该接口）。
// 与 AccountStore 同理：抽成接口让传输层可单测，不必起 MySQL。
type ChannelStore interface {
	Bind(ctx context.Context, channel, bindAccount, channelAccount string) error
	FindByChannel(ctx context.Context, channel, channelAccount string) (*idataaccount.EChannel, error)
}

// Config 构造账号服核心能力的入参。
type Config struct {
	// Account 账号表（必填；非 MySQL 数据层时为 nil，构造期即报错）。
	Account AccountStore
	// Channel 渠道绑定表（可空；接了渠道才需要）。
	Channel ChannelStore
	// Secret JWT 签名密钥（HS256，必填）。
	Secret string
	// TTL 签发 token 的有效期；<=0 回落到 DefaultTokenTTL。
	TTL time.Duration
	// Issuer JWT 签发者（iss 声明）；空=DefaultIssuer。
	Issuer string
}

// dummyPasswordHash 用于「账号不存在」路径的时序对齐：账号存在时走 bcrypt
// （几十~百毫秒），不存在时此前直接快速返回 —— 耗时可被用来枚举已注册账号。
// 启动时生成一次（失败返回空串，仅意味着该保护退化，记日志可见）。
var dummyPasswordHash = func() string {
	h, err := idataaccount.HashPassword("clover-placeholder-password")
	if err != nil {
		logger.Errorf("auth: init dummy password hash failed (timing alignment disabled): %v", err)
		return ""
	}
	return h
}()

// Service 账号服核心能力：注册 / 登录 / 验签 / 渠道登录 + 撞库防护。
//
// 与 master 的 *state.State 同构：持有本域全部运行态（这里只有撞库计数与签名密钥），
// 由 server 子包把传输层挂上来。**不感知 HTTP，也不感知角色宿主**——所以它能被
// 消息通道、HTTP、乃至将来的其它入口共用同一份实现。
type Service struct {
	account  AccountStore
	channel  ChannelStore
	verifier ChannelVerifier
	guard    *loginGuard
	secret   []byte
	ttl      time.Duration
	issuer   string
}

// New 构造账号服核心能力。
//
// 校验前置且显式报错：缺密钥 / 缺账号表 / 注册了渠道校验器却没有渠道表，
// 都**启动即失败**，而不是运行期才在请求里暴露问题（静默降级会让整条登录链路难以排查）。
// 渠道校验器从本包注册表读取（业务经 pkg/app.RegisterChannelVerifier 注册）。
func New(cfg Config) (*Service, error) {
	if cfg.Secret == "" {
		return nil, errors.New("auth: jwt_secret is required (must match game servers)")
	}
	if len(cfg.Secret) < minJWTSecretLen {
		// 只校验非空不足以自保：短密钥（HS256 对称密钥）可被离线爆破后
		// 伪造任意 owner 的 token。启动期 fail-fast。
		return nil, fmt.Errorf("auth: jwt_secret too short (%d < %d bytes); use a long random secret", len(cfg.Secret), minJWTSecretLen)
	}
	if cfg.Account == nil {
		// 账号表依赖 MySQL（data.tier 需为 redis_mysql / snapshot）。
		return nil, errors.New("auth: account store unavailable; auth requires MySQL data tier (check data.tier / data.mysql)")
	}
	verifier := currentChannelVerifier()
	//nolint:staticcheck // 渠道表是接口；调用方已保证只在非 nil 时传入。
	if verifier != nil && cfg.Channel == nil {
		return nil, errors.New("auth: ChannelVerifier registered but channel store unavailable (channel login requires MySQL data tier)")
	}

	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = DefaultTokenTTL
	}
	issuer := cfg.Issuer
	if issuer == "" {
		issuer = DefaultIssuer
	}
	return &Service{
		account:  cfg.Account,
		channel:  cfg.Channel,
		verifier: verifier,
		guard:    newLoginGuard(),
		secret:   []byte(cfg.Secret),
		ttl:      ttl,
		issuer:   issuer,
	}, nil
}

// Issuer 返回生效的签发者（供启动日志展示）。
func (s *Service) Issuer() string { return s.issuer }

// TTL 返回生效的 token 有效期（供启动日志展示）。
func (s *Service) TTL() time.Duration { return s.ttl }

// signupErrText 注册失败对外的**统一文案**（防账号枚举）。
//
// 为什么一处文案：注册接口天然会暴露「这个账号是否已存在」（已存在 = 失败、不存在 = 建号成功），
// 若再让「已存在」与「参数不合法」返回不同文案 / 不同状态码，攻击者就能用一批弱口令
// 一次性把「哪些账号存在」筛出来（撞库与定向钓鱼的第一步）。因此对外只说一句不可区分的
// **注册失败**，真实原因只进服务端日志；状态码同样统一为 400（见 auth/server 的 statusOf）。
const signupErrText = "注册失败：账号不可用或提交内容不符合要求"

// credentialErrText 登录凭证失败对外的**统一文案**（防账号枚举）。
//
// 「账号不存在」与「密码错误」必须逐字一致，否则响应体本身就是枚举信道：
// 攻击者拿一批账号试一遍就能分出「哪些存在」。真实原因只进服务端日志。
// 单独抽成常量（而非两处各写一遍字面量）是为了让后来者无法只改其中一处。
// #nosec G101 -- 这里是对外**错误文案**（不含任何凭据，也不是密钥/口令字面量）；
// gosec 仅因标识符名里含 credential 而报「hardcoded credentials」，属误报。
const credentialErrText = "账号或密码错误"

// Signup 注册并直接签发 token（注册即登录），客户端无需再调一次登录。
func (s *Service) Signup(ctx context.Context, account, password string) (*TokenResp, *Error) {
	if account == "" || password == "" {
		// 空字段仍归 KindBadParam（状态码同为 400），文案与其它注册失败一致，避免可区分。
		logger.Warnf("auth: signup rejected (empty account or password)")
		return nil, errf(KindBadParam, signupErrText)
	}
	if err := s.account.Register(ctx, account, password); err != nil {
		if errors.Is(err, idataaccount.ErrAccountExists) {
			logger.Warnf("auth: signup %q rejected (account exists)", account)
			return nil, errf(KindAccountExists, signupErrText)
		}
		if errors.Is(err, idataaccount.ErrInvalidAccountParam) {
			// 账号名不合法 / 密码为空等：归为请求错误，日志留全量细节。
			logger.Warnf("auth: signup %q rejected (bad param): %v", account, err)
			return nil, errf(KindBadParam, signupErrText)
		}
		// MySQL 连接失败 / 超时 / 驱动错误等：是服务端故障，不能错报成「客户端参数错误」，
		// 更不能把 err.Error()（含表名、驱动、SQL 细节）回写给客户端。
		logger.Errorf("auth: signup %q failed: %v", account, err)
		return nil, errf(KindInternal, "注册失败，请稍后重试")
	}
	token, exp, err := s.issue(account)
	if err != nil {
		logger.Errorf("auth: signup %q token issue failed: %v", account, err)
		return nil, errf(KindInternal, "签发 token 失败")
	}
	logger.Infof("auth: signup ok account=%s", account)
	return &TokenResp{Success: true, Owner: account, Token: token, Exp: exp}, nil
}

// Login 登录：按请求体是否带 channel 分派渠道登录，否则校验账号密码并签发 token。
//
// 内置撞库防护（见 guard.go）：按「账号」与「账号 + 来源 IP」双维度计数、
// 连续失败达阈值后按**指数退避**锁定，并有一个「只计失败」的全局失败预算兜大范围枚举。
// 凭证校验发生在账号服，所以这里才是权威防护点——游戏服侧的防护对 token 登录无效
// （token 模式不带账号名）。来源 IP 由传输层经 CredReq.ClientIP 填入（非线协议字段）。
//
// 刻意不调用 account Store 的 Login：那个方法生成的是引擎内部会话 token 并刷新 login_time，
// 而账号服签发的是 JWT；两者语义不同，混用会让 token 责任边界变模糊。
func (s *Service) Login(ctx context.Context, req CredReq) (*TokenResp, *Error) {
	// 渠道登录：{channel, ticket} 与账号密码共用同一接口。
	if req.Channel != "" {
		return s.ChannelLogin(ctx, req.Channel, req.Ticket)
	}
	if req.Account == "" || req.Password == "" {
		// 拒绝路径必须留日志（此前这条是静默 400，排查「客户端漏传字段」时无线索）。
		logger.Warnf("auth: login rejected (empty account or password)")
		return nil, errf(KindBadParam, "account 与 password 不能为空")
	}

	// 撞库防护：账号（或「账号+来源」/ 全局）已因连续失败被锁定，直接拒绝。
	// 注意不 RecordFailure：锁定期内继续尝试不该无限延长锁定时间。
	if reason, blocked := s.guard.shouldBlock(req.Account, req.ClientIP); blocked {
		logger.Warnf("auth: login %q blocked (%s) source=%q", req.Account, reason, req.ClientIP)
		return nil, errf(KindAccountLocked, "尝试次数过多，请稍后再试")
	}

	acc, err := s.account.Load(ctx, req.Account)
	if err != nil {
		if !errors.Is(err, idataaccount.ErrAccountNotFound) {
			// 数据库抖动/超时/不可用：不是凭证错误，绝不能累计失败计数——
			// 否则 DB 短暂故障即把正常账号累计到阈值并锁定 5 分钟（拒绝服务）。
			logger.Errorf("auth: login %q account load failed: %v", req.Account, err)
			return nil, errf(KindInternal, "登录服务暂时不可用，请稍后重试")
		}
		// 账号不存在与密码错误返回同一响应：避免账号枚举。
		// 同时做耗时对齐：对不存在的账号也执行一次 bcrypt（丢弃结果），
		// 抹平「快速 Load 失败」与「密码错走 bcrypt」的时间差（时序侧信道）。
		idataaccount.VerifyPassword(req.Password, dummyPasswordHash)
		logger.Warnf("auth: login %q rejected: account not found (source=%q)", req.Account, req.ClientIP)
		s.guard.recordFailure(req.Account, req.ClientIP)
		return nil, errf(KindBadCredential, credentialErrText)
	}
	if !acc.VerifyPassword(req.Password) {
		logger.Warnf("auth: login %q wrong password (source=%q)", req.Account, req.ClientIP)
		s.guard.recordFailure(req.Account, req.ClientIP)
		return nil, errf(KindBadCredential, credentialErrText)
	}
	// 凭证正确：清除失败计数。
	s.guard.reset(req.Account, req.ClientIP)

	token, exp, err := s.issue(req.Account)
	if err != nil {
		logger.Errorf("auth: login %q token issue failed: %v", req.Account, err)
		return nil, errf(KindInternal, "签发 token 失败")
	}
	logger.Infof("auth: login ok account=%s", req.Account)
	return &TokenResp{Success: true, Owner: req.Account, Token: token, Exp: exp}, nil
}

// Verify 校验 token：**游戏服每次登录都会调它**换取 owner（见 auth.verify_addr），
// 同时可供运维排查与外部系统确认 token 有效性。
//
// token 无效（过期 / 签名不符 / 伪造）**不是错误**：返回 Valid=false + Err，
// 由传输层用 200 表达，便于游戏服统一解析；5xx 留给账号服自身故障。
func (s *Service) Verify(token string) *VerifyResp {
	claims, err := jwt.Verify(token, s.secret)
	if err != nil {
		return &VerifyResp{Valid: false, Err: err.Error()}
	}
	// 校验签发者：此前完全不比对 iss —— 多环境共用同一密钥时，
	// 其它环境签发的 token 在本环境同样有效，无法区分签发来源。
	if claims.Iss != s.issuer {
		logger.Warnf("auth: verify rejected token: issuer mismatch (got %q want %q)", claims.Iss, s.issuer)
		return &VerifyResp{Valid: false, Err: "token issuer mismatch"}
	}
	return &VerifyResp{Valid: true, Owner: claims.Sub, Exp: claims.Exp}
}

// ChannelLogin 渠道登录：票据 → 渠道账号 → 主账号 → 签发 token。
//
// 刻意不做撞库防护：票据由渠道侧签发，无法枚举；且本流程没有可猜的密码。
func (s *Service) ChannelLogin(ctx context.Context, channel, ticket string) (*TokenResp, *Error) {
	if utf8.RuneCountInString(channel) > MaxChannelLen {
		// 请求参数非法，不是服务端故障：拦成 400。放行只会在 Bind 处撞 MySQL 的列宽限制，
		// 把「客户端传错」错报成 500，掩盖真实原因。
		logger.Warnf("auth: channel login rejected: channel too long (%d > %d)",
			utf8.RuneCountInString(channel), MaxChannelLen)
		return nil, errf(KindBadParam, "channel 过长（最多 32 字符）")
	}
	if ticket == "" {
		// 拒绝路径必须留日志（此前静默 400，无法区分「渠道侧没给票据」与「票据错」）。
		logger.Warnf("auth: channel login %q rejected: missing ticket", channel)
		return nil, errf(KindBadParam, "缺少 ticket")
	}
	if len(ticket) > maxTicketLen {
		// 票据此前无任何长度校验即透传 verifier：超长输入会直达渠道实现/日志，
		// 是内存与日志放大的入口。长度上限按「票据是短凭证」的常识取值。
		logger.Warnf("auth: channel login rejected: ticket too long (%d > %d)", len(ticket), maxTicketLen)
		return nil, errf(KindBadParam, "ticket 过长")
	}
	if s.verifier == nil {
		// 未接渠道：明确区分于「票据错误」，否则业务会去查渠道 SDK 而问题其实在服务端。
		logger.Warnf("auth: channel login %q rejected: no ChannelVerifier registered", channel)
		return nil, errf(KindChannelUnsupported, "账号服未接入该渠道（业务需注册 ChannelVerifier）")
	}
	if s.channel == nil {
		// 渠道绑定表依赖 MySQL；New 已在构造期拦截，此处是防御性兜底。
		logger.Errorf("auth: channel login %q rejected: channel store unavailable", channel)
		return nil, errf(KindChannelUnavailable, "渠道存储不可用")
	}

	channelAccount, err := s.verifier.Verify(ctx, channel, ticket)
	if err != nil {
		// 票据无效属「凭证错误」：不回显底层原因（避免帮助攻击者探测），但服务端留日志。
		logger.Warnf("auth: channel %q ticket verify failed: %v", channel, err)
		return nil, errf(KindTicketInvalid, "渠道票据校验失败")
	}
	if channelAccount == "" {
		// 校验器返回空标识属于实现错误：宁可失败，也不要凭空造出一个账号。
		logger.Errorf("auth: channel %q verifier returned empty channelAccount (verifier bug)", channel)
		return nil, errf(KindInternal, "渠道票据校验异常")
	}

	bindAccount, rerr := s.resolveChannelAccount(ctx, channel, channelAccount)
	if rerr != nil {
		logger.Errorf("auth: channel %q resolve account failed: %v", channel, rerr)
		return nil, errf(KindInternal, "渠道登录失败")
	}

	token, exp, err := s.issue(bindAccount)
	if err != nil {
		logger.Errorf("auth: channel %q token issue failed: %v", channel, err)
		return nil, errf(KindInternal, "签发 token 失败")
	}
	logger.Infof("auth: channel login ok channel=%s account=%s", channel, bindAccount)
	return &TokenResp{Success: true, Owner: bindAccount, Token: token, Exp: exp}, nil
}

// issue 为 owner 签发 JWT，返回 token 与其过期时间（Unix 秒）。
func (s *Service) issue(owner string) (string, int64, error) {
	exp := time.Now().Add(s.ttl).Unix()
	token, err := jwt.Sign(jwt.Claims{Sub: owner, Iss: s.issuer, Exp: exp}, s.secret)
	if err != nil {
		return "", 0, err
	}
	return token, exp, nil
}

// resolveChannelAccount 把渠道账号解析成主账号：已绑定直接用；未绑定则建号 + 绑定（注册即登录）。
func (s *Service) resolveChannelAccount(ctx context.Context, channel, channelAccount string) (string, error) {
	c, err := s.channel.FindByChannel(ctx, channel, channelAccount)
	if err == nil {
		return c.BindAccount, nil
	}
	if !errors.Is(err, idataaccount.ErrChannelNotFound) {
		return "", err
	}

	// 首次渠道登录：建号 + 绑定，语义与 /auth/signup 的「注册即登录」一致。
	account := channelAccountName(channel, channelAccount)
	if err := s.ensureAccountExists(ctx, account); err != nil {
		return "", err
	}
	if err := s.channel.Bind(ctx, channel, account, channelAccount); err != nil {
		// 并发首登：唯一键冲突说明另一请求已绑定成功，按「已绑定」返回，不报错。
		if errors.Is(err, idataaccount.ErrChannelExists) {
			if c, e := s.channel.FindByChannel(ctx, channel, channelAccount); e == nil {
				return c.BindAccount, nil
			}
		}
		return "", err
	}
	logger.Infof("auth: channel %q first login → created account=%s", channel, account)
	return account, nil
}

// ErrChannelAccountConflict 渠道账号名落在一个**非渠道占位**账号上时返回。
//
// 账号名 = {channel}_{channelAccount}_{sha256 前 8 字节}，完全由客户端可控的输入决定，
// 可被预测/复现，因此「Load 到同名账号」**不能**推出「这个账号属于本次渠道登录」：
// 若那个账号是玩家自注册的普通账号、或另一个渠道建的，直接复用会把本次渠道登录
// **绑定到他人账号**上（渠道登录即他人身份）。只有确认对方是渠道占位账号才允许复用。
var ErrChannelAccountConflict = errors.New("auth: channel account name conflicts with an existing non-channel account")

// channelPlaceholderPrefix 渠道自动建号所用占位密码的前缀（见 randomPlaceholderPassword）。
const channelPlaceholderPrefix = "!ch_"

// isChannelPlaceholderPassword 判断密码是否为渠道自动建号的占位密码。
func isChannelPlaceholderPassword(pw string) bool {
	return strings.HasPrefix(pw, channelPlaceholderPrefix)
}

// ensureAccountExists 确保账号存在（不存在则建号）。
//
// 渠道账号没有可用密码——它只能通过渠道登录进入，因此用随机串占位：
// 不可猜、不对外暴露，密码登录路径事实上无法命中它。
func (s *Service) ensureAccountExists(ctx context.Context, account string) error {
	// 用 Load 判存在：AccountStore 刻意只暴露 Register / Load 两个方法（便于替换实现），
	// 为判存在再引入一个 Exists 只会让接口与替身无谓变宽。
	if acc, err := s.account.Load(ctx, account); err == nil {
		// 复用前必须确认它是渠道占位账号：否则就是「名字撞上了别人的账号」，
		// 继续绑定等于把渠道会话交给对方（见 ErrChannelAccountConflict）。
		if acc == nil || !isChannelPlaceholderPassword(acc.Password) {
			return fmt.Errorf("%w (account=%s)", ErrChannelAccountConflict, account)
		}
		return nil // 账号已存在，且确为渠道占位账号
	} else if !errors.Is(err, idataaccount.ErrAccountNotFound) {
		return err
	}

	pw, err := randomPlaceholderPassword()
	if err != nil {
		// 包装上下文：此前裸 error 在调用链上与 DB 错误无从区分。
		return fmt.Errorf("auth: generate placeholder password for %s: %w", account, err)
	}
	if err := s.account.Register(ctx, account, pw); err != nil {
		// 并发首登：另一请求已建号，视为成功（后续 Bind 会带唯一键约束）。
		if errors.Is(err, idataaccount.ErrAccountExists) {
			return nil
		}
		return err
	}
	return nil
}

// randomPlaceholderPassword 生成渠道账号的占位密码（24 字节随机 + 固定前缀）。
// 前缀用于事后人工辨认「这个账号是渠道自动建的、没有可用密码」。
func randomPlaceholderPassword() (string, error) {
	s, err := id.RandomHex(24)
	if err != nil {
		return "", err
	}
	return channelPlaceholderPrefix + s, nil
}

// channelAccountName 由渠道标识生成主账号名：{channel}_{channelAccount}_{短哈希}。
//
// 为什么要规范化 + 截断 + 哈希后缀：账号名有长度与字符集约束（3-64 位字母/数字/_ . @ -，
// 见 account.Store.Register），而渠道标识与渠道账号（如微信 openid）都可能很长且含非法字符。
// 截断保证不超长，短哈希保证「截断后仍不碰撞」（不同渠道账号截断到同一前缀的概率极低）。
func channelAccountName(channel, channelAccount string) string {
	const (
		maxLen = 64
		// suffix 恒为 "_" + 16 位 hex（8 字节哈希）= 17 个字符。
		// 此前用 4 字节哈希（8 位 hex，2^-32 碰撞）：不同渠道账号截断/碰撞到同一账号名时，
		// 会话会被绑定到他人账号（渠道登录即他人身份）；8 字节把碰撞概率降到 2^-64。
		suffixLen = 17
		// 渠道标识部分的字节上限：还要给「末尾下划线 + 渠道账号至少 1 字符 + suffix」留位置。
		// channel 同样由客户端可控，不截断会让整个账号名突破 64 位上限 → Register 判非法 → 首登 500。
		maxChannelPart = maxLen - suffixLen - 2
	)

	prefix := sanitizeAccountPart(channel)
	if len(prefix) > maxChannelPart {
		prefix = prefix[:maxChannelPart]
	}
	prefix += "_"
	rest := sanitizeAccountPart(channelAccount)

	// 16 字符（8 字节）哈希后缀：同一渠道内碰撞概率约 2^-64。
	// 哈希用**原始**输入计算：截断只影响可读前缀，不参与稳定性与唯一性。
	sum := sha256.Sum256([]byte(channel + "\x00" + channelAccount))
	suffix := "_" + hex.EncodeToString(sum[:8])

	keep := maxLen - len(prefix) - len(suffix)
	if keep < 1 {
		keep = 1
	}
	if len(rest) > keep {
		rest = rest[:keep]
	}

	name := prefix + rest + suffix
	if len(name) < 3 {
		// 兜底：渠道名为空的极端情况下仍要满足最小长度约束。
		name = "u" + suffix
	}
	return name
}

// sanitizeAccountPart 只保留账号名允许的字符（其余替换为下划线），保证生成的名字必然合法。
func sanitizeAccountPart(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '.', r == '@', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
