package state

import (
	"context"
	"testing"
	"time"

	idataaccount "github.com/qw576483/clover-server-engine/internal/domain/data/account"
)

// fakeAccounts 是 AccountStore 的测试替身（抽成接口的初衷即「让传输层可单测，不必起 MySQL」）。
type fakeAccounts struct {
	acc     *idataaccount.EAccount
	loadErr error
	regErr  error
}

func (f *fakeAccounts) Register(context.Context, string, string) error { return f.regErr }

func (f *fakeAccounts) Load(context.Context, string) (*idataaccount.EAccount, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.acc, nil
}

func newTestService(t *testing.T, fs *fakeAccounts) *Service {
	t.Helper()
	s, err := New(Config{Account: fs, Secret: "0123456789abcdef0123456789abcdef", TTL: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestSignupFailureTextIsUnified 注册失败对外只有一句不可区分的文案：
// 「账号已存在」与「参数不合法」若文案 / Kind 不同，攻击者用一批弱口令试注册即可筛出
// 「哪些账号存在」——这正是撞库与定向钓鱼的第一步。
func TestSignupFailureTextIsUnified(t *testing.T) {
	ctx := context.Background()

	svcExists := newTestService(t, &fakeAccounts{regErr: idataaccount.ErrAccountExists})
	_, errExists := svcExists.Signup(ctx, "alice", "password123")

	svcBadParam := newTestService(t, &fakeAccounts{regErr: idataaccount.ErrInvalidAccountParam})
	_, errBadParam := svcBadParam.Signup(ctx, "alice", "password123")

	if errExists == nil || errBadParam == nil {
		t.Fatalf("两个失败路径都应返回错误：exists=%v badParam=%v", errExists, errBadParam)
	}
	if errExists.Text != errBadParam.Text {
		t.Errorf("注册失败文案不一致：exists=%q badParam=%q（可被用来枚举账号）", errExists.Text, errBadParam.Text)
	}
	if errExists.Text != signupErrText {
		t.Errorf("注册失败文案 = %q, want %q", errExists.Text, signupErrText)
	}

	// 空字段也必须走同一句文案（状态码同为 400）。
	svc := newTestService(t, &fakeAccounts{})
	_, errEmpty := svc.Signup(ctx, "", "")
	if errEmpty == nil || errEmpty.Text != signupErrText {
		t.Errorf("空字段注册文案 = %v, want %q", errEmpty, signupErrText)
	}
	if errEmpty.Kind != KindBadParam || errExists.Kind != KindAccountExists {
		t.Errorf("Kind 应保留真实原因供传输层映射日志：empty=%d exists=%d", errEmpty.Kind, errExists.Kind)
	}
}

// TestLoginCredentialTextIsUnified 「账号不存在」与「密码错误」必须返回逐字相同的文案与同一个 Kind，
// 否则响应体本身就是账号枚举信道。
func TestLoginCredentialTextIsUnified(t *testing.T) {
	ctx := context.Background()

	// 账号不存在
	svcMissing := newTestService(t, &fakeAccounts{loadErr: idataaccount.ErrAccountNotFound})
	_, errMissing := svcMissing.Login(ctx, CredReq{Account: "ghost", Password: "whatever"})

	// 密码错误（账号确实存在）
	hash, err := idataaccount.HashPassword("correct-horse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	svcWrong := newTestService(t, &fakeAccounts{acc: &idataaccount.EAccount{Account: "alice", Password: hash}})
	_, errWrong := svcWrong.Login(ctx, CredReq{Account: "alice", Password: "wrong-password"})

	if errMissing == nil || errWrong == nil {
		t.Fatalf("两个失败路径都应返回错误：missing=%v wrong=%v", errMissing, errWrong)
	}
	if errMissing.Text != errWrong.Text {
		t.Errorf("登录失败文案不一致：notFound=%q wrongPassword=%q（两者都可枚举账号）", errMissing.Text, errWrong.Text)
	}
	if errMissing.Text != credentialErrText {
		t.Errorf("登录失败文案 = %q, want %q", errMissing.Text, credentialErrText)
	}
	if errMissing.Kind != errWrong.Kind {
		t.Errorf("登录失败 Kind 不一致：%d vs %d（状态码会随 Kind 变化，构成枚举信道）",
			errMissing.Kind, errWrong.Kind)
	}
}

// TestLoginEmptyParamRejected 缺字段是请求错误（400），与凭证错误区分开——
// 它不携带任何「账号是否存在」的信息，不构成枚举信道。
func TestLoginEmptyParamRejected(t *testing.T) {
	svc := newTestService(t, &fakeAccounts{})
	_, err := svc.Login(context.Background(), CredReq{Account: "", Password: ""})
	if err == nil || err.Kind != KindBadParam {
		t.Fatalf("空字段登录 = %v, want KindBadParam", err)
	}
}

// TestSignupInternalErrorDoesNotLeak 存储故障既不能错报成「客户端参数错误」，
// 也不能把驱动 / SQL 细节回写给客户端。
func TestSignupInternalErrorDoesNotLeak(t *testing.T) {
	svc := newTestService(t, &fakeAccounts{regErr: errFakeDB})
	_, err := svc.Signup(context.Background(), "alice", "password123")
	if err == nil || err.Kind != KindInternal {
		t.Fatalf("存储故障 = %v, want KindInternal", err)
	}
	if err.Text == signupErrText {
		t.Errorf("存储故障不应复用「注册失败」文案（会把服务端故障伪装成参数问题）：%q", err.Text)
	}
}

// TestLoginRecordsSourceIPIntoGuard 传输层填的 CredReq.ClientIP 必须真的进入撞库计数：
// 「账号 + 来源」维度的阈值（maxPairAttempts）低于账号维度，同一来源连续失败时应先命中它，
// 且另一来源在同一时刻仍可登录（证明来源维度是按来源分别计数的，不是被忽略）。
func TestLoginRecordsSourceIPIntoGuard(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, &fakeAccounts{loadErr: idataaccount.ErrAccountNotFound})

	srcA, srcB := "10.1.1.1", "10.2.2.2"
	for i := 0; i < maxPairAttempts; i++ {
		if _, err := svc.Login(ctx, CredReq{Account: "alice", Password: "pw", ClientIP: srcA}); err == nil {
			t.Fatalf("账号不存在应登录失败")
		}
	}
	// 同一来源第 +1 次：应因「账号+来源」锁定被拒（KindAccountLocked），而不是凭证错误。
	_, err := svc.Login(ctx, CredReq{Account: "alice", Password: "pw", ClientIP: srcA})
	if err == nil || err.Kind != KindAccountLocked {
		t.Fatalf("同来源连续 %d 次失败后应返回 KindAccountLocked，got %v", maxPairAttempts, err)
	}
	// 另一来源此时尚未达任何阈值：拿到的应是凭证错误（说明来源维度确实按来源分开计数）。
	if _, errB := svc.Login(ctx, CredReq{Account: "alice", Password: "pw", ClientIP: srcB}); errB == nil || errB.Kind != KindBadCredential {
		t.Fatalf("另一来源不该被同源的锁定牵连，got %v", errB)
	}
}

// errFakeDB 模拟 MySQL 连接失败等驱动级错误。
var errFakeDB = errFake("fake: mysql connection refused")

type errFake string

func (e errFake) Error() string { return string(e) }
