package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/qw576483/clover-server-engine/internal/domain/auth/state"
)

// TestIPLimiterGuardIsPerSource 限流必须按来源维度生效，且拒绝时回统一错误码 429。
// 背景：per-account 撞库防护只按账号计数，攻击者换账号名即可无限刷登录接口。
func TestIPLimiterGuardIsPerSource(t *testing.T) {
	l := newIPLimiter(1, 1) // 1 req/s, burst 1：第 1 个放行，其后被拒
	served := 0
	h := l.guard(state.PathLogin, func(w http.ResponseWriter, r *http.Request) { served++ })

	call := func(remote string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, state.PathLogin, strings.NewReader(`{}`))
		req.RemoteAddr = remote
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	if rec := call("10.0.0.1:1234"); rec.Code != http.StatusOK {
		t.Fatalf("首个请求 = %d, want 200", rec.Code)
	}
	for i := 0; i < 3; i++ {
		rec := call("10.0.0.1:1234")
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("第 %d 个超限请求 = %d, want 429", i+2, rec.Code)
		}
		var body state.TokenResp
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("429 回包不是 JSON: %v", err)
		}
		if body.Err != rateLimitedResp.Err {
			t.Errorf("429 文案 = %q, want %q（限流文案必须统一，不区分账号/步骤）", body.Err, rateLimitedResp.Err)
		}
	}
	if served != 1 {
		t.Errorf("被放行次数 = %d, want 1", served)
	}

	// 另一个来源不受影响 —— 限流键必须是「来源」，不是全局限流。
	if rec := call("10.0.0.2:1234"); rec.Code != http.StatusOK {
		t.Fatalf("另一来源首个请求 = %d, want 200（说明限流按来源维度，未误伤他人）", rec.Code)
	}
	if served != 2 {
		t.Errorf("被放行次数 = %d, want 2", served)
	}
}

// TestIPLimiterDefaultFallback 非法参数回落内置默认，避免配 0 / 负数导致「限流失效」或「全拒」。
func TestIPLimiterDefaultFallback(t *testing.T) {
	l := newIPLimiter(0, 0)
	if l.rate != defaultIPRatePerSec || l.burst != defaultIPRateBurst {
		t.Errorf("newIPLimiter(0,0) = (%g, %d), want (%d, %d)", l.rate, l.burst, defaultIPRatePerSec, defaultIPRateBurst)
	}
	if l.burst < 1 {
		t.Errorf("burst = %d, want >= 1", l.burst)
	}
}

// TestStatusOfUnifiedCodes 状态码不得成为账号枚举信道：
// 「账号已存在」必须与「参数不合法」同为 400；凭证类一律 401（不区分不存在 / 密码错 / 票据无效）。
func TestStatusOfUnifiedCodes(t *testing.T) {
	cases := []struct {
		kind state.Kind
		want int
	}{
		{state.KindBadParam, http.StatusBadRequest},
		{state.KindAccountExists, http.StatusBadRequest},
		{state.KindBadCredential, http.StatusUnauthorized},
		{state.KindTicketInvalid, http.StatusUnauthorized},
		{state.KindAccountLocked, http.StatusTooManyRequests},
		{state.KindChannelUnsupported, http.StatusNotImplemented},
		{state.KindChannelUnavailable, http.StatusServiceUnavailable},
		{state.KindInternal, http.StatusInternalServerError},
	}
	for _, c := range cases {
		if got := statusOf(c.kind); got != c.want {
			t.Errorf("statusOf(%d) = %d, want %d", c.kind, got, c.want)
		}
	}
}

// TestClientIPIgnoresForwardedHeaders 来源标识**只能**取自 TCP 对端地址：
// X-Forwarded-For / X-Real-IP 由客户端完全可控，采信它等于让攻击者每个请求换一个 key，
// 「按来源限流」与「按来源锁定」会同时失效。这条断言把该决定钉住。
func TestClientIPIgnoresForwardedHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, state.PathLogin, strings.NewReader(`{}`))
	req.RemoteAddr = "10.1.2.3:5555"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	req.Header.Set("X-Real-IP", "5.6.7.8")
	if got := clientIP(req); got != "10.1.2.3" {
		t.Errorf("clientIP = %q, want 10.1.2.3（不得采信客户端可伪造的转发头）", got)
	}
	// RemoteAddr 无端口（某些测试/代理形态）时原样返回，不 panic。
	req.RemoteAddr = "10.1.2.3"
	if got := clientIP(req); got != "10.1.2.3" {
		t.Errorf("clientIP(无端口) = %q, want 10.1.2.3", got)
	}
}

// TestWriteTokenUsesDomainText 传输层只做「Kind → 状态码」与「Text → body」的搬运，
// 不得自行改写文案（否则统一文案会在两个地方各写一半、迟早漂移）。
func TestWriteTokenUsesDomainText(t *testing.T) {
	rec := httptest.NewRecorder()
	writeToken(rec, nil, &state.Error{Kind: state.KindBadCredential, Text: "统一文案-X"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401", rec.Code)
	}
	var body state.TokenResp
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("回包不是 JSON: %v", err)
	}
	if body.Err != "统一文案-X" {
		t.Errorf("err = %q, want 统一文案-X（应原样透传领域的 Text）", body.Err)
	}

	// 成功路径：200 + token。
	rec = httptest.NewRecorder()
	writeToken(rec, &state.TokenResp{Success: true, Owner: "alice", Token: "t"}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("成功回包 code = %d, want 200", rec.Code)
	}
}
