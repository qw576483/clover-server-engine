package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	apptypes "github.com/qw576483/clover-server-engine/pkg/app/types"
)

// TestNewAdminServerRejectsUnsafeListenAddr 构造期必须挡住「无 token + 非回环」：
// 该配置等于把 /admin/shutdown、/admin/drain、/admin/gateway/upstream、/log/level、
// /debug/pprof 以零鉴权状态开在所有同网可达者面前。
func TestNewAdminServerRejectsUnsafeListenAddr(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:8041", ":8041", "192.168.1.10:8041"} {
		s, err := NewAdminServer(AdminConfig{ListenAddr: addr})
		if !errors.Is(err, apptypes.ErrNonLoopbackWithoutToken) {
			t.Fatalf("NewAdminServer(listen_addr=%q) err = %v, want ErrNonLoopbackWithoutToken", addr, err)
		}
		if s != nil {
			t.Errorf("NewAdminServer(listen_addr=%q) 返回了非 nil 服务，安全配置不成立时不应构造", addr)
		}
	}
}

// TestNewAdminServerAllowsLoopbackWithoutToken 回环 + 无 token 是默认部署形态，必须放行。
func TestNewAdminServerAllowsLoopbackWithoutToken(t *testing.T) {
	s, err := NewAdminServer(AdminConfig{ListenAddr: apptypes.DefaultListenAddr})
	if err != nil {
		t.Fatalf("NewAdminServer(loopback): unexpected err = %v", err)
	}
	if s == nil {
		t.Fatal("NewAdminServer(loopback) = nil, want server")
	}
	if s.Addr() != "" {
		t.Errorf("Addr() = %q before Start, want empty", s.Addr())
	}
	if len(s.Routes()) == 0 {
		t.Error("Routes() 为空，内置探针路由应已注册")
	}
}

// TestNewAdminServerAllowsNonLoopbackWithToken token 是「允许绑非回环」的唯一开关。
func TestNewAdminServerAllowsNonLoopbackWithToken(t *testing.T) {
	s, err := NewAdminServer(AdminConfig{ListenAddr: "0.0.0.0:8041", Token: "s3cret"})
	if err != nil {
		t.Fatalf("NewAdminServer(non-loopback + token): unexpected err = %v", err)
	}
	if s == nil {
		t.Fatal("NewAdminServer(non-loopback + token) = nil, want server")
	}
}

// TestNewAdminServerDisabled 关闭开关：返回 (nil, nil)，调用方对 nil 接收者操作是安全的。
func TestNewAdminServerDisabled(t *testing.T) {
	s, err := NewAdminServer(AdminConfig{Disable: true})
	if err != nil {
		t.Fatalf("NewAdminServer(disable): unexpected err = %v", err)
	}
	if s != nil {
		t.Fatalf("NewAdminServer(disable) = %v, want nil", s)
	}
	// nil 接收者的空操作契约。ctx 传 context.TODO 而非 nil：Stop 对 nil 接收者在第一行就
	// return、根本不会读 ctx，传 nil 只是让静态检查报 SA1012 而不增加任何覆盖。
	s.Handle("/x", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	s.Stop(context.TODO())
	if s.Addr() != "" || s.Routes() != nil {
		t.Error("nil AdminServer 的 Addr/Routes 应为零值")
	}
}

// TestAdminGuardGuardsHighRiskEndpoints 高危端点必须凭令牌可达；探针端点不设门禁。
func TestAdminGuardGuardsHighRiskEndpoints(t *testing.T) {
	const token = "s3cret-token"
	s, err := NewAdminServer(AdminConfig{ListenAddr: apptypes.DefaultListenAddr, Token: token})
	if err != nil {
		t.Fatalf("NewAdminServer: %v", err)
	}

	get := func(path string, hdr map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		s.guard(s.mux).ServeHTTP(rec, req)
		return rec
	}

	// 高危端点（含 /deadletter 整组）：无令牌 / 错令牌 → 401。
	guarded := []string{
		"/log/level", "/debug/pprof/", "/admin/shutdown", "/admin/drain",
		"/deadletter", "/deadletter/dlq", "/deadletter/pending", "/deadletter/dlq/retry", "/deadletter/dlq/remove",
	}
	for _, path := range guarded {
		if got := get(path, nil).Code; got != http.StatusUnauthorized {
			t.Errorf("GET %s 无令牌 = %d, want 401", path, got)
		}
		if got := get(path, map[string]string{adminTokenHeader: "wrong"}).Code; got != http.StatusUnauthorized {
			t.Errorf("GET %s 错令牌 = %d, want 401", path, got)
		}
	}
	if got := get("/log/level", map[string]string{adminTokenHeader: token}).Code; got != http.StatusOK {
		t.Errorf("GET /log/level 带正确 X-Admin-Token = %d, want 200", got)
	}
	if got := get("/log/level", map[string]string{"Authorization": "Bearer " + token}).Code; got != http.StatusOK {
		t.Errorf("GET /log/level 带正确 Authorization: Bearer = %d, want 200", got)
	}

	// 401 回包必须带 WWW-Authenticate，且不泄漏「令牌对不对」以外的信息。
	rec := get("/log/level", nil)
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("401 响应缺少 WWW-Authenticate 头")
	}

	// /deadletter 带正确令牌必须穿过门禁。不要求 200：测试进程里 currentGame 为 nil，
	// 分发器会回 503 —— 「不是 401」本身就证明请求已到达端点、没被门禁误伤。
	if got := get("/deadletter/dlq", map[string]string{adminTokenHeader: token}).Code; got == http.StatusUnauthorized {
		t.Errorf("GET /deadletter/dlq 带正确 X-Admin-Token 仍 401，门禁误伤")
	}

	// 探针 / 指标端点不在门禁内（否则监控抓取与编排健康检查会被令牌挡住）。
	if got := get("/ping", nil).Code; got != http.StatusOK {
		t.Errorf("GET /ping 无令牌 = %d, want 200（探针不应被令牌挡）", got)
	}
	if got := get("/routes", nil).Code; got != http.StatusOK {
		t.Errorf("GET /routes 无令牌 = %d, want 200", got)
	}
}

// TestAdminGuardLoopbackWithoutTokenPassthrough 无 token 时放行是安全的——
// 前提是 Normalize 已保证地址只可能是回环（见 TestAdminConfigNormalizeRejectsNonLoopbackWithoutToken）。
func TestAdminGuardLoopbackWithoutTokenPassthrough(t *testing.T) {
	s, err := NewAdminServer(AdminConfig{ListenAddr: apptypes.DefaultListenAddr})
	if err != nil {
		t.Fatalf("NewAdminServer: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/log/level", nil)
	rec := httptest.NewRecorder()
	s.guard(s.mux).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /log/level 无 token 回环 = %d, want 200", rec.Code)
	}
}

// TestIsGuardedAdminPath 门禁前缀表：新增「能关服 / 切上游 / 改日志级别 / 读内存 / 改死信队列」
// 的端点时必须落在这张表内，否则等于新开一个无鉴权的高危入口。
//
// /deadletter 整组在列（含只读的 /deadletter/dlq 与 /deadletter/pending）：
// 其 retry / remove 有业务副作用，只读分支也暴露事件体与玩家标识。
func TestIsGuardedAdminPath(t *testing.T) {
	guarded := []string{
		"/admin/shutdown", "/admin/drain", "/admin/gateway/upstream", "/log/level", "/debug/pprof/heap",
		"/deadletter", "/deadletter/", "/deadletter/dlq", "/deadletter/dlq/retry", "/deadletter/dlq/remove", "/deadletter/pending",
	}
	open := []string{"/ping", "/routes", "/metrics", "/watchdog"}
	for _, p := range guarded {
		if !isGuardedAdminPath(p) {
			t.Errorf("isGuardedAdminPath(%q) = false, want true", p)
		}
	}
	for _, p := range open {
		if isGuardedAdminPath(p) {
			t.Errorf("isGuardedAdminPath(%q) = true, want false", p)
		}
	}
}
