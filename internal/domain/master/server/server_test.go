package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
	"github.com/qw576483/clover-server-engine/internal/transport/tcpmsg"
)

// 本文件的断言对应 bug 条目 27 / 58 / 70：
//   - 调用方身份校验：非回环 + 空 token 必须启动失败；装了闸门后未握手的业务帧必须被拒；
//   - 参数校验：空 player_id / uid / node_id / board、倒挂的排名区间必须被拒。

const testToken = "s3cret-shared-token"

// startLoopbackServer 起一个只绑回环的 master 服务端（addr 用 :0 由系统分配端口）。
func startLoopbackServer(t *testing.T, token string) string {
	t.Helper()
	srv, err := Serve("127.0.0.1:0", state.NewState(), token)
	if err != nil {
		t.Fatalf("Serve(127.0.0.1:0, token=%q): %v", token, err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return srv.Addr()
}

// TestServeRejectsNonLoopbackWithoutToken 是「无鉴权 master RPC 不可暴露」的核心断言：
// 非回环绑定 + 空共享密钥必须在 listen 之前就失败（否则这条通道上的 session/rank 写接口
// 对所有同网可达者开放）。
func TestServeRejectsNonLoopbackWithoutToken(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", ":0", "192.168.1.10:0"} {
		if _, err := Serve(addr, state.NewState(), ""); err == nil {
			t.Errorf("Serve(addr=%q, token=\"\") 应拒绝启动，实际返回 nil", addr)
		}
	}
	// 反向断言：回环 + 空 token 是默认部署，必须能起（不能因加固把本机开发打死）。
	if _, err := Serve("127.0.0.1:0", state.NewState(), ""); err != nil {
		t.Errorf("Serve(127.0.0.1:0, token=\"\") 应能启动，实际: %v", err)
	}
}

// TestServeAcceptsNonLoopbackWithToken 配了共享密钥就允许绑非回环（内网多机部署）。
func TestServeAcceptsNonLoopbackWithToken(t *testing.T) {
	srv, err := Serve("0.0.0.0:0", state.NewState(), testToken)
	if err != nil {
		t.Fatalf("Serve(0.0.0.0:0, token 非空) 应能启动: %v", err)
	}
	_ = srv.Stop()
}

// emptyIDCases 覆盖条目 27/70 点名的写接口：空 ID / 空 board / 倒挂区间必须被拒。
func emptyIDCases() []struct {
	name      string
	msgID     uint32
	req       any
	wantInErr string
} {
	return []struct {
		name      string
		msgID     uint32
		req       any
		wantInErr string
	}{
		{"session new", state.MsgSessionNew, state.SessionNewReq{}, "player_id"},
		{"session validate", state.MsgSessionValidate, state.SessionValidateReq{Token: "t"}, "player_id"},
		{"session delete", state.MsgSessionDelete, state.SessionDeleteReq{}, "player_id"},
		{"session current", state.MsgSessionCurrent, state.SessionCurrentReq{}, "player_id"},
		{"session refresh", state.MsgSessionRefresh, state.SessionRefreshReq{Token: "t"}, "player_id"},
		{"player register empty uid", state.MsgPlayerRegister, state.PlayerRegisterReq{NodeID: "n1"}, "uid"},
		{"player register empty node_id", state.MsgPlayerRegister, state.PlayerRegisterReq{UID: "u1"}, "node_id"},
		{"player remove", state.MsgPlayerRemove, state.PlayerRemoveReq{}, "uid"},
		{"player lookup", state.MsgPlayerLookup, state.PlayerLookupReq{}, "uid"},
		{"rank top", state.MsgRankTop, state.RankTopReq{N: 10}, "board"},
		{"rank by rank range empty board", state.MsgRankByRankRange, state.RankByRankReq{Start: 1, Stop: 10}, "board"},
		{"rank by rank range inverted", state.MsgRankByRankRange, state.RankByRankReq{Board: "b", Start: 10, Stop: 3}, "invalid rank range"},
		{"rank clear", state.MsgRankClear, state.RankClearReq{}, "board"},
		{"rank backup", state.MsgRankBackup, state.RankBackupReq{}, "board"},
		{"rank restore", state.MsgRankRestore, state.RankRestoreReq{}, "board"},
	}
}

// TestHandlersRejectEmptyIDsAndBadRanges 在真实 TCP 链路上逐条断言「非法参数被拒」。
// 走真实链路而不是直接调 handler：handler 只注册进 tcpmsg.Server 的私有表，
// 且需要同时验证「回包形态」是 OK=false 而不是连接断开（调用方能拿到原因）。
func TestHandlersRejectEmptyIDsAndBadRanges(t *testing.T) {
	addr := startLoopbackServer(t, "")
	cli, err := tcpmsg.Dial(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, c := range emptyIDCases() {
		var resp state.Resp
		if err := cli.CallContext(ctx, c.msgID, c.req, &resp); err != nil {
			t.Fatalf("%s: 调用失败（期望 OK=false 的回包）: %v", c.name, err)
		}
		if resp.OK {
			t.Errorf("%s: OK=true，非法参数被放行", c.name)
			continue
		}
		if !strings.Contains(resp.Error, c.wantInErr) {
			t.Errorf("%s: error=%q，期望包含 %q", c.name, resp.Error, c.wantInErr)
		}
	}
}

// TestHandlersStillAcceptValidRequests 是负控：校验不能写成「一律拒绝」。
func TestHandlersStillAcceptValidRequests(t *testing.T) {
	addr := startLoopbackServer(t, "")
	cli, err := tcpmsg.Dial(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var newResp state.SessionNewResp
	if err := cli.CallContext(ctx, state.MsgSessionNew, state.SessionNewReq{PlayerID: "u1"}, &newResp); err != nil {
		t.Fatalf("session new: %v", err)
	}
	if !newResp.OK || newResp.Token == "" {
		t.Fatalf("session new: OK=%t token=%q，合法请求应签发 token", newResp.OK, newResp.Token)
	}

	var regResp state.Resp
	if err := cli.CallContext(ctx, state.MsgPlayerRegister, state.PlayerRegisterReq{UID: "u1", NodeID: "n1"}, &regResp); err != nil {
		t.Fatalf("player register: %v", err)
	}
	if !regResp.OK {
		t.Fatalf("player register: %s", regResp.Error)
	}

	var topResp state.RankTopResp
	if err := cli.CallContext(ctx, state.MsgRankTop, state.RankTopReq{Board: "b", N: 10}, &topResp); err != nil {
		t.Fatalf("rank top: %v", err)
	}
	if !topResp.OK {
		t.Fatalf("rank top: %s", topResp.Error)
	}

	// stop 传负数表示「取到末尾」，这是 memrank 的既有契约，不能被区间校验误伤。
	var rangeResp state.RankByRankResp
	if err := cli.CallContext(ctx, state.MsgRankByRankRange, state.RankByRankReq{Board: "b", Start: 1, Stop: -1}, &rangeResp); err != nil {
		t.Fatalf("rank by rank range(start=1,stop=-1): %v", err)
	}
	if !rangeResp.OK {
		t.Fatalf("rank by rank range(start=1,stop=-1): %s", rangeResp.Error)
	}
}

// TestConnAuthGate 验证连接级鉴权闸门（条目 27/58/70 的「调用方身份校验」）：
// 未握手 / 密钥错 ⇒ 拒；密钥对 ⇒ 放行且业务可用。
func TestConnAuthGate(t *testing.T) {
	addr := startLoopbackServer(t, testToken)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 未握手：业务帧必须被拒（回包 OK=false 或直接断连，两者都算拒绝）。
	plain, err := tcpmsg.Dial(addr)
	if err != nil {
		t.Fatalf("dial(no auth) %s: %v", addr, err)
	}
	var denied state.Resp
	err = plain.CallContext(ctx, state.MsgSessionNew, state.SessionNewReq{PlayerID: "u1"}, &denied)
	if err == nil && denied.OK {
		t.Errorf("未握手的连接调 MsgSessionNew 被放行（应拒绝）")
	}
	_ = plain.Close()

	// 密钥错：握手失败 ⇒ Dial 直接失败（不返回一条从未鉴权成功的连接）。
	if c, err := tcpmsg.Dial(addr, tcpmsg.WithAuth(state.MsgAuth, "wrong-token")); err == nil {
		_ = c.Close()
		t.Errorf("错误共享密钥的握手必须失败")
	}

	// 密钥对：握手成功 + 业务可用（负控）。
	auth, err := tcpmsg.Dial(addr, tcpmsg.WithAuth(state.MsgAuth, testToken))
	if err != nil {
		t.Fatalf("dial(with auth): %v", err)
	}
	defer func() { _ = auth.Close() }()
	var okResp state.SessionNewResp
	if err := auth.CallContext(ctx, state.MsgSessionNew, state.SessionNewReq{PlayerID: "u1"}, &okResp); err != nil {
		t.Fatalf("鉴权连接调 session new: %v", err)
	}
	if !okResp.OK {
		t.Fatalf("鉴权连接调 session new: %s", okResp.Error)
	}
}
