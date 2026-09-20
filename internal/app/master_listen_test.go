package app

import "testing"

// TestValidateMasterListen 与 admin 控制面同一口径：「要么只绑回环，要么带共享密钥」。
// 两者都不满足 ⇒ 拒绝启动（fail-fast，不是只告警）。
// 依据：master 内部 RPC 上挂着 MsgSessionNew / MsgPlayerRegister / MsgRank* 等
// **无调用方身份校验**的写接口，任何能连到该端口的人都能改他人登录态与排行榜。
func TestValidateMasterListen(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		token   string
		wantErr bool
	}{
		{"未配置地址=不启用（不会监听，无需校验）", "", "", false},
		{"默认回环 + 无 token（加固前行为，默认部署零感知）", "127.0.0.1:8021", "", false},
		{"localhost + 无 token", "localhost:8021", "", false},
		{"[::1] + 无 token", "[::1]:8021", "", false},
		{"通配地址 + 无 token ⇒ 拒绝启动", "0.0.0.0:8021", "", true},
		{"空 host（=所有网卡）+ 无 token ⇒ 拒绝启动", ":8021", "", true},
		{"内网地址 + 无 token ⇒ 拒绝启动", "192.168.1.10:8021", "", true},
		{"通配地址 + token ⇒ 允许（需额外网络隔离）", "0.0.0.0:8021", "s3cret", false},
		{"内网地址 + token ⇒ 允许", "192.168.1.10:8021", "s3cret", false},
		{"回环 + token ⇒ 允许（token 不强制，但配了就校验）", "127.0.0.1:8021", "s3cret", false},
	}
	for _, c := range cases {
		err := validateMasterListen(c.addr, c.token)
		if c.wantErr && err == nil {
			t.Errorf("%s: validateMasterListen(%q, token=%q) = nil, want 拒绝启动", c.name, c.addr, c.token)
		}
		if !c.wantErr && err != nil {
			t.Errorf("%s: validateMasterListen(%q, token=%q) = %v, want nil", c.name, c.addr, c.token, err)
		}
	}
}
