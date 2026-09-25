package types

import (
	"errors"
	"testing"
	"time"
)

// TestIsLoopbackAddr 锁定「拿不准按非回环处理」的保守语义：
// 这条判据是「无 token 能不能绑该地址」的唯一依据，放宽任何一格都等于放行远程控制面。
func TestIsLoopbackAddr(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:8041", true},
		{"127.0.0.1", true},
		{"localhost:8041", true},
		{"[::1]:8041", true},
		{"::1", true},
		// 通配 / 空主机 = 监听所有网卡，不是回环。
		{"0.0.0.0:8041", false},
		{":8041", false},
		{"", false},
		// 内网 / 公网地址，以及需要 DNS 才能判定者（一律按非回环）。
		{"192.168.1.10:8041", false},
		{"clover-internal:8041", false},
	}
	for _, c := range cases {
		if got := IsLoopbackAddr(c.addr); got != c.want {
			t.Errorf("IsLoopbackAddr(%q) = %t, want %t", c.addr, got, c.want)
		}
	}
}

// TestAdminConfigNormalizeRejectsNonLoopbackWithoutToken 是 admin 安全边界的核心断言：
// 未配 token + 非回环必须**报错拒绝**，且**不得**把地址静默改写成回环
// ——静默改写会让运维以为配置已生效，实际根本没绑到内网（用一次假成功换一次真事故）。
func TestAdminConfigNormalizeRejectsNonLoopbackWithoutToken(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:8041", ":8041", "192.168.1.10:8041"} {
		c := AdminConfig{ListenAddr: addr}
		err := c.Normalize()
		if !errors.Is(err, ErrNonLoopbackWithoutToken) {
			t.Fatalf("Normalize(listen_addr=%q) err = %v, want ErrNonLoopbackWithoutToken", addr, err)
		}
		if c.ListenAddr != addr {
			t.Errorf("Normalize(listen_addr=%q) 静默改写了地址为 %q；必须原样保留并报错", addr, c.ListenAddr)
		}
		// 默认值回落仍应生效（报错不代表可以留零值给后续误用）。
		if c.ShutdownTimeout != DefaultShutdownTimeout {
			t.Errorf("ShutdownTimeout = %v, want %v", c.ShutdownTimeout, DefaultShutdownTimeout)
		}
	}
}

// TestAdminConfigNormalizeAcceptsNonLoopbackWithToken 反向断言：
// 配了 token 就允许绑非回环（内网多机运维的控制面场景），不能被上一条误伤。
func TestAdminConfigNormalizeAcceptsNonLoopbackWithToken(t *testing.T) {
	c := AdminConfig{ListenAddr: "0.0.0.0:8041", Token: "s3cret"}
	if err := c.Normalize(); err != nil {
		t.Fatalf("Normalize with token: unexpected err = %v", err)
	}
	if c.ListenAddr != "0.0.0.0:8041" {
		t.Errorf("ListenAddr = %q, want 0.0.0.0:8041 (token 非空时不应改写)", c.ListenAddr)
	}
}

// TestAdminConfigNormalizeDisabledSkipsCheck admin 关闭时根本不监听，端口与安全无关，
// 不该因为一个用不到的地址把进程挡在启动之外。
func TestAdminConfigNormalizeDisabledSkipsCheck(t *testing.T) {
	c := AdminConfig{Disable: true, ListenAddr: "0.0.0.0:8041"}
	if err := c.Normalize(); err != nil {
		t.Fatalf("Normalize(disable=true): unexpected err = %v", err)
	}
}

// TestAdminConfigNormalizeDefaults 零值回落（仓库「零值即默认」约定）。
func TestAdminConfigNormalizeDefaults(t *testing.T) {
	c := AdminConfig{}
	if err := c.Normalize(); err != nil {
		t.Fatalf("Normalize(zero): unexpected err = %v", err)
	}
	if c.ListenAddr != DefaultListenAddr {
		t.Errorf("ListenAddr = %q, want %q", c.ListenAddr, DefaultListenAddr)
	}
	if c.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %v, want %v", c.ShutdownTimeout, DefaultShutdownTimeout)
	}
	if DefaultShutdownTimeout != 5*time.Second {
		t.Errorf("DefaultShutdownTimeout = %v, want 5s", DefaultShutdownTimeout)
	}
}
