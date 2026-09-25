package nats

import "testing"

// TestResolveMaxReconnect 钉住 MaxReconnect 的三种语义必须**可区分**：
// 显式配 0 = 不重连，不得被当成「未设置」而静默改写成无限重连。
func TestResolveMaxReconnect(t *testing.T) {
	cases := []struct {
		name string
		in   *int
		want int
	}{
		{"未设置(nil) → 无限重连", nil, -1},
		{"显式 0 → 不重连", MaxReconnectPtr(0), 0},
		{"显式 3 → 最多重连 3 次", MaxReconnectPtr(3), 3},
		{"显式 -1 → 无限重连", MaxReconnectPtr(-1), -1},
	}
	for _, c := range cases {
		if got := resolveMaxReconnect(c.in); got != c.want {
			t.Fatalf("%s：resolveMaxReconnect = %d，期望 %d", c.name, got, c.want)
		}
	}
}

// TestDefaultConfigMaxReconnect：默认配置必须仍是「无限重连」（与部署侧文档记载的
// `max_reconnect` 默认 -1 一致），且该默认值来自**显式赋值**而非「未设置」。
func TestDefaultConfigMaxReconnect(t *testing.T) {
	conf := DefaultConfig()
	if conf.MaxReconnect == nil {
		t.Fatal("DefaultConfig().MaxReconnect 为 nil：默认配置必须显式给出默认值（-1 = 无限重连）")
	}
	if got := resolveMaxReconnect(conf.MaxReconnect); got != defaultMaxReconnect {
		t.Fatalf("默认配置解析出的 maxReconnect = %d，期望 %d", got, defaultMaxReconnect)
	}
}
