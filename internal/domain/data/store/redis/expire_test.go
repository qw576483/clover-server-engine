package redis

import (
	"testing"
	"time"
)

// TestTTLMillis 钉住 CAS 脚本的 TTL 参数归一规则：
//   - 0 / 负值 → 0 = 不过期（负值必须夹紧，Redis 的 `PX -1` 会**直接删除** key）；
//   - 正不足 1ms → 向上取整为 1ms（向下取整会变成 0 = 不过期，与本意相反）。
func TestTTLMillis(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{"0 = 不过期", 0, 0},
		{"负值夹紧为 0", -time.Second, 0},
		{"1s → 1000ms", time.Second, 1000},
		{"500ms → 500ms", 500 * time.Millisecond, 500},
		{"1ms → 1ms", time.Millisecond, 1},
		{"不足 1ms 向上取整为 1ms", time.Nanosecond, 1},
	}
	for _, c := range cases {
		if got := ttlMillis("probe:key", c.in); got != c.want {
			t.Fatalf("%s：ttlMillis(%v) = %d，期望 %d", c.name, c.in, got, c.want)
		}
	}
}
