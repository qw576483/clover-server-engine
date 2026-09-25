package udp

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport/net/demux"
)

// TestReadLoopFromExitsOnStop 回归用例（bug 台账 #9）：
// StartFrom 启动的读循环必须在**本服务 Stop 后**退出，不能只靠「上游关闭 rawCh」。
//
// 复现口径：用一个**永不被关闭**的 ch 起 StartFrom（模拟「Stop 时上游尚未关 channel」），
// Stop 之后再投一条数据报 —— 读循环若已退出，handler 不会被再次调用；若仍挂在 range 上，
// handler 会被调用。
func TestReadLoopFromExitsOnStop(t *testing.T) {
	var got atomic.Int64
	srv := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, func(c *Conn, data []byte) {
		got.Add(1)
	})
	ch := make(chan *demux.Datagram, 8) // 刻意不关闭：Stop 必须能独立收回读循环
	if err := srv.StartFrom(ch); err != nil {
		t.Fatalf("StartFrom: %v", err)
	}
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
	ch <- &demux.Datagram{Data: []byte{0x01}, Addr: addr}
	if !waitCount(&got, 1, time.Second) {
		t.Fatalf("read loop did not dispatch first datagram")
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Stop 之后投递：读循环已退出 ⇒ 不再派发。
	ch <- &demux.Datagram{Data: []byte{0x02}, Addr: addr}
	if waitCount(&got, 2, 500*time.Millisecond) {
		t.Fatalf("read loop still running after Stop (handler called %d times)", got.Load())
	}
}

// waitCount 在 timeout 内轮询等待计数达到 want；达到返回 true。
func waitCount(v *atomic.Int64, want int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v.Load() >= want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return v.Load() >= want
}
