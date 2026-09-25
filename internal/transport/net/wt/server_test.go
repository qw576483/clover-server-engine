package wt

import (
	"sync"
	"sync/atomic"
	"testing"
)

// MaxConns 的额度必须把「正在 Upgrade 的连接」也算进去：
// 检查与占位在同一把锁内完成（reserve），登记时把预占额度转成活跃连接。
func TestServerReserveRespectsMaxConns(t *testing.T) {
	s := NewServer(ServerConfig{MaxConns: 2}, nil)

	if !s.reserve() {
		t.Fatal("第一个额度应预占成功")
	}
	if !s.reserve() {
		t.Fatal("第二个额度应预占成功")
	}
	if s.reserve() {
		t.Fatal("已达 MaxConns 仍预占成功")
	}
	s.release()
	if !s.reserve() {
		t.Fatal("归还额度后应可再次预占")
	}
}

// 并发预占：无论多少连接同时升级，授予的额度总数恰好是 MaxConns。
func TestServerReserveConcurrent(t *testing.T) {
	const maxConns = 8
	s := NewServer(ServerConfig{MaxConns: maxConns}, nil)

	var granted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.reserve() {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := granted.Load(); got != maxConns {
		t.Fatalf("并发预占成功 %d 个，应恰好 %d 个", got, maxConns)
	}
}

// MaxConns 为 0 表示不限流：任何并发都应预占成功。
func TestServerReserveUnlimited(t *testing.T) {
	s := NewServer(ServerConfig{}, nil)
	for i := 0; i < 100; i++ {
		if !s.reserve() {
			t.Fatalf("MaxConns=0 时第 %d 次预占被拒", i)
		}
	}
}
