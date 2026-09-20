package logbuf

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/log/state"
)

// ---------------- 测试替身 ----------------

// fakeWriter 记录收到的批次，并可按次数注入写入失败。
type fakeWriter struct {
	mu      sync.Mutex
	addr    string
	batches [][]state.LogEntry
	failN   int // 前 N 次写入返回错误
	closed  bool
}

func (w *fakeWriter) WriteBatch(_ string, entries []state.LogEntry) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failN > 0 {
		w.failN--
		return 0, errors.New("injected write failure")
	}
	cp := make([]state.LogEntry, len(entries))
	copy(cp, entries)
	w.batches = append(w.batches, cp)
	return len(entries), nil
}

func (w *fakeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *fakeWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, b := range w.batches {
		n += len(b)
	}
	return n
}

func (w *fakeWriter) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// fakeDialer 按调用次序轮询返回地址；空列表模拟「发现不到实例」。
type fakeDialer struct {
	mu    sync.Mutex
	addrs []string
	i     int
}

func (d *fakeDialer) Pick() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.addrs) == 0 {
		return ""
	}
	a := d.addrs[d.i%len(d.addrs)]
	d.i++
	return a
}

// registry 记录每个地址被 dial 的次数与最后的连接，便于断言「重连新建了连接」。
type registry struct {
	mu      sync.Mutex
	dials   map[string]int
	writers map[string]*fakeWriter
	failOne map[string]int // 地址 -> 还需让几条新连接首次写入失败
}

func newRegistry() *registry {
	return &registry{
		dials:   map[string]int{},
		writers: map[string]*fakeWriter{},
		failOne: map[string]int{},
	}
}

func (r *registry) dial(addr string) (Writer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dials[addr]++
	failN := 0
	if r.failOne[addr] > 0 {
		failN = 1
		r.failOne[addr]--
	}
	w := &fakeWriter{addr: addr, failN: failN}
	r.writers[addr] = w
	return w, nil
}

func (r *registry) dialCount(addr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dials[addr]
}

func (r *registry) writer(addr string) *fakeWriter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writers[addr]
}

// newTestBuffer 建一个只靠显式 Flush 驱动的缓冲器（FlushInterval 设得极大）。
func newTestBuffer(t *testing.T, opts Options) *Buffer {
	t.Helper()
	opts.Source = "test-source"
	opts.FlushInterval = time.Hour
	b, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(b.Close)
	return b
}

// ---------------- 测试 ----------------

// TestSendDistributesAcrossInstances 多实例下每一片日志轮询选实例，日志分散到全部实例。
func TestSendDistributesAcrossInstances(t *testing.T) {
	reg := newRegistry()
	b := newTestBuffer(t, Options{
		MaxBatch: 1, // 每条一片 ⇒ 每片各选一次实例
		Dialer:   &fakeDialer{addrs: []string{"log-1", "log-2", "log-3"}},
		Dial:     reg.dial,
	})

	const n = 6
	for i := 0; i < n; i++ {
		b.AddLog("player", "p1", "login", "info")
	}
	b.Flush()

	for _, addr := range []string{"log-1", "log-2", "log-3"} {
		w := reg.writer(addr)
		if w == nil {
			t.Fatalf("instance %s never used: logs were not distributed", addr)
		}
		if got := w.count(); got != 2 {
			t.Fatalf("instance %s: want 2 entries, got %d", addr, got)
		}
	}
	if st := b.Stats(); st.Sent != n {
		t.Fatalf("sent: want %d, got %d", n, st.Sent)
	}
}

// TestNoInstanceFallsBackToStatic 发现列表为空时回退静态地址，不丢日志。
func TestNoInstanceFallsBackToStatic(t *testing.T) {
	reg := newRegistry()
	b := newTestBuffer(t, Options{
		Addr:   "static-log",
		Dialer: &fakeDialer{}, // 空列表
		Dial:   reg.dial,
	})

	b.AddLog("player", "p1", "login", "info")
	b.Flush()

	w := reg.writer("static-log")
	if w == nil || w.count() != 1 {
		t.Fatalf("static fallback not used: writer=%v", w)
	}
}

// TestWriteFailureDropsConnectionAndReconnects 写入失败：弃用连接、计入丢弃、下一批自动重连。
func TestWriteFailureDropsConnectionAndReconnects(t *testing.T) {
	reg := newRegistry()
	reg.failOne["log-1"] = 1 // 第一条连接写入必失败

	b := newTestBuffer(t, Options{
		Dialer: &fakeDialer{addrs: []string{"log-1"}},
		Dial:   reg.dial,
	})

	b.AddLog("player", "p1", "login", "first")
	b.Flush()

	st := b.Stats()
	if st.Dropped != 1 {
		t.Fatalf("dropped: want 1, got %d", st.Dropped)
	}
	if st.Reconnect != 1 {
		t.Fatalf("reconnect: want 1, got %d", st.Reconnect)
	}
	if got := reg.dialCount("log-1"); got != 1 {
		t.Fatalf("dial count after failure: want 1, got %d", got)
	}
	if w := reg.writer("log-1"); w == nil || !w.isClosed() {
		t.Fatal("failed connection should have been closed")
	}

	// 下一批：连接已被弃用，应重新 dial 并成功。
	b.AddLog("player", "p1", "login", "second")
	b.Flush()

	if got := reg.dialCount("log-1"); got != 2 {
		t.Fatalf("dial count after retry: want 2, got %d", got)
	}
	st = b.Stats()
	if st.Sent != 1 {
		t.Fatalf("sent after retry: want 1, got %d", st.Sent)
	}
	if st.Dropped != 1 {
		t.Fatalf("dropped should stay 1, got %d", st.Dropped)
	}
}

// TestDialFailureIsCounted 建连失败不再静默：计入丢弃。
func TestDialFailureIsCounted(t *testing.T) {
	b := newTestBuffer(t, Options{
		Dialer: &fakeDialer{addrs: []string{"log-down"}},
		Dial:   func(string) (Writer, error) { return nil, errors.New("connection refused") },
	})

	b.AddLog("player", "p1", "login", "info")
	b.Flush()

	if st := b.Stats(); st.Dropped != 1 {
		t.Fatalf("dropped: want 1, got %d", st.Dropped)
	}
}

// TestConnectionReusedAcrossFlushes 同一实例的连接跨批次复用，不每批重建。
func TestConnectionReusedAcrossFlushes(t *testing.T) {
	reg := newRegistry()
	b := newTestBuffer(t, Options{
		Dialer: &fakeDialer{addrs: []string{"log-1"}},
		Dial:   reg.dial,
	})

	for i := 0; i < 3; i++ {
		b.AddLog("player", "p1", "login", "info")
		b.Flush()
	}

	if got := reg.dialCount("log-1"); got != 1 {
		t.Fatalf("dial count: want 1 (connection reused), got %d", got)
	}
	if got := reg.writer("log-1").count(); got != 3 {
		t.Fatalf("entries: want 3, got %d", got)
	}
}

// TestNewRequiresSomeTarget 既无 Dialer 也无静态地址时应报错，而不是静默丢弃全部日志。
func TestNewRequiresSomeTarget(t *testing.T) {
	if _, err := New(Options{Source: "s"}); err == nil {
		t.Fatal("New should fail when neither Dialer nor Addr is set")
	}
}
