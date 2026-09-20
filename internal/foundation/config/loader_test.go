package config

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// replaceFile 用 Rename 原子替换目标文件；Windows 上目标文件正被读取时
// Rename 会以「Access is denied」（共享冲突）失败，故带重试。
func replaceFile(t *testing.T, from, to string) bool {
	t.Helper()
	for attempt := 0; ; attempt++ {
		if err := os.Rename(from, to); err == nil {
			return true
		} else if attempt >= 500 {
			t.Errorf("写入方替换配置失败（重试 %d 次）: %v", attempt, err)
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

type fileCfg struct {
	Listen string `mapstructure:"listen"`
	Count  int    `mapstructure:"count"`
}

// Load 必须「读入 viper + 解码 target」在同一把写锁内一次完成。
//
// 缺陷形态：旧实现在 ReadInConfig 之后释放写锁，再单独 Unmarshal —— 中间并发的热加载
// （Watch 的 reloadOnce 或并发的 Load）会把 viper 换成另一份配置，解码结果成为
// 「新文件 + 旧内存」的混合体。
//
// 本用例真的换配置：写入方不断在两个**完整**版本之间原子替换配置文件（临时文件 + Rename），
// 读取方并发 Load，断言任一时刻解出的都是其中一个完整版本。
// 只对同一份从不改写的文件反复 Load 是假并发——旧实现同样能过。
func TestLoaderLoadDecodesConfig(t *testing.T) {
	variantA := "listen: \":8001\"\ncount: 3\n"
	variantB := "listen: \":9002\"\ncount: 7\n"
	path := filepath.Join(t.TempDir(), "server.yaml")
	if err := os.WriteFile(path, []byte(variantA), 0o600); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	l := NewFileLoader(path)

	var out fileCfg
	if err := l.Load(&out); err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if out.Listen != ":8001" || out.Count != 3 {
		t.Fatalf("解码结果 = %+v，期望 listen=:8001 count=3", out)
	}
	if got := l.GetString("listen"); got != ":8001" {
		t.Fatalf("GetString(listen) = %q，期望 :8001", got)
	}
	if got := l.GetInt("count"); got != 3 {
		t.Fatalf("GetInt(count) = %d，期望 3", got)
	}

	// 写入方：在两个完整版本之间原子切换（先写临时文件再 Rename，避免读到半截内容）。
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		tmp := path + ".tmp"
		for i := 0; ; i++ {
			select {
			case <-stopWriter:
				return
			default:
			}
			content := variantA
			if i%2 == 1 {
				content = variantB
			}
			if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
				t.Errorf("写入方写临时文件失败: %v", err)
				return
			}
			if !replaceFile(t, tmp, path) {
				return
			}
		}
	}()

	// 读取方：并发重载 + 读取，任一时刻解出的都必须是完整配置。
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				var c fileCfg
				err := l.Load(&c)
				// Windows 上「Rename 替换」与「打开读取」会短暂互斥（共享冲突），
				// 属测试环境的文件语义而非被测逻辑：退避重试，断言仍落在解码结果上。
				for retry := 0; err != nil && retry < 100; retry++ {
					time.Sleep(time.Millisecond)
					err = l.Load(&c)
				}
				if err != nil {
					t.Errorf("并发 Load 失败: %v", err)
					return
				}
				switch {
				case c.Listen == ":8001" && c.Count == 3:
				case c.Listen == ":9002" && c.Count == 7:
				default:
					t.Errorf("并发解码出混合配置: %+v", c)
					return
				}
				_ = l.GetString("listen")
				_ = l.GetInt("count")
			}
		}()
	}
	wg.Wait()
	close(stopWriter)
	<-writerDone
	_ = l.Close()
}
