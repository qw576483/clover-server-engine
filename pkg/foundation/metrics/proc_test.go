package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestProcessCPUSeconds 进程 CPU 累计秒必须能读到、且单调不减。
func TestProcessCPUSeconds(t *testing.T) {
	first, ok := ProcessCPUSeconds()
	if !ok {
		t.Skip("当前平台不支持进程 CPU 采集（proc_other.go），跳过")
	}
	if first < 0 {
		t.Fatalf("累计 CPU 秒不应为负：%v", first)
	}
	// 忙等一小会儿，确保 CPU 计数器一定推进（sleep 可能一点都不消耗 CPU）。
	deadline := time.Now().Add(20 * time.Millisecond)
	spin := 0
	for time.Now().Before(deadline) {
		spin++
	}
	_ = spin
	second, ok := ProcessCPUSeconds()
	if !ok {
		t.Fatal("第二次采样不应失败")
	}
	if second < first {
		t.Fatalf("累计 CPU 秒应单调不减：first=%v second=%v", first, second)
	}
}

// TestCollectRuntimeTo_ProcessCPU 进程 CPU 指标必须随采集导出；
// 平台不支持时则**不导出**（既不导出假数据，也不报错）。
func TestCollectRuntimeTo_ProcessCPU(t *testing.T) {
	reg := NewRegistry()
	_, supported := ProcessCPUSeconds()

	CollectRuntimeTo(reg)
	var buf bytes.Buffer
	if err := reg.WriteText(&buf); err != nil {
		t.Fatalf("导出指标失败: %v", err)
	}
	out := buf.String()

	if !supported {
		if strings.Contains(out, MetricProcCPUSeconds) || strings.Contains(out, MetricProcCPURatio) {
			t.Fatalf("平台不支持采集时不应导出进程 CPU 指标：\n%s", out)
		}
		return
	}
	if !strings.Contains(out, "# TYPE "+MetricProcCPUSeconds+" gauge") {
		t.Fatalf("应导出 %s：\n%s", MetricProcCPUSeconds, out)
	}
	if !strings.Contains(out, MetricProcCPUSeconds+" ") {
		t.Fatalf("应至少有一条 %s 样本：\n%s", MetricProcCPUSeconds, out)
	}

	// 第二次采集：有了基准，才能算出使用率（首次采样只建基准，故意不导出比率）。
	time.Sleep(2 * time.Millisecond)
	CollectRuntimeTo(reg)
	buf.Reset()
	if err := reg.WriteText(&buf); err != nil {
		t.Fatalf("导出指标失败: %v", err)
	}
	if !strings.Contains(buf.String(), MetricProcCPURatio+" ") {
		t.Fatalf("第二次采集应导出 %s：\n%s", MetricProcCPURatio, buf.String())
	}
}
