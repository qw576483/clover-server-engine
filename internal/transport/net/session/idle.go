package session

import "time"

// DefaultIdleTimeout 未配置空闲上限时的默认值（与 ws / tcp 保持同一量级）。
const DefaultIdleTimeout = 60 * time.Second

// IdleScanner 周期扫描连接表并回收空闲连接。
//
// 抽成泛型：quic、udp、wt 三个传输服务器若各写一份**同构**的清理循环
// （ticker = timeout/2、done 信号退出、关闭标记早退、锁内收集、锁外关闭），
// 差别只有连接类型与判活字段名（lastActiveTime vs lastReadTime）；
// 循环节奏与退出语义只在这里定义一次，改一处即三处生效。
//
// 约定：
//   - Collect 必须在**锁内**完成「判定 + 摘除」，但**不得**在锁内关闭连接——
//     连接自身的 Close 会回调服务器去 delete(conns)，同一把非递归锁重入即死锁。
//     把待关闭列表返回出来、由 Close 在锁外执行，是这套流程存在的意义。
//   - Timeout 小于等于 0 时取 DefaultIdleTimeout；ticker 周期取 timeout/2，
//     保证连接最坏多存活半个空闲窗口。
type IdleScanner[C any] struct {
	// Timeout 空闲上限；<=0 取 DefaultIdleTimeout。
	Timeout time.Duration
	// Done 服务器关闭信号；关闭后立即退出循环。
	Done <-chan struct{}
	// Stopped 返回服务器是否已停止。每轮扫描前检查，避免关闭后继续触碰连接表。
	Stopped func() bool
	// Collect 锁内收集并摘除过期连接，返回待关闭列表（timeout 为归一化后的实际值）。
	Collect func(now time.Time, timeout time.Duration) []C
	// Close 锁外关闭一个连接。
	Close func(C)
}

// Run 阻塞运行扫描循环，直到 Done 关闭或 Stopped 为真。
func (s IdleScanner[C]) Run() {
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = DefaultIdleTimeout
	}
	// timeout 为极小正值（<2ns）时 timeout/2 == 0，NewTicker(0) 会 panic：
	// 向上保底到 1ns，保证被除后仍为正。
	half := timeout / 2
	if half <= 0 {
		half = time.Nanosecond
	}
	ticker := time.NewTicker(half)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-s.Done:
			return
		}
		if s.Stopped != nil && s.Stopped() {
			return
		}
		if s.Collect == nil || s.Close == nil {
			continue
		}
		for _, c := range s.Collect(time.Now(), timeout) {
			s.Close(c)
		}
	}
}
