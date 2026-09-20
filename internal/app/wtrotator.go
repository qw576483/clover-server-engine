package app

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"clover-server-engine/pkg/foundation/logger"
)

// 本文件实现 WebTransport 证书的「运行期间自动轮换」。
//
// 背景：WebTransport 证书固定（serverCertificateHashes）要求证书有效期 < 2 周，
// 网关自签的 wt.pem 每 10 天签发一次。如果只在启动时签发/检查（prepareWTCert），
// 长跑进程会在证书过期后直接连不上，必须重启才能恢复。因此检查必须在运行期执行：
// 后台 ticker 周期性复用 wtCertNeedsRotate 的规则，到期/不合规就重新签发并原子切换，
// 旧连接不受影响（握手已完成），新连接经 /wt-cert-hash 动态拉取到新哈希后照常建立。
//
// wtRotateInterval 运行期检查周期。轮换提前量 3 天，默认每小时检查一次足够；
// 每次检查仅一次文件读取 + 证书解析，开销可忽略。
// 可用环境变量 CLOVER_WT_ROTATE_INTERVAL（Go duration 格式，如 10s/5m）覆盖，
// 便于测试与按需调参。
func wtRotateInterval() time.Duration {
	if v := os.Getenv("CLOVER_WT_ROTATE_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return time.Hour
}

// wtCertEntry 当前生效的证书快照：创建后不可变，轮换时整体替换（原子指针）。
type wtCertEntry struct {
	tlsConf *tls.Config
	hash    string
}

// WTCertRotator 持有 WebTransport 证书的「动态视图」：
//
//   - TLS 握手：tls.Config.GetCertificate 回调每次从原子快照取最新证书。
//     quic-go 的 http3 在建立监听时会 Clone 传入的 tls.Config（浅拷贝）：
//     Clone 保留 GetCertificate 回调，但 Certificates slice 头是拷贝的，
//     原地改 Certificates 对握手线程不可见且存在数据竞争，故必须走回调。
//   - 哈希下发：/wt-cert-hash 每次请求经 WTCertHashFunc 动态读取快照，
//     轮换后前端下次建连自动拿到新哈希，无需重启任何服务。
//   - 轮换：Start 启动的后台循环周期性检查，到期即重签并原子切换。
type WTCertRotator struct {
	cur      atomic.Pointer[wtCertEntry]
	certPath string
	keyPath  string
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewWTCertRotator 构造轮换器并加载初始证书，复用 prepareWTCert 的
// 「首次自动签发 / 启动时校验轮换」逻辑。
func NewWTCertRotator(wtCertPath, wtKeyPath, tlsCertPath string) (*WTCertRotator, error) {
	certPath, keyPath := wtCertPaths(wtCertPath, wtKeyPath, tlsCertPath)
	conf, hash, err := prepareWTCert(certPath, keyPath, tlsCertPath)
	if err != nil {
		return nil, err
	}
	r := &WTCertRotator{certPath: certPath, keyPath: keyPath, stopCh: make(chan struct{})}
	// 握手动态取证的钩子：必须在 Server 使用该 tls.Config 之前挂上。
	conf.GetCertificate = r.getCertificate
	r.cur.Store(&wtCertEntry{tlsConf: conf, hash: hash})
	return r, nil
}

// TLSConfig 返回当前生效的 TLS 配置，供 WT server 建立 QUIC 监听。
func (r *WTCertRotator) TLSConfig() *tls.Config {
	if e := r.cur.Load(); e != nil {
		return e.tlsConf
	}
	return nil
}

// Hash 返回当前生效的证书哈希（hex）；轮换后自动返回新值。
func (r *WTCertRotator) Hash() string {
	if e := r.cur.Load(); e != nil {
		return e.hash
	}
	return ""
}

// getCertificate 是 tls.Config.GetCertificate 回调：每次握手从原子快照取最新证书。
// 返回的 *tls.Certificate 属于不可变快照，切换后旧快照不再被引用，指针安全。
func (r *WTCertRotator) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	e := r.cur.Load()
	if e == nil || len(e.tlsConf.Certificates) == 0 {
		return nil, fmt.Errorf("webtransport: rotator has no certificate")
	}
	return &e.tlsConf.Certificates[0], nil
}

// Start 启动运行期间自动轮换后台循环，直到 Stop 被调用。
func (r *WTCertRotator) Start() {
	go func() {
		t := time.NewTicker(wtRotateInterval())
		defer t.Stop()
		for {
			select {
			case <-r.stopCh:
				return
			case <-t.C:
				rotated, err := r.RotateIfNeeded()
				if err != nil {
					// 文件读不出来/临时错误只记录，不中断循环，下个周期重试。
					logger.Errorf("webtransport: cert rotate check failed: %v", err)
					continue
				}
				if rotated {
					logger.Infof("webtransport: certificate rotated in-service, new hash=%s", r.Hash())
				}
			}
		}
	}()
}

// Stop 停止后台轮换循环（幂等）。
func (r *WTCertRotator) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

// RotateIfNeeded 检查当前证书是否满足轮换条件，需要则重新签发并原子切换生效。
// 返回是否发生了轮换。不满足任何条件时返回 false, nil。
func (r *WTCertRotator) RotateIfNeeded() (bool, error) {
	cert, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return false, fmt.Errorf("webtransport: load current cert: %w", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return false, fmt.Errorf("webtransport: parse current cert: %w", err)
	}
	if !wtCertNeedsRotate(leaf, time.Now()) {
		return false, nil
	}
	if err := generateWTCert(r.certPath, r.keyPath); err != nil {
		return false, err
	}
	newCert, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return false, fmt.Errorf("webtransport: reload rotated cert: %w", err)
	}
	conf := newWTTLSConfig(newCert)
	conf.GetCertificate = r.getCertificate
	r.cur.Store(&wtCertEntry{tlsConf: conf, hash: certHashHex(newCert)})
	return true, nil
}
