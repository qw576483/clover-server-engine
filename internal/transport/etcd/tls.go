package etcd

import (
	"crypto/tls"

	"github.com/qw576483/clover-server-engine/internal/shared/tlsutil"
)

// buildTLSConfig 根据 TLSConfig 构建 *tls.Config。
// CAFile 为空使用系统根证书池；CertFile/KeyFile 为空跳过客户端证书。
//
// 构建逻辑在 internal/shared/tlsutil（与 nats 共用一份），本函数只做
// 「本包 TLSConfig 字段 → 三个路径」的适配。
func buildTLSConfig(t *TLSConfig) (*tls.Config, error) {
	return tlsutil.Build(t.CertFile, t.KeyFile, t.CAFile)
}
