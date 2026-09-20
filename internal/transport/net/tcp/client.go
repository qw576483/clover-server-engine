package tcp

import (
	"net"

	"github.com/qw576483/clover-server-engine/internal/transport/net/session"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// Dial 拨号建立出站 TCP 连接（长度头协议 + 心跳）。
func Dial(cfg ClientConfig, handler Handler) (*Conn, error) {
	c := cfg.normalize()
	netConn, err := net.DialTimeout("tcp", c.Address, c.DialTimeout)
	if err != nil {
		return nil, err
	}
	// 应用 TCP 调优参数（Nagle、KeepAlive、读写缓冲）。
	applyClientTuning(netConn, c)
	conn := newConn(netConn, session.NewConnID(), netConn.RemoteAddr().String(),
		c.MaxMsgSize, c.HeartbeatInterval, handler)
	// 必须在 start() 之前标注角色：start 会立刻上报连接建立指标，
	// 标晚了这条连接会被计入 server 侧，导致主动连接被统计成入站连接。
	conn.markRole(roleClient)
	conn.start()
	logger.Infof("tcp dialed %s", c.Address)
	return conn, nil
}

// applyClientTuning 在客户端 net.Conn 上应用 TCP 调优参数。
func applyClientTuning(netConn net.Conn, cfg ClientConfig) {
	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(cfg.NoDelay)
		if cfg.KeepAlivePeriod > 0 {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(cfg.KeepAlivePeriod)
		}
		if cfg.ReadBufferSize > 0 {
			_ = tcpConn.SetReadBuffer(cfg.ReadBufferSize)
		}
		if cfg.WriteBufferSize > 0 {
			_ = tcpConn.SetWriteBuffer(cfg.WriteBufferSize)
		}
	}
}
