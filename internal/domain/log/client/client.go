// Package client 提供 log 服的 TCP 客户端（game 侧使用）。
package client

import (
	"clover-server-engine/internal/domain/log/state"
	"clover-server-engine/internal/transport/tcpmsg"
)

// Client log 服客户端：连接 log 服 TCP 端口，支持批量上报日志。
type Client struct {
	conn *tcpmsg.Client
}

// Dial 连接到 log 服 TCP 地址，返回就绪的 Client。
func Dial(addr string) (*Client, error) {
	conn, err := tcpmsg.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// WriteBatch 批量上报日志。
func (c *Client) WriteBatch(source string, entries []state.LogEntry) (int, error) {
	var resp state.LogBatchResp
	if err := c.conn.Call(state.MsgLogBatch, &state.LogBatchReq{
		Source:  source,
		Entries: entries,
	}, &resp); err != nil {
		return 0, err
	}
	if !resp.OK {
		return 0, &RespError{Msg: resp.Error}
	}
	return resp.Written, nil
}

// Close 关闭客户端连接。
func (c *Client) Close() error {
	return c.conn.Close()
}

// RespError log 服返回的业务错误。
type RespError struct {
	Msg string
}

func (e *RespError) Error() string {
	return "logsvc: " + e.Msg
}
