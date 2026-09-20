package client

import (
	"context"
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
)

// PlayerClient 玩家定位远程客户端。
// 由调用方自己组装：pc := NewPlayerClient(c)。
//
// 多 master 分片部署时无需额外装配：Client 自身按 key 路由（见 Client.ForKey），
// 定位的三个操作都按 uid 落到属主分片。
type PlayerClient struct{ cli *Client }

// NewPlayerClient 从原始 master TCP 客户端创建玩家定位客户端。
func NewPlayerClient(c *Client) *PlayerClient { return &PlayerClient{cli: c} }

// call 把请求投递到 uid 归属的分片（未启用分片路由时即自身连接）。
func (p *PlayerClient) call(ctx context.Context, uid string, msgID uint32, req, resp any) error {
	if p == nil || p.cli == nil {
		return fmt.Errorf("%w: master client not initialized", ErrClientUnavailable)
	}
	c := p.cli.ForKey(uid)
	if c == nil {
		return fmt.Errorf("%w: master shard unavailable for uid %s", ErrClientUnavailable, uid)
	}
	return c.Call(ctx, msgID, req, resp)
}

// PlayerNode 查询玩家所在节点。
func (p *PlayerClient) PlayerNode(ctx context.Context, uid string) (string, error) {
	var resp state.PlayerLookupResp
	if err := p.call(ctx, uid, state.MsgPlayerLookup, state.PlayerLookupReq{UID: uid}, &resp); err != nil {
		return "", err
	}
	if !resp.Found {
		return "", fmt.Errorf("%w: %s", ErrPlayerNotFound, uid)
	}
	return resp.NodeID, nil
}

// RegisterPlayer 注册玩家→节点映射。
func (p *PlayerClient) RegisterPlayer(ctx context.Context, uid, nodeID string) error {
	var resp state.Resp
	if err := p.call(ctx, uid, state.MsgPlayerRegister, state.PlayerRegisterReq{UID: uid, NodeID: nodeID}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: register player %s: %s", uid, resp.Error)
	}
	return nil
}

// RemovePlayer 移除玩家定位。nodeID 为空时无条件删除；非空时仅当 uid 映射与 nodeID 一致时才删除。
func (p *PlayerClient) RemovePlayer(ctx context.Context, uid, nodeID string) error {
	var resp state.Resp
	if err := p.call(ctx, uid, state.MsgPlayerRemove, state.PlayerRemoveReq{UID: uid, NodeID: nodeID}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: remove player %s: %s", uid, resp.Error)
	}
	return nil
}
