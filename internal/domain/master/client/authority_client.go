package client

import (
	"context"
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
)

// AuthorityClient 节点权威远程客户端。
// 由调用方自己组装：ac := NewAuthorityClient(c)。
// 实现了 state.Authority 接口。
type AuthorityClient struct{ cli *Client }

// NewAuthorityClient 从原始 master TCP 客户端创建 authority 客户端。
func NewAuthorityClient(c *Client) *AuthorityClient { return &AuthorityClient{cli: c} }

// 确保实现 state.Authority 接口。
var _ state.Authority = (*AuthorityClient)(nil)

func (a *AuthorityClient) RegisterNode(ctx context.Context, node state.Node) error {
	var resp state.Resp
	if err := a.cli.Call(ctx, state.MsgRegisterNode, state.RegisterReq{Node: node}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: register node %s: %s", node.ID, resp.Error)
	}
	return nil
}

func (a *AuthorityClient) RemoveNode(ctx context.Context, nodeID string) error {
	var resp state.Resp
	if err := a.cli.Call(ctx, state.MsgRemoveNode, state.RemoveReq{NodeID: nodeID}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: remove node %s: %s", nodeID, resp.Error)
	}
	return nil
}

// NodesByType 查询指定类型下当前存活的节点 ID 列表（扩展方法，非 Authority 接口）。
func (a *AuthorityClient) NodesByType(ctx context.Context, typ string) ([]string, error) {
	var resp state.NodesByTypeResp
	if err := a.cli.Call(ctx, state.MsgNodesByType, state.NodesByTypeReq{Type: typ}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("master: nodes by type %s: %s", typ, resp.Error)
	}
	return resp.NodeIDs, nil
}

// NodesByTag 查询包含指定 tag 的存活节点 ID 列表。
func (a *AuthorityClient) NodesByTag(ctx context.Context, tag string) ([]string, error) {
	var resp state.NodesByTagResp
	if err := a.cli.Call(ctx, state.MsgNodesByTag, state.NodesByTagReq{Tag: tag}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("master: nodes by tag %s: %s", tag, resp.Error)
	}
	return resp.NodeIDs, nil
}
