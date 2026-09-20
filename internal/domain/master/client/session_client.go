package client

import (
	"context"
	"fmt"

	"github.com/qw576483/clover-server-engine/internal/domain/master/state"
)

// SessionClient session token 远程客户端。
// 由调用方自己组装：sc := NewSessionClient(c)。
//
// 多 master 分片部署时无需额外装配：所有操作都经 Client.ForKey(playerID) 落到
// 该玩家的属主分片，因此 memory 后端在分片下同样正确（不强制 Redis）。
type SessionClient struct{ cli *Client }

// NewSessionClient 从原始 master TCP 客户端创建 session 客户端。
func NewSessionClient(c *Client) *SessionClient { return &SessionClient{cli: c} }

// call 把请求投递到 playerID 归属的分片（未启用分片路由时即自身连接）。
func (s *SessionClient) call(ctx context.Context, playerID string, msgID uint32, req, resp any) error {
	if s == nil || s.cli == nil {
		return fmt.Errorf("%w: master client not initialized", ErrClientUnavailable)
	}
	c := s.cli.ForKey(playerID)
	if c == nil {
		return fmt.Errorf("%w: master shard unavailable for player %s", ErrClientUnavailable, playerID)
	}
	return c.Call(ctx, msgID, req, resp)
}

func (s *SessionClient) NewSessionToken(ctx context.Context, playerID string) (string, error) {
	var resp state.SessionNewResp
	if err := s.call(ctx, playerID, state.MsgSessionNew, state.SessionNewReq{PlayerID: playerID}, &resp); err != nil {
		return "", err
	}
	if !resp.OK {
		return "", fmt.Errorf("master: session new failed: %s", resp.Error)
	}
	return resp.Token, nil
}

func (s *SessionClient) ValidateSessionToken(ctx context.Context, playerID, token string) (bool, error) {
	var resp state.SessionValidateResp
	if err := s.call(ctx, playerID, state.MsgSessionValidate, state.SessionValidateReq{PlayerID: playerID, Token: token}, &resp); err != nil {
		return false, err
	}
	if !resp.OK {
		return false, fmt.Errorf("master: session validate failed: %s", resp.Error)
	}
	return resp.Valid, nil
}

func (s *SessionClient) DeleteSessionToken(ctx context.Context, playerID string) error {
	var resp state.SessionDeleteResp
	if err := s.call(ctx, playerID, state.MsgSessionDelete, state.SessionDeleteReq{PlayerID: playerID}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: session delete failed: %s", resp.Error)
	}
	return nil
}

func (s *SessionClient) CurrentSessionToken(ctx context.Context, playerID string) (string, error) {
	var resp state.SessionCurrentResp
	if err := s.call(ctx, playerID, state.MsgSessionCurrent, state.SessionCurrentReq{PlayerID: playerID}, &resp); err != nil {
		return "", err
	}
	if !resp.OK {
		return "", fmt.Errorf("master: session current failed: %s", resp.Error)
	}
	return resp.Token, nil
}

// RefreshSessionToken 续期 session token（sliding TTL）。
func (s *SessionClient) RefreshSessionToken(ctx context.Context, playerID, token string) error {
	var resp state.SessionRefreshResp
	if err := s.call(ctx, playerID, state.MsgSessionRefresh, state.SessionRefreshReq{PlayerID: playerID, Token: token}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: session refresh failed: %s", resp.Error)
	}
	return nil
}
