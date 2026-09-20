package client

import (
	"context"
	"encoding/json"
	"fmt"

	"clover-server-engine/internal/domain/master/state"
	"clover-server-engine/pkg/domain/master"
)

// RankClient 排行榜远程客户端，通过 master TCP 连接访问排行榜服务。
// 由调用方自己组装：rc := NewRankClient(c)。
//
// 多 master 分片部署时无需额外装配：所有带 board 的操作都经 Client.ForKey(board)
// 落到该榜的属主分片（同一榜恒落同一分片）；BackupAll / RestoreAll 这类
// 「所有榜」的操作会广播到全部分片。
type RankClient struct{ cli *Client }

// NewRankClient 从原始 master TCP 客户端创建排行榜客户端。
func NewRankClient(c *Client) *RankClient { return &RankClient{cli: c} }

// 确保 RankClient 实现了 state.RankService 接口。
var _ state.RankService = (*RankClient)(nil)

// call 把请求投递到 board 归属的分片（未启用分片路由时即自身连接）。
func (r *RankClient) call(ctx context.Context, board string, msgID uint32, req, resp any) error {
	if r == nil || r.cli == nil {
		return fmt.Errorf("%w: master client not initialized", ErrClientUnavailable)
	}
	c := r.cli.ForKey(board)
	if c == nil {
		return fmt.Errorf("%w: master shard unavailable for board %s", ErrClientUnavailable, board)
	}
	return c.Call(ctx, msgID, req, resp)
}

// broadcast 把「所有榜」的操作分发到全部分片；未启用分片路由时只发自身连接。
// 返回首个错误（不中断其余分片：尽量让每个分片都完成备份/恢复）。
// 建连失败、被跳过的分片必须计入错误：否则分片宕机时「全部成功」实际漏做某分片
// 而无错误上报（静默部分成功）。
func (r *RankClient) broadcast(ctx context.Context, msgID uint32) error {
	if r == nil || r.cli == nil {
		return fmt.Errorf("%w: master client not initialized", ErrClientUnavailable)
	}
	r.cli.mu.RLock()
	shard := r.cli.shard
	r.cli.mu.RUnlock()

	var clients []*Client
	var firstErr error
	if shard != nil {
		var missing []string
		clients, missing = shard.BroadcastTargets()
		if len(missing) > 0 {
			firstErr = fmt.Errorf("%w: broadcast msgID=%d: shard(s) unreachable: %v", ErrClientUnavailable, msgID, missing)
		}
	} else {
		// 未启用分片路由：只有自身连接可发（单 master 部署）。
		clients = []*Client{r.cli}
	}
	for _, c := range clients {
		if c == nil {
			continue
		}
		var resp state.Resp
		if err := c.Call(ctx, msgID, nil, &resp); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !resp.OK && firstErr == nil {
			firstErr = fmt.Errorf("master: msgID=%d: %s", msgID, resp.Error)
		}
	}
	return firstErr
}

func (r *RankClient) Top(ctx context.Context, board string, n int) ([]master.RankMember, error) {
	var resp state.RankTopResp
	if err := r.call(ctx, board, state.MsgRankTop, state.RankTopReq{Board: board, N: n}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("master: rank top %s: %s", board, resp.Error)
	}
	return resp.Entries, nil
}

func (r *RankClient) Add(ctx context.Context, board, member string, score float64, extra json.RawMessage) error {
	var resp state.Resp
	if err := r.call(ctx, board, state.MsgRankAdd, state.RankAddReq{Board: board, Member: member, Score: score, Extra: extra}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: rank add %s: %s", board, resp.Error)
	}
	return nil
}

func (r *RankClient) AddOnlyUpdateScore(ctx context.Context, board, member string, score float64) error {
	var resp state.Resp
	if err := r.call(ctx, board, state.MsgRankAddHigher, state.RankAddHigherReq{Board: board, Member: member, Score: score}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: rank addhigher %s: %s", board, resp.Error)
	}
	return nil
}

func (r *RankClient) Incr(ctx context.Context, board, member string, delta float64) (float64, error) {
	var resp state.RankIncrResp
	if err := r.call(ctx, board, state.MsgRankIncr, state.RankIncrReq{Board: board, Member: member, Delta: delta}, &resp); err != nil {
		return 0, err
	}
	if !resp.OK {
		return 0, fmt.Errorf("master: rank incr %s: %s", board, resp.Error)
	}
	return resp.Score, nil
}

func (r *RankClient) IncrOnlyUpdateScore(ctx context.Context, board, member string, delta float64) (float64, error) {
	var resp state.RankIncrResp
	if err := r.call(ctx, board, state.MsgRankIncrHigher, state.RankIncrHigherReq{Board: board, Member: member, Delta: delta}, &resp); err != nil {
		return 0, err
	}
	if !resp.OK {
		return 0, fmt.Errorf("master: rank incrhigher %s: %s", board, resp.Error)
	}
	return resp.Score, nil
}

func (r *RankClient) GetMember(ctx context.Context, board, member string) (master.RankMember, bool, error) {
	var resp state.RankGetResp
	if err := r.call(ctx, board, state.MsgRankGet, state.RankGetReq{Board: board, Member: member}, &resp); err != nil {
		return master.RankMember{}, false, err
	}
	if !resp.OK {
		return master.RankMember{}, false, fmt.Errorf("master: rank get %s: %s", board, resp.Error)
	}
	return resp.Entry, resp.Found, nil
}

func (r *RankClient) GetRank(ctx context.Context, board, member string) (int, bool, error) {
	var resp state.RankGetRankResp
	if err := r.call(ctx, board, state.MsgRankGetRank, state.RankGetRankReq{Board: board, Member: member}, &resp); err != nil {
		return 0, false, err
	}
	if !resp.OK {
		return 0, false, fmt.Errorf("master: rank getrank %s: %s", board, resp.Error)
	}
	return resp.Rank, resp.Found, nil
}

func (r *RankClient) GetByRankRange(ctx context.Context, board string, start, stop int) ([]master.RankMember, error) {
	var resp state.RankByRankResp
	if err := r.call(ctx, board, state.MsgRankByRankRange, state.RankByRankReq{Board: board, Start: start, Stop: stop}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("master: rank byrank %s: %s", board, resp.Error)
	}
	return resp.Entries, nil
}

func (r *RankClient) GetByScoreRange(ctx context.Context, board string, min, max float64) ([]master.RankMember, error) {
	var resp state.RankByScoreResp
	if err := r.call(ctx, board, state.MsgRankByScoreRange, state.RankByScoreReq{Board: board, Min: min, Max: max}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("master: rank byscore %s: %s", board, resp.Error)
	}
	return resp.Entries, nil
}

func (r *RankClient) Remove(ctx context.Context, board, member string) error {
	var resp state.Resp
	if err := r.call(ctx, board, state.MsgRankRemove, state.RankRemoveReq{Board: board, Member: member}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: rank remove %s: %s", board, resp.Error)
	}
	return nil
}

func (r *RankClient) Clear(ctx context.Context, board string, deleteBackup bool) error {
	var resp state.Resp
	if err := r.call(ctx, board, state.MsgRankClear, state.RankClearReq{Board: board, DeleteBackup: deleteBackup}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: rank clear %s: %s", board, resp.Error)
	}
	return nil
}

func (r *RankClient) Len(ctx context.Context, board string) (int, error) {
	var resp state.RankLenResp
	if err := r.call(ctx, board, state.MsgRankLen, state.RankLenReq{Board: board}, &resp); err != nil {
		return 0, err
	}
	if !resp.OK {
		return 0, fmt.Errorf("master: rank len %s: %s", board, resp.Error)
	}
	return resp.Total, nil
}

// BackupAll 备份所有排行榜：分片部署下需覆盖每个分片（各分片只持有自己的榜）。
func (r *RankClient) BackupAll(ctx context.Context) error {
	return r.broadcast(ctx, state.MsgRankBackupAll)
}

func (r *RankClient) Backup(ctx context.Context, board string) error {
	var resp state.Resp
	if err := r.call(ctx, board, state.MsgRankBackup, state.RankBackupReq{Board: board}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: rank backup %s: %s", board, resp.Error)
	}
	return nil
}

// RestoreAll 从 Redis 恢复所有排行榜：与 BackupAll 对称，覆盖每个分片。
func (r *RankClient) RestoreAll(ctx context.Context) error {
	return r.broadcast(ctx, state.MsgRankRestoreAll)
}

func (r *RankClient) Restore(ctx context.Context, board string) error {
	var resp state.Resp
	if err := r.call(ctx, board, state.MsgRankRestore, state.RankRestoreReq{Board: board}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: rank restore %s: %s", board, resp.Error)
	}
	return nil
}

func (r *RankClient) SetRankThresholds(ctx context.Context, board string, ts master.Thresholds) error {
	var resp state.Resp
	if err := r.call(ctx, board, state.MsgRankSetThresholds, state.RankSetThresholdsReq{Board: board, Thresholds: ts}, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("master: rank set_thresholds %s: %s", board, resp.Error)
	}
	return nil
}
