package etcd

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/qw576483/clover-server-engine/internal/shared/retry"
	ilog "github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// ErrWatchExists 表示同一个 key / prefix 上已有活跃监听。
//
// 一个 key（或 prefix）只维持一个 watch 通道，重复注册不会叠加回调；
// 调用方需据此判断：要么复用已有监听，要么先取消旧的再注册。
var ErrWatchExists = errors.New("etcd: watch already registered")

// WatchEvent 是一次 KV 变更（WatchPrefixLoop 交付的载荷）。
type WatchEvent struct {
	// Key 变更的键：带调用方传入的前缀原样交付，**不做剥离**（是否剥离由调用方决定，
	// 避免 Backend 与 Store 双方各剥一次把逻辑键截错）。
	Key string
	// Value 变更后的值；删除事件为空。
	Value string
	// IsDelete 为 true 表示删除事件。
	IsDelete bool
}

// Watch 监听单个 key 变化，触发回调；连接断开自动重连，ctx 取消即停止。
// 单 key watch 与 prefix watch 生命周期统一——同样登记 cancel 到 watches 表，
// 使 Client.Close 能主动取消，避免仅依赖调用方 ctx 造成泄漏。
//
// 同一 key 已有活跃监听时返回 ErrWatchExists：一个新的回调不会被已存在的
// watch 通道携带，若此时静默返回成功，调用方会误以为回调已挂上。
// 监听退出（ctx 取消 / Client.Close）后该 key 自动释放，可再次注册。
func (c *Client) Watch(ctx context.Context, key string, cb func()) error {
	if cb == nil {
		return errors.New("etcd: watch callback cannot be nil")
	}
	// 单 key 与 prefix 可能同名，用前缀区分命名空间避免互相覆盖。
	// cb 同时作为「压缩重置」通知：cb 的语义本就是「有变化，请重读」，
	// 而压缩重置恰恰意味着「有变化被漏掉了」，必须让它再读一次。
	return c.registerWatch(ctx, "key:"+key, func(ctx context.Context) {
		c.watchLoop(ctx, key, false, func([]WatchEvent) { cb() }, cb)
	})
}

// WatchPrefix 监听某前缀下所有 key 的变化，触发回调（常用于服务发现）。
// 相同前缀复用同一 watch，避免重复注册造成资源浪费；
// 已有活跃监听时返回 ErrWatchExists，由调用方决定复用还是先取消再注册。
func (c *Client) WatchPrefix(ctx context.Context, prefix string, cb func()) error {
	if cb == nil {
		return errors.New("etcd: watch callback cannot be nil")
	}
	// 与 Watch 同理：压缩重置后补一次 cb，否则服务发现列表会长期停留在过期快照上。
	return c.registerWatch(ctx, prefix, func(ctx context.Context) {
		c.watchLoop(ctx, prefix, true, func([]WatchEvent) { cb() }, cb)
	})
}

// WatchPrefixLoop **阻塞**运行前缀 watch 的重连循环（直到 ctx 取消），
// 把每个事件批次交给 onEvents。与 WatchPrefix 的区别有两点：
//
//   - 交付载荷：WatchPrefix 只通知「有变化」，本方法交付 key / value / 是否删除，
//     供需要维护本地镜像的调用方使用（如 globalstore 的全局 KV 后端）；
//   - goroutine 归属：本方法在**调用方**的 goroutine 里跑，不登记到 Client 的 watches 表，
//     调用方自行决定生命周期（例如用自己的 WaitGroup 让 Close 等到 watch 退出）。
//
// 两者共用同一个 watchLoop 骨架，因此续看 / 压缩重置 / 退避 / 回调 panic 回收的行为完全一致。
func (c *Client) WatchPrefixLoop(ctx context.Context, prefix string, onEvents func([]WatchEvent)) {
	c.WatchPrefixLoopResync(ctx, prefix, onEvents, nil)
}

// WatchPrefixLoopResync 与 WatchPrefixLoop 等价，但额外接收「压缩重置」通知 onReset。
//
// etcd 压缩后 lastRev 只能归零、改从最新 revision 续看：被压掉的那段事件**永久取不回来**
// （协议上就没有回放），维护本地镜像的调用方唯一正确的补救是收到 onReset 后做一次全量重同步
// （重新 List 本前缀并覆盖镜像），否则镜像会长期缺键、且外层看不出任何异常。
// onReset 为 nil 时行为与 WatchPrefixLoop 完全一致。
func (c *Client) WatchPrefixLoopResync(ctx context.Context, prefix string, onEvents func([]WatchEvent), onReset func()) {
	if onEvents == nil {
		return
	}
	c.watchLoop(ctx, prefix, true, onEvents, onReset)
}

// registerWatch 登记一个由 Client 管理的 watch（同 key/prefix 去重、Close 时可取消），
// 并在后台运行 run。监听退出（ctx 取消 / Client.Close）后自动从表中释放，可再次注册。
func (c *Client) registerWatch(ctx context.Context, regKey string, run func(context.Context)) error {
	c.watchesMu.Lock()
	if _, exists := c.watches[regKey]; exists {
		c.watchesMu.Unlock()
		return fmt.Errorf("%w: %q", ErrWatchExists, regKey)
	}
	// 已关闭的客户端拒绝登记：Close 的 cancel-all 在 watchesMu 内执行，
	// 若放行，登记会发生在 cancel-all 之后 → 该 watch 永不被取消、也不进 wg.Wait。
	select {
	case <-c.closed:
		c.watchesMu.Unlock()
		return errors.New("etcd: client closed")
	default:
	}
	childCtx, cancel := context.WithCancel(ctx)
	c.watches[regKey] = cancel
	// wg.Add 与登记放在同一临界区：否则 Close 可在「登记完成、goroutine 未启动」之间
	// 完成 cancel + wg.Wait（计数仍为 0 立即返回），watch goroutine 在 Close 返回后才
	// 启动、永不被等待（WaitGroup 的 Add/Wait 竞态）。
	c.wg.Add(1)
	c.watchesMu.Unlock()

	go func() {
		defer c.wg.Done()
		defer func() {
			c.watchesMu.Lock()
			delete(c.watches, regKey)
			c.watchesMu.Unlock()
			cancel()
		}()
		run(childCtx)
	}()
	return nil
}

// watchLoop 是**唯一**的 watch 重连骨架，Watch / WatchPrefix / WatchPrefixLoop / WatchPrefixLoopResync
// 全部跑在它上面。
//
//	lastRev 续看    ：重连时从 lastRev+1 续看，否则 etcd 默认从「当前最新」开始，
//	                  断线期间发生的事件会永久丢失。
//	压缩重置        ：事件已压缩（Revision 不可恢复）时把 lastRev 归零，避免带着
//	                  过期 revision 无限重连失败；同时调用 onReset —— 那段事件已经取不回来，
//	                  只有调用方自己全量重同步才能补上（nil 表示调用方不需要该通知）。
//	指数退避        ：2s 起步逐次翻倍、上限 30s；收到有效响应即重置，避免一次长断线后
//	                  后续每次闪断都从上限起步。曲线由 retry 包统一定义。
//	回调 panic 回收 ：同步调用回调但加 recover，否则回调 panic 会击垮 watch 协程（及进程）。
func (c *Client) watchLoop(ctx context.Context, key string, prefix bool, onEvents func([]WatchEvent), onReset func()) {
	// 退避曲线统一由 retry.Backoff 提供：同一套 Policy 供 Do 与全仓库的常驻循环复用。
	// 抖动用于打散多实例同时重连造成的惊群。
	bo := retry.NewBackoff(retry.Policy{
		BaseDelay:  watchRetryInterval,
		MaxDelay:   30 * time.Second,
		Multiplier: 2,
		Jitter:     0.2,
	})
	var lastRev int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		opts := []clientv3.OpOption{}
		if prefix {
			opts = append(opts, clientv3.WithPrefix())
		}
		if lastRev > 0 {
			opts = append(opts, clientv3.WithRev(lastRev+1))
		}
		wch := c.cli.Watch(ctx, key, opts...)
		ilog.LogInfo("etcd watch started", zap.String("key", key), zap.Bool("prefix", prefix), zap.Int64("from_rev", lastRev+1))

		for resp := range wch {
			if werr := resp.Err(); werr != nil {
				// 压缩判定优先用协议字段 CompactRevision，仅在字段缺失时退回错误文案匹配。
				if resp.CompactRevision != 0 || strings.Contains(werr.Error(), "compacted") {
					ilog.LogWarn("etcd watch compacted, resetting revision",
						zap.String("key", key), zap.Int64("compact_rev", resp.CompactRevision), zap.String("err", werr.Error()))
					lastRev = 0
					// 通知调用方去全量重同步：这里能做的只有「从最新续看」，
					// 被压掉的事件段协议上不存在回放路径。
					if onReset != nil {
						func() {
							defer func() {
								if r := recover(); r != nil {
									// 带堆栈：无堆栈的 panic 日志无法定位到具体回调行号。
									ilog.LogError("etcd watch reset callback panic",
										zap.String("key", key), zap.Any("panic", r),
										zap.String("stack", string(debug.Stack())))
								}
							}()
							onReset()
						}()
					}
					break
				}
				ilog.LogWarn("etcd watch error, will retry",
					zap.String("key", key), zap.String("err", werr.Error()))
				break // 跳出内层循环，外层重新建立 watch
			}
			// 收到有效响应说明连接已恢复，退避重置，避免一次长断线后
			// 后续每次闪断都从上限起步。
			bo.Reset()
			if resp.Header.Revision > lastRev {
				lastRev = resp.Header.Revision
			}
			if len(resp.Events) == 0 {
				continue
			}
			evs := make([]WatchEvent, 0, len(resp.Events))
			for _, ev := range resp.Events {
				evs = append(evs, WatchEvent{
					Key:      string(ev.Kv.Key),
					Value:    string(ev.Kv.Value),
					IsDelete: ev.Type == clientv3.EventTypeDelete,
				})
			}
			// 同步调用回调，但需 recover：否则回调 panic 会击垮 watch 协程（及进程）。
			func() {
				defer func() {
					if r := recover(); r != nil {
						// 带堆栈：无堆栈的 panic 日志无法定位到具体回调行号。
						ilog.LogError("etcd watch callback panic",
							zap.String("key", key), zap.Any("panic", r),
							zap.String("stack", string(debug.Stack())))
					}
				}()
				onEvents(evs)
			}()
		}

		// wch 关闭：连接断开或 ctx 取消
		if ctx.Err() != nil {
			return
		}
		delay := bo.Next()
		ilog.LogWarn("etcd watch channel closed, retry",
			zap.String("key", key), zap.Duration("interval", delay), zap.Int("failures", bo.Failures()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
