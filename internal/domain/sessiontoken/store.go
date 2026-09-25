package sessiontoken

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sync"
	"time"

	masterclient "github.com/qw576483/clover-server-engine/internal/domain/master/client"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/id"
)

// Store 管理 session token 的生命周期：
//   - 优先使用 master 的 session 客户端（远程存储）
//   - 本地 sync.Map 仅用于 master 不可用时的降级回退
//   - 本地 token 带 TTL 过期机制，防止 token 永久有效
type Store struct {
	sc              *masterclient.SessionClient // nil 表示只使用本地存储
	local           sync.Map                    // playerID → *localTokenEntry（本地回退 + 缓存）
	ttl             time.Duration               // 本地 token 有效期，默认 24 小时
	refreshInterval time.Duration               // 续期节流：距上次续期不足该值则跳过远程调用
}

// localTokenEntry 包装 token 和创建时间，用于 TTL 过期判断。
type localTokenEntry struct {
	Token     string
	CreatedAt time.Time
}

// DefaultTTL 本地 token 的默认有效期，与 master 侧 master_session_token.ttl 的默认值一致。
const DefaultTTL = 24 * time.Hour

// NewStore 创建 session store（本地回退 TTL 取 DefaultTTL）。
//
//	mc 为 nil 时仅使用本地存储（适用于单机/测试场景）。
func NewStore(mc *masterclient.Client) *Store {
	return NewStoreWithTTL(mc, DefaultTTL)
}

// NewStoreWithTTL 创建 session store，并显式指定**本地回退 TTL**。
//
// 为什么必须能配：本地 TTL 是 master 不可达时唯一的有效性判据。若它与 master 侧
// master_session_token.ttl 不一致，就会「接受 master 已过期的 token」或
// 「拒绝 master 仍有效的 token」——两边各写死一个 24h 只是碰巧相等，改任一边即失衡。
// 调用方应传 master 配置里 session token TTL 的实际值；ttl <= 0 回落 DefaultTTL。
func NewStoreWithTTL(mc *masterclient.Client, ttl time.Duration) *Store {
	var sc *masterclient.SessionClient
	if mc != nil {
		sc = masterclient.NewSessionClient(mc)
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	// 滑动续期节流取 TTL 的一半，保证活跃连接在过期前被续上。
	return &Store{sc: sc, ttl: ttl, refreshInterval: ttl / 2}
}

// New 生成新的 session token 并记录（远程 + 本地缓存）。
func (s *Store) New(ctx context.Context, playerID string) (string, error) {
	// 本地模式（sc==nil）不访问远程，直接生成本地 token。
	if s.sc == nil {
		token, err := newLocalToken()
		if err != nil {
			return "", err
		}
		s.local.Store(playerID, &localTokenEntry{Token: token, CreatedAt: time.Now()})
		return token, nil
	}
	token, err := s.sc.NewSessionToken(ctx, playerID)
	if err != nil {
		// 刻意不做本地降级（与 Validate 的本地回退不对称）：本地生成的 token
		// 在 master 恢复后无法通过远程校验，会表现为「能登录、随即被踢」的
		// 不稳定会话；新登录失败可由客户端显式重试，fail-fast 更可预期。
		// （若需支持抖动期开新会话，需 master 侧接受本地签发 token —— 协议变更。）
		return "", err
	}
	s.local.Store(playerID, &localTokenEntry{Token: token, CreatedAt: time.Now()})
	return token, nil
}

// Validate 验证 session token（远程优先，失败时回退到本地比对）。
// 本地比对时会检查 TTL，过期 token 视为无效。
func (s *Store) Validate(ctx context.Context, playerID, token string) (bool, error) {
	// 本地模式（sc==nil）直接本地比对。
	if s.sc == nil {
		return s.validateLocal(playerID, token), nil
	}
	valid, err := s.sc.ValidateSessionToken(ctx, playerID, token)
	if err == nil {
		return valid, nil
	}
	// 远程失败时回退到本地比对。本地无该 player 记录时返回 false——与「token 无效」
	// 不可区分（调用方可能因此误踢在线玩家），必须留日志区分这两种失败。
	logger.Warnf("sessiontoken: validate %s: master unavailable (%v), fallback to local cache", playerID, err)
	return s.validateLocal(playerID, token), nil
}

// validateLocal 本地比对 token 并检查 TTL 过期。
func (s *Store) validateLocal(playerID, token string) bool {
	v, ok := s.local.Load(playerID)
	if !ok {
		return false
	}
	entry, ok := v.(*localTokenEntry)
	if !ok {
		// 防御：map 中出现非预期类型时不能直接断言（panic）。
		logger.Warnf("sessiontoken: unexpected local entry type %T for %s", v, playerID)
		return false
	}
	if s.ttl > 0 && time.Since(entry.CreatedAt) > s.ttl {
		// token 已过期，删除。
		s.local.Delete(playerID)
		return false
	}
	// 常量时间比较：普通 == 会在首字节不同处短路，构成时序侧信道
	//（与 jwt 包签名比较的 hmac.Equal 口径一致）。
	return subtle.ConstantTimeCompare([]byte(entry.Token), []byte(token)) == 1
}

// Current 返回当前有效的 token（本地缓存优先）。
func (s *Store) Current(playerID string) (string, bool) {
	v, ok := s.local.Load(playerID)
	if !ok {
		return "", false
	}
	entry, ok := v.(*localTokenEntry)
	if !ok {
		logger.Warnf("sessiontoken: unexpected local entry type %T for %s", v, playerID)
		return "", false
	}
	if s.ttl > 0 && time.Since(entry.CreatedAt) > s.ttl {
		s.local.Delete(playerID)
		return "", false
	}
	return entry.Token, true
}

// Delete 删除指定 player 的 session token（远程 + 本地）。
// 远程删除失败时保留本地并返回：本地副本是 master 不可用时的降级回退，
// 若连带删除，master 抖动期间所有校验（含正常重连）都会被误判失败。
func (s *Store) Delete(ctx context.Context, playerID string) {
	if s.sc != nil {
		if err := s.sc.DeleteSessionToken(ctx, playerID); err != nil {
			logger.Errorf("sessiontoken: delete remote token for %s failed: %v (local entry kept)", playerID, err)
			return
		}
	}
	s.local.Delete(playerID)
}

// Refresh 续期 session token（sliding TTL）：按 refreshInterval 节流远程续期调用，
// 远端成功后滑动本地缓存 CreatedAt。
// 续期失败不影响当前连接有效性；错误仅返回给上层记录/监控。
func (s *Store) Refresh(ctx context.Context, playerID string) error {
	v, ok := s.local.Load(playerID)
	if !ok {
		return nil // 本地无缓存：无 token 可续，静默跳过
	}
	entry, ok := v.(*localTokenEntry)
	if !ok {
		logger.Warnf("sessiontoken: unexpected local entry type %T for %s", v, playerID)
		return nil
	}
	// 节流：距上次刷新（以 CreatedAt 近似）不足 refreshInterval 则跳过。
	if time.Since(entry.CreatedAt) < s.refreshInterval {
		return nil
	}
	// 先远程续期、成功后再滑动本地 TTL：先本地滑动时，远端续期失败会让本地 TTL
	// 已被延长，叠加「远程失败回退本地比对」语义，会让远端已失效的 token
	// 在本地继续被接受最长一个 ttl。
	if s.sc != nil {
		if err := s.sc.RefreshSessionToken(ctx, playerID, entry.Token); err != nil {
			return err
		}
	}
	// CAS 写入而非直接 Store：与并发 New/Refresh 交错时，直接 Store 会把
	// 已被替换的新 token 用旧 token 覆盖回本地（lost update，此后本地校验通过
	// 却是旧 token）。CAS 失败表示期间已有并发替换，放弃本次滑动即可。
	if !s.local.CompareAndSwap(playerID, v, &localTokenEntry{Token: entry.Token, CreatedAt: time.Now()}) {
		return nil
	}
	return nil
}

// newLocalToken 生成本地模式的随机 token（复用 pkg/shared/id 的统一实现）。
// 随机数失败时返回错误而不是退化为可预测的 local-<UnixNano>：
// 那会让 token 强度与可猜测性直接崩塌（宁可拒绝生成）。
func newLocalToken() (string, error) {
	s, err := id.RandomHex(16)
	if err != nil {
		return "", fmt.Errorf("sessiontoken: generate local token: %w", err)
	}
	return s, nil
}
