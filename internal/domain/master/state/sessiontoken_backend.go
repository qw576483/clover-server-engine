package state

import (
	"context"
	"errors"
	"sync"
	"time"

	redis "github.com/qw576483/clover-server-engine/internal/domain/data/store/redis"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// errTokenBackendUnavailable redis 后端未配置（UseRedisSessionToken(nil, ...) 或配置错误）。
var errTokenBackendUnavailable = errors.New("master/state: redis session token backend not configured")

// sessionTokenEntry 内存后端中一条 token 记录（含创建时间，用于 TTL 过期判断）。
type sessionTokenEntry struct {
	Token     string
	CreatedAt time.Time
}

// sessionTokenBackend 抽象 session token 的存储后端，支持内存与 Redis 两种实现。
//
// 内存后端只在 master 进程内保存，master 重启后全部失效（玩家需重新登录）；
// Redis 后端由 Redis 自身 TTL 持久化，master 重启后仍有效。
type sessionTokenBackend interface {
	Set(ctx context.Context, playerID, token string) error
	// Get 返回 token 及是否存在；err 非 nil 表示存储后端故障（无法判定），
	// 与「key 不存在」严格区分，供上层降级（如回退本地比对）而非误判为 token 无效。
	Get(ctx context.Context, playerID string) (string, bool, error)
	// Refresh 续期（sliding TTL）：校验当前存储的 token 与给定 token 一致后，
	// 把有效期从当前时刻重新起算；不匹配/不存在/已过期则不动，避免误刷被替换的 token。
	Refresh(ctx context.Context, playerID, token string) error
	Delete(ctx context.Context, playerID string) error
}

// memory 后端
type memorySessionBackend struct {
	mu  sync.RWMutex
	m   map[string]sessionTokenEntry
	ttl time.Duration
}

func newMemorySessionBackend(ttl time.Duration) *memorySessionBackend {
	return &memorySessionBackend{m: make(map[string]sessionTokenEntry), ttl: ttl}
}

func (b *memorySessionBackend) Set(_ context.Context, playerID, token string) error {
	b.mu.Lock()
	b.m[playerID] = sessionTokenEntry{Token: token, CreatedAt: time.Now()}
	b.mu.Unlock()
	return nil
}

func (b *memorySessionBackend) Get(_ context.Context, playerID string) (string, bool, error) {
	b.mu.RLock()
	entry, ok := b.m[playerID]
	valid := ok && !b.expired(entry)
	b.mu.RUnlock()
	if !ok {
		return "", false, nil
	}
	if !valid {
		// 过期条目必须删除：只返回 false 不删会让过期 token 永久驻留 map，
		// 长跑进程内存随历史 playerID 只增不减（对齐 sessiontoken/store.go 的 Delete 行为）。
		b.mu.Lock()
		if cur, ok2 := b.m[playerID]; ok2 && b.expired(cur) { // 双检：可能已被并发 Set 刷新
			delete(b.m, playerID)
		}
		b.mu.Unlock()
		return "", false, nil
	}
	return entry.Token, true, nil
}

func (b *memorySessionBackend) Refresh(_ context.Context, playerID, token string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.m[playerID]
	if !ok || b.expired(entry) || entry.Token != token {
		// 不存在/已过期/token 不符：不续期。留日志（此前完全静默——
		// 「用已被替换的旧 token 续期」属非预期分支，需要可观测）。
		logger.Warnf("master/state: session token refresh skipped for %s (exists=%v expired=%v match=%v)",
			playerID, ok, ok && b.expired(entry), ok && entry.Token == token)
		return nil
	}
	entry.CreatedAt = time.Now() // 内存后端同步刷新 CreatedAt
	b.m[playerID] = entry
	return nil
}

func (b *memorySessionBackend) Delete(_ context.Context, playerID string) error {
	b.mu.Lock()
	delete(b.m, playerID)
	b.mu.Unlock()
	return nil
}

// expired 判断 entry 是否超过有效期（ttl<=0 表示永不过期）。
func (b *memorySessionBackend) expired(entry sessionTokenEntry) bool {
	return b.ttl > 0 && time.Since(entry.CreatedAt) > b.ttl
}

// redis 后端

type redisSessionBackend struct {
	cli    *redis.Client
	prefix string
	ttl    time.Duration
}

func (b *redisSessionBackend) Set(ctx context.Context, playerID, token string) error {
	if b == nil || b.cli == nil {
		// UseRedisSessionToken(nil, ...) / 配置错误时，此前首次调用即 nil 解引用 panic。
		return errTokenBackendUnavailable
	}
	ttl := b.ttl
	if ttl < 0 {
		// 负值语义 = 永不过期（与 memory 后端 ttl<=0 一致；配置文档称 -1 表示永不过期）。
		// 注意不能原样交给 go-redis：-1（nanosecond，即 KeepTTL 哨兵）会走 KEEPTTL
		// 沿用旧 key 的剩余 TTL，与「永不过期」承诺相反。
		ttl = 0
	}
	return b.cli.Set(ctx, b.prefix+playerID, token, ttl)
}

func (b *redisSessionBackend) Get(ctx context.Context, playerID string) (string, bool, error) {
	if b == nil || b.cli == nil {
		return "", false, errTokenBackendUnavailable
	}
	v, err := b.cli.Get(ctx, b.prefix+playerID)
	if err != nil {
		// key 不存在是「token 无效」而非后端故障；其余错误（连接失败/超时）视为后端故障。
		if errors.Is(err, redis.ErrNil) {
			return "", false, nil
		}
		return "", false, err
	}
	return v, true, nil
}

func (b *redisSessionBackend) Delete(ctx context.Context, playerID string) error {
	if b == nil || b.cli == nil {
		return errTokenBackendUnavailable
	}
	_, err := b.cli.Del(ctx, b.prefix+playerID)
	return err
}

// Refresh 校验 token 后刷新 TTL（sliding TTL）。
// 先 Get 校验当前值与传入 token 一致，再用 Expire 把 TTL 从当前时刻重新起算，
// 避免对已替换/已消失的 token 误续期。
func (b *redisSessionBackend) Refresh(ctx context.Context, playerID, token string) error {
	if b == nil || b.cli == nil {
		return errTokenBackendUnavailable
	}
	if b.ttl <= 0 {
		return nil // 永不过期：无需续期
	}
	key := b.prefix + playerID
	v, err := b.cli.Get(ctx, key)
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			logger.Warnf("master/state: session token refresh for %s: token not found (already expired/deleted)", playerID)
			return nil // key 不存在：无需续期
		}
		return err // 后端故障：交由上层降级，不误当成功
	}
	if v != token {
		// token 不一致：说明存储里已被替换（顶号/重新登录），续期请求基于旧 token。
		logger.Warnf("master/state: session token refresh for %s: token mismatch (stored token was replaced)", playerID)
		return nil
	}
	_, err = b.cli.Expire(ctx, key, b.ttl)
	return err
}
