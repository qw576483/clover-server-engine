// Package retry 提供通用的失败重试与指数退避能力。
//
// 纯标准库实现、线程安全、零外部依赖。
//
// 典型用法：
//
//	err := retry.Do(ctx, retry.DefaultPolicy(), func(attempt int) error {
//	    return publish()
//	})
//
// 不可重试的错误用 retry.Permanent(err) 包装，Do 会立即返回不再重试：
//
//	return retry.Permanent(fmt.Errorf("参数非法: %w", err))
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"clover-server-engine/pkg/shared/rand"
)

// Policy 重试策略。
//
// 退避序列（不含抖动）为 BaseDelay * Multiplier^(attempt-1)，并被 MaxDelay 截断。
// Jitter 为抖动比例（0~1），实际延迟在 [d*(1-Jitter), d*(1+Jitter)] 区间内随机取值，
// 用于打散大量并发重试造成的惊群（thundering herd）。
type Policy struct {
	// MaxAttempts 最大尝试次数（含首次）。<=0 视为 1（只尝试一次，不重试）。
	MaxAttempts int
	// BaseDelay 首次重试前的基础延迟。<=0 时取 100ms。
	BaseDelay time.Duration
	// MaxDelay 单次退避延迟上限。<=0 表示不设上限。
	MaxDelay time.Duration
	// Multiplier 退避倍率。<=1 时退化为固定间隔（取 1）。
	Multiplier float64
	// Jitter 抖动比例，取值 [0,1]。0 表示不抖动。
	Jitter float64
}

// 默认策略参数：最多 5 次、基础 100ms、上限 10s、倍率 2、抖动 0.2。
const (
	defaultMaxAttempts = 5
	defaultBaseDelay   = 100 * time.Millisecond
	defaultMaxDelay    = 10 * time.Second
	defaultMultiplier  = 2.0
	defaultJitter      = 0.2
)

// DefaultPolicy 返回引擎默认重试策略：最多 5 次、基础 100ms、上限 10s、倍率 2、抖动 0.2。
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: defaultMaxAttempts,
		BaseDelay:   defaultBaseDelay,
		MaxDelay:    defaultMaxDelay,
		Multiplier:  defaultMultiplier,
		Jitter:      defaultJitter,
	}
}

// normalize 归一化非法字段，使零值 Policy 也能安全使用。
func (p Policy) normalize() Policy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = defaultBaseDelay
	}
	if p.Multiplier < 1 {
		p.Multiplier = 1
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter > 1 {
		p.Jitter = 1
	}
	return p
}

// permanentError 包装「不可重试」的错误。
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent 将 err 标记为不可重试：Do 遇到该错误立即终止并原样返回它。
// 返回的错误保留 Permanent 标记（Unwrap 到原始 err），因此外层再套一层 Do
// 时仍会被识别为不可重试，而 errors.Is/As 依旧可以穿透到原始错误。
// err 为 nil 时返回 nil。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent 判断 err 是否被标记为不可重试。
func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}

// Retryable 判断 err 是否可重试：nil 与 Permanent 包装的错误不可重试，其余均可重试。
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	return !IsPermanent(err)
}

// retryRand 包级加密安全随机源，用于抖动计算，天然 goroutine 无锁。
var retryRand = rand.NewSource()

// jitterFactor 返回 [1-jitter, 1+jitter] 区间内的随机系数。
func jitterFactor(jitter float64) float64 {
	if jitter <= 0 {
		return 1
	}
	return 1 + jitter*(2*retryRand.Float64()-1)
}

// NextDelay 返回第 attempt 次尝试失败后、下一次重试前应等待的时长。
//
// attempt 从 1 开始计数（1 表示首次尝试刚失败）。attempt <= 0 时按 1 处理。
// 结果已应用 Jitter 抖动、并统一收敛到 MaxDelay 上限（MaxDelay<=0 时收敛到
// MaxInt64 而非溢出），保证非负且不超过承诺的单次退避上限。
func NextDelay(p Policy, attempt int) time.Duration {
	p = p.normalize()
	if attempt < 1 {
		attempt = 1
	}
	// 计算无抖动的指数退避基值 BaseDelay * Multiplier^(attempt-1)。
	//
	// 注意：attempt 很大时 math.Pow 会返回 +Inf，而 time.Duration(+Inf) 的
	// 转换结果未定义（实测为 0），会绕过 MaxDelay 截断。因此这里全程在
	// float64 域内比较并优先按 MaxDelay 收敛，绝不把 Inf/溢出值交给 Duration。
	var delay time.Duration
	// 不设上限（MaxDelay<=0）时以「Duration 可表示的最大值」为界：
	// 有限但巨大的 d（如 2^63 量级）转 Duration 同样溢出，必须先收敛再转换。
	maxF := float64(math.MaxInt64)
	if p.MaxDelay > 0 {
		maxF = float64(p.MaxDelay)
	}
	exp := math.Pow(p.Multiplier, float64(attempt-1))
	d := float64(p.BaseDelay) * exp
	switch {
	case math.IsNaN(d):
		// 理论不可达，保底取 BaseDelay。
		delay = p.BaseDelay
	case math.IsInf(d, 0) || d >= maxF:
		// 溢出或超过上限：直接收敛到上限。
		if p.MaxDelay > 0 {
			delay = p.MaxDelay
		} else {
			delay = time.Duration(math.MaxInt64)
		}
	default:
		delay = time.Duration(d)
	}

	// 抖动在 float64 域内施加、之后再统一收敛——两个都踩过的坑：
	//  1. 若先转 Duration 再乘抖动系数，则在「不设上限」（MaxDelay<=0，
	//     delay=MaxInt64）时乘积 > MaxInt64，float64→Duration 溢出为负、
	//     被修正成 0 → 退避间隔归零形成忙等；
	//  2. 若抖动施加在 MaxDelay 截断之后，实际等待最高可达
	//     MaxDelay×(1+Jitter)，突破「MaxDelay 为单次退避上限」的承诺。
	// 先乘抖动再收敛到上限，两个问题一起消掉。
	v := float64(delay)
	if p.Jitter > 0 {
		v *= jitterFactor(p.Jitter)
	}
	if v < 0 {
		v = 0
	}
	if p.MaxDelay > 0 && v > float64(p.MaxDelay) {
		return p.MaxDelay
	}
	// 无上限时收敛到 MaxInt64：注意 float64(MaxInt64) 恰为 2^63（大于 MaxInt64），
	// 直接把 v==2^63 转回 Duration 仍是溢出，必须显式返回 MaxInt64。
	if v >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(v)
}

// Backoff 是「按连续失败次数取下一次等待时长、成功即重置」的状态机。
//
// 与 Do 的分工：
//   - Do 用于**有次数上限**的一次性重试（耗尽返回错误）；
//   - Backoff 用于**不设次数上限**的常驻循环——accept / read / watch / 重连这类
//     循环不能退出，只需要「失败时按同一套曲线退避、成功后立刻恢复灵敏」。
//
// 曲线与 Do 完全一致（同一个 Policy + NextDelay），因此全仓库只需维护一条退避策略。
// 此前各处手写 `backoff *= 2; if backoff > max {...}`，上限（1s / 30s）与是否带抖动
// 已经各不相同。
//
// 并发不安全：每个循环持有自己的 Backoff。
type Backoff struct {
	policy  Policy
	attempt int
}

// NewBackoff 创建退避状态机。policy 的非法字段按 Policy.normalize 归一化，
// 因此零值 Policy 也能安全使用（BaseDelay 落到 100ms）。
func NewBackoff(policy Policy) *Backoff { return &Backoff{policy: policy.normalize()} }

// Next 返回本次失败后应等待的时长，并把失败计数 +1。
// 首次调用返回 BaseDelay；之后按倍率增长并被 MaxDelay 截断（含抖动）。
func (b *Backoff) Next() time.Duration {
	b.attempt++
	return NextDelay(b.policy, b.attempt)
}

// Reset 在一次成功后调用：清空失败计数，下次 Next 重新从 BaseDelay 开始。
func (b *Backoff) Reset() { b.attempt = 0 }

// Failures 返回当前连续失败次数（便于日志节流与排障）。
func (b *Backoff) Failures() int { return b.attempt }

// Do 按 policy 执行 fn 直到成功、遇到不可重试错误、耗尽次数或 ctx 取消。
//
//   - fn 接收当前尝试序号（从 1 开始），返回 nil 表示成功。
//   - fn 返回 Permanent 包装的错误时立即终止，原样返回该错误（保留标记，
//     errors.Is/As 可穿透到原始错误）。
//   - 次数耗尽时返回最后一次的错误。
//   - 等待退避期间 ctx 被取消，返回 ctx.Err() 与最后错误的组合。
//
// ctx 为 nil 时按 context.Background() 处理。
func Do(ctx context.Context, policy Policy, fn func(attempt int) error) error {
	if fn == nil {
		return errors.New("retry: fn is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p := policy.normalize()

	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		// 每轮开始前先检查取消，避免 ctx 已取消仍白跑一次 fn。
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("retry: canceled after %d attempts: %w (last error: %v)", attempt-1, err, lastErr)
			}
			return fmt.Errorf("retry: canceled: %w", err)
		}

		err := fn(attempt)
		if err == nil {
			return nil
		}
		// 不可重试：立即返回，但**保留 Permanent 包装**。
		// 以前这里 unwrap 掉标记，导致该 err 再进入外层 Do（嵌套重试 / 上层重试器）时
		// 被重新视为可重试，永久性错误被反复重试。
		// 包装实现了 Unwrap()，调用方的 errors.Is/As 仍可穿透到原始错误。
		if IsPermanent(err) {
			return err
		}
		lastErr = err

		// 已是最后一次尝试，不再等待，直接跳出返回最后错误。
		if attempt == p.MaxAttempts {
			break
		}

		delay := NextDelay(p, attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("retry: canceled after %d attempts: %w (last error: %v)", attempt, ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
	return fmt.Errorf("retry: exhausted after %d attempts: %w", p.MaxAttempts, lastErr)
}
