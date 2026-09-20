// Package logger 提供日志采样器扩展。

// Logger 采样器基于 zap 的 Sampling 设计，支持按概率/每秒条数进行日志采样，
// 用于高频路径降低日志 IO 压力。

// 使用示例：

//	sampler := logger.NewSampler(logger.SamplerConfig{
//	    Initial:    100,
//	    Thereafter: 100,
//	    Tick:       time.Second,
//	})
//
// sampledLogger := sampler.Wrap(baseLogger)
package logger

import (
	"sync"
	"sync/atomic"
	"time"
)

// SamplerConfig 采样器配置。
type SamplerConfig struct {
	// Initial 每秒前 N 条不采样（保持完整记录）。
	Initial int
	// Thereafter 之后每 N 条采样一条。
	Thereafter int
	// Tick 采样窗口大小（默认 1 秒）。
	Tick time.Duration
}

// normalize 为零值字段补默认值：仅当显式未设置（零值）时才回退，
// 负值视为合法并原样保留。

// 此处不依赖 shared/config：那组 DefXxx 是引擎配置装配用的辅助函数，
// 采样器只需三次零值判断，直接内联可避免让 pkg 依赖引擎装配层。
func (c *SamplerConfig) normalize() {
	if c.Initial == 0 {
		c.Initial = 100
	}
	if c.Thereafter == 0 {
		c.Thereafter = 100
	}
	if c.Tick == 0 {
		c.Tick = time.Second
	}
}

// Sampler 日志采样器。
type Sampler struct {
	cfg       SamplerConfig
	count     atomic.Int64
	tickStart atomic.Int64 // unix nano
	mu        sync.Mutex
}

// NewSampler 创建采样器。
func NewSampler(cfg SamplerConfig) *Sampler {
	cfg.normalize()
	s := &Sampler{cfg: cfg}
	s.tickStart.Store(time.Now().UnixNano())
	return s
}

// Allow 判断当前这条日志是否应该输出。
// 每秒前 Initial 条一定输出；之后每隔 Thereafter 条输出一条。
func (s *Sampler) Allow() bool {
	now := time.Now().UnixNano()
	tickStart := s.tickStart.Load()

	// 检查是否跨过 tick 窗口
	if now-tickStart >= int64(s.cfg.Tick) {
		s.mu.Lock()
		// 双重检查
		if now-s.tickStart.Load() >= int64(s.cfg.Tick) {
			s.count.Store(0)
			s.tickStart.Store(now)
		}
		s.mu.Unlock()
	}

	n := s.count.Add(1)
	if n <= int64(s.cfg.Initial) {
		return true
	}
	// Thereafter 采样间隔
	return (n-int64(s.cfg.Initial))%int64(s.cfg.Thereafter) == 0
}

// LevelSampler 按日志级别的采样器。
type LevelSampler struct {
	samplers map[string]*Sampler
}

// NewLevelSampler 按级别创建采样器。
// levels: 需要采样的级别名（如 "info", "warn"）。
func NewLevelSampler(levels []string, cfg SamplerConfig) *LevelSampler {
	cfg.normalize()
	ls := &LevelSampler{samplers: make(map[string]*Sampler)}
	for _, lv := range levels {
		ls.samplers[lv] = NewSampler(cfg)
	}
	return ls
}

// Allow 对指定级别采样判断。
func (ls *LevelSampler) Allow(level string) bool {
	s, ok := ls.samplers[level]
	if !ok {
		return true // 未配置的级别不采样
	}
	return s.Allow()
}
