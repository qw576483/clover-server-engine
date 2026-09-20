// Package config 提供引擎内统一的配置默认值辅助函数。
package config

import (
	"strings"
	"time"
)

// DefDuration 若 v == 0 返回 d，否则返回 v。
// 注意：仅当显式未设置（零值）时才回退默认值；负值（如 -1）视为合法，原样返回。
func DefDuration(v, d time.Duration) time.Duration {
	if v == 0 {
		return d
	}
	return v
}

// DefInt 若 v == 0 返回 d，否则返回 v。
// 注意：仅当显式未设置（零值）时才回退默认值；负值（如 -1）视为合法，原样返回。
func DefInt(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}

// DefString 若 v == "" 或纯空白返回 d，否则返回 TrimSpace(v)。
func DefString(v, d string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return d
	}
	return v
}
