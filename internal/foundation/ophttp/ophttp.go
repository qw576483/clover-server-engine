// Package ophttp 提供运维 / 控制面 HTTP 的最小响应工具。
//
// 抽出来的原因：JSON 回包（Content-Type + 状态码 + 编码）散在账号服、admin 控制面、
// 跨服死信端点、日志级别端点各写一份会漂移——有的设 Content-Type、有的没设；
// 有的记录编码错误、有的静默忽略；错误体字段也不统一。
//
// 约定：一律 `application/json; charset=utf-8` + 显式状态码；编码失败只记 warn，
// 因为响应头已发出、无从补救。
//
// 注意：本包不供 pkg/foundation/logger 使用（logger 的级别端点自带 3 行写手）——
// 那会形成 ophttp → logger → ophttp 的循环依赖；logger 侧已单独补齐 Content-Type。
package ophttp

import (
	"encoding/json"
	"net/http"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// JSON 以 JSON 回包：设置 Content-Type，写入状态码，编码 v。
func JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Warnf("ophttp: encode json response failed: %v", err)
	}
}

// Error 以 JSON 回包一个错误对象：{"error": msg}。
func Error(w http.ResponseWriter, code int, msg string) {
	JSON(w, code, map[string]any{"error": msg})
}
