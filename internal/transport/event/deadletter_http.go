package event

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"clover-server-engine/internal/foundation/ophttp"
	"clover-server-engine/pkg/foundation/logger"
)

// // 死信队列人工介入 HTTP 入口

// 只导出 http.Handler，不自行监听端口、不改 internal/app，接线由上层统一完成：

//	admin := event.NewDeadLetterAdmin(bus.DLQ(), bus)
//	mux.Handle("/debug/event/dlq",        admin.ListHandler())
//	mux.Handle("/debug/event/dlq/retry",  admin.RetryHandler())
//	mux.Handle("/debug/event/dlq/remove", admin.RemoveHandler())
//	mux.Handle("/debug/event/pending",    admin.PendingHandler())

// 也可直接用 admin.Register(mux, "/debug/event") 一次挂全。
// // PendingLister 提供在途（重试中）投递查询能力。
type PendingLister interface {
	ListPending() []PendingInfo
}

// DeadLetterAdmin 死信队列的人工介入 HTTP 门面。
type DeadLetterAdmin struct {
	dlq     DeadLetterQueue
	pending PendingLister
}

// NewDeadLetterAdmin 构造人工介入门面。pending 可为 nil（此时不提供在途查询）。
func NewDeadLetterAdmin(dlq DeadLetterQueue, pending PendingLister) *DeadLetterAdmin {
	return &DeadLetterAdmin{dlq: dlq, pending: pending}
}

// Register 将全部 handler 挂载到 mux 的 prefix 下。
// prefix 为空取 "/debug/event"。
// 多次调用同一 prefix 会静默跳过（http.ServeMux 重复 pattern 会 panic）。
func (a *DeadLetterAdmin) Register(mux *http.ServeMux, prefix string) {
	if mux == nil {
		return
	}
	if prefix == "" {
		prefix = "/debug/event"
	}
	// http.ServeMux 不允许重复 pattern；用 recover 拦截 panic，避免重复注册时进程崩溃。
	defer func() {
		if r := recover(); r != nil {
			// 重复 pattern（「多次调用同一 prefix」是受支持用法）静默跳过，
			// 但必须留下一条日志；其余 panic（非法 pattern 等）属真实错误，
			// 不能吞掉：打日志后原样上抛，否则整段管理路由静默丢失。
			if isDuplicatePatternPanic(r) {
				logger.Warnf("event: deadletter admin routes already registered under %s, skip: %v", prefix, r)
				return
			}
			logger.Errorf("event: deadletter admin register under %s: %v", prefix, r)
			panic(r)
		}
	}()
	mux.Handle(prefix+"/dlq", a.ListHandler())
	mux.Handle(prefix+"/dlq/retry", a.RetryHandler())
	mux.Handle(prefix+"/dlq/remove", a.RemoveHandler())
	mux.Handle(prefix+"/pending", a.PendingHandler())
}

// isDuplicatePatternPanic 判断 recover 到的值是否来自「pattern 重复注册」。
// http.ServeMux 对重复注册 panic 的文案固定为 "http: multiple registrations for <pattern>"，
// 只据此区分「受支持的重复注册」与「非法 pattern 等真实错误」。
func isDuplicatePatternPanic(r any) bool {
	return strings.Contains(fmt.Sprint(r), "multiple registrations")
}

// ListHandler GET {prefix}/dlq?limit=100 —— 列出死信。
func (a *DeadLetterAdmin) ListHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET")
			return
		}
		if a.dlq == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "死信队列未配置")
			return
		}
		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				limit = n
			}
		}
		items, err := a.dlq.List(r.Context(), limit)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if items == nil {
			items = []DeadLetter{}
		}
		ophttp.JSON(w, http.StatusOK, map[string]any{"total": len(items), "items": items})
	})
}

// RetryHandler POST {prefix}/dlq/retry?id=xxx —— 人工重投指定死信。
func (a *DeadLetterAdmin) RetryHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 有副作用，限定 POST，避免被浏览器/爬虫 GET 误触发。
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 POST")
			return
		}
		if a.dlq == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "死信队列未配置")
			return
		}
		id := r.URL.Query().Get("id")
		if id == "" {
			writeJSONError(w, http.StatusBadRequest, "缺少 id 参数")
			return
		}
		if err := a.dlq.Retry(r.Context(), id); err != nil {
			writeJSONError(w, dlqErrStatus(err), err.Error())
			return
		}
		ophttp.JSON(w, http.StatusOK, map[string]any{"ok": true, "id": id})
	})
}

// RemoveHandler POST {prefix}/dlq/remove?id=xxx —— 删除指定死信。
func (a *DeadLetterAdmin) RemoveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 POST")
			return
		}
		if a.dlq == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "死信队列未配置")
			return
		}
		id := r.URL.Query().Get("id")
		if id == "" {
			writeJSONError(w, http.StatusBadRequest, "缺少 id 参数")
			return
		}
		if err := a.dlq.Remove(r.Context(), id); err != nil {
			writeJSONError(w, dlqErrStatus(err), err.Error())
			return
		}
		ophttp.JSON(w, http.StatusOK, map[string]any{"ok": true, "id": id})
	})
}

// PendingHandler GET {prefix}/pending —— 列出在途（重试中）的投递，便于排障。
func (a *DeadLetterAdmin) PendingHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET")
			return
		}
		if a.pending == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "在途查询未配置")
			return
		}
		items := a.pending.ListPending()
		if items == nil {
			items = []PendingInfo{}
		}
		ophttp.JSON(w, http.StatusOK, map[string]any{"total": len(items), "items": items})
	})
}

// dlqErrStatus 将死信操作错误映射为 HTTP 状态码。
func dlqErrStatus(err error) int {
	switch {
	case errors.Is(err, ErrDeadLetterNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrRetryNotSupported):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// writeJSONError 输出 JSON 错误响应。
//
// 保留本包装而非直接用 ophttp.Error：这里沿用 {"ok": false, "error": ...} 的错误体，
// 是死信端点既有契约，改动会影响已接入的运维工具。
func writeJSONError(w http.ResponseWriter, code int, msg string) {
	ophttp.JSON(w, code, map[string]any{"ok": false, "error": msg})
}
