package app

import (
	"encoding/json"
	"net/http"

	"github.com/qw576483/clover-server-engine/internal/transport/event"
)

// deadLetterDispatcher 返回懒加载分发器：
// 每次请求才解析 bus 并构造 event.DeadLetterAdmin，
// 避免「构造期 bus 尚未存在」导致 503。
func deadLetterDispatcher() http.Handler {
	dispatch := func(r *http.Request) http.Handler {
		g := currentGame.Load()
		if g == nil || g.crossNodeBus == nil {
			return nil
		}
		a := event.NewDeadLetterAdmin(g.crossNodeBus.DLQ(), g.crossNodeBus)
		switch {
		case r.URL.Path == "/deadletter/dlq" || r.URL.Path == "/deadletter/dlq/":
			return a.ListHandler()
		case r.URL.Path == "/deadletter/dlq/retry" || r.URL.Path == "/deadletter/dlq/retry/":
			return a.RetryHandler()
		case r.URL.Path == "/deadletter/dlq/remove" || r.URL.Path == "/deadletter/dlq/remove/":
			return a.RemoveHandler()
		case r.URL.Path == "/deadletter/pending" || r.URL.Path == "/deadletter/pending/":
			return a.PendingHandler()
		default:
			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"endpoints": []string{
						"/deadletter/dlq",
						"/deadletter/dlq/retry",
						"/deadletter/dlq/remove",
						"/deadletter/pending",
					},
				})
			})
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := dispatch(r)
		if h == nil {
			http.Error(w, "deadletter not available (crossNodeBus not ready)", http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	})
}
