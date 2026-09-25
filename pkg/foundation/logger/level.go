package logger

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 动态日志级别（运行时热切换）

// 设计要点：
//  1. zap core 构建时使用 zap.AtomicLevel（见 zap.go 中 InitZap），它内部是
//     atomic.Int32，SetLevel 后已构建的 core 立即生效，无需重建 logger；
//  2. AtomicLevel 保存为包级变量 globalLevel（atomic.Pointer 存储），
//     InitZap 每次都会复用同一个 AtomicLevel 实例并把它设置为 cfg.Level，
//     因此重复 InitZap 不会让此前拿到的 handler / watcher 失效。

// ErrInvalidLevel 非法日志级别字符串（不在 debug/info/warn/error/panic/fatal 之内）。
var ErrInvalidLevel = errors.New("logger: invalid level")

// globalLevel 全局可原子调节的日志级别。
// 用 atomic.Pointer 存指针而非直接存值，是为了让 levelHolder() 的惰性初始化
// 与并发的 SetLevel/GetLevel 之间无 data race。
var globalLevel atomic.Pointer[zap.AtomicLevel]

// levelHolder 返回全局 AtomicLevel，首次调用时惰性创建（默认 info）。
// InitZap 之前调用 SetLevel/GetLevel 同样安全：级别先记录在此，
// 随后的 InitZap 会以 cfg.Level 覆盖它（cfg.Level 作为初始级别）。
func levelHolder() *zap.AtomicLevel {
	if l := globalLevel.Load(); l != nil {
		return l
	}
	lv := zap.NewAtomicLevelAt(zapcore.InfoLevel)
	// CompareAndSwap 保证并发首次调用只有一个实例胜出，其余复用胜出者。
	if globalLevel.CompareAndSwap(nil, &lv) {
		return &lv
	}
	return globalLevel.Load()
}

// AtomicLevel 返回全局 zap.AtomicLevel（供 InitZap 构建 core 使用，也可供高级场景直接操作）。
func AtomicLevel() zap.AtomicLevel { return *levelHolder() }

// ParseLevel 将字符串解析为 zapcore.Level，非法值返回 ErrInvalidLevel。
// 与内部 parseLevel（未知值静默降级 info）不同：本函数用于「用户显式设置」场景，必须报错。
func ParseLevel(s string) (zapcore.Level, error) {
	lv, ok := levelMap[strings.ToLower(strings.TrimSpace(s))]
	if !ok {
		return zapcore.InfoLevel, ErrInvalidLevel
	}
	return lv, nil
}

// SetLevel 运行时热切换全局日志级别。
// s 取值：debug/info/warn/error/panic/fatal（大小写不敏感、忽略首尾空白）；
// 非法值返回 ErrInvalidLevel 且不改变当前级别。
func SetLevel(s string) error {
	lv, err := ParseLevel(s)
	if err != nil {
		return err
	}
	levelHolder().SetLevel(lv)
	return nil
}

// SetLevelValue 以 zapcore.Level 直接设置全局日志级别（无需字符串解析）。
func SetLevelValue(lv zapcore.Level) { levelHolder().SetLevel(lv) }

// GetLevel 返回当前全局日志级别的小写字符串（如 "info"）。
func GetLevel() string { return levelHolder().Level().String() }

// GetLevelValue 返回当前全局日志级别的 zapcore.Level 值。
func GetLevelValue() zapcore.Level { return levelHolder().Level() }

// HTTP 管理端点
// levelPayload 是 LevelHandler 的请求/响应 JSON 体。
type levelPayload struct {
	Level string `json:"level"`
}

// levelErrPayload 是 LevelHandler 的错误响应体。
type levelErrPayload struct {
	Error string `json:"error"`
}

// LevelHandler 返回日志级别管理 HTTP 端点。

//	GET           → 200 {"level":"info"}
//	PUT / POST    → 从 body {"level":"debug"} 或 query ?level=debug 读取目标级别
//	                成功 200 返回新级别；非法级别 400 {"error":"..."}
//	其他方法       → 405

// 典型用法：mux.Handle("/debug/loglevel", logger.LevelHandler())
func LevelHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, levelPayload{Level: GetLevel()})
		case http.MethodPut, http.MethodPost:
			lv, err := extractLevel(r)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, levelErrPayload{Error: err.Error()})
				return
			}
			if err := SetLevel(lv); err != nil {
				writeJSON(w, http.StatusBadRequest, levelErrPayload{Error: err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, levelPayload{Level: GetLevel()})
		default:
			w.Header().Set("Allow", "GET, PUT, POST")
			writeJSON(w, http.StatusMethodNotAllowed, levelErrPayload{Error: "method not allowed"})
		}
	})
}

// extractLevel 从请求中提取目标级别：优先 JSON body，其次 query ?level=。
// body 为空且 query 为空时返回错误。
func extractLevel(r *http.Request) (string, error) {
	if r.Body != nil {
		// 限制读取体积，避免恶意超大 body 打爆内存。
		dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<16))
		var p levelPayload
		if err := dec.Decode(&p); err == nil && strings.TrimSpace(p.Level) != "" {
			return p.Level, nil
		}
	}
	if q := r.URL.Query().Get("level"); strings.TrimSpace(q) != "" {
		return q, nil
	}
	return "", ErrInvalidLevel
}

// writeJSON 写出 JSON 响应（内部小工具，忽略编码错误——响应头已发出无从补救）。
//
// 不复用 internal/foundation/ophttp：本包更底层，而 ophttp 依赖 logger，
// 反向引用会形成循环依赖。Content-Type 与 ophttp 保持一致。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// LevelWatcher 是「级别变更来源」的通用抽象：返回一个持续产出级别字符串的 channel。
// 之所以不直接耦合 etcd client，是为了让 logger 保持 foundation 最底层、零内部依赖的定位
// （见 README「依赖关系」一节），同时便于单测注入假数据源。
type LevelWatcher func(ctx context.Context) (<-chan string, error)

// WatchLevel 启动后台 goroutine 监听级别变更源，每收到一个合法级别就 SetLevel。
// 非法级别会被忽略（记录 warn 日志），不中断监听；
// ctx 取消或 channel 关闭时 goroutine 退出。

// 本函数立即返回，只在「建立监听」阶段可能返回错误。
func WatchLevel(ctx context.Context, w LevelWatcher) error {
	if w == nil {
		return errors.New("logger: nil level watcher")
	}
	ch, err := w(ctx)
	if err != nil {
		return err
	}
	if ch == nil {
		return errors.New("logger: level watcher returned nil channel")
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-ch:
				if !ok {
					return
				}
				old := GetLevel()
				if err := SetLevel(s); err != nil {
					Warn("logger: ignore invalid level from watcher", zap.String("level", s))
					continue
				}
				Info("logger: level changed",
					zap.String("from", old), zap.String("to", GetLevel()))
			}
		}
	}()
	return nil
}

// KVGetWatcher 是 etcd 等 KV 存储需要满足的最小接口，
// 只依赖「取一次值」+「注册变更回调」两个能力，不引入 etcd 具体类型，
// 因此 internal/transport/etcd.Client 可以直接满足（其 Get/Watch 签名一致）。
type KVGetWatcher interface {
	Get(ctx context.Context, key string) (string, error)
	Watch(ctx context.Context, key string, cb func()) error
}

// PollInterval 是 KVLevelWatcher 在「变更回调不可用」时的兜底轮询间隔。
const PollInterval = 30 * time.Second

// KVLevelWatcher 把一个 KV 存储（典型如 etcd）的指定 key 适配成 LevelWatcher。

// 用法：

//	logger.WatchLevel(ctx, logger.KVLevelWatcher(etcdClient, "/clover/log/level"))

// 行为：
//  1. 启动时立即 Get 一次，作为初始值下发；
//  2. 注册 Watch 回调，key 变更时重新 Get 并下发；
//  3. Watch 注册失败时降级为 PollInterval 定时轮询，保证最终一致。
func KVLevelWatcher(kv KVGetWatcher, key string) LevelWatcher {
	return func(ctx context.Context) (<-chan string, error) {
		if kv == nil {
			return nil, errors.New("logger: nil kv client")
		}
		if strings.TrimSpace(key) == "" {
			return nil, errors.New("logger: empty level key")
		}
		// 带缓冲，避免 etcd 回调 goroutine 在消费者忙时被阻塞。
		out := make(chan string, 8)
		// outMu 串行化「发送」与「关闭」，并记住是否已关闭：二者不互斥时，
		// closer 协程在 ctx 结束后 close(out)，而 emit 的 select 里
		// `case out <- v` 与 `case <-ctx.Done()` / `default` 可能同时就绪，
		// 关闭 channel 上的 send 分支同样算就绪 → 运行时可随机选中它，
		// 直接 panic「send on closed channel」（发生在 etcd watch 回调协程里，recover 兜不住）。
		// send 的三个分支全部非阻塞，因此持锁时间可控，不会阻塞 etcd 回调。
		var outMu sync.Mutex
		outClosed := false
		closeOut := func() {
			outMu.Lock()
			if !outClosed {
				outClosed = true
				close(out)
			}
			outMu.Unlock()
		}
		// emit 非阻塞下发；channel 满说明消费者跟不上，丢弃本次（后续变更仍会覆盖）。
		emit := func(v string) {
			if v == "" {
				return
			}
			outMu.Lock()
			if outClosed {
				outMu.Unlock()
				return
			}
			select {
			case out <- v:
			case <-ctx.Done():
			default:
			}
			outMu.Unlock()
		}
		pull := func() {
			v, err := kv.Get(ctx, key)
			if err != nil {
				return
			}
			emit(strings.TrimSpace(v))
		}
		pull() // 初始值
		if err := kv.Watch(ctx, key, pull); err != nil {
			// Watch 不可用时降级轮询，不向上抛错——日志级别属于「尽力而为」的运维能力。
			go func() {
				t := time.NewTicker(PollInterval)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						closeOut()
						return
					case <-t.C:
						pull()
					}
				}
			}()
			return out, nil
		}
		// Watch 生效时，仅在 ctx 结束后关闭 channel，终止 WatchLevel 的消费 goroutine。
		go func() {
			<-ctx.Done()
			closeOut()
		}()
		return out, nil
	}
}
