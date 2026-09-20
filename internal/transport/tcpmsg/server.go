package tcpmsg

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	netpkg "github.com/qw576483/clover-server-engine/internal/transport/net/tcp"

	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/safe"
)

// logThrottleEvery 逐帧错误日志的降频步长：首次 + 每 logThrottleEvery 次输出一条。
// 坏包 / 未知消息号 / handler 失败都能被异常客户端以收帧频率触发（同类日志不得刷屏）。
const logThrottleEvery = 1000

// HandlerFunc TCP 请求处理器：接收连接和请求 body，返回响应 body。
// conn 不为 nil，允许 handler 通过 conn.SetValue 在连接上挂载键值数据。
type HandlerFunc func(conn *netpkg.Conn, requestID uint32, body []byte) ([]byte, error)

// ConnAuthResult 连接级鉴权钩子的判定结果。
type ConnAuthResult struct {
	// Allow 是否放行本帧（false ⇒ 拒绝，配合 Reply/Close 使用）。
	Allow bool
	// Reply 回给对端的响应 body（nil = 不回）。无论 Allow 与否都会回。
	Reply []byte
	// Handled Reply 即为本帧的最终响应：回包后**不进入业务派发**
	// （握手帧自己不注册 handler，用它与业务帧区分）。
	Handled bool
	// Close 回包后关闭连接（鉴权失败的连接一律关掉，避免攻击者复用同一条连接反复试探）。
	Close bool
}

// ConnAuthFunc 连接级鉴权钩子：**每帧派发前**调用一次，由上层（如 master 域）实现。
//
// 语义：上层典型实现是「本连接已通过握手 → 放行；否则只接受握手帧，其余一律拒绝并关连接」，
// 用 conn.Value 挂载「已鉴权」标记（与 master 绑定 nodeID 的做法同源）。
// 用钩子而不是把鉴权写进本包，是因为「用什么消息号握手、共享密钥怎么比」属于**领域契约**，
// 传输包只提供拦截点。
type ConnAuthFunc func(conn *netpkg.Conn, msgID uint32, body []byte) ConnAuthResult

// Server TCP 消息服务端：监听端口、接收连接、分发到注册的 handler。
type Server struct {
	addr string

	mu       sync.RWMutex
	handlers map[uint32]HandlerFunc // msgID → handler

	srv    *netpkg.Server
	stopCh chan struct{}

	// connAuth 连接级鉴权钩子（nil = 不鉴权，仅限只绑回环的内部通道）。
	// 用 atomic.Pointer 而非裸字段：SetConnAuth 由装配期调用、onFrame 在运行期读，
	// 即便有人把设置时机挪到 Listen 之后也不能出现数据竞争。
	connAuth atomic.Pointer[ConnAuthFunc]

	// shuttingDown 标记：Stop() 调用后置 true，OnDisconnect 回调据此跳过主动清理
	// （避免 master 主动关机时把自己的所有节点误删）。
	shuttingDown atomic.Bool

	// 逐帧错误日志降频计数（decode 失败 / 未知消息号 / handler 失败 / 鉴权拒绝）。
	decodeErrLogCount  atomic.Uint64
	unknownMsgLogCount atomic.Uint64
	handlerErrLogCount atomic.Uint64
	authRejectLogCount atomic.Uint64

	// OnDisconnect 回调：连接断开时触发（传入 Conn，可从 conn.Value 读取 nodeID）。
	OnDisconnect func(conn *netpkg.Conn)
}

// NewServer 创建服务端（未启动）。
func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		handlers: make(map[uint32]HandlerFunc),
		stopCh:   make(chan struct{}),
	}
}

// SetConnAuth 安装连接级鉴权钩子（fn 为 nil 时清除）。
// 应在 Listen 之前调用；重复调用以最后一次为准。
func (s *Server) SetConnAuth(fn ConnAuthFunc) {
	if fn == nil {
		s.connAuth.Store(nil)
		return
	}
	s.connAuth.Store(&fn)
}

// Register 注册 handler。
func (s *Server) Register(msgID uint32, handler HandlerFunc) {
	s.mu.Lock()
	s.handlers[msgID] = handler
	s.mu.Unlock()
}

// handler 并发安全地取出已注册 handler。
func (s *Server) handler(msgID uint32) (HandlerFunc, bool) {
	s.mu.RLock()
	h, ok := s.handlers[msgID]
	s.mu.RUnlock()
	return h, ok
}

// Listen 绑定监听地址并启动 accept 循环（不阻塞）。
// 端口被占用、地址非法等错误在此同步返回，调用方无需等后台日志即可感知启动失败。
func (s *Server) Listen() error {
	srv := netpkg.NewServer(netpkg.ServerConfig{
		ListenAddr:        s.addr,
		HeartbeatInterval: 30 * time.Second,
	}, s.onFrame)
	if err := srv.Start(); err != nil {
		return err
	}
	s.srv = srv
	logger.Infof("tcpmsg: listening on %s", srv.Addr())
	return nil
}

// Addr 返回实际监听地址（Listen 之后有效；支持 addr 里的端口写 0 由系统分配）。
// 未监听时返回配置的地址。
func (s *Server) Addr() string {
	if s.srv != nil {
		return s.srv.Addr()
	}
	return s.addr
}

// ListenAndServe 开始监听并处理请求，阻塞直到 Stop()。
func (s *Server) ListenAndServe() error {
	if err := s.Listen(); err != nil {
		return err
	}
	<-s.stopCh
	return nil
}

// Stop 停止服务端。
func (s *Server) Stop() error {
	// 先标记 shuttingDown，避免 srv.Stop() 关闭所有连接时误触发 OnDisconnect 清理节点。
	s.shuttingDown.Store(true)
	select {
	case <-s.stopCh:
		// already stopped
	default:
		close(s.stopCh)
	}
	if s.srv != nil {
		return s.srv.Stop()
	}
	return nil
}

func (s *Server) onFrame(conn *netpkg.Conn, data []byte) {
	// 每个连接仅启动一次 disconnect 监控 goroutine。
	if conn.MarkMonitored() {
		safe.GoSafe(func() {
			<-conn.ClosedCh()
			// 主动关机（srv.Stop 关闭连接）时不触发回调，避免误删活节点。
			if s.shuttingDown.Load() {
				return
			}
			logger.Infof("tcpmsg: connection %s disconnected", conn.ConnID())
			if s.OnDisconnect != nil {
				s.OnDisconnect(conn)
			}
		})
	}

	msgID, requestID, body, err := Decode(data)
	if err != nil {
		if n := s.decodeErrLogCount.Add(1); n == 1 || n%logThrottleEvery == 0 {
			logger.Errorf("tcpmsg: decode error: %v（同类累计 %d 次，已降频输出）", err, n)
		}
		return
	}

	// 连接级鉴权：**在查 handler 之前**拦（未鉴权连接不该有能力区分
	// 「消息号存在」与「不存在」，否则枚举本身成了探测信道）。
	if auth := s.connAuth.Load(); auth != nil {
		res := (*auth)(conn, msgID, body)
		if len(res.Reply) > 0 {
			if frame := Encode(requestID, 0, res.Reply); frame != nil {
				if err := conn.Send(frame); err != nil {
					logger.Errorf("tcpmsg: send auth reply msgID=%d: %v", msgID, err)
				}
			}
		}
		if !res.Allow {
			if n := s.authRejectLogCount.Add(1); n == 1 || n%logThrottleEvery == 0 {
				logger.Warnf("tcpmsg: rejected unauthenticated msgID=%d from %s（同类累计 %d 次，已降频输出）",
					msgID, conn.RemoteAddr(), n)
			}
			if res.Close {
				// 延迟关闭（而非立即 Close）：Close 会立刻关 socket，而上面的拒绝回包
				// 还在发送队列里，紧邻执行会把回包丢掉 —— 对端只看到「断连」、
				// 读不到「为什么被拒」，排查成本差一个数量级。每连接只安排一次。
				conn.CloseGracefully()
			}
			return
		}
		if res.Handled {
			// 握手帧：回包已发出，不进业务派发。
			return
		}
	}

	handler, ok := s.handler(msgID)
	if !ok {
		if n := s.unknownMsgLogCount.Add(1); n == 1 || n%logThrottleEvery == 0 {
			logger.Errorf("tcpmsg: unknown msgID=%d（同类累计 %d 次，已降频输出）", msgID, n)
		}
		return
	}

	resp, err := handler(conn, requestID, body)
	if err != nil {
		if n := s.handlerErrLogCount.Add(1); n == 1 || n%logThrottleEvery == 0 {
			logger.Errorf("tcpmsg: handler msgID=%d error: %v（同类累计 %d 次，已降频输出）", msgID, err, n)
		}
		// 即使 handler 报错，也尽量把错误响应发回客户端，避免客户端超时等待。
		if resp == nil {
			// JSON 转义：直接拼接 err.Error() 时含引号/换行会产出非法 JSON，客户端解析失败。
			if b, jerr := json.Marshal(map[string]string{"error": err.Error()}); jerr == nil {
				resp = b
			} else {
				logger.Errorf("tcpmsg: marshal error reply msgID=%d: %v", msgID, jerr)
				resp = []byte(`{"error":"internal error"}`)
			}
		}
	}

	// handler 返回 (nil, nil)（成功且无 body）时补一个 JSON null：
	// 协议上「无 body」用 null 表示，否则会发出仅含帧头的空 body 帧，
	// 客户端 json.Unmarshal 空切片直接报错（把无 body 误判为坏包）。
	if len(resp) == 0 {
		resp = []byte("null")
	}

	// 注意：handler 返回的 resp 已经是序列化好的 JSON []byte，
	// 不能再用 MarshalReply（会二次 json.Marshal 导致 []byte 被 base64 编码），
	// 直接用 Encode 封装帧头即可。回包 msgID=0，靠 requestID 匹配。
	frame := Encode(requestID, 0, resp)
	if frame == nil {
		logger.Errorf("tcpmsg: encode reply msgID=%d: payload too large", msgID)
		return
	}

	if err := conn.Send(frame); err != nil {
		logger.Errorf("tcpmsg: send reply msgID=%d: %v", msgID, err)
	}
}
