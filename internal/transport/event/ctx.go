package event

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/pkg/foundation/logger"
	ujson "clover-server-engine/pkg/shared/json"
	"clover-server-engine/pkg/shared/traceid"
)

// // Ctx：请求上下文——回包 + 玩家身份 + 全链路追踪

// 数据加载由 app.Game 提供：

//	game.LoadStruct(c, schema, id, &v)            // 默认修改意图
//	game.LoadRecord(c, schema, id)                // 记录加载

// 仅读取时用 data.ReadOnly()：

//	game.LoadStruct(c, schema, id, &v, data.ReadOnly())

// 自动落库/增量同步由 Logic.commitEdits 在 handler 返回后统一执行。
// // LoadMode / LoadOption / ReadOnly：数据加载选项（internal-only，不再通过 pkg 暴露）。
type LoadMode = data.LoadMode

const (
	LoadMutable  = data.LoadMutable
	LoadReadOnly = data.LoadReadOnly
)

type LoadOption = data.LoadOption

// ReadOnly 透传至 data.ReadOnly。
func ReadOnly() LoadOption { return data.ReadOnly() }

// SyncRegistry 系统级「数据类型 → 自动同步消息号」注册表。

// SyncRegistry 类型→消息号注册表：框架在自动同步时按 key.Type 查此表分发。
// 已注册类型走业务消息号（如 settings→MsgSettingsSync），未注册走 EPushDataSync 统一通道。
// 类型→消息号的映射由系统在启动期登记（如 g.RegisterSync("settings", def.MsgSettingsSync)）。
type SyncRegistry struct {
	mu sync.RWMutex
	m  map[string]uint32
}

// NewSyncRegistry 构造系统级同步注册表。
func NewSyncRegistry() *SyncRegistry { return &SyncRegistry{m: map[string]uint32{}} }

// RegisterSync 登记某数据类型的自动同步消息号；提交时框架据此自动广播变更。
func (r *SyncRegistry) RegisterSync(typeName string, msgID uint32) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.m[typeName] = msgID
	r.mu.Unlock()
}

// SyncMsgID 返回某数据类型的自动同步消息号；未登记时 ok=false。
func (r *SyncRegistry) SyncMsgID(typeName string) (uint32, bool) {
	if r == nil {
		return 0, false
	}
	r.mu.RLock()
	id, ok := r.m[typeName]
	r.mu.RUnlock()
	return id, ok
}

// Ctx 派发给业务 handler 的上下文。框架在派发前已完成解信封、加载玩家（若可）、注入，
// 业务只需取用，不直接碰网络 / 账号 / 数据库。
type Ctx struct {
	ctx       context.Context
	account   string // 当前连接的账号名（登录时客户端传入的 Account 字段）
	playerID  string // 当前连接的角色 ID（业务 handler 通过 SetPlayerID 设置）
	requestID uint32 // 请求关联 ID（客户端生成，回包原样带回；推送为 0）
	msgID     uint32
	connID    string
	body      []byte
	payload   any    // 领域事件载荷（OnEvent handler 可用 c.Payload() 取回）
	line      string // 线路标识：tcp/ws/udp/quic/wt

	// reply 回包暂存槽（由 Logic 在 dispatch 前创建并注入）。
	// 业务通过 Game.Reply → c.MarkReplied → slot.mark() 写入；
	// 框架错误覆盖通过 slot.force() 写入。
	reply *replySlot

	// bag Logic 基础设施注入（session / sync / ctl），由 Logic 创建，Ctx 仅握指针。
	bag *dispatchBag

	// connBag 连接级自定义 KV 存储（与 connID 绑定，同连接所有请求共享）。
	connBag *sync.Map
}

// SetNoAutoReply 抑制「框架自动给本次请求回包」。业务已自行回包，或本就不回包时调用。
func (c *Ctx) SetNoAutoReply() { c.reply.setNoAuto() }

// NoAutoReply 是否被设为抑制框架自动回包。
func (c *Ctx) NoAutoReply() bool { return c.reply.getNoAuto() }

// SetNoPush 关闭「数据变更自动推送」。
func (c *Ctx) SetNoPush() { c.reply.setNoPush() }

// NoPush 是否关闭数据变更自动推送。
func (c *Ctx) NoPush() bool { return c.reply.getNoPush() }

// Context 返回底层 context（框架已注入玩家对象等）。
// 若 ctx 为 nil（异常构造），回退到 Background() 避免 nil panic。
func (c *Ctx) Context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// NewAccountCtx 构造一个仅设置 account/owner 的最小 Ctx，
// 用于非请求来源（如连接生命周期事件）作为 EmitEvent 的 parent，
// 使事件 handler 内 c.Account() 与 c.Context() 中的 owner 都可用。
func NewAccountCtx(ctx context.Context, account string) *Ctx {
	if ctx == nil {
		ctx = context.Background()
	}
	if account != "" {
		ctx = proto.WithOwner(ctx, account)
	}
	return &Ctx{ctx: ctx, account: account}
}

// TraceID 返回本次请求的全链路追踪 ID（由网关生成、经逻辑服继承、随回包回流）。
// 未携带 trace（如进程内直接调用 Dispatch）时返回空串。业务可在日志/下游调用中透传此值。
func (c *Ctx) TraceID() string {
	if s := traceid.FromContext(c.ctx); s != nil {
		return s.TraceID()
	}
	return ""
}

// SpanID 返回当前处理阶段的 Span ID（每个环节独立，用于定位具体哪一步耗时/报错）。
func (c *Ctx) SpanID() string {
	if s := traceid.FromContext(c.ctx); s != nil {
		return s.SpanID()
	}
	return ""
}

// Account 返回当前连接的账号名（登录时客户端传入的 Account 字段）。
func (c *Ctx) Account() string { return c.account }

// Line 返回消息来源的线路标识（协议类型）。
func (c *Ctx) Line() string { return c.line }

// PlayerID 返回当前连接的角色 ID。创角或进入游戏时 SetPlayerID 一次，
// 后续同连接所有请求自动可用；断线后自动清理。
func (c *Ctx) PlayerID() string { return c.playerID }

// IsLoggedIn 当前连接是否已登录。
// 判据是账号名非空；PlayerID 是「角色 ID」（创角 / 进入游戏后才有值），二者不可混用。
func (c *Ctx) IsLoggedIn() bool { return c.account != "" }

// RequestID 返回本次请求的关联 ID（客户端生成，回包原样带回）。
// 推送消息的 requestID 为 0。
func (c *Ctx) RequestID() uint32 { return c.requestID }

// Session 返回当前请求的数据会话（identity map + pending 编辑列表）。
// 业务 handler 通过 game.LoadStruct(c, schema, id, &v) 使用，Ctx 内部自取。
// bag 为 nil（如 NewAccountCtx 构造的轻量 Ctx）时返回 nil，由调用方判空——
// 直接解引用会 panic。
func (c *Ctx) Session() *data.Session {
	if c == nil || c.bag == nil {
		return nil
	}
	return c.bag.session
}

// SetPlayerID 设置当前连接的角色 ID，同时写入 connBag["player_id"]，跨请求持久化。
func (c *Ctx) SetPlayerID(pid string) {
	if c.playerID == pid {
		return // 同一次派发内重复调用，跳过
	}
	// connBag 已持久化相同 playerID（BeforeDispatch 每消息都会 RestorePlayerID）：
	// 仅为本次派发恢复字段，不再重复发布 GWControlBind，避免网关刷屏重复索引。
	if old, ok := c.ConnValue("player_id"); ok && old == pid {
		c.playerID = pid
		return
	}
	c.playerID = pid
	c.SetConnValue("player_id", pid)

	// 通知网关追加双 key 索引（account + playerID 共存），
	// 使得 Alert/Push/自动同步 均能以 PlayerID 路由到本连接。
	// bag 为 nil（NewAccountCtx 场景）时无下行链路可用，跳过广播而不是解引用 nil。
	if c.bag != nil && c.bag.ctlPub != nil && c.bag.ctlSubject != "" {
		bind := proto.GWControlBind{ConnID: c.connID, PlayerID: pid}
		b, err := ujson.Marshal(bind)
		if err != nil {
			logger.Errorf("ctx: marshal GWControlBind: %v", err)
			return
		}
		if err := c.bag.ctlPub.Publish(c.bag.ctlSubject, b); err != nil {
			logger.Errorf("ctx: publish GWControlBind to %s: %v", c.bag.ctlSubject, err)
		}
	}
}

// SetConnValue 向当前连接的 KV 存储写入自定义数据（key 为字符串，value 任意类型）。
// 同连接后续所有请求均可通过 ConnValue 读取。断线时自动清理。
func (c *Ctx) SetConnValue(key string, value any) {
	if c.connBag != nil {
		c.connBag.Store(key, value)
	}
}

// ConnValue 从当前连接的 KV 存储读取自定义数据。
func (c *Ctx) ConnValue(key string) (any, bool) {
	if c.connBag != nil {
		return c.connBag.Load(key)
	}
	return nil, false
}

// SetConnBag 注入连接级 KV 存储（由 Game 层 BeforeDispatch 调用，业务无需关心）。
func (c *Ctx) SetConnBag(bag *sync.Map) {
	c.connBag = bag
}

// MsgID 返回本次请求的消息号。
func (c *Ctx) MsgID() uint32 { return c.msgID }

// ConnID 返回来源连接 ID（回包路由用，业务一般无需关心）。
func (c *Ctx) ConnID() string { return c.connID }

// Body 返回请求体的只读副本（调用方修改不影响其他 handler 或框架后续处理）。
func (c *Ctx) Body() []byte { return append([]byte(nil), c.body...) }

// BodyRef 返回原始请求体引用（零分配，只读场景）。
// 调用方切勿修改返回的切片，否则会影响其他 handler 或框架后续处理。
func (c *Ctx) BodyRef() []byte { return c.body }

// BindMsg 把请求体按 JSON 解出到 dst；业务自行定义请求结构体。
func (c *Ctx) BindMsg(dst any) error { return ujson.Unmarshal(c.body, dst) }

// Payload 取回领域事件载荷（OnEvent handler 用）。
func (c *Ctx) Payload() any { return c.payload }

// MarkReplied 标记本次请求已回包（仅存储回包数据，不做编码）。同一 Ctx 仅首次生效，
// 即"先到先得"语义。业务 handler 应通过 Game.Reply / Game.ReplyRaw 写入回包。
// 回包按 requestID 配对，不携带业务消息号（普通回包 msgID=0；错误回包由框架强制 EMsgError）。
func (c *Ctx) MarkReplied(body []byte) {
	c.reply.mark(body)
}

// BindEvent 将领域事件载荷反序列化到 v（v 必须为 *T 指针）。
// 优先走类型断言赋值（零分配，适合 MMO 高频事件），
// 类型不匹配时回退 JSON Marshal→Unmarshal。
func (c *Ctx) BindEvent(v any) error {
	if c.payload == nil {
		return errors.New("event: nil payload")
	}
	pv := reflect.ValueOf(c.payload)
	// 处理「带类型的 nil」——如 payload = (*T)(nil)。此时 c.payload != nil
	// （接口含类型信息），但底层指针为 nil：Marshal 得到 "null"，Unmarshal 后 v 不变，
	// 调用方再解引用会 panic。这里显式判定为无效载荷，返回错误而非放行。
	if pv.Kind() == reflect.Ptr && pv.IsNil() {
		return errors.New("event: nil payload (typed-nil pointer)")
	}
	// 快速路径：反射直接赋值，复用 pv 避免二次 reflect.ValueOf。
	dst := reflect.ValueOf(v)
	if dst.Kind() == reflect.Ptr && dst.Elem().CanSet() {
		if pv.Type().AssignableTo(dst.Elem().Type()) {
			dst.Elem().Set(pv)
			return nil
		}
	}
	// 回退：类型不匹配时 JSON 序列化/反序列化。
	b, err := ujson.Marshal(c.payload)
	if err != nil {
		return fmt.Errorf("event: marshal payload: %w", err)
	}
	return ujson.Unmarshal(b, v)
}

func mustEncode(v any) []byte {
	b, _ := ujson.Marshal(v)
	return b
}

// SyncItem 一条待推送的同步项（供 pushBatch 使用）。
type SyncItem struct {
	Type   string
	MsgID  uint32
	Body   []byte
	HasReg bool // 是否有注册同步消息号（一次查定避免 pushBatchGroup 二次查询 syncReg）
}

// TargetID 返回推送路由 ID：PlayerID 优先，未设置时回退到 Account。
func (c *Ctx) TargetID() string {
	if c.playerID != "" {
		return c.playerID
	}
	return c.account
}
