package app

import (
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	iaccessor "github.com/qw576483/clover-server-engine/internal/domain/data/accessor"
	imysql "github.com/qw576483/clover-server-engine/internal/domain/data/store/mysql"
	iredis "github.com/qw576483/clover-server-engine/internal/domain/data/store/redis"
	"github.com/qw576483/clover-server-engine/internal/domain/data/visibility"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/internal/transport/event"
	"github.com/qw576483/clover-server-engine/internal/transport/nats"
	"github.com/qw576483/clover-server-engine/internal/transport/net/push"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// Core 是 Game 与 MasterGame 的共享内核，嵌入后自动继承全部数据访问/push/alert/monitor/timer 方法。
type Core struct {
	*event.Logic
	Timer *TimeEvent

	// 数据存储
	store *data.Store

	// 下行链路（NATS 推送）
	entityAcc     *iaccessor.Accessor
	notifyNC      *nats.Client
	notifyPub     data.Publisher
	notifySubject string

	// 同步注册
	syncReg *event.SyncRegistry

	// 监控表
	monitorData   map[string]map[data.OwnerType]string
	monitorDataMu sync.RWMutex

	// 基础设施
	cfg       *Config
	nodeID    string
	closeOnce sync.Once
}

// newCore 构造共享内核。Logic 与 Timer 由调用方注入。
func newCore(cfg *Config, store *data.Store, syncReg *event.SyncRegistry) *Core {
	return &Core{
		store:       store,
		syncReg:     syncReg,
		cfg:         cfg,
		monitorData: map[string]map[data.OwnerType]string{},
	}
}

// 数据访问器
func (co *Core) Config() *Config                     { return co.cfg }
func (co *Core) Data() *data.Store                   { return co.store }
func (co *Core) Bus() event.Bus                      { return co.Logic.Bus() }
func (co *Core) HTTPAddr() string                    { return co.Logic.HTTPAddr() }
func (co *Core) EntityAccessor() *iaccessor.Accessor { return co.entityAcc }
func (co *Core) Publisher() data.Publisher           { return co.notifyPub }
func (co *Core) NotifySubject() string               { return co.notifySubject }
func (co *Core) GetRedis() *iredis.Client {
	if co.store != nil {
		return co.store.RedisClient()
	}
	return nil
}
func (co *Core) GetNats() *nats.Client { return co.notifyNC }
func (co *Core) GetMysql() *imysql.Client {
	if co.store != nil {
		return co.store.MySQLClient()
	}
	return nil
}

// 数据加载
func (co *Core) LoadStruct(c *event.Ctx, schema data.StructSchema, id string, v any, opts ...data.LoadOption) error {
	data.RegisterType(schema.OwnerType, schema.Type)
	visibility.Register(schema.OwnerType, schema.Type, schema.Visibility)
	return c.Session().LoadStruct(c.Context(), schema.Key(id), v, opts...)
}

func (co *Core) LoadRecord(c *event.Ctx, schema data.RecordSchema, id string) (*data.Record, error) {
	data.RegisterType(schema.OwnerType, schema.Type)
	visibility.Register(schema.OwnerType, schema.Type, schema.Visibility)
	return c.Session().LoadRecord(c.Context(), schema.Key(id), schema.Cols, schema.ColTypes)
}

// Push 推送
func (co *Core) PushToPlayer(playerID string, msgID uint32, body []byte, opts ...proto.DeliveryMode) error {
	if co.notifyPub == nil || co.notifySubject == "" {
		return nil
	}

	// 默认使用可靠传输模式（关键业务必须保证送达）
	mode := proto.DeliveryModeReliable
	if len(opts) > 0 {
		mode = opts[0]
	}

	return push.Push(&push.PlayerChannel{
		Pub:          co.notifyPub,
		Subject:      co.notifySubject,
		PlayerID:     playerID,
		DeliveryMode: mode,
	}, msgID, body)
}

func (co *Core) PushToPlayerJSON(playerID string, msgID uint32, v any, opts ...proto.DeliveryMode) error {
	body, err := ujson.Marshal(v)
	if err != nil {
		return err
	}
	return co.PushToPlayer(playerID, msgID, body, opts...)
}

func (co *Core) PushToScene(scene proto.ESceneBroadcaster, msgID uint32, body []byte, opts ...proto.DeliveryMode) error {
	mode := proto.DeliveryModeReliable
	if len(opts) > 0 {
		mode = opts[0]
	}
	return push.Push(&push.SceneChannel{Scene: scene, DeliveryMode: mode}, msgID, body)
}

func (co *Core) PushToSceneJSON(scene proto.ESceneBroadcaster, msgID uint32, v any, opts ...proto.DeliveryMode) error {
	body, err := ujson.Marshal(v)
	if err != nil {
		return err
	}
	return co.PushToScene(scene, msgID, body, opts...)
}

func (co *Core) PushToAll(msgID uint32, body []byte, opts ...proto.DeliveryMode) error {
	if co.notifyPub == nil || co.notifySubject == "" {
		return nil
	}
	mode := proto.DeliveryModeReliable
	if len(opts) > 0 {
		mode = opts[0]
	}
	return push.Push(&push.AllChannel{
		Pub:          co.notifyPub,
		Subject:      co.notifySubject,
		DeliveryMode: mode,
	}, msgID, body)
}

func (co *Core) PushToAllJSON(msgID uint32, v any, opts ...proto.DeliveryMode) error {
	body, err := ujson.Marshal(v)
	if err != nil {
		return err
	}
	return co.PushToAll(msgID, body, opts...)
}

// Alert 弹窗
func (co *Core) AlertToPlayer(playerID string, a *push.EAlertNotify) error {
	if co.notifyPub == nil || co.notifySubject == "" {
		return nil
	}
	return push.Alert(&push.PlayerChannel{
		Pub:          co.notifyPub,
		Subject:      co.notifySubject,
		PlayerID:     playerID,
		DeliveryMode: proto.DeliveryModeReliable,
	}, a)
}

func (co *Core) AlertToScene(scene proto.ESceneBroadcaster, a *push.EAlertNotify) error {
	return push.Alert(&push.SceneChannel{Scene: scene, DeliveryMode: proto.DeliveryModeReliable}, a)
}

func (co *Core) AlertToAll(a *push.EAlertNotify) error {
	if co.notifyPub == nil || co.notifySubject == "" {
		return nil
	}
	return push.Alert(&push.AllChannel{
		Pub:          co.notifyPub,
		Subject:      co.notifySubject,
		DeliveryMode: proto.DeliveryModeReliable,
	}, a)
}

// 回包
//
// Reply / ReplyRaw 只此一份：Game / MasterGame / LogGame / AuthGame 都嵌入 *Core，
// 由提升获得同一实现。此前四个角色各写一份，行为已经漂移（只有 log / auth 记录
// marshal 失败，game / master 静默丢弃），此处统一为「失败必记日志」。
//
// Reply 以结构体 JSON 编码回包给客户端。同一 Ctx 仅首次生效；
// 回包按 requestID 配对，不携带业务消息号（帧中 msgID=0）。
func (co *Core) Reply(c *event.Ctx, v any) {
	b, err := ujson.Marshal(v)
	if err != nil {
		logger.Errorf("app: marshal reply failed (msgID=%d): %v", c.MsgID(), err)
		return
	}
	c.MarkReplied(b)
}

// ReplyRaw 以原始字节回包（需自定义编码时，按 requestID 配对）。
func (co *Core) ReplyRaw(c *event.Ctx, body []byte) { c.MarkReplied(body) }

// 同步注册
func (co *Core) RegisterSync(typeName string, msgID uint32) {
	if co.syncReg != nil {
		co.syncReg.RegisterSync(typeName, msgID)
	}
}

// MonitorStore 监控存储接口，供外部使用。
type MonitorStore = monitorStore

type monitorStore interface {
	DumpMonitor(playerID string) []byte
	ImportMonitor(playerID string, data []byte)
}

// 监控表
func (co *Core) MonitorData(playerID string, ownerType data.OwnerType, entityID string) {
	co.monitorDataMu.Lock()
	if em, ok := co.monitorData[playerID]; ok {
		em[ownerType] = entityID
	} else {
		co.monitorData[playerID] = map[data.OwnerType]string{ownerType: entityID}
	}
	co.monitorDataMu.Unlock()
}

func (co *Core) UnmonitorData(playerID string, ownerType data.OwnerType) {
	co.monitorDataMu.Lock()
	if em, ok := co.monitorData[playerID]; ok {
		delete(em, ownerType)
		if len(em) == 0 {
			delete(co.monitorData, playerID)
		}
	}
	co.monitorDataMu.Unlock()
}

func (co *Core) ClearMonitorData(playerID string) {
	co.monitorDataMu.Lock()
	delete(co.monitorData, playerID)
	co.monitorDataMu.Unlock()
}

func (co *Core) DumpMonitor(playerID string) []byte {
	co.monitorDataMu.RLock()
	d := co.monitoredDataLocked(playerID)
	co.monitorDataMu.RUnlock()
	b, err := ujson.Marshal(d)
	if err != nil {
		// 编码失败不能静默返回 nil：调用方无法区分「无数据」与「编码失败」。
		logger.Errorf("app: dump monitor data failed (playerID=%s): %v", playerID, err)
		return nil
	}
	return b
}

func (co *Core) ImportMonitor(playerID string, raw []byte) {
	var d map[data.OwnerType]string
	if err := ujson.Unmarshal(raw, &d); err != nil || len(d) == 0 {
		return
	}
	co.monitorDataMu.Lock()
	if _, ok := co.monitorData[playerID]; !ok {
		co.monitorData[playerID] = d
	} else {
		for k, v := range d {
			co.monitorData[playerID][k] = v
		}
	}
	co.monitorDataMu.Unlock()
}

func (co *Core) MonitorStore() MonitorStore {
	if co == nil {
		return nil
	}
	return co
}

// monitoredDataLocked 返回该玩家监控数据的副本。
// 调用方**必须**已持有 co.monitorDataMu（Read 或 Write 皆可）——按仓库约定用 Locked 后缀表达。
func (co *Core) monitoredDataLocked(playerID string) map[data.OwnerType]string {
	if em, ok := co.monitorData[playerID]; ok {
		out := make(map[data.OwnerType]string, len(em))
		for k, v := range em {
			out[k] = v
		}
		return out
	}
	return nil
}

// 清理
func (co *Core) closeCoreBackends() {
	if co.Timer != nil {
		co.Timer.Close()
	}
	if co.notifyNC != nil {
		// 关闭失败只影响资源释放（连接/句柄），不改变退出流程；留日志便于排查泄漏。
		if err := co.notifyNC.Close(); err != nil {
			logger.Warnf("app: close notify NATS conn: %v", err)
		}
	}
	if co.store != nil {
		if err := co.store.Close(); err != nil {
			logger.Warnf("app: close store: %v", err)
		}
	}
}
