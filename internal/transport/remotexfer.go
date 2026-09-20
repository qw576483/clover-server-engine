// package transport 提供 MMO 集群级能力：全局 KV（gstore）、服务发现（etcd）、消息（nats）。
//
// 本文件实现「跨机器对象迁移」原语所需的两块最小能力：
// 1. scene->node 路由表（基于 gstore 全局 KV，跨进程/跨机共享）：场景创建时由所在节点
// RegisterScene 写入自身 nodeID；迁移时 LookupScene 查询目标场景落在哪台机器。
// 2. 跨机迁移指令的编解码与发布（NATS subject 自定），对端收到后在目标场景
// EnterOwnerType(objID, kind, x, z) 即可，玩家三元键持久数据随指令无需搬运。
//
// 本包刻意不引用 internal/domain/mmo，避免循环依赖；对端如何把收到的指令应用到 SceneManager
// 由 mmo 包（HandleRemoteTransfer）与 app 启动时订阅 wiring 负责。
package transport

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/runtime/globalstore"
	"github.com/qw576483/clover-server-engine/internal/transport/pubsub"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
)

// RemoteTransferSubject 是跨机对象迁移指令的 subject 前缀。
// 实际投递用「前缀 + 目标节点 ID」的**定向** subject（见 RemoteTransferSubjectFor）。
const RemoteTransferSubject = "mmo.transfer.remote"

// RemoteTransferSubjectFor 返回发往指定节点的定向迁移 subject。
//
// 为什么定向而不是广播：迁移是点对点语义，广播会让集群里每个节点都收到与自己无关的
// 指令（只能靠对端 GetScene 失败来丢弃），节点一多就是纯浪费。
//
// 为什么用十进制节点 ID 而不是节点地址：NATS subject 不允许 ':' 这类字符，
// 而节点地址形如 "127.0.0.1:8011"。
func RemoteTransferSubjectFor(nodeID uint64) string {
	return RemoteTransferSubject + "." + conv.FormatUint(nodeID)
}

// DefaultSceneTTL 是 scene->node 路由登记的默认存活时长。
// 登记必须带 TTL——否则场景销毁 / 节点崩溃会遗留过期 nodeID，
// 导致迁移把对象送往一台已经死掉的机器。节点需周期性 RefreshScene 续期。
const DefaultSceneTTL = 30 * time.Second

// ErrSceneNodeUnknown 表示路由表登记的节点值非法（非整数），无法解析为目标 nodeID。
var ErrSceneNodeUnknown = errors.New("cluster: scene node value not an integer")

// RemoteTransfer 是跨机对象迁移指令载荷。
// 对端收到后 EnterOwnerTypeInstance(objID, kind, instance, pos) 保留源实例归属；
// 玩家三元键持久数据落在共享 data.Store，随指令无需搬运。
// 落点是**三维坐标**：多层地形 / 飞行玩法下，只带 (X,Z) 会把对象落到 y=0 的地面，
// 跨图后直接从楼上或空中掉下来。
// Version 为迁移版本号，用于断连后续传时对端校验幂等/乱序；
// SrcNode 标识源节点，便于对端校验与日志追踪。

// Body 为可选的物理体快照：带上它，跨机迁移才与同机迁移（Scene.TransferTo 会整份
// 拷贝 Body 并 AddBody）语义一致。
//
// 此前本载荷**不含 Body**，跨机只重建「成员」（AOI / instance 归属 / 三维落点），
// 导致 Mass / Radius / Velocity / Force / Static 全部丢失 —— 现象是"跨图后手感变了、
// 击退不再生效"。现补齐为**可选字段**（omitempty）：没挂物理体的对象不增加任何字节；
// 旧版本节点读到该字段会被 JSON 解码忽略（不会报错），只是仍然不重建 Body。
type RemoteTransfer struct {
	DstScene  uint64         `json:"dst_scene"`
	ObjID     uint64         `json:"obj_id"`
	X         float64        `json:"x"`
	Y         float64        `json:"y"`
	Z         float64        `json:"z"`
	OwnerType data.OwnerType `json:"owner_type"`
	Instance  uint32         `json:"instance"`
	Version   uint64         `json:"version,omitempty"`
	SrcNode   string         `json:"src_node,omitempty"`
	// Body 可选物理体快照；nil 表示源对象没有物理体（或源端为不支持该字段的旧版本）。
	Body *RemoteBody `json:"body,omitempty"`
}

// RemoteBody 是跨机迁移携带的物理体快照。
//
// 为什么不直接复用 mmo.Body：本包刻意不引用 internal/domain/mmo（避免循环依赖，见包注释），
// 因此这里定义一份字段一一对应的线格式结构体，由 mmo 侧在发送前填充、接收后转回 mmo.Body。
// 字段与 mmo.Body 保持一致：Mass / Radius / Position / Velocity / Force / Static。
type RemoteBody struct {
	Mass     float64    `json:"mass,omitempty"`
	Radius   float64    `json:"radius,omitempty"`
	Position [3]float64 `json:"pos"`
	Velocity [3]float64 `json:"vel,omitempty"`
	Force    [3]float64 `json:"force,omitempty"`
	Static   bool       `json:"static,omitempty"`
}

// RouteStore 管理 scene->node 路由表，基于 gstore 全局 KV（跨进程/跨机共享）。
// 建议用独立命名空间（如 "route/"）隔离其他全局键。
type RouteStore struct {
	s *globalstore.Store
}

// NewRouteStore 用给定 gstore Store 构造路由表。
func NewRouteStore(s *globalstore.Store) *RouteStore { return &RouteStore{s: s} }

func sceneKey(sceneID uint64) string { return "scene:" + conv.FormatUint(sceneID) }

// RegisterScene 登记 scene 与其所在 nodeID（场景创建时由对应节点调用）。
// 带 DefaultSceneTTL 写入，节点崩溃后登记会自动过期，避免遗留僵尸路由。
// 场景存活期间需周期性调用 RefreshScene 续期，间隔应小于 TTL。
func (r *RouteStore) RegisterScene(ctx context.Context, sceneID, nodeID uint64) error {
	return r.RegisterSceneTTL(ctx, sceneID, nodeID, DefaultSceneTTL)
}

// RegisterSceneTTL 以自定义 TTL 登记 scene->node 路由；ttl<=0 时退化为 DefaultSceneTTL。
func (r *RouteStore) RegisterSceneTTL(ctx context.Context, sceneID, nodeID uint64, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultSceneTTL
	}
	return r.s.Put(ctx, sceneKey(sceneID), conv.FormatUint(nodeID), globalstore.WithTTL(ttl))
}

// RefreshScene 续期 scene->node 登记（场景存活期间周期调用，防止 TTL 过期）。
// 等价于以相同 nodeID 重新登记。
func (r *RouteStore) RefreshScene(ctx context.Context, sceneID, nodeID uint64) error {
	return r.RegisterSceneTTL(ctx, sceneID, nodeID, DefaultSceneTTL)
}

// UnregisterScene 主动注销 scene->node 登记（场景正常销毁时调用）。
func (r *RouteStore) UnregisterScene(ctx context.Context, sceneID uint64) error {
	return r.s.Delete(ctx, sceneKey(sceneID))
}

// LookupScene 查询 scene 所在 nodeID；未登记返回 (0,false,nil)，解析失败返回错误。
func (r *RouteStore) LookupScene(ctx context.Context, sceneID uint64) (nodeID uint64, ok bool, err error) {
	v, err := r.s.Get(ctx, sceneKey(sceneID))
	if err == globalstore.ErrKeyNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false, ErrSceneNodeUnknown
	}
	return n, true, nil
}

// Publisher 发布接口（统一定义在 transport/pubsub）。
type Publisher = pubsub.Publisher

// JetPublisher 是可选的 JetStream 持久发布接口。
// 跨机迁移指令必须「至少一次」送达——普通 Publish 是 at-most-once，
// NATS 抖动/瞬断会静默丢消息导致迁移失败且无人察觉。若底层发布器实现了本接口，
// PublishRemoteTransfer 会优先走 JetStream 持久发布（带重试/去重）。
type JetPublisher interface {
	JetPublishRaw(ctx context.Context, subject string, data []byte) error
}

// EncodeRemoteTransfer 把迁移指令序列化为 NATS 载荷。
func EncodeRemoteTransfer(rt RemoteTransfer) ([]byte, error) {
	return json.Marshal(rt)
}

// DecodeRemoteTransfer 反序列化迁移指令。
func DecodeRemoteTransfer(b []byte) (RemoteTransfer, error) {
	var rt RemoteTransfer
	err := json.Unmarshal(b, &rt)
	return rt, err
}

// PublishRemoteTransfer 经 NATS 向**指定节点**发布一条定向跨机迁移指令。
//
// dstNode 由调用方经 RouteStore.LookupScene 查得（见 SceneManager.TransferRemote）：
// 拿不到登记就不该发——目标场景可能不存在，或所在节点已崩溃（登记 TTL 过期已摘除）。
//
// 优先使用 JetStream 持久发布（at-least-once），避免瞬断丢指令导致迁移静默失败；
// 仅当发布器不支持 JetStream 时才回退到普通 Publish（如测试替身）。
// 同时校验 DstNode/DstScene/ObjID/Instance 合法性，避免无效路由。
//
// ctx：JetStream 发布（含重试/退避）的取消与超时依据。此前固定用
// context.Background() —— 调用方已取消/超时后仍会继续重试发布，且没有任何时间上界。
// 传 nil 时退化为 context.Background()（保持旧行为）。
func PublishRemoteTransfer(ctx context.Context, pub Publisher, dstNode uint64, rt RemoteTransfer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if dstNode == 0 {
		return errors.New("cluster: remote transfer destination node must be non-zero")
	}
	if rt.DstScene == 0 {
		return errors.New("cluster: remote transfer destination scene must be non-zero")
	}
	if rt.ObjID == 0 {
		return errors.New("cluster: remote transfer object ID must be non-zero")
	}
	if rt.Instance == 0 {
		return errors.New("cluster: remote transfer instance must be non-zero")
	}
	b, err := EncodeRemoteTransfer(rt)
	if err != nil {
		return err
	}
	subject := RemoteTransferSubjectFor(dstNode)
	if jp, ok := pub.(JetPublisher); ok {
		return jp.JetPublishRaw(ctx, subject, b)
	}
	return pub.Publish(subject, b)
}
