// 跨机对象迁移的接线层：scene→node 路由登记 + 迁移指令的接收端。
//
// 为什么需要这个文件：mmo 的迁移原语（SceneManager.TransferRemote / HandleRemoteTransfer）
// 与传输层（transport.RouteStore / PublishRemoteTransfer）都已实现，但缺三样接线，
// 导致跨机迁移**发出去没人处理、静默失效**：
//
//  1. 场景创建时没人登记 scene→node 路由（RouteStore 全仓零调用）；
//  2. 路由带 TTL（30s），没人周期性续期，登记必然过期；
//  3. 迁移指令发出去没人订阅（HandleRemoteTransfer 全仓零调用）。
//
// 本文件把三点补齐，并做成 SceneManager 的**可选装配**：单机部署不注入则不启用，
// 行为与「没有这套能力」完全一致（TransferRemote 返回 ErrNoRoute）。
package mmo

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/qw576483/clover-server-engine/internal/transport"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// ErrSceneRouteNotFound 表示路由表里查不到目标场景所在的节点：
// 目标场景不存在，或所在节点已崩溃（登记 TTL 过期被自动摘除）。
var ErrSceneRouteNotFound = errors.New("mmo: destination scene route not found")

// SceneRoute 场景→节点路由表的能力面，用于跨机对象迁移的寻址。
//
// 真身是 internal/transport.RouteStore（基于全局 KV，登记带 TTL）；
// 这里只抽「迁移需要的三个方法」，让 mmo 域不依赖具体实现，也便于替换为测试替身。
type SceneRoute interface {
	// RegisterScene 登记场景所在节点（场景创建时调用）。
	RegisterScene(ctx context.Context, sceneID, nodeID uint64) error
	// RefreshScene 续期登记（场景存活期间周期调用，防止 TTL 过期）。
	RefreshScene(ctx context.Context, sceneID, nodeID uint64) error
	// UnregisterScene 注销登记（场景销毁时调用）。
	UnregisterScene(ctx context.Context, sceneID uint64) error
	// LookupScene 查询场景所在节点；未登记返回 (0,false,nil)。
	LookupScene(ctx context.Context, sceneID uint64) (nodeID uint64, ok bool, err error)
}

// WithClusterRoute 开启「跨机对象迁移」：注入 scene→node 路由表与本节点 ID。
//
// 注入后：
//   - CreateScene / DestroyScene 自动登记 / 注销路由；
//   - Run 期间周期性续期（间隔为 transport.DefaultSceneTTL 的 1/3）；
//   - TransferRemote 会先经路由表查到目标节点，**定向**投递迁移指令。
//
// nodeID 必须在集群内唯一（0 表示未配置，此时等于未开启）。
// 未注入本选项时，TransferRemote 返回 ErrNoRoute —— 单机部署不需要它。
func WithClusterRoute(route SceneRoute, nodeID uint64) Option {
	return func(o *options) {
		o.route = route
		o.nodeID = nodeID
	}
}

// WithRemoteTransferSubscriber 注入订阅能力，用于接收跨机迁移指令（接收端接线）。
//
// 只注入订阅而不注入 WithClusterRoute 时不会开启接收端——因为「本节点 ID」缺失，
// 无法订阅定向 subject。两者必须成对使用。
func WithRemoteTransferSubscriber(sub Subscriber) Option {
	return func(o *options) { o.remoteSub = sub }
}

// sceneRouteIOTimeout 场景路由 KV 读写的兜底超时。
//
// 路由登记/注销/查询走的是全局 KV（网络 IO）。用 context.Background() 意味着
// 底层 IO 一旦卡住就没有任何退出路径，调用线程被永久挂住；这里统一给一个上限，
// 超时至少能返回错误并由调用方记录日志。
const sceneRouteIOTimeout = 3 * time.Second

// sceneRouteCtx 生成带超时的 context（替代裸 context.Background()）。
func sceneRouteCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), sceneRouteIOTimeout)
}

// routeRefreshInterval 场景路由续期间隔。
//
// 取 DefaultSceneTTL 的 1/3：单次续期失败或网络抖动时还有两次机会，登记不会过期。
// 若直接用 TTL 作间隔，任何一次失败都会让路由在 30s 后消失，跨机迁移随即丢失目标。
func routeRefreshInterval() time.Duration { return transport.DefaultSceneTTL / 3 }

// clusterEnabled 是否已开启跨机迁移（需要路由表 + 非零节点 ID）。
func (sm *SceneManager) clusterEnabled() bool {
	return sm.opts.route != nil && sm.opts.nodeID != 0
}

// registerSceneRoute 登记场景路由。未开启集群模式时空操作。
func (sm *SceneManager) registerSceneRoute(sceneID uint64) {
	if !sm.clusterEnabled() {
		return
	}
	ctx, cancel := sceneRouteCtx()
	defer cancel()
	if err := sm.opts.route.RegisterScene(ctx, sceneID, sm.opts.nodeID); err != nil {
		// 登记失败不阻断场景创建：单机仍然可用，只是该场景暂时无法被跨机寻址。
		logger.Warnf("mmo: register scene route scene=%d node=%d failed: %v", sceneID, sm.opts.nodeID, err)
		return
	}
	logger.Infof("mmo: scene %d registered to node %d (ttl=%s)", sceneID, sm.opts.nodeID, transport.DefaultSceneTTL)
}

// unregisterSceneRoute 注销场景路由（场景正常销毁时调用）。
func (sm *SceneManager) unregisterSceneRoute(sceneID uint64) {
	if !sm.clusterEnabled() {
		return
	}
	ctx, cancel := sceneRouteCtx()
	defer cancel()
	if err := sm.opts.route.UnregisterScene(ctx, sceneID); err != nil {
		logger.Warnf("mmo: unregister scene route scene=%d failed: %v", sceneID, err)
		return
	}
	logger.Infof("mmo: scene %d route unregistered", sceneID)
}

// refreshSceneRoutes 续期全部本地场景的路由登记。
//
// 只续期「本节点当前持有」的场景：已在别处接管的场景不该被本节点续期，
// 否则会把路由钉死在一台已经不持有该场景的机器上。
func (sm *SceneManager) refreshSceneRoutes(ctx context.Context) {
	if !sm.clusterEnabled() {
		return
	}
	sm.mu.RLock()
	ids := make([]uint64, 0, len(sm.scenes))
	for id := range sm.scenes {
		ids = append(ids, id)
	}
	sm.mu.RUnlock()

	for _, id := range ids {
		if err := sm.opts.route.RefreshScene(ctx, id, sm.opts.nodeID); err != nil {
			logger.Warnf("mmo: refresh scene route scene=%d node=%d failed: %v", id, sm.opts.nodeID, err)
		}
	}
}

// wireRemoteTransfer 订阅本节点的定向迁移 subject，收到后应用到本地场景。
//
// 未开启集群模式、或未注入订阅能力时空操作（单机部署不需要接收端）。
// 订阅失败只告警不阻断：场景管理器本身仍可用，只是收不到跨机迁入的对象。
func (sm *SceneManager) wireRemoteTransfer() {
	if !sm.clusterEnabled() || sm.opts.remoteSub == nil {
		return
	}
	subject := transport.RemoteTransferSubjectFor(sm.opts.nodeID)
	err := sm.opts.remoteSub.Subscribe(subject, func(_ string, payload []byte) {
		rt, derr := transport.DecodeRemoteTransfer(payload)
		if derr != nil {
			logger.Errorf("mmo: decode remote transfer on node %d failed: %v", sm.opts.nodeID, derr)
			return
		}
		// 接收端必须自己再校验一遍：payload 来自 NATS，任何能往该 subject 发消息的
		// 一方都能伪造它（发送端的校验拦不住伪造包）。DstScene/ObjID 为零会把对象
		// 落到非法实例，NaN/Inf 坐标会污染 AOI 与物理积分。
		if rt.DstScene == 0 || rt.ObjID == 0 ||
			math.IsNaN(rt.X) || math.IsInf(rt.X, 0) ||
			math.IsNaN(rt.Y) || math.IsInf(rt.Y, 0) ||
			math.IsNaN(rt.Z) || math.IsInf(rt.Z, 0) {
			logger.Errorf("mmo: reject invalid remote transfer payload: dst_scene=%d obj=%d pos=(%v,%v,%v)",
				rt.DstScene, rt.ObjID, rt.X, rt.Y, rt.Z)
			return
		}
		if herr := sm.HandleRemoteTransfer(rt); herr != nil {
			// 目标场景不在本节点 / 已销毁：指令投错了目标。打日志而不是静默丢弃——
			// 静默会让「跨机迁移不生效」这种问题在线上查不出来。
			logger.Warnf("mmo: handle remote transfer obj=%d dst_scene=%d src_node=%s failed: %v",
				rt.ObjID, rt.DstScene, rt.SrcNode, herr)
			return
		}
		logger.Infof("mmo: remote transfer applied obj=%d -> scene=%d instance=%d (from node %s)",
			rt.ObjID, rt.DstScene, rt.Instance, rt.SrcNode)
	})
	if err != nil {
		logger.Warnf("mmo: subscribe remote transfer %s failed (non-fatal): %v", subject, err)
		return
	}
	sm.remoteSubject = subject
	logger.Infof("mmo: remote transfer receiver wired on node %d (subject=%s)", sm.opts.nodeID, subject)
}

// unsubscribeRemoteTransfer 反注册跨机迁移的接收端订阅（与 wireRemoteTransfer 对称）。
//
// 幂等：未开启接收端（或已被 Stop 退过一次）时空操作。
// 订阅方未实现 Unsubscriber 时降级为告警 —— 此时只能等底层客户端 Close 才释放，
// 但至少让「订阅没退掉」这件事可观测，而不是静默留着。
func (sm *SceneManager) unsubscribeRemoteTransfer() {
	sm.mu.Lock()
	subject := sm.remoteSubject
	sm.remoteSubject = ""
	sm.mu.Unlock()
	if subject == "" || sm.opts.remoteSub == nil {
		return
	}
	u, ok := sm.opts.remoteSub.(Unsubscriber)
	if !ok {
		logger.Warnf("mmo: 跨机迁移订阅方 %T 不支持反注册，订阅 %s 只能等底层客户端 Close 才能释放",
			sm.opts.remoteSub, subject)
		return
	}
	if err := u.Unsubscribe(subject); err != nil {
		logger.Warnf("mmo: unregister remote transfer subscription %s failed: %v", subject, err)
		return
	}
	logger.Infof("mmo: remote transfer receiver unsubscribed (subject=%s)", subject)
}
