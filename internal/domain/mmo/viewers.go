package mmo

import (
	"strconv"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/pkg/shared/proto"
)

// 本文件提供「先算观看者、再推送」这套 AOI 推送纪律的**唯一实现**。
//   - 引擎只在**实体进出视野**时产事件（WireEntitySync 负责下发 enter/leave）；
//     移动 / 属性变化这类"高频、需要按视野路由"的消息，引擎只有
//     Scene.Broadcast（全员）与 Scene.SendTo（单点）两个端点，需按视野自行遍历。
//   - 而遍历时必须**只推给真实玩家**：`Scene.Around` 返回的是视野内的**所有对象**，
//     里面包含没有连接的 NPC / 怪物 / 假人。把它们的 id 当推送目标**不会报错**，
//     却会被引擎照原样投递出去 —— 实测客户端因此**同一条事件收到 4 份**
//     （1 个玩家 + 3 个假人各推一次）。这是"AOI 接收者列表 = 观看者列表"的基本纪律。
//   - 过滤的判据也在引擎这边：对象进场景时登记的实体类型（EnterOwnerType），
//     经 Scene.MemberKind 反查即可 —— 业务不需要自建 objID→类型 的表（两份真值必然漂移）。
//
// 归属：真身在 internal/domain/mmo（引擎实现层）；`pkg/domain/mmo` 只做门面
// （接口别名 + 函数转发）。
//
// 用法：
//
//	ids := mmo.PlayerViewers(scene, selfObjID, viewRadius)
//	mmo.PushToViewers(g, scene, selfObjID, viewRadius, def.PushMove, body)   // g 是 *app.Game
type PlayerPusher interface {
	PushToPlayer(playerID string, msgID uint32, v any, opts ...proto.DeliveryMode) error
}

// ViewerScene 是「观看者计算」所需的**最小场景能力面**（只要求 AOI 查询与实体类型反查）。
//
// 用最小接口而不是直接吃 *Scene / 门面 Scene：既让引擎内部的 *Scene 与门面 SceneFacade 都能原样传入，
// 也让单测可以用一个两方法的假场景验证过滤纪律（不必造出完整场景）。
// 编译期断言（见 facade.go）：*Scene 与 SceneFacade 都满足本接口。
type ViewerScene interface {
	// Around 返回 center 附近 radius 内的全部对象 id（含 center 自身）。
	Around(objID uint64, radius float64) []uint64
	// MemberKind 返回对象在本场景登记的实体类型；ok=false 表示不在本场景。
	MemberKind(objID uint64) (data.OwnerType, bool)
}

// PlayerViewers 返回 center 视野内**真实玩家**的 objID 列表（含 center 自己）。
//
// 判据：`MemberKind == data.OwnerPlayer`。玩家以 OwnerPlayer 进场景、NPC/怪物以 OwnerObject 进场景
// （见 data.OwnerType 常量）；用 EnterOwnerType 之外的路径进场景的对象不会被当成观看者。
func PlayerViewers(scene ViewerScene, center uint64, radius float64) []uint64 {
	if scene == nil || radius <= 0 {
		return nil
	}
	around := scene.Around(center, radius)
	out := make([]uint64, 0, len(around))
	for _, id := range around {
		if k, ok := scene.MemberKind(id); ok && k == data.OwnerPlayer {
			out = append(out, id)
		}
	}
	return out
}

// ViewerIDs 与 PlayerViewers 同义，返回**推送目标**的字符串形式。
//
// 为什么单独给字符串版：网关的下行路由表按 account / playerID 的**字符串**索引，
// 推送目标最终必须是字符串。转换口径集中在这里，避免各业务各写一遍 strconv。
func ViewerIDs(scene ViewerScene, center uint64, radius float64) []string {
	ids := PlayerViewers(scene, center, radius)
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, strconv.FormatUint(id, 10))
	}
	return out
}

// PushToViewers 把一条消息推给 center 视野内的真实玩家（含自己），返回成功 / 失败条数。
//
// 失败**不中断**推送（一个玩家掉线不该让其余人收不到），由返回值汇总，调用方按需降频告警
// （推荐 logger.Oncef，避免高频推送把日志刷爆）。
//
// p 传 `*app.Game` 即可（PushToPlayer 签名与 PlayerPusher 逐字一致）；
// 用最小接口而不是直接吃 *app.Game，是为了不形成 domain → app 的反向依赖。
func PushToViewers(p PlayerPusher, scene ViewerScene, center uint64, radius float64, msgID uint32, v any) (sent, failed int) {
	if p == nil {
		return 0, 0
	}
	for _, pid := range ViewerIDs(scene, center, radius) {
		if err := p.PushToPlayer(pid, msgID, v); err != nil {
			failed++
			continue
		}
		sent++
	}
	return sent, failed
}

// PushToViewersOf 与 PushToViewers 同义，但把「谁能看」与「推给谁」解耦：
// watchers 由调用方给定（如已经算好的一份观看者列表，多个消息复用它，避免重复算 AOI）。
func PushToViewersOf(p PlayerPusher, watchers []uint64, msgID uint32, v any) (sent, failed int) {
	if p == nil {
		return 0, 0
	}
	for _, id := range watchers {
		if err := p.PushToPlayer(strconv.FormatUint(id, 10), msgID, v); err != nil {
			failed++
			continue
		}
		sent++
	}
	return sent, failed
}
