// #nosec G115 -- 取模结果恒小于 len(Spawns)（正数）。

package mapdata

import "clover-server-engine/pkg/shared/geom"

// 本文件补齐出生点的**分配**能力（SpawnAt 只回答"第 i 个出生点在哪"，
// 不回答"这个玩家该去哪个出生点"）。
//
// 为什么这属于引擎而不是每个业务各自写：
//   - 消费者是引擎自己的容器：出生点列表由 Map 解码并净化（spawn.go），
//     业务拿到的只是 `[]geom.Vec3` + 越界回落，分配规则却得自己发明一遍；
//   - 每个"多人在同一张图"的项目都要写一遍，且写错的失败是**静默**的：
//     所有人叠在同一个点上时，AOI 事件、视野互见、互相挡路全部表现异常，
//     但服务端不会有任何报错（只是"看起来人只看到自己"）。

// SpawnFor 按 key 轮转取出生点：key 建议用 objID / 角色 id（稳定且分布均匀）。
//
// 为什么按 key 轮转而不是随机：轮转让**同一个玩家每次进图落到同一个点**
// （可复现、可排障、断线重连不会突然出现在地图另一头），同时把不同玩家摊开。
// 地图没有出生点时回落到原点（与 SpawnAt 同一口径）。
//
// ★ 取模必须用**出生点个数**。反面写法 `% (len+1)` 会让 0 号点被分到两次，
// 在"只有一个出生点"的地图上更是每次都走越界回落 —— 看着能跑，其实在偷懒。
func (m *Map) SpawnFor(key uint64) geom.Vec3 {
	if m == nil {
		return geom.Vec3{}
	}
	if len(m.Spawns) == 0 {
		return m.Origin
	}
	return m.SpawnAt(int(key % uint64(len(m.Spawns))))
}

// SpawnNear 在 center 附近找一个**可走**的落点：沿 X 轴两侧交替尝试
// （center±step、center±2*step …），返回 (落点, 是否找到)。
//
// 用途：把召唤物 / 怪物 / 陪玩 NPC 刷在指定对象旁边（"按一下就出现在我身边"）。
// 直接刷在 center 上会和人重叠，刷在不可走格里则会被卡住 / 被服务端拒绝移动。
//
// tries <= 0 时取 4；walkable 判据用地图自己的 WalkableAt（越界 = 不可走）。
// 找不到任何可走点时返回 (center, false)，**不替调用方瞎猜落点** ——
// 由调用方决定回落到 SpawnFor 还是报错（引擎不知道"这次刷怪失败能不能容忍"）。
func (m *Map) SpawnNear(center geom.Vec3, step float64, tries int) (geom.Vec3, bool) {
	if m == nil {
		return center, false
	}
	if tries <= 0 {
		tries = 4
	}
	if step <= 0 {
		step = 1.0
	}
	// 先试 center 本身（调用方给的近点常常已经可走，没必要强行偏移）。
	if m.WalkableAt(center.X, center.Z) {
		return center, true
	}
	for i := 1; i <= tries; i++ {
		offset := float64(i) * step
		for _, dx := range [2]float64{offset, -offset} {
			cand := geom.Vec3{X: center.X + dx, Y: center.Y, Z: center.Z}
			if m.WalkableAt(cand.X, cand.Z) {
				return cand, true
			}
		}
	}
	return center, false
}
