package mmo

import "github.com/qw576483/clover-server-engine/internal/domain/mmo"

// 本文件把「服务端写 → 客户端 WorldSync 认」的 EPushDataSync 包壳**契约**固定成唯一出处。
//
// 背景：引擎的实体同步有两条路 ——
//  1. **进出视野**（enter / leave）：由 WireEntitySync 自动下发，业务不用管；
//  2. **移动 / 属性变化**：引擎只在进出视野时产事件，这类"高频、按视野路由"的消息**由业务下发**
//     （见 viewers.go 的 PushToViewers）。
//
// 第 2 条要走 EPushDataSync，而它的 body 是「外层 key = 事件名」的包壳：
//
//	{"move":     {"entity_id": 1001, "position": {"x": 12.5, "y": 0, "z": -3}}}
//	{"property": {"entity_id": 1001, "properties": {"hp": 88, "max_hp": 100}}}
//
// 这个形状此前**没有任何构造器**：服务端只能手写 map，写错 key 就静默丢包
// （客户端匹配不上就 return，不抛错、不进日志），而且这段包壳散落在每个业务的 handler 里 ——
// 每个项目重写一遍。与"地图管线"同一条判据：两端共用同一份契约，分处维护必然漂移。
//
// ★ 客户端侧是**宽容匹配**（Runtime/Network/WorldSync.cs）：事件名接受
// move / position / updateposition、property / properties / attr / attribute / update 等别名，
// 字段名接受 entity_id / object_id / id、position / pos、properties / attrs，
// 并做大小写不敏感匹配（因为服务端手动拼装时键的大小写并不统一）。
// 宽容是**兼容手段**，不是新代码可以随便写变体的许可：
// 从这里出去的才是规范形态，别再造第二套。
//
// 本包是**门面**（见 `结构规则.md` §5.1）：实现真身在 `internal/domain/mmo/datasync.go`，
// 这里只做常量转发与函数转发，不含任何实现体。
const (
	// DataSyncEventMove 位置变化的事件名（外层 key）。
	DataSyncEventMove = mmo.DataSyncEventMove
	// DataSyncEventProperty 属性变化的事件名（外层 key）。
	DataSyncEventProperty = mmo.DataSyncEventProperty
)

// DataSyncMsgID 移动 / 属性变化推送所用的消息号（= proto.EPushDataSync，引擎推送段 4003）。
//
// ★ 必须是引擎的 EPushDataSync：这是客户端 OnEntityMove / OnEntityProperty 的唯一触发路径，
// 换成业务消息号不会被 WorldSync 消费（表现为"属性推送收不到"，且服务端没有任何报错）。
const DataSyncMsgID uint32 = mmo.DataSyncMsgID

// NewMovePatch 构造一条「位置变化」的 EPushDataSync body。
//
//	mmo.PushToViewers(g, scene, objID, radius, mmo.DataSyncMsgID, mmo.NewMovePatch(objID, pos))
var NewMovePatch = mmo.NewMovePatch

// NewPropertyPatch 构造一条「属性变化」的 EPushDataSync body。
//
// props 的键就是客户端 OnEntityProperty 收到的属性名（如 "hp" / "max_hp"）。
// ★ 血量类属性请**成对下发**：只推 hp 时客户端拿不到上限，血条会按错的分母画
// （表现：60 血的怪物被画成"一出来就残血"），所以进入视野的初值至少要带 max_hp。
var NewPropertyPatch = mmo.NewPropertyPatch
