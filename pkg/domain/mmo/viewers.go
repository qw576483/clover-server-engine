package mmo

import "github.com/qw576483/clover-server-engine/internal/domain/mmo"

// 本文件提供「先算观看者、再推送」这套 AOI 推送纪律的**唯一入口**。
// 为什么必须有（每个 AOI 项目都要写一遍，且写错是静默的）：
//   - 引擎只在**实体进出视野**时产事件（WireEntitySync 负责下发 enter/leave）；
//     移动 / 属性变化这类"高频、需要按视野路由"的消息，引擎此前只有
//     Scene.Broadcast（全员）与 Scene.SendTo（单点）两个端点 —— 业务只能自己遍历。
//   - 而遍历时必须**只推给真实玩家**：`Scene.Around` 返回的是视野内的**所有对象**，
//     里面包含没有连接的 NPC / 怪物 / 假人。把它们的 id 当推送目标**不会报错**，
//     却会被引擎照原样投递出去 —— 实测客户端因此**同一条事件收到 4 份**
//     （1 个玩家 + 3 个假人各推一次）。这是"AOI 接收者列表 = 观看者列表"的基本纪律。
//   - 过滤的判据也在引擎这边：对象进场景时登记的实体类型（EnterOwnerType），
//     经 Scene.MemberKind 反查即可 —— 业务不需要自建 objID→类型 的表（两份真值必然漂移）。
//
// 用法：
//
//	ids := mmo.PlayerViewers(scene, selfObjID, viewRadius)
//	mmo.PushToViewers(g, scene, selfObjID, viewRadius, def.PushMove, body)   // g 是 *app.Game
//
// 本包是**门面**（见 `结构规则.md` §5.1）：实现真身在 `internal/domain/mmo/viewers.go`，
// 这里只做接口别名与函数转发，不含任何实现体。

// PlayerPusher 最小推送能力面（`*app.Game` 直接满足）。
type PlayerPusher = mmo.PlayerPusher

// ViewerScene 是「观看者计算」所需的最小场景能力面（AOI 查询 + 实体类型反查）。
// 门面 Scene 接口（`mmo.Scene`）是它的超集，可直接传入；单测也可用只实现这两个方法的假场景。
type ViewerScene = mmo.ViewerScene

// PlayerViewers 返回 center 视野内**真实玩家**的 objID 列表（含 center 自己）。
//
// 判据：`MemberKind == data.OwnerPlayer`。玩家以 OwnerPlayer 进场景、NPC/怪物以 OwnerObject 进场景
// （见 data.OwnerType 常量）；用 EnterOwnerType 之外的路径进场景的对象不会被当成观看者。
var PlayerViewers = mmo.PlayerViewers

// ViewerIDs 与 PlayerViewers 同义，返回**推送目标**的字符串形式。
//
// 为什么单独给字符串版：网关的下行路由表按 account / playerID 的**字符串**索引，
// 推送目标最终必须是字符串。转换口径集中在这里，避免各业务各写一遍 strconv。
var ViewerIDs = mmo.ViewerIDs

// PushToViewers 把一条消息推给 center 视野内的真实玩家（含自己），返回成功 / 失败条数。
//
// 失败**不中断**推送（一个玩家掉线不该让其余人收不到），由返回值汇总，调用方按需降频告警
// （推荐 logger.Oncef，避免高频推送把日志刷爆）。
//
// p 传 `*app.Game` 即可（PushToPlayer 签名与 PlayerPusher 逐字一致）；
// 用最小接口而不是直接吃 *app.Game，是为了不形成 domain → app 的反向依赖。
var PushToViewers = mmo.PushToViewers

// PushToViewersOf 与 PushToViewers 同义，但把「谁能看」与「推给谁」解耦：
// watchers 由调用方给定（如已经算好的一份观看者列表，多个消息复用它，避免重复算 AOI）。
var PushToViewersOf = mmo.PushToViewersOf
