package logger

import "sync"

// 本文件提供「同一 key 只打一条日志」的去重闸门（Once* 系列）。
//
// 为什么引擎级必须有它（不是可有可无的糖）：
//  1. 日志硬约束要求「凡是没按预期的分支都必须留一条日志」，而**高频路径**
//     （每帧 / 20Hz 上行 / 定时器 / AOI 推送失败 / 兜底判空）照实打就会刷屏 ——
//     刷屏的代价不只是难看：**真问题会被淹掉**。实测代价：某项目断线期间 20Hz 移动帧
//     被网关按未鉴权拒绝，一次断线刷出 3262 条同样的告警。
//  2. 已有的 Sampler 是按**时间窗口**采样（同一个 key 每秒仍可打 N 条），
//     而"这条失败我已经报过了"要的是**按 key 去重**；两者语义不同，不可互相替代。
//  3. 在它出现之前，引擎与业务都是各自手搓一份 sync.Map 计数去噪
//     （`internal/domain/mmo/entitysync.go` 的 viewPushFailf 就是其中一份），
//     等于每个项目重写一遍 —— 这正是"该下沉"的判据。
//
// 定位：Once 只做**去重闸门**，日志内容与级别仍走 Info/Warn/Error 同一套出口
// （格式、落盘、级别热改全部一致），不是另一套日志系统。
// 反面：**不许**用它掩盖"本该每次都报"的错误（如参数校验失败）—— 那是丢证据，不是降噪。
//
// ★ key 的取值纪律（直接决定内存形态）：
//   - 取**有限集合**的常量，或"常量 + 有限维度"（`move.reject.rate`、`map.load_fail`）；
//   - **不要**把 objID / playerID 直接拼进 key：那是无界集合，sync.Map 会随对象数持续膨胀；
//     确需按对象去重时，用 OnceReset(key) 在对象销毁 / 离场时清理。
//   - **空 key 不做去重**（退化为每次输出）：空 key 指不出调用点，
//     "全局只打第一条"会把后续完全不同的失败一起静默掉 —— 宁可多打，不可漏报。
var onceSeen sync.Map // key string -> struct{}

// once 是统一去重入口：key 首次出现返回 true（调用方据此真的输出日志）。
func once(key string) bool {
	if key == "" {
		return true
	}
	_, loaded := onceSeen.LoadOrStore(key, struct{}{})
	return !loaded
}

// Oncef 按 key 去重输出一条 **Warn** 日志；本次真的输出了则返回 true。
// key 为空时退化为每次都输出（见本文件顶部纪律）。
func Oncef(key string, template string, args ...any) bool {
	return onceLog(key, LogWarnf, template, args...)
}

// OnceDebugf 同 Oncef，级别 Debug。
func OnceDebugf(key string, template string, args ...any) bool {
	return onceLog(key, LogDebugf, template, args...)
}

// OnceInfof 同 Oncef，级别 Info。
func OnceInfof(key string, template string, args ...any) bool {
	return onceLog(key, LogInfof, template, args...)
}

// OnceWarnf 同 Oncef，显式 Warn（语义上等同 Oncef，便于调用点自述级别）。
func OnceWarnf(key string, template string, args ...any) bool {
	return onceLog(key, LogWarnf, template, args...)
}

// OnceErrorf 同 Oncef，级别 Error。
func OnceErrorf(key string, template string, args ...any) bool {
	return onceLog(key, LogErrorf, template, args...)
}

func onceLog(key string, logf func(string, ...any), template string, args ...any) bool {
	if !once(key) {
		return false
	}
	logf(template, args...)
	return true
}

// OnceSeen 该 key 是否已经输出过（排障 / 测试用）。
func OnceSeen(key string) bool {
	_, ok := onceSeen.Load(key)
	return ok
}

// OnceReset 忘掉一个 key，使下次调用重新输出。
// 用途：按对象拼 key 的场景在对象销毁 / 离场时清理，避免 sync.Map 无界增长。
func OnceReset(key string) {
	onceSeen.Delete(key)
}

// OnceResetAll 清空全部去重标记（测试用；也可用于重启业务模块时的整体复位）。
func OnceResetAll() {
	onceSeen.Range(func(k, _ any) bool {
		onceSeen.Delete(k)
		return true
	})
}

// OnceLen 当前已记住的 key 数（排障用：持续增长说明 key 里带上了无界维度）。
func OnceLen() int {
	n := 0
	onceSeen.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
