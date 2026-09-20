package event

// MatchPattern 通配符模式匹配。
//
// 支持两种通配符（与 MQTT topic 类似）：
//   - `*`  单级通配符：匹配一层 topic segment（不含点号）。
//   - `**` 多级通配符：匹配多层（必须单独出现，不与其他字符组合）；模式末尾的 `**` 不匹配零层。
//
// 示例：
//
//	MatchPattern("player.*", "player.login")  → true
//	MatchPattern("player.*", "player")         → false
//	MatchPattern("room.*.join", "room.abc.join") → true
//	MatchPattern("room.*.join", "room.abc.leave") → false
//	MatchPattern("*.created", "player.created")   → true
//	MatchPattern("player.**", "player.login")    → true
//	MatchPattern("player.**", "player")           → false（末尾 ** 不匹配零层）
func MatchPattern(pattern, eventType string) bool {
	return matchSegments(splitTopic(pattern), splitTopic(eventType))
}

// splitTopic 将 topic 字符串按 "." 分割为 segments。
func splitTopic(topic string) []string {
	if topic == "" {
		return nil
	}
	segments := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(topic); i++ {
		if topic[i] == '.' {
			segments = append(segments, topic[start:i])
			start = i + 1
		}
	}
	segments = append(segments, topic[start:])
	return segments
}

// matchSegments 递归匹配 pattern segments 与 eventType segments。
func matchSegments(pattern, event []string) bool {
	pi, ei := 0, 0
	for pi < len(pattern) && ei < len(event) {
		p := pattern[pi]
		if p == "**" {
			// ** 匹配剩余所有段
			if pi == len(pattern)-1 {
				return true
			}
			// ** 出现在中间：跳过中间段，尝试匹配后续
			for ei < len(event) {
				if matchSegments(pattern[pi+1:], event[ei:]) {
					return true
				}
				ei++
			}
			return matchSegments(pattern[pi+1:], nil)
		}
		if p == "*" || p == event[ei] {
			pi++
			ei++
		} else {
			return false
		}
	}
	// 双方同时耗尽
	return pi == len(pattern) && ei == len(event)
}
