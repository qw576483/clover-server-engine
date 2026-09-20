package data

import "sync"

// playerOnlineSet 跟踪本节点当前在线的玩家。用于区分 TierSnapshot 的读写路径：
//   - 在线（isOnline=true）：内存优先 → MySQL 兜底
//   - 离线（isOnline=false）：Redis 优先 → MySQL 兜底（跨节点可读）
//
// key 格式为 "Owner/ID"，如 "player/uid123"。
type playerOnlineSet struct {
	mu sync.RWMutex
	m  map[string]struct{}
}

func (o *playerOnlineSet) set(owner OwnerType, id string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.m == nil {
		o.m = make(map[string]struct{})
	}
	o.m[string(owner)+"/"+id] = struct{}{}
}

func (o *playerOnlineSet) del(owner OwnerType, id string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.m != nil {
		delete(o.m, string(owner)+"/"+id)
	}
}

func (o *playerOnlineSet) has(owner OwnerType, id string) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.m == nil {
		return false
	}
	_, ok := o.m[string(owner)+"/"+id]
	return ok
}

// SetOnline 标记玩家已在本节点上线。后续其 TierSnapshot 数据 Save/Load 走内存路径。
// 业务方应在玩家登录完成后调用（通常在 OnPlayerEnter / OnPlayerJoin 之后）。
func (s *Store) SetOnline(owner OwnerType, id string) {
	s.online.set(owner, id)
}

// SetOffline 标记玩家已从本节点下线。后续其 TierSnapshot 数据 Save/Load 自动切到 Redis 路径，
// 保证跨节点 LoadAll 可读到最新值。
// 业务方应在玩家断开连接时调用（通常在 OnPlayerLeave 中），并在此之前先调 FlushPlayer
// 将残余脏内存数据落库。
func (s *Store) SetOffline(owner OwnerType, id string) {
	s.online.del(owner, id)
}

// isOnline 返回该 owner+id 是否在本节点在线。
func (s *Store) isOnline(owner OwnerType, id string) bool {
	return s.online.has(owner, id)
}
