package gwcore

import (
	"sync"
	"testing"

	iconn "github.com/qw576483/clover-server-engine/internal/transport/gateway/conn"
)

// TestCleanupFiresOnDisconnectWithSingleIndexedSession 复现缺陷（清理切片越界 panic ⇒ 断线回调永不触发）。
//
// 复现什么缺陷：会话断开时 cleanup 要从 idIndex 的 []*Session 里摘掉本会话。旧写法是
//
//	sessions = append(sessions[:i], sessions[i+1:]...)
//	sessions[len(sessions)-1] = nil // 清尾
//
// 一旦该索引里**只有这一条会话**（单人一个账号连接 = 最常见的断开场景），
// append 之后切片长度已变成 0，紧接着的「清尾」就是对 sessions[-1] 赋值
// ⇒ panic: index out of range [-1]。
//
// 修复前什么现象：cleanup 在 idIndex 摘除处 panic（真实链路里被 safe.GoSafe 兜住、
// 只打栈不停进程），而派发断线事件的 fireHardDisconnect 在该处**之后**（session.go 尾部），
// 于是 g.OnDisconnect 注册的回调永不触发 —— 现象就是「客户端断了，服务端 0 条业务断线日志」。
// 本用例直接调 cleanup（真实断连路径 <s.bc.ClosedCh() → cleanup 的同一函数），
// 修复前在这里 panic、用例红。
//
// 修复后什么断言：cleanup 不 panic，onDisconnect 回调收到 (connID, owner)，
// 且 idIndex 里的 key 被真正清空（不留僵尸索引去接 NATS 推送）。
func TestCleanupFiresOnDisconnectWithSingleIndexedSession(t *testing.T) {
	const connID, uid = "conn-1", "player-1"

	var mu sync.Mutex
	var events []string
	g, err := New(Config{}, WithOnDisconnect(func(cid, owner string) {
		mu.Lock()
		events = append(events, cid+"|"+owner)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 造一条「已登录」的会话：bc 已绑定 owner（bound=true），并已写入 idIndex（真实路径由登录回包提取 owner 后写入）。
	bc := iconn.Wrap(&stubSession{connID: connID})
	bc.BindPlayer(uid)
	s := &Session{bc: bc, connID: connID}
	ss := g.sessionShardOf(connID)
	ss.mu.Lock()
	ss.sessions[connID] = s
	ss.mu.Unlock()
	key := idPrefixAccount + uid
	is := g.idShardOf(key)
	is.mu.Lock()
	is.idIndex[key] = []*Session{s} // ★ 只有一条：删掉它索引就空了（触发 sessions[-1]）
	is.mu.Unlock()
	g.currentConns.Add(1)

	g.cleanup(connID) // 修复前：此处 panic: index out of range [-1]

	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	if len(got) != 1 || got[0] != connID+"|"+uid {
		t.Fatalf("断线回调未按预期触发：got=%v，期望 [%s|%s]", got, connID, uid)
	}

	is.mu.RLock()
	_, still := is.idIndex[key]
	is.mu.RUnlock()
	if still {
		t.Fatalf("idIndex[%s] 未清空：残留僵尸索引会把 NATS 推送路由到已断开的会话", key)
	}
}

// TestCleanupFiresOnDisconnectWithSingleExtraIDIndex 复现同一缺陷的**第二段**（业务端到端实测栈）。
//
// 复现什么缺陷：cleanup 派发断线事件**之前**还有一段索引清理 ——
// `removeExtraIDs` → `removeFromIDIndex`（摘掉 "p:playerID" 这类额外索引），
// 那里是同一段「append 截断之后再清尾」⇒ 该索引里只有一条时同样
//
//	panic: index out of range [-1]
//	gwcore.(*Gateway).removeFromIDIndex  session.go
//	gwcore.(*Gateway).removeExtraIDs       session.go
//	gwcore.(*Gateway).cleanup              session.go
//
// （这正是修完账号那一段后、业务端到端仍然「0 条断线日志 + panic」的原因：
// 真实链路里角色 playerID 索引一定存在，由 GWConnect 的 GWControlBind 追加。）
//
// 修复后什么断言：cleanup 不 panic、断线回调被调到、"p:" 索引被清空。
func TestCleanupFiresOnDisconnectWithSingleExtraIDIndex(t *testing.T) {
	const connID, uid, playerID = "conn-2", "account-2", "player-2"

	var mu sync.Mutex
	var events []string
	g, err := New(Config{}, WithOnDisconnect(func(cid, owner string) {
		mu.Lock()
		events = append(events, cid+"|"+owner)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	bc := iconn.Wrap(&stubSession{connID: connID})
	bc.BindPlayer(uid)
	s := &Session{bc: bc, connID: connID}
	// 账号索引给两条：让第一段（账号）索引清理走"非唯一"分支，把本用例**隔离**在额外索引那一段上。
	other := &Session{bc: iconn.Wrap(&stubSession{connID: "conn-other"}), connID: "conn-other"}
	other.bc.BindPlayer(uid)
	accKey := idPrefixAccount + uid
	accShard := g.idShardOf(accKey)
	accShard.mu.Lock()
	accShard.idIndex[accKey] = []*Session{other, s}
	accShard.mu.Unlock()

	// 额外索引（playerID 维度）只有本会话一条 —— 触发 removeFromIDIndex 的 sessions[-1]。
	s.bindMu.Lock()
	s.extraIDs = []string{idPrefixPlayer + playerID}
	s.bindMu.Unlock()
	pKey := idPrefixPlayer + playerID
	pShard := g.idShardOf(pKey)
	pShard.mu.Lock()
	pShard.idIndex[pKey] = []*Session{s}
	pShard.mu.Unlock()

	ss := g.sessionShardOf(connID)
	ss.mu.Lock()
	ss.sessions[connID] = s
	ss.mu.Unlock()
	g.currentConns.Add(1)

	g.cleanup(connID) // 修复前：此处 panic（removeFromIDIndex 的 sessions[-1]）

	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	if len(got) != 1 || got[0] != connID+"|"+uid {
		t.Fatalf("断线回调未按预期触发：got=%v，期望 [%s|%s]", got, connID, uid)
	}
	pShard.mu.RLock()
	_, still := pShard.idIndex[pKey]
	pShard.mu.RUnlock()
	if still {
		t.Fatalf("idIndex[%s] 未清空：残留僵尸索引会把 NATS 推送路由到已断开的会话", pKey)
	}
}

// TestCleanupKeepsSiblingSessionsInIndex 对照组：同一 owner 多端在线（索引里 ≥2 条）时，
// 断开其中一条**不得**影响另一条 —— 旧写法的「清尾」写到 sessions[len-1] 上，
// 长度算对时也会把最后一条**存活**会话误置为 nil（索引里出现 nil 会话）。
// 修复后断言：只剩存活那条，且无 nil 空洞。
func TestCleanupKeepsSiblingSessionsInIndex(t *testing.T) {
	const uid = "player-multi"
	var mu sync.Mutex
	var events []string
	g, err := New(Config{}, WithOnDisconnect(func(cid, owner string) {
		mu.Lock()
		events = append(events, cid+"|"+owner)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mk := func(connID string) *Session {
		bc := iconn.Wrap(&stubSession{connID: connID})
		bc.BindPlayer(uid)
		s := &Session{bc: bc, connID: connID}
		ss := g.sessionShardOf(connID)
		ss.mu.Lock()
		ss.sessions[connID] = s
		ss.mu.Unlock()
		return s
	}
	first, second := mk("conn-a"), mk("conn-b")
	key := idPrefixAccount + uid
	is := g.idShardOf(key)
	is.mu.Lock()
	is.idIndex[key] = []*Session{first, second}
	is.mu.Unlock()
	g.currentConns.Add(2)

	g.cleanup("conn-a")

	is.mu.RLock()
	left := append([]*Session(nil), is.idIndex[key]...)
	is.mu.RUnlock()
	if len(left) != 1 || left[0] != second {
		t.Fatalf("多端在线时断开一条应只摘掉自己：left=%v（期望只剩 conn-b）", left)
	}
	mu.Lock()
	n := len(events)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("断线回调应触发 1 次，实际 %d 次", n)
	}
}
