package gwcore

import (
	"sync"
	"testing"

	iconn "github.com/qw576483/clover-server-engine/internal/transport/gateway/conn"
)

// TestCleanupFiresOnDisconnectWithSingleIndexedSession 覆盖「idIndex 里只有这一条会话」时
// 的断线清理路径（单人一个账号连接 = 最常见的断开场景）：
// cleanup 要从 idIndex 的 []*Session 里摘掉本会话。
//
// 本用例直接调 cleanup（真实断连路径 <s.bc.ClosedCh() → cleanup 的同一函数）。
//
// 断言：cleanup 不 panic，onDisconnect 回调收到 (connID, owner)，
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
	is.idIndex[key] = []*Session{s} // ★ 只有一条：删掉它索引就空了
	is.mu.Unlock()
	g.currentConns.Add(1)

	g.cleanup(connID)

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

// TestCleanupFiresOnDisconnectWithSingleExtraIDIndex 覆盖 cleanup 派发断线事件**之前**的
// 额外索引清理路径 —— `removeExtraIDs` → `removeFromIDIndex`（摘掉 "p:playerID" 这类额外索引），
// 该索引里只有一条时会走到「append 截断之后再清尾」这一步（见 session.go 的越界防护注释）。
//
// 真实链路里角色 playerID 索引一定存在，由 GWConnect 的 GWControlBind 追加。
//
// 断言：cleanup 不 panic、断线回调被调到、"p:" 索引被清空。
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

	g.cleanup(connID)

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
// 断开其中一条**不得**影响另一条 —— 「清尾」若写到 sessions[len-1] 上，
// 长度算对时也会把最后一条**存活**会话误置为 nil（索引里出现 nil 会话）。
// 断言：只剩存活那条，且无 nil 空洞。
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
