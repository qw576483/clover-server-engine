// Package state 是 master 核心状态层：接口定义、数据载体与内存 State 实现。
// server、client 均依赖本包的类型，避免循环引用。
package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	redis "github.com/qw576483/clover-server-engine/internal/domain/data/store/redis"
	"github.com/qw576483/clover-server-engine/internal/domain/master/metrics"
	"github.com/qw576483/clover-server-engine/pkg/domain/master"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/memrank"
)

// NodeType 节点类型：不同角色节点在 master 中分开调度。
const (
	// NodeTypeGame 游戏服：承载场景地图 + 玩家玩法。
	NodeTypeGame = "game"
	// NodeTypeBattle 战斗服：只跑房间战斗。
	NodeTypeBattle = "battle"
)

// Node 描述一个 master 服务节点及其元数据。
// json tag 一律 snake_case，与 wire.go 同文件其余字段（node_id 等）保持一致；
// 无 tag 时线上键名是 Go 默认的 ID/Addr/Type（与协议文档口径不符）。
type Node struct {
	ID       string   `json:"id"`
	Addr     string   `json:"addr"`
	Type     string   `json:"type"`               // NodeTypeGame / NodeTypeBattle
	Tags     []string `json:"tags,omitempty"`     // 业务标签：启动时从配置注入，master 侧按 tag 查询节点
	Capacity int      `json:"capacity,omitempty"` // 最大可承载负载
	Load     int      `json:"load,omitempty"`     // 当前合成负载分（由外部心跳上报）
}

// 错误定义。
var (
	ErrNodeNotFound = errors.New("master/state: node not found")
	ErrNoNodes      = errors.New("master/state: no available nodes")
)

// —— 接口 ——
//
// Authority 是 master 节点注册的最小权威接口。
type Authority interface {
	RegisterNode(ctx context.Context, node Node) error
	RemoveNode(ctx context.Context, nodeID string) error
}

// RankService 是排行榜服务接口。
type RankService interface {
	Top(ctx context.Context, board string, n int) ([]master.RankMember, error)
	Add(ctx context.Context, board string, member string, score float64, extra json.RawMessage) error
	AddOnlyUpdateScore(ctx context.Context, board, member string, score float64) error
	Incr(ctx context.Context, board, member string, delta float64) (float64, error)
	IncrOnlyUpdateScore(ctx context.Context, board, member string, delta float64) (float64, error)
	GetMember(ctx context.Context, board string, member string) (master.RankMember, bool, error)
	GetRank(ctx context.Context, board, member string) (int, bool, error)
	GetByRankRange(ctx context.Context, board string, start, stop int) ([]master.RankMember, error)
	GetByScoreRange(ctx context.Context, board string, min, max float64) ([]master.RankMember, error)
	Remove(ctx context.Context, board, member string) error
	Clear(ctx context.Context, board string, deleteBackup bool) error
	Len(ctx context.Context, board string) (int, error)
	BackupAll(ctx context.Context) error
	Backup(ctx context.Context, board string) error
	RestoreAll(ctx context.Context) error
	Restore(ctx context.Context, board string) error
	SetRankThresholds(ctx context.Context, board string, ts master.Thresholds) error
}

// —— State：节点注册 + 玩家定位 + 排行榜 + session token ——

type State struct {
	mu           sync.RWMutex
	nodes        map[string]Node
	byType       map[string]map[string]struct{}
	byTag        map[string]map[string]struct{} // tag → set[nodeID]
	players      map[string]string              // uid → nodeID
	nodePlayers  map[string]map[string]struct{} // nodeID → set[uid] 反向索引，O(1) 定位节点下所有玩家
	rankMgr      *memrank.Manager
	tokenBackend sessionTokenBackend   // session token 存储后端（memory / redis）
	redis        *redis.Client         // 可选：排行榜备份/恢复
	onNodeReg    []func(nodeID string) // 节点注册回调（健康探测器纳入跟踪）
	onNodeDel    []func(nodeID string) // 节点摘除回调
}

func NewState() *State {
	return &State{
		nodes:        make(map[string]Node),
		byType:       make(map[string]map[string]struct{}),
		byTag:        make(map[string]map[string]struct{}),
		players:      make(map[string]string),
		nodePlayers:  make(map[string]map[string]struct{}),
		rankMgr:      memrank.NewManager(),
		tokenBackend: newMemorySessionBackend(24 * time.Hour),
	}
}

// —— Authority 实现 ——

func (s *State) RegisterNode(_ context.Context, node Node) error {
	// 节点 ID 直接来自网络请求（server.go 的 MsgRegisterNode），空 ID 会污染
	// nodes/byType/byTag 与 NodeIDsByType 等查询结果，必须在入口拒绝。
	if node.ID == "" {
		return fmt.Errorf("master/state: register node: id must not be empty")
	}
	if node.Addr == "" {
		return fmt.Errorf("master/state: register node %s: addr must not be empty", node.ID)
	}
	if node.Type == "" {
		// 空 Type 会把节点塞进 byType[""]，污染按类型查询/选点转发。
		return fmt.Errorf("master/state: register node %s: type must not be empty", node.ID)
	}
	// Tags 深拷贝：直接存入调用方传入的切片时，调用方后续 append/改写会绕过
	// byTag 索引维护，使 NodesByTag/NodeIDsByTag 与 s.nodes 不一致。
	if len(node.Tags) > 0 {
		node.Tags = append([]string(nil), node.Tags...)
	}
	s.mu.Lock()
	// 必须在写入 map **之前**判断，否则永远为 true。
	_, existed := s.nodes[node.ID]
	var oldType string
	// 若节点已存在，先清理旧的 byTag / byType 索引（tags 与 type 都可能变更）。
	if existed {
		old := s.nodes[node.ID]
		oldType = old.Type
		// 类型变更时必须同时摘掉旧类型的索引：NodesByType / NodeIDsByType 只校验
		// 「节点还在」，不校验类型一致，漏摘会让旧类型下长期挂着一个幽灵节点
		// （按类型选节点做转发时就会投到一个不干这活儿的节点上）。
		if old.Type != node.Type {
			delete(s.byType[old.Type], node.ID)
			if len(s.byType[old.Type]) == 0 {
				delete(s.byType, old.Type)
			}
		}
		for _, tag := range old.Tags {
			delete(s.byTag[tag], node.ID)
			if len(s.byTag[tag]) == 0 {
				delete(s.byTag, tag)
			}
		}
	}
	s.nodes[node.ID] = node
	if s.byType[node.Type] == nil {
		s.byType[node.Type] = make(map[string]struct{})
	}
	s.byType[node.Type][node.ID] = struct{}{}
	// 维护 byTag 索引。
	for _, tag := range node.Tags {
		if s.byTag[tag] == nil {
			s.byTag[tag] = make(map[string]struct{})
		}
		s.byTag[tag][node.ID] = struct{}{}
	}
	s.mu.Unlock()

	// 指标必须与 RemoveNodeWithReason 严格配对（后者按删除时的当前类型 Dec）：
	// - 首次注册：按新类型 Inc；
	// - 换 Type 重注册：旧类型此前 Inc 过、若不处理会永久 +1，且摘除时按新类型
	//   Dec 会让新类型变负（game→battle→删除后 nodes{game}=1、nodes{battle}=-1）。
	//   故先按旧类型配对递减（retag 原因），再按新类型递增。
	if !existed {
		metrics.NodeRegistered(string(node.Type))
	} else if oldType != node.Type {
		metrics.NodeRemoved(oldType, metrics.RemoveReasonRetag)
		metrics.NodeRegistered(string(node.Type))
	}

	// 在锁外触发注册回调，避免回调重入 State 造成死锁。
	s.fireNodeRegistered(node.ID)
	return nil
}

func (s *State) RemoveNode(ctx context.Context, nodeID string) error {
	return s.RemoveNodeWithReason(ctx, nodeID, metrics.RemoveReasonAPI)
}

// RemoveNodeWithReason 按指定原因摘除节点，reason 用于 metrics 区分摘除来源
// （API 正常下线 / Dead 心跳超时自动摘除）。
func (s *State) RemoveNodeWithReason(_ context.Context, nodeID string, reason string) error {
	s.mu.Lock()
	node, ok := s.nodes[nodeID]
	if !ok {
		s.mu.Unlock()
		return ErrNodeNotFound
	}
	// 走到这里说明节点确实存在且即将被删除，与 RegisterNode 的 Inc 严格配对。
	// 上面的 !ok 提前返回保证了重复摘除不会重复递减。
	metrics.NodeRemoved(string(node.Type), reason)
	delete(s.byType[node.Type], nodeID)
	if len(s.byType[node.Type]) == 0 {
		delete(s.byType, node.Type)
	}
	// 清理 byTag 索引。
	for _, tag := range node.Tags {
		delete(s.byTag[tag], nodeID)
		if len(s.byTag[tag]) == 0 {
			delete(s.byTag, tag)
		}
	}
	delete(s.nodes, nodeID)
	// 清掉指向该节点的玩家定位，否则 PlayerNode 会一直返回已下线的节点，
	// 导致跨节点转发投递到不存在的目标。
	// 通过反向索引 nodePlayers 直接定位该节点下的所有玩家，O(k) 而非 O(N)。
	for uid := range s.nodePlayers[nodeID] {
		delete(s.players, uid)
	}
	delete(s.nodePlayers, nodeID)
	s.mu.Unlock()

	s.fireNodeRemoved(nodeID)
	return nil
}

// cloneNode 返回节点的深拷贝（Tags 切片复制）：读接口返回值与内部状态隔离，
// 调用方改写 Tags 不会绕过 byTag 索引维护。
func cloneNode(n Node) Node {
	if len(n.Tags) > 0 {
		n.Tags = append([]string(nil), n.Tags...)
	}
	return n
}

// NodeIDsByType 返回指定类型下当前仍存活（已注册且未被移除）的节点 ID 列表，
// 按 ID 升序（与 AllNodes/AliveNodes 口径一致，选点转发顺序可复现）。
// 节点在断线时由 RemoveNode 清理，因此该结果可反映实时存活状态。
func (s *State) NodeIDsByType(typ string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.byType[typ]
	out := make([]string, 0, len(set))
	for id := range set {
		if _, ok := s.nodes[id]; ok {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// NodesByType 返回指定类型的所有节点，按 ID 升序。
func (s *State) NodesByType(typ string) []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.byType[typ]
	out := make([]Node, 0, len(set))
	for id := range set {
		if n, ok := s.nodes[id]; ok {
			out = append(out, cloneNode(n))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// NodesByTag 返回包含指定 tag 的所有存活节点，按 ID 升序。
func (s *State) NodesByTag(tag string) []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.byTag[tag]
	out := make([]Node, 0, len(set))
	for id := range set {
		if n, ok := s.nodes[id]; ok {
			out = append(out, cloneNode(n))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// NodeIDsByTag 返回包含指定 tag 的存活节点 ID 列表，按 ID 升序。
func (s *State) NodeIDsByTag(tag string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.byTag[tag]
	out := make([]string, 0, len(set))
	for id := range set {
		if _, ok := s.nodes[id]; ok {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// NodeByID 返回指定节点的注册信息（Tags 为副本）。
func (s *State) NodeByID(nodeID string) (Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return Node{}, false
	}
	return cloneNode(n), true
}

// AllNodes 返回全部已注册节点，按节点 ID 升序，结果稳定可测。
func (s *State) AllNodes() []Node {
	s.mu.RLock()
	out := make([]Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, cloneNode(n))
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// UpdateLoad 更新节点的当前负载分。
func (s *State) UpdateLoad(nodeID string, load int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return ErrNodeNotFound
	}
	n.Load = load
	s.nodes[nodeID] = n
	return nil
}

// OnNodeRegistered 注册"节点注册成功"回调，供健康探测器纳入跟踪。
func (s *State) OnNodeRegistered(fn func(nodeID string)) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	s.onNodeReg = append(s.onNodeReg, fn)
	s.mu.Unlock()
}

// OnNodeRemoved 注册"节点摘除"回调。
func (s *State) OnNodeRemoved(fn func(nodeID string)) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	s.onNodeDel = append(s.onNodeDel, fn)
	s.mu.Unlock()
}

// fireNodeRegistered 在锁外触发节点注册回调。
func (s *State) fireNodeRegistered(nodeID string) {
	s.mu.RLock()
	fns := append([]func(string){}, s.onNodeReg...)
	s.mu.RUnlock()
	for _, fn := range fns {
		fn(nodeID)
	}
}

// fireNodeRemoved 在锁外触发节点摘除回调。
func (s *State) fireNodeRemoved(nodeID string) {
	s.mu.RLock()
	fns := append([]func(string){}, s.onNodeDel...)
	s.mu.RUnlock()
	for _, fn := range fns {
		fn(nodeID)
	}
}

// —— RankService 实现 ——

func (s *State) Top(_ context.Context, board string, n int) ([]master.RankMember, error) {
	if n <= 0 {
		return s.rankMgr.All(board), nil
	}
	return s.rankMgr.Top(board, n), nil
}

func (s *State) Add(_ context.Context, board string, member string, score float64, extra json.RawMessage) error {
	b := s.rankMgr.GetOrCreate(board)
	b.Set(member, score, extra)
	return nil
}

func (s *State) AddOnlyUpdateScore(_ context.Context, board, member string, score float64) error {
	b := s.rankMgr.GetOrCreate(board)
	_, ok := b.AddOnlyUpdateScore(member, score, nil)
	if !ok {
		return fmt.Errorf("rank: member %q not found in board %q", member, board)
	}
	return nil
}

func (s *State) Incr(_ context.Context, board, member string, delta float64) (float64, error) {
	b := s.rankMgr.GetOrCreate(board)
	return b.Incr(member, delta), nil
}

func (s *State) IncrOnlyUpdateScore(_ context.Context, board, member string, delta float64) (float64, error) {
	b := s.rankMgr.GetOrCreate(board)
	score, ok := b.IncrOnlyUpdateScore(member, delta)
	if !ok {
		return 0, fmt.Errorf("rank: member %q not found in board %q", member, board)
	}
	return score, nil
}

func (s *State) GetMember(_ context.Context, board string, member string) (master.RankMember, bool, error) {
	rm, ok := s.rankMgr.GetMember(board, member)
	return rm, ok, nil
}

func (s *State) GetRank(_ context.Context, board, member string) (int, bool, error) {
	rm, ok := s.rankMgr.GetMember(board, member)
	if !ok {
		return 0, false, nil
	}
	return rm.Rank, true, nil
}

func (s *State) GetByRankRange(_ context.Context, board string, start, stop int) ([]master.RankMember, error) {
	// 业务端按 1-based 排名传参（第1名=start=1），
	// 整条链路（Manager.GetByRankRange → SortedSet.GetByRankRange）统一 1-based。
	return s.rankMgr.GetByRankRange(board, start, stop), nil
}

func (s *State) GetByScoreRange(_ context.Context, board string, min, max float64) ([]master.RankMember, error) {
	// 纯查询不建榜：GetOrCreate 会让一次读操作凭空产生一个空榜，
	// 既污染 Boards()/Names() 快照，也会让备份与持久化多出无意义条目。
	b, ok := s.rankMgr.Get(board)
	if !ok {
		return nil, nil
	}
	members := b.GetByScoreRange(memrank.RangeOpts{Min: min, Max: max})
	ts := s.rankMgr.GetThresholds(board)
	return ts.AdjustMembers(members), nil
}

func (s *State) Remove(_ context.Context, board, member string) error {
	b, ok := s.rankMgr.Get(board)
	if !ok {
		return fmt.Errorf("rank: board %q not found", board)
	}
	b.Remove(member)
	return nil
}

func (s *State) Clear(_ context.Context, board string, deleteBackup bool) error {
	b, ok := s.rankMgr.Get(board)
	if !ok {
		return fmt.Errorf("rank: board %q not found", board)
	}
	b.Clear()
	// 先清门槛再注销：Manager.Unregister 只删 boards、不动 thresholds，
	// 反复 Clear 会让 thresholds 里堆积已注销榜单的残留条目。
	s.rankMgr.SetRankThresholds(board, nil)
	s.rankMgr.Unregister(board)
	if deleteBackup {
		if rdb := s.rdb(); rdb != nil {
			key := rankBackupKey(board)
			dctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if _, err := rdb.Del(dctx, key); err != nil {
				// 删除备份 key 失败不能静默：残留的旧备份会在下次 Restore 时复活已清理的榜。
				logger.Errorf("rank: clear backup key %s failed: %v", key, err)
			}
			cancel()
		}
	}
	return nil
}

func (s *State) Len(_ context.Context, board string) (int, error) {
	b, ok := s.rankMgr.Get(board)
	if !ok {
		return 0, nil
	}
	return b.Len(), nil
}

// —— RankService 备份/恢复 ——

const rankBackupPrefix = "clover:rank:backup:"

// rankBackupAllKey 全量备份 key。
//
// 注意与单榜 key 的区别：此前单榜 key = prefix+board、全量 = prefix+"all"，
// 名恰为 "all" 的榜与全量快照共用同一 Redis key —— Backup("all") 与 BackupAll()、
// Clear("all", true) 与全量快照互相覆盖/互相删除。单榜 key 改为带 "board:" 段后不再冲突。
const rankBackupAllKey = rankBackupPrefix + "all"

func rankBackupKey(board string) string {
	return rankBackupPrefix + "board:" + board
}

// rankBackupAll 的序列化壳。
type rankBackupShell struct {
	Boards map[string]rankBackupEntry `json:"boards"`
}

type rankBackupEntry struct {
	Members    []master.RankMember `json:"members"`
	Thresholds master.Thresholds   `json:"thresholds,omitempty"`
}

// SetRedis 注入 Redis 客户端（供备份/恢复使用）。
func (s *State) SetRedis(rdb *redis.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redis = rdb
}

// ErrRedisUnavailable 未注入 Redis 时的备份/恢复错误。
var ErrRedisUnavailable = errors.New("rank: redis client not configured")

// rdb 在锁保护下取出 Redis 客户端；未注入时返回 nil。
// redis 字段由 SetRedis 写入，直接裸读存在数据竞争，故统一走这里。
func (s *State) rdb() *redis.Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.redis
}

// RDB 返回底层 Redis 客户端（供 MasterGame.Stop 关闭等场景使用）。
// 仅在已通过 SetRedis 注入时非 nil。
func (s *State) RDB() *redis.Client { return s.rdb() }

// SetRankThresholds 设置段位门槛。
// 先校验再写入：memrank 内部对非法门槛只以标准库日志告警并拒绝写入，
// 接口层此前无条件返回 nil（回 OK:true）——配置静默不生效、调用方无从感知。
func (s *State) SetRankThresholds(_ context.Context, board string, ts master.Thresholds) error {
	if err := ts.Validate(); err != nil {
		logger.Warnf("rank: set thresholds for board %q rejected: %v", board, err)
		return fmt.Errorf("rank: invalid thresholds for board %q: %w", board, err)
	}
	s.rankMgr.SetRankThresholds(board, ts)
	return nil
}

// BackupAll 备份全部排行榜到 Redis（含门槛）。
func (s *State) BackupAll(ctx context.Context) error {
	rdb := s.rdb()
	if rdb == nil {
		return ErrRedisUnavailable
	}
	shell := rankBackupShell{Boards: make(map[string]rankBackupEntry)}
	names := s.rankMgr.Names()
	for _, name := range names {
		members, ok := s.rankMgr.GetAll(name)
		if !ok {
			continue
		}
		// 不能因为 members 为空就跳过：RestoreAll 会先清空全部榜单再按备份重建，
		// 漏掉空榜会导致它的门槛配置在一次 Backup/Restore 往返后彻底丢失。
		if members == nil {
			members = []master.RankMember{}
		}
		shell.Boards[name] = rankBackupEntry{
			Members:    members,
			Thresholds: s.rankMgr.GetThresholds(name),
		}
	}
	return rdb.SetJSON(ctx, rankBackupAllKey, shell, 0)
}

// Backup 备份单个排行榜到 Redis（含门槛）。
func (s *State) Backup(ctx context.Context, board string) error {
	rdb := s.rdb()
	if rdb == nil {
		return ErrRedisUnavailable
	}
	members, ok := s.rankMgr.GetAll(board)
	entry := rankBackupEntry{
		Members:    members,
		Thresholds: s.rankMgr.GetThresholds(board),
	}
	if !ok || entry.Members == nil {
		entry.Members = []master.RankMember{}
	}
	return rdb.SetJSON(ctx, rankBackupKey(board), entry, 0)
}

// RestoreAll 从 Redis 恢复全部排行榜（含门槛，清空现有再写入）。
func (s *State) RestoreAll(ctx context.Context) error {
	rdb := s.rdb()
	if rdb == nil {
		return ErrRedisUnavailable
	}
	var shell rankBackupShell
	if err := rdb.GetJSON(ctx, rankBackupAllKey, &shell); err != nil {
		return fmt.Errorf("rank restore all: %w", err)
	}
	if shell.Boards == nil {
		// 空壳（如 {"boards":null}）：很可能是损坏/空写入。若继续执行，下面的
		// 「清空所有现有榜」会把全部榜单清掉且不报错、不留痕（数据全灭）。
		logger.Errorf("rank: restore all refused: backup payload has no boards (possibly corrupt)")
		return fmt.Errorf("rank restore all: backup contains no boards, refuse to wipe current state")
	}

	// 清空所有现有排行榜
	for _, name := range s.rankMgr.Names() {
		if b, ok := s.rankMgr.Get(name); ok {
			b.Clear()
		}
		s.rankMgr.SetRankThresholds(name, nil)
		s.rankMgr.Unregister(name)
	}

	for name, entry := range shell.Boards {
		board := s.rankMgr.GetOrCreate(name)
		for _, m := range entry.Members {
			board.Set(m.Member, m.Score, m.Extra)
		}
		if len(entry.Thresholds) > 0 {
			s.rankMgr.SetRankThresholds(name, entry.Thresholds)
		}
	}
	return nil
}

// Restore 从 Redis 恢复单个排行榜（含门槛，清空现有再恢复）。
func (s *State) Restore(ctx context.Context, board string) error {
	rdb := s.rdb()
	if rdb == nil {
		return ErrRedisUnavailable
	}
	var entry rankBackupEntry
	if err := rdb.GetJSON(ctx, rankBackupKey(board), &entry); err != nil {
		return fmt.Errorf("rank restore %q: %w", board, err)
	}
	if b, ok := s.rankMgr.Get(board); ok {
		b.Clear()
		s.rankMgr.SetRankThresholds(board, nil)
		s.rankMgr.Unregister(board)
	}
	// 即使成员为空也要重建榜单并恢复门槛：提前 return 会让空榜的门槛配置丢失。
	boardObj := s.rankMgr.GetOrCreate(board)
	for _, m := range entry.Members {
		boardObj.Set(m.Member, m.Score, m.Extra)
	}
	if len(entry.Thresholds) > 0 {
		s.rankMgr.SetRankThresholds(board, entry.Thresholds)
	}
	return nil
}

// —— 玩家定位 ——

func (s *State) RegisterPlayer(_ context.Context, uid, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 若玩家已登记在其他节点，先从旧节点的反向索引中移除。
	if oldNode, ok := s.players[uid]; ok && oldNode != nodeID {
		delete(s.nodePlayers[oldNode], uid)
		if len(s.nodePlayers[oldNode]) == 0 {
			delete(s.nodePlayers, oldNode)
		}
	}
	s.players[uid] = nodeID
	// 维护 nodeID → set[uid] 反向索引。
	if s.nodePlayers[nodeID] == nil {
		s.nodePlayers[nodeID] = make(map[string]struct{})
	}
	s.nodePlayers[nodeID][uid] = struct{}{}
	return nil
}

// RemovePlayer 移除玩家定位。若 nodeID 非空，仅当 uid 当前映射到的节点与 nodeID 一致时才删除；
// nodeID 为空则无条件删除（用于正常下线场景）。
func (s *State) RemovePlayer(_ context.Context, uid, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.players[uid]
	if !ok {
		return nil
	}
	if nodeID != "" && existing != nodeID {
		return nil // 版本不匹配：玩家已迁移到其他节点，保留新节点的记录
	}
	// 从反向索引中移除。
	delete(s.nodePlayers[existing], uid)
	if len(s.nodePlayers[existing]) == 0 {
		delete(s.nodePlayers, existing)
	}
	delete(s.players, uid)
	return nil
}

func (s *State) PlayerNode(_ context.Context, uid string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodeID, ok := s.players[uid]
	return nodeID, ok
}

// —— Session Token（跨节点断线重连验证）——
// token 存储后端可配置（memory / redis）：memory 仅进程内保存，master 重启后全部失效
// （玩家需重新登录）；redis 由自身 TTL 持久化，master 重启后 token 仍有效。
//
// UseMemorySessionToken 将 session token 存储切换为内存后端（默认，进程内保存）。
func (s *State) UseMemorySessionToken(ttl time.Duration) {
	s.mu.Lock()
	s.tokenBackend = newMemorySessionBackend(ttl)
	s.mu.Unlock()
}

// UseRedisSessionToken 将 session token 存储切换为 Redis 后端（由 Redis TTL 持久化）。
// prefix 为 key 前缀（多游戏服共用 Redis 时用于隔离，默认见 master.SessionTokenConfig.KeyPrefix）。
func (s *State) UseRedisSessionToken(cli *redis.Client, prefix string, ttl time.Duration) {
	s.mu.Lock()
	s.tokenBackend = &redisSessionBackend{cli: cli, prefix: prefix, ttl: ttl}
	s.mu.Unlock()
}

// NewSessionToken 为玩家签发一个新 token 并覆盖旧值（旧 token 立即失效）。
//
// 随机数读取失败时返回错误而不签发：退化成全零（或可预测）的 token
// 等于给所有人发同一把钥匙。写入后端失败同样返回错误——
// 只签发不落库的 token 无法通过后续校验，属于无效凭证。
func (s *State) NewSessionToken(ctx context.Context, playerID string) (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("state: generate session token: %w", err)
	}
	token := hex.EncodeToString(buf)
	// 锁只用于取出后端引用：真正的写入（redis 时是网络调用）不持锁——
	// Redis 抖动时不会阻塞全部 State 读写（节点注册/定位/排行榜/session）。
	s.mu.RLock()
	backend := s.tokenBackend
	s.mu.RUnlock()
	if err := backend.Set(ctx, playerID, token); err != nil {
		return "", fmt.Errorf("state: store session token for %s: %w", playerID, err)
	}
	return token, nil
}

// ValidateSessionTokenResult 校验 session token。
// err 非 nil 表示存储后端故障（无法判定），严格区别于「token 无效」返回 (false, nil)，
// 供上层降级（如回退本地比对）而不把后端抖动误当成鉴权失败。
func (s *State) ValidateSessionTokenResult(ctx context.Context, playerID, token string) (bool, error) {
	if token == "" {
		return false, nil
	}
	// 锁只用于取出后端引用：网络 IO 不持锁（不拖住全部 State 操作）。
	s.mu.RLock()
	backend := s.tokenBackend
	s.mu.RUnlock()
	got, ok, err := backend.Get(ctx, playerID)
	if err != nil {
		return false, err
	}
	return ok && got == token, nil
}

func (s *State) ValidateSessionToken(ctx context.Context, playerID, token string) bool {
	valid, err := s.ValidateSessionTokenResult(ctx, playerID, token)
	if err != nil {
		// 后端故障时无法判定，保守返回 false（仅 redis 后端可能出现；memory 后端 err 恒 nil）。
		// 必须留日志：调用方（auth 校验链路）会把 false 当「token 无效」踢连接，
		// 与真正的无效无法区分，日志是唯一线索。
		logger.Warnf("master/state: validate session token for %s: backend error (treated as invalid): %v", playerID, err)
		return false
	}
	return valid
}

// DeleteSessionToken 删除 session token。
// 注意：接口无返回值（签名契约），后端删除失败只能记录日志、无法上报给调用方。
func (s *State) DeleteSessionToken(ctx context.Context, playerID string) {
	s.mu.RLock()
	backend := s.tokenBackend
	s.mu.RUnlock()
	if err := backend.Delete(ctx, playerID); err != nil {
		logger.Errorf("master/state: delete session token for %s: %v", playerID, err)
	}
}

// RefreshSessionToken 续期 session token（sliding TTL）。
// 仅当存储中的 token 与给定 token 一致时续期；err 非 nil 表示存储后端故障。
func (s *State) RefreshSessionToken(ctx context.Context, playerID, token string) error {
	if token == "" {
		return nil
	}
	s.mu.RLock()
	backend := s.tokenBackend
	s.mu.RUnlock()
	return backend.Refresh(ctx, playerID, token)
}

// CurrentSessionToken 返回当前 token。
// 后端故障与「无 token」此前不可区分（都返回空串，全量同步会下发空 session_token）；
// 当前签名不变，故后端故障时至少留日志。
func (s *State) CurrentSessionToken(ctx context.Context, playerID string) string {
	s.mu.RLock()
	backend := s.tokenBackend
	s.mu.RUnlock()
	got, ok, err := backend.Get(ctx, playerID)
	if err != nil {
		logger.Warnf("master/state: current session token for %s: backend error: %v (return empty)", playerID, err)
		return ""
	}
	if !ok {
		return ""
	}
	return got
}
