// Package idgen 通用对象号生成器。

// 对象身份必须有一个集中、单调、可持久化的分配器，而非业务各处随意拼 id：

//   - 生成结果直接是 object.ObjectID{Type, Seq}：即 Manager 注册 / 消息派发共用的那套编码。
//   - MemSequencer：纯内存、进程级（测试 / 临时对象）。
//   - StoreSequencer：把每个 (node, 类型) 的号段上限持久化到 data.Store，重启后从库恢复、绝不重复分配。
//   - WithNode：多服架构把 node 编入 ObjectID.Seq 低位，保证跨服分配出的对象号全局唯一。

// 用法：

//	// 单服：持久对象号
//	gen := idgen.NewStoreGenerator(store, 0, 100)
//	id, _ := gen.Next(idgen.TypePlayer) // -> object.ObjectID{Type:TypePlayer, Seq:1}

// // 多服：保证跨服对象号全局唯一
// gen := idgen.NewStoreGenerator(store, nodeID, 100).WithNode(nodeID, 8)
// id, _ := gen.Next(idgen.TypePlayer)
package idgen

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"clover-server-engine/pkg/domain/data"
	"clover-server-engine/pkg/domain/object"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/shared/conv"
)

// storeIOTimeout 号段 Load/Save 的单次上限。
// Next/Reserve 无 ctx 形参（公开签名不变），这里给内部 IO 加兜底超时，
// 避免底层存储无响应时把调用者（含停机流程）永久挂住。
const storeIOTimeout = 5 * time.Second

// ErrSeqOverflow 表示对象序号已超出当前 nodeBits 配置下的可用位宽。
var ErrSeqOverflow = errors.New("idgen: object sequence overflow")

const objectSeqBits = 48 // ObjectID.Seq 可用位宽

// Sequencer 对象序号来源。给定对象类型，返回在该类型内单调递增、全局唯一（于该 node）、不重复的序号。

// Next / Reserve 分配出的序号满足：不重复、不回退；跨进程重启后从持久化号段续接（StoreSequencer）。
type Sequencer interface {
	// Next 返回指定类型的下一个序号（已 +1）。
	Next(objType uint16) (uint64, error)
	// Reserve 预留 n 个连续序号，返回第一段起始序号（含）；调用方独占 [start, start+n)。
	Reserve(objType uint16, n uint64) (uint64, error)
	// Last 返回指定类型当前已分配的最大序号（从未分配过为 0）。
	Last(objType uint16) uint64
}

// // MemSequencer：纯内存、进程级、重启归零。适合测试与单进程临时对象。
// // MemSequencer 内存序列器。
type MemSequencer struct {
	mu    sync.Mutex
	lasts map[uint16]uint64
}

// NewMemSequencer 构造内存序列器。
func NewMemSequencer() *MemSequencer {
	return &MemSequencer{lasts: make(map[uint16]uint64)}
}

// Next 返回下一个序号。溢出时返回错误而非静默回绕。
func (s *MemSequencer) Next(t uint16) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lasts[t] == ^uint64(0) {
		return 0, ErrSeqOverflow
	}
	s.lasts[t]++
	return s.lasts[t], nil
}

// Reserve 预留 n 个连续序号，返回起始。
// hwm+n 超出 uint64 表示范围会回绕到已发过的号段（序号重复），故先校验再推进，
// 溢出时返回 ErrSeqOverflow 且保持 hwm 不变。
func (s *MemSequencer) Reserve(t uint16, n uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n == 0 {
		// 空号段也统一「返回起始号」语义（起始 = 已发最大号 + 1）：
		// 旧实现返回 s.Last（已发出的号），调用方按起始号使用会拿到已占用号。
		if s.lasts[t] == ^uint64(0) {
			return 0, ErrSeqOverflow
		}
		return s.lasts[t] + 1, nil
	}
	if n > ^uint64(0)-s.lasts[t] {
		return 0, ErrSeqOverflow
	}
	start := s.lasts[t] + 1
	s.lasts[t] += n
	return start, nil
}

// Last 返回当前最大序号。
func (s *MemSequencer) Last(t uint16) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lasts[t]
}

// // StoreSequencer：落库序列器，号段上限持久化到 data.Store，重启不重复。
// // StoreSequencer 落库序列器：把每个 (node, 类型) 的号段上限（ceiling）持久化到 data.Store。

// 采用「号段分配」策略：store 中只记录已预留的号段上限（ceiling），内存从 ceiling 往下逐个发出；
// 触及 ceiling 时再向前预留一个 step 并落库。进程崩溃最多浪费一个号段尾部的号（绝不重复），
// 重启后从 ceiling 续接——与主流号段分配一致，且对「干净重启」同样安全（不会把已发出但未用完的号重发）。
type StoreSequencer struct {
	store data.Store
	node  uint16
	step  uint64

	mu      sync.Mutex
	hwm     map[uint16]uint64 // 内存已分配最大序号
	ceiling map[uint16]uint64 // 已向 store 预留的号段上限（=store 持久值）；hwm 触及即再预留一段
	loaded  map[uint16]bool   // 该类型是否已从库加载过
}

// NewStoreSequencer 构造落库序列器。node 区分分片（多服各自往前、不抢号）；step 为号段步长（0 默认 100）。
func NewStoreSequencer(store data.Store, node uint16, step uint64) *StoreSequencer {
	if store == nil {
		// nil store 会在 Next/Reserve 的 Load/Save 处解引用 panic（且无上下文）。
		// 与 gobject.New 同口径：构造期拒收，返回 nil 并留痕。
		logger.Warnf("idgen.NewStoreSequencer: nil store; sequencer not created")
		return nil
	}
	if step == 0 {
		step = 100
	}
	return &StoreSequencer{
		store:   store,
		node:    node,
		step:    step,
		hwm:     make(map[uint16]uint64),
		ceiling: make(map[uint16]uint64),
		loaded:  make(map[uint16]bool),
	}
}

func (s *StoreSequencer) key(t uint16) data.Key {
	return data.Key{
		Owner: data.OwnerMeta,
		ID:    "idgen:" + conv.FormatUint(uint64(s.node)),
		Type:  conv.FormatUint(uint64(t)),
	}
}

// ensureLoaded 从库恢复该类型的号段上限（ceiling）。store 中存的是「已预留的号段上限」，
// 因此重启后从 ceiling 续接，绝不会把上一段已分配但未用完的号重新发出（最多浪费一个号段）。
func (s *StoreSequencer) ensureLoaded(t uint16) error {
	if s.loaded[t] {
		return nil
	}
	loadCtx, cancel := context.WithTimeout(context.Background(), storeIOTimeout)
	raw, err := s.store.Load(loadCtx, s.key(t))
	cancel()
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			s.ceiling[t] = 0
			s.hwm[t] = 0
			s.loaded[t] = true
			return nil
		}
		return err
	}
	if len(raw) < 8 {
		s.ceiling[t] = 0
		s.hwm[t] = 0
	} else {
		v := binary.BigEndian.Uint64(raw)
		s.ceiling[t] = v
		s.hwm[t] = v
	}
	s.loaded[t] = true
	return nil
}

// reserveSegment 当需要更多号时，把 ceiling 向前推进至少一个 step 并落库（号段分配：store 只记上限）。

// 按缺口一次算出推进量（向上取整到 step 的整数倍），而不是逐个 step 累加：
// need 极大时逐步累加要循环天文数字次，等同于卡死。
// 推进量做溢出校验：ceiling 回绕会把已发出过的号段重新发出。
func (s *StoreSequencer) reserveSegment(t uint16, need uint64) error {
	if s.ceiling[t] < need {
		step := s.step
		if step == 0 {
			step = 1 // 步长非法（零值构造）时退化为逐个推进，兼顾除零与死循环
		}
		grow := need - s.ceiling[t]
		if r := grow % step; r != 0 {
			// 补齐到 step 的整数倍；贴近 uint64 上限补不齐时按 need 精确预留。
			if pad := step - r; pad <= ^uint64(0)-grow {
				grow += pad
			}
		}
		if grow > ^uint64(0)-s.ceiling[t] {
			return ErrSeqOverflow
		}
		s.ceiling[t] += grow
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, s.ceiling[t])
	saveCtx, cancel := context.WithTimeout(context.Background(), storeIOTimeout)
	defer cancel()
	return s.store.Save(saveCtx, s.key(t), buf)
}

// Next 返回下一个序号（按号段续接落库）。
func (s *StoreSequencer) Next(t uint16) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoaded(t); err != nil {
		return 0, err
	}
	if s.hwm[t] == ^uint64(0) {
		return 0, ErrSeqOverflow
	}
	if s.hwm[t] >= s.ceiling[t] {
		if err := s.reserveSegment(t, s.hwm[t]+1); err != nil {
			return 0, err
		}
	}
	s.hwm[t]++
	return s.hwm[t], nil
}

// Reserve 预留 n 个连续序号，返回起始。
// hwm+n 溢出会回绕到已发过的号段（序号重复），故先校验再落库预留，
// 溢出时返回 ErrSeqOverflow 且不改动 hwm / ceiling。
func (s *StoreSequencer) Reserve(t uint16, n uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n == 0 {
		// 空号段同样返回「起始号 = 已发最大号 + 1」（先 ensureLoaded 拿到的 hwm 才是真值）。
		if err := s.ensureLoaded(t); err != nil {
			return 0, err
		}
		if s.hwm[t] == ^uint64(0) {
			return 0, ErrSeqOverflow
		}
		return s.hwm[t] + 1, nil
	}
	if err := s.ensureLoaded(t); err != nil {
		return 0, err
	}
	if n > ^uint64(0)-s.hwm[t] {
		return 0, ErrSeqOverflow
	}
	start := s.hwm[t] + 1
	end := s.hwm[t] + n
	if end > s.ceiling[t] {
		if err := s.reserveSegment(t, end); err != nil {
			return 0, err
		}
	}
	s.hwm[t] = end
	return start, nil
}

// Last 返回当前最大序号。
func (s *StoreSequencer) Last(t uint16) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hwm[t]
}

// // Generator：把序号封装成 object.ObjectID，并提供可选的跨服分区。
// // Generator 对象号生成器。持有底层 Sequencer，产出 object.ObjectID。
type Generator struct {
	seq      Sequencer
	node     uint16
	nodeBits uint
}

// NewGenerator 用指定 Sequencer 构造生成器。
func NewGenerator(seq Sequencer) *Generator {
	if seq == nil {
		// nil sequencer 会让 Next/Reserve 在 g.seq.Next(...) 处解引用 panic。
		logger.Warnf("idgen.NewGenerator: nil sequencer; generator not created")
		return nil
	}
	return &Generator{seq: seq}
}

// NewMemGenerator 构造纯内存生成器（单进程 / 测试）。
func NewMemGenerator() *Generator {
	return NewGenerator(NewMemSequencer())
}

// NewStoreGenerator 构造落库生成器（持久对象号）。node 区分分片；step 号段步长（0 默认 100）。
func NewStoreGenerator(store data.Store, node uint16, step uint64) *Generator {
	// NewStoreSequencer 对 nil store 返回 nil；NewGenerator 对 nil sequencer 返回 nil —— 链路整体返回 nil 并已在各自处留痕。
	return NewGenerator(NewStoreSequencer(store, node, step))
}

// WithNode 开启跨服分区（多服架构）：把 node 编入 ObjectID.Seq 低位 nodeBits 位，
// 保证多服分配出的对象号全局唯一。

// 约束：nodeBits 须在 [1,16]，node < 2^nodeBits，且 nodeBits < objectSeqBits（须留计数器位）。
// 任一约束不满足立即返回 nil（配置错误应尽早暴露，避免 make 时 overflow / seqBits 下溢回绕致对象号重复）。
// 不调用 WithNode 时 nodeBits=0，对象号即纯单调序号（可读、易解析）。
func (g *Generator) WithNode(node uint16, nodeBits uint) *Generator {
	// nodeBits 必须落在有效区间：至少 1 位、至多 16 位，且须给计数器留下位宽。
	if nodeBits == 0 || nodeBits > 16 || nodeBits >= objectSeqBits {
		// 返回 nil 是文档化契约（配置错误尽早暴露），但必须留痕：
		// 静默 nil 经 objstore.WithGenerator 链路透传，现场无从判断是参数错还是别的问题。
		logger.Warnf("idgen.WithNode: invalid nodeBits=%d; generator NOT configured (returns nil)", nodeBits)
		return nil
	}
	// node 必须能被 nodeBits 位无损编码，否则跨服对象号会因高位被截断而碰撞。
	if uint64(node) >= (uint64(1) << nodeBits) {
		logger.Warnf("idgen.WithNode: node=%d exceeds %d bits; generator NOT configured (returns nil)", node, nodeBits)
		return nil
	}
	g.node = node
	g.nodeBits = nodeBits
	return g
}

func (g *Generator) make(objType uint16, seq uint64) (object.ObjectID, error) {
	if g.nodeBits == 0 {
		if seq >= (uint64(1) << objectSeqBits) {
			return object.ObjectID{}, ErrSeqOverflow
		}
		return object.NewObjectID(objType, seq), nil
	}
	// 校验 node 可编码进 nodeBits 位，避免高位被 mask 截断致跨服碰撞。
	// #nosec G115 -- nodeBits 在 WithNode 中已约束为 [1,16]，(1<<nodeBits)-1 ≤ 65535 可安全落入 uint16。
	maxNode := uint16((uint64(1) << g.nodeBits) - 1)
	if g.node > maxNode {
		return object.ObjectID{}, fmt.Errorf("idgen: node %d exceeds max %d for %d bits", g.node, maxNode, g.nodeBits)
	}
	// ObjectID.Seq 仅 48 位；其中低 nodeBits 位编入 node，剩余 (48-nodeBits) 位作计数器。
	// 若计数器超出可用位宽，必须报错而不是静默截断，否则跨服对象号会碰撞。
	seqBits := objectSeqBits - g.nodeBits
	if seqBits == 0 {
		return object.ObjectID{}, fmt.Errorf("idgen: nodeBits %d consumes all %d seq bits", g.nodeBits, objectSeqBits)
	}
	maxCounter := (uint64(1) << seqBits) - 1
	if seq > maxCounter {
		return object.ObjectID{}, ErrSeqOverflow
	}
	mask := uint64((uint64(1) << g.nodeBits) - 1)
	// #nosec G115 -- nodeBits <= 16，mask 已在 uint16 范围内。
	combined := (seq << g.nodeBits) | uint64(g.node&uint16(mask))
	return object.NewObjectID(objType, combined), nil
}

// Next 分配一个对象号。
func (g *Generator) Next(objType uint16) (object.ObjectID, error) {
	seq, err := g.seq.Next(objType)
	if err != nil {
		return object.ObjectID{}, err
	}
	return g.make(objType, seq)
}

// Reserve 预留 n 个连续对象号，返回第一个。
func (g *Generator) Reserve(objType uint16, n uint64) (object.ObjectID, error) {
	seq, err := g.seq.Reserve(objType, n)
	if err != nil {
		return object.ObjectID{}, err
	}
	return g.make(objType, seq)
}

// Last 返回指定类型底层计数器的当前最大序号（nodeBits>0 时为未打包的计数器值）。
func (g *Generator) Last(objType uint16) uint64 {
	return g.seq.Last(objType)
}

// 常用对象类型常量。

// 唯一真值定义在 pkg/domain/object，本包只做别名透传，避免两处枚举撞值 / 语义漂移。
// 业务需要扩展时，请从 TypeBizStart 之后自行定义，勿与下列值冲突。
const (
	TypePlayer = object.TypePlayer // 玩家
	TypeScene  = object.TypeScene  // 场景 / 地图区块
)

// idgen 专有扩展类型：接在 pkg/domain/object 已占用区段之后，保证全局唯一。
const (
	TypeItem   uint16 = 8  // 物品 / 道具
	TypeMail   uint16 = 9  // 邮件
	TypeNpc    uint16 = 10 // NPC
	TypeGlobal uint16 = 11 // 全局共享对象
)
