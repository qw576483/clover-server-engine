// 乐观并发控制（Optimistic Concurrency Control）。
//
// 为每个 GameObject 引入一个单调自增的版本号，提供：
//
//   - Version()：当前对象版本（Load 时从持久化读出；新对象为 0）。
//   - Save()：始终落库并把版本 +1。
//   - SaveIfVersion(ctx, expected)：乐观检查——若持久化的版本已不等于 expected（说明有人
//     在你读取后改动过），立即返回 ErrVersionConflict 且不写库；否则正常 Save（版本 +1）。
//     这正是「读取 → 展示 → 用户编辑 → 提交」工作流的安全写回路径：提交时带上
//     读取时的版本，版本变了就提示「数据已被他人修改，请重新拉取」，杜绝静默覆盖。
//
// 版本独立存储于 Key{OwnerObject, id, "ver"}（与 props/records/children 同级），不污染属性袋；
// 版本是「逐对象」的，子对象各有自己的版本，互不牵连。
//
// 设计取舍：detect-conflict 而非 atomic-CAS。SaveIfVersion 在写前重新读取持久化版本做比较，
// 能在单对象同步读写路径有效拦截并发覆盖；若要更强的原子性（高并发跨服），叠加 lock.Locker
// （每对象互斥）或与支持 CAS 的存储后端配合即可。
package gobject

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/pkg/shared/conv"
)

// verLocks 是「逐对象 id」的全局互斥锁表（每个不同 id 一把独立锁）。
//
// verMu 是每 GameObject 实例的独立锁——repo.Load(id) 两次得到不同实例时各自的 verMu 互相隔离，
// 无法拦截跨实例并发写（版本可能非单调、乐观检查失效）。这里用一个「按 id 精确映射」的
// 进程级锁表，把同一 id 的「读版本→比较→写数据→写版本」整段串行化，保证 SaveIfVersion 的
// 乐观 CAS 对同一 id 的所有实例都成立（不再被跨实例并发绕过）。
//
// 用「每 id 一把锁」而非「哈希分片」：级联子对象保存会在持父锁期间再取子锁，
// 若采用固定分片可能因不同 id 落同一分片导致父子死锁；每 id 独立锁则父子必为不同锁，无死锁。
//
// 说明：这是进程内的全局互斥；跨进程仍需后端 CAS 或分布式锁（见 SaveIfVersion 注释）。
var verLocks sync.Map // id(string) -> *sync.Mutex

// idLock 取某对象 id 对应的全局锁（同一 id 恒定返回同一把锁）。
func idLock(id string) *sync.Mutex {
	if m, ok := verLocks.Load(id); ok {
		return m.(*sync.Mutex)
	}
	m, _ := verLocks.LoadOrStore(id, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// forgetVerLock 释放某对象 id 对应的全局锁（对象已彻底删除后调用，防止 verLocks 无限膨胀）。
func forgetVerLock(id string) { verLocks.Delete(id) }

// ErrVersionConflict 表示 SaveIfVersion 检测到对象自加载后被他人改动（乐观并发控制拦截）。
// 若在线玩家在编辑期间保存过，返回此错误，
// 提示工具重新拉取最新数据再改，避免覆盖丢失更新。
var ErrVersionConflict = errors.New("gobject: version conflict (object changed since loaded)")

// verKey 版本号的持久化键（与 props/records/children 同级，互不污染）。
func (g *GameObject) verKey() data.Key {
	return data.Key{Owner: data.OwnerObject, ID: g.id.String(), Type: "ver"}
}

// loadVersion 从持久化读出当前版本（缺失视为 0）。
func (g *GameObject) loadVersion(ctx context.Context) (int64, error) {
	raw, err := g.store.Load(ctx, g.verKey())
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// saveVersion 把版本号写回持久化。
func (g *GameObject) saveVersion(ctx context.Context, v int64) error {
	return g.store.Save(ctx, g.verKey(), []byte(conv.FormatInt(v)))
}

// Version 返回对象当前版本号（Load 时从持久化读出；新对象为 0）。
// 读取对象后保留此值，提交改动时传入 SaveIfVersion 做乐观检查。
func (g *GameObject) Version() int64 {
	g.mu.RLock()
	v := g.version
	g.mu.RUnlock()
	return v
}

// bumpVersionLocked 读取最新持久化版本并 +1 写回（保证版本单调，即使并发 Save 也不会回退）。
// 调用方须已持有 g.verMu。
//
// 重要：verMu 是每对象实例的独立锁；若 repo.Load(id) 两次得到不同实例，各实例 verMu 隔离，
// 乐观并发检测仅对单实例有效。要全局防并发覆盖，需在上层叠加业务级锁（每 id 全局互斥）
// 或使用后端 CAS（如 etcd CompareAndSwap 或 MySQL SELECT FOR UPDATE）。
func (g *GameObject) bumpVersionLocked(ctx context.Context) error {
	cur, err := g.loadVersion(ctx)
	if err != nil {
		return err
	}
	g.mu.Lock()
	// 若内存版号高于持久化（多实例并发写入），取持久化版号 +1 以保单调；
	// 写后版号 = max(cur, g.version) + 1。
	prev := g.version
	if cur < g.version {
		cur = g.version
	}
	g.version = cur + 1
	v := g.version
	g.mu.Unlock()
	if err := g.saveVersion(ctx, v); err != nil {
		// 版本写库失败：回滚内存版号，避免内存与持久化漂移
		// （否则后续 SaveIfVersion 以内存版号为 expected 必然误判冲突）。
		g.mu.Lock()
		g.version = prev
		g.mu.Unlock()
		return err
	}
	return nil
}

// SaveIfVersion 乐观并发写回：若持久化版本已不等于 expected（对象被他人改动），
// 返回 ErrVersionConflict 且不写库；否则等价 Save（落库 + 版本 +1 + 自动同步）。
//
// 典型工作流：
//
//	g, _ := repo.Load(ctx, id)
//	ver := g.Version()            // 记住读取时的版本
//	g.Props().SetInt("gold", 999) // 用户编辑
//	if err := repo.SaveVersioned(ctx, g, ver); errors.Is(err, gobject.ErrVersionConflict) {
//	    // 数据已被在线玩家改动，提示用户重新拉取再改
//	}
func (g *GameObject) SaveIfVersion(ctx context.Context, expected int64) error {
	// 全局 per-id 锁：把整段乐观 CAS（读版本→比较→写数据→写版本）跨实例串行化，
	// 使不同 GameObject 实例（同一 id）的并发写回也能被乐观检查正确拦截、版本单调。
	gl := idLock(g.id.String())
	gl.Lock()
	defer gl.Unlock()
	g.verMu.Lock()
	defer g.verMu.Unlock()
	cur, err := g.loadVersion(ctx)
	if err != nil {
		return err
	}
	if cur != expected {
		return ErrVersionConflict
	}
	if err := g.saveBody(ctx); err != nil {
		return err
	}
	g.mu.Lock()
	prev := g.version
	// 与 bumpVersionLocked 取 max 一致：内存版号可能高于持久化版号（多实例并发写入），
	// 直接写 cur+1 会把内存版号回退，破坏单调性 —— 后续以内存版本作 expected 会误判冲突。
	if cur < g.version {
		cur = g.version
	}
	g.version = cur + 1
	v := g.version
	g.mu.Unlock()
	if err := g.saveVersion(ctx, v); err != nil {
		// 数据体已落库但版本写失败：回滚内存版号保持与持久化一致；
		// 持久化版本停留在 cur，下次 SaveIfVersion(expected=cur) 仍可通过并重写版本。
		g.mu.Lock()
		g.version = prev
		g.mu.Unlock()
		return err
	}
	return nil
}
