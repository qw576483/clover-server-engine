package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	imysql "github.com/qw576483/clover-server-engine/internal/domain/data/store/mysql"
	pdata "github.com/qw576483/clover-server-engine/pkg/domain/data"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	"github.com/qw576483/clover-server-engine/pkg/shared/timeutil"
)

// MySQL 持久层
// newMySQLBackend 由 MySQLConfig 构造持久层后端（*imysql.Client 实现 persistentStore）。
func newMySQLBackend(cfg imysql.MySQLConfig) (persistentStore, error) {
	return imysql.NewClient(cfg)
}

// getMySQL 从 MySQL 读取单条数据；无结果返回 ErrNotFound。
func (s *Store) getMySQL(ctx context.Context, key Key) ([]byte, error) {
	if s.mysql == nil {
		// 与 CreateTable:43 / persistDirty:70 的防御保持一致：缺 MySQL 后端的 Store
		// （memory 配置 + 声明了持久 Tier 的 Schema）会在首次 Load 直接 nil panic。
		return nil, fmt.Errorf("data: mysql not initialized for key %s", s.redisKey(key))
	}
	var row struct {
		Data []byte `db:"data"`
	}
	err := s.mysql.Get(ctx, &row, s.selectSQL(), key.Owner, key.ID, key.Type)
	if errors.Is(err, imysql.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return row.Data, nil
}

// CreateTable 执行 CREATE TABLE IF NOT EXISTS（表结构与 Key 对应）。
func (s *Store) CreateTable(ctx context.Context) error {
	// 门控只看本 Store 是否具备 MySQL 后端：叠加全局 pdata.TierHasPersistent()
	// 早退，AutoCreateTable=true 但 Schema 未声明持久化 Tier 时表不会创建，
	// 随后任何 upsert 都会报 1146（表不存在）。
	if s.mysql == nil {
		if pdata.TierHasPersistent() {
			return fmt.Errorf("data: no persistent backend configured, cannot create table")
		}
		return nil
	}
	if _, err := s.mysql.Exec(ctx, s.createTableSQL()); err != nil {
		return fmt.Errorf("data: create table %s: %w", s.table, err)
	}
	return nil
}

// SQL 拼装
func (s *Store) upsertSQL() string {
	// 引入 data_version 自增列作为乐观锁版本号，
	// 业务可在读取后比对版本判断是否被并发更新。
	return fmt.Sprintf("INSERT INTO %s (owner_type, owner_id, type, data, updated_at, data_version) VALUES (?, ?, ?, ?, NOW(3), 1) ON DUPLICATE KEY UPDATE data = VALUES(data), updated_at = NOW(3), data_version = data_version + 1", s.table)
}

func (s *Store) selectSQL() string {
	return fmt.Sprintf("SELECT data, data_version FROM %s WHERE owner_type = ? AND owner_id = ? AND type = ?", s.table)
}

func (s *Store) selectAllForOwnerSQL(types int) string {
	return fmt.Sprintf("SELECT type, data, data_version FROM %s WHERE owner_type = ? AND owner_id = ? AND type IN (%s)", s.table, placeholders(types))
}

// placeholders 返回 n 个 "?"。
// n<=0 时返回空串：生成的 SQL 会语法错误（如 `IN ()`），显式暴露调用方 bug，
// 而不是静默匹配空集。调用方必须保证 n>=1（selectAllForOwnerSQL 的现有调用点
// 均有 len>0 保护；LoadAll 在 len(types)==0 时提前返回）。
func placeholders(n int) string {
	switch {
	case n <= 0:
		return ""
	case n == 1:
		return "?"
	}
	return "?" + strings.Repeat(",?", n-1)
}

// LoadAll 批量加载 (ownerType, id) 下指定类型列表的全部数据。
// 先 Redis MGET 缓存 → 未命中一次 MySQL SELECT type, data WHERE ... AND type IN (...)。
// 替代 N+1 的 ListTypes+逐条 Load 模式，Snapshot 调用方直接使用。
func (s *Store) LoadAll(ctx context.Context, ownerType OwnerType, id string, types []string) (map[string][]byte, error) {
	if len(types) == 0 {
		return nil, nil
	}
	result := make(map[string][]byte, len(types))
	missed := types

	// 1. 先查 Redis 缓存（MGET 批量）。
	// rdb 先取出并判空：Redis 客户端已 Close（底层句柄置空）时不能对 nil 解引用 panic，
	// 此时跳过缓存层直接走内存/MySQL。
	if s.redis != nil {
		if rdb := s.redis.Raw(); rdb != nil {
			rkeys := make([]string, len(types))
			for i, t := range types {
				rkeys[i] = s.redisKey(Key{Owner: ownerType, ID: id, Type: t})
			}
			vals, mgErr := rdb.MGet(ctx, rkeys...).Result()
			if mgErr != nil {
				// 不能静默：MGET 出错时全部视为未命中并回源 MySQL（vals 为 nil），
				// 否则 Redis 故障被掩盖成正常的缓存未命中。
				logger.Warnf("data: LoadAll MGET failed (owner=%s/%s, %d keys): %v — falling back to MySQL for all", ownerType, id, len(rkeys), mgErr)
			}
			missed = make([]string, 0, len(types))
			for i, t := range types {
				if i < len(vals) {
					if raw, ok := vals[i].(string); ok && len(raw) > 0 {
						result[t] = []byte(raw)
						continue
					}
				}
				missed = append(missed, t)
			}
		}
	}

	// 2. 对于 TierSnapshot / TierMemory 类型，检查本地内存缓存
	// （TierSnapshot：离线事件写入的数据可能尚未 flush 到 MySQL；
	//   TierMemory：数据只存在于进程内内存，step1/step3 都不覆盖它）。
	if len(missed) > 0 {
		stillMissed := make([]string, 0, len(missed))
		for _, t := range missed {
			key := Key{Owner: ownerType, ID: id, Type: t}
			if tier := s.getTier(key); tier == TierSnapshot || tier == TierMemory {
				if b, ok := s.memGet(key); ok {
					result[t] = b
					continue
				}
			}
			stillMissed = append(stillMissed, t)
		}
		missed = stillMissed
	}

	// 3. 缓存未命中的从 MySQL 批量加载
	if len(missed) > 0 && s.mysql != nil {
		// 脏键先落库：单条读 getCache 在 MySQL 回源前会先 flush 脏键，
		// 批量读若不 flush 会读到 MySQL 旧值 —— 同一 Tier 下「批量读」与「单条读」
		// 返回不同数据。
		for _, t := range missed {
			k := Key{Owner: ownerType, ID: id, Type: t}
			if s.isDirty(k) {
				if err := s.flushOne(ctx, k); err != nil {
					logger.Warnf("data: LoadAll flush dirty key %s before backfill failed: %v", s.redisKey(k), err)
				}
			}
		}
		query := s.selectAllForOwnerSQL(len(missed))
		args := make([]any, 2+len(missed))
		args[0] = string(ownerType)
		args[1] = id
		for i, t := range missed {
			args[2+i] = t
		}
		var rows []struct {
			Type string `db:"type"`
			Data []byte `db:"data"`
		}
		if err := s.mysql.Select(ctx, &rows, query, args...); err != nil {
			return nil, err
		}
		for _, row := range rows {
			result[row.Type] = row.Data
			// 回灌 Redis 缓存（客户端缺失/已关闭时跳过，不影响本次读取结果）
			rk := s.redisKey(Key{Owner: ownerType, ID: id, Type: row.Type})
			if s.redis == nil {
				continue
			}
			if rdb := s.redis.Raw(); rdb != nil {
				if err := rdb.Set(ctx, rk, row.Data, s.cacheTTL).Err(); err != nil {
					logger.Errorf("data: LoadAll backfill redis %s: %v", rk, err)
				}
			}
		}
	}
	return result, nil
}

func (s *Store) deleteSQL() string {
	return fmt.Sprintf("DELETE FROM %s WHERE owner_type = ? AND owner_id = ? AND type = ?", s.table)
}

func (s *Store) createTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    owner_type   VARCHAR(16) NOT NULL,
    owner_id     VARCHAR(64) NOT NULL,
    type         VARCHAR(64) NOT NULL,
    data         LONGBLOB    NOT NULL,
    updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    data_version BIGINT      NOT NULL DEFAULT 1,
    PRIMARY KEY (owner_type, owner_id, type),
    INDEX idx_owner (owner_type, owner_id),
    INDEX idx_type  (type),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
}

// maxTableNameLen 表名最大长度，防止超长表名导致的异常。
const maxTableNameLen = 64

// SQLExec 抽象 SQL 执行器（MySQL 客户端实现此接口），供 account/player/order 等业务存储包共用。
// 避免各子包重复定义同一接口。
type SQLExec interface {
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	Get(ctx context.Context, dest any, query string, args ...any) error
	Select(ctx context.Context, dest any, query string, args ...any) error
	Close() error
}

// SanitizeTable 仅保留 [a-zA-Z0-9_]，防止表名注入；空、非字母数字或超过长度上限返回 defaultName。
// 返回时包裹反引号以兼容 MySQL 保留字。
func SanitizeTable(name, defaultName string) string {
	if strings.TrimSpace(name) == "" {
		return "`" + defaultName + "`"
	}
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	cleaned := b.String()
	if len(cleaned) == 0 || len(cleaned) > maxTableNameLen {
		return "`" + defaultName + "`"
	}
	return "`" + cleaned + "`"
}

// NowSQL 返回当前时间的 MySQL datetime 格式字符串（2006-01-02 15:04:05），供各业务表共用。
func NowSQL() string {
	return timeutil.NowStr()
}

// sanitizeTable 内部版本：默认表名为 "data"。
func sanitizeTable(name string) string { return SanitizeTable(name, "data") }
