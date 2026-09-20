package state

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	imysql "clover-server-engine/internal/domain/data/store/mysql"
	"clover-server-engine/pkg/foundation/logger"
	"clover-server-engine/pkg/foundation/logstore"
)

// maxInsertRows 单条多值 INSERT 的最大行数：超过则拆成多条语句执行，
// 避免整批超出 max_allowed_packet 导致「一条 INSERT 失败 = 整批日志丢失」。
const maxInsertRows = 200

// truncateRunes 把 s 截断到最多 n 个字符，满足 VARCHAR(n) 的字符长度限制。
// 任一超长字段会让整批 INSERT 失败——必须在拼 SQL 前截断。
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// truncateBytesUTF8 把 s 截断到最多 max 字节且不破坏 UTF-8 边界（TEXT 按字节计）。
func truncateBytesUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// MySQLState log 服 MySQL 落盘实现：把批量日志写入 biz_log 表。
// 相比文件方案，支持按 owner_id / type / time 检索，便于后续接入分析。
type MySQLState struct {
	mu    sync.Mutex
	cli   *imysql.Client
	table string
}

// 编译期断言：MySQL 实现满足业务可插拔的后端契约（真身见 pkg/foundation/logstore）。
var _ logstore.Backend = (*MySQLState)(nil)

// NewMySQL 创建 MySQL 落盘 State。conf 为 MySQL 连接配置，table 为日志表名（默认 biz_log）。
// 启动时自动建表（幂等）。
func NewMySQL(conf imysql.MySQLConfig, table string) (*MySQLState, error) {
	if table == "" {
		table = "biz_log"
	}
	// 表名进 SQL 的路径无法用占位符参数化（DDL/DML 里标识符只能是字面量），
	// 所以必须在这里把它限成安全标识符：配置来源将来若变成可外部改写（运营后台 / 配置中心），
	// 没有这层校验就是一个现成的注入面。
	if !isSafeTableName(table) {
		return nil, fmt.Errorf("logsvc/mysql: invalid table name %q (want [A-Za-z_][A-Za-z0-9_]*, <=64 chars)", table)
	}
	cli, err := imysql.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("logsvc/mysql: connect: %w", err)
	}
	s := &MySQLState{cli: cli, table: table}
	if err := s.ensureTable(); err != nil {
		_ = cli.Close()
		return nil, err
	}
	logger.Infof("logsvc/mysql: state ready, table=%s", table)
	return s, nil
}

// isSafeTableName 校验表名是安全的 SQL 标识符（[A-Za-z_][A-Za-z0-9_]*, 长度 <= 64）。
// 表名无法用占位符参数化，只能拼进 DDL/DML，因此必须在入口限定字符集。
func isSafeTableName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false // MySQL 标识符不能以数字开头
			}
		default:
			return false
		}
	}
	return true
}

// ensureTable 幂等建表。日志表用普通 InnoDB + 时间/归属索引即可；
// 若后续量级上来再迁移 ClickHouse（列式 + 按天分区），此处不引入 MySQL 原生分区，
// 避免分区键必须进主键的限制与运维复杂度。
func (s *MySQLState) ensureTable() error {
	if s.cli == nil {
		return fmt.Errorf("logsvc/mysql: state is closed")
	}
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		time DATETIME(3) NOT NULL,
		source VARCHAR(64) NOT NULL DEFAULT '',
		owner_type VARCHAR(32) NOT NULL DEFAULT '',
		owner_id VARCHAR(64) NOT NULL DEFAULT '',
		type VARCHAR(64) NOT NULL DEFAULT '',
		info TEXT,
		sub_type VARCHAR(64) NOT NULL DEFAULT '',
		reason VARCHAR(255) NOT NULL DEFAULT '',
		sub_reason VARCHAR(255) NOT NULL DEFAULT '',
		level VARCHAR(16) NOT NULL DEFAULT '',
		trace_id VARCHAR(64) NOT NULL DEFAULT '',
		PRIMARY KEY (id),
		KEY idx_time (time),
		KEY idx_owner (owner_type, owner_id, time),
		KEY idx_type (type, time)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.cli.Exec(ctx, q); err != nil {
		return fmt.Errorf("logsvc/mysql: create table %s: %w", s.table, err)
	}
	return nil
}

// WriteBatch 实现 LogService：批量 INSERT 日志。
// 按 maxInsertRows 拆分为多条 INSERT：单条语句过大必然触发 max_allowed_packet
// 失败并导致整批日志丢失。返回实际写入条数（失败时返回已写部分）。
func (s *MySQLState) WriteBatch(source string, entries []LogEntry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cli == nil {
		// Close 已把 cli 置 nil：关闭流程中的在途 WriteBatch 此前会空指针 panic。
		return 0, fmt.Errorf("logsvc/mysql: state is closed")
	}

	written := 0
	for start := 0; start < len(entries); start += maxInsertRows {
		end := start + maxInsertRows
		if end > len(entries) {
			end = len(entries)
		}
		n, err := s.writeChunkLocked(source, entries[start:end])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// writeChunkLocked 写入一小批（≤maxInsertRows）。调用方须持有 s.mu。
func (s *MySQLState) writeChunkLocked(source string, entries []LogEntry) (int, error) {
	// 拼多值 INSERT，一次往返写入整批。
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(s.table)
	sb.WriteString(" (time, source, owner_type, owner_id, type, info, sub_type, reason, sub_reason, level, trace_id) VALUES ")
	args := make([]any, 0, len(entries)*11)
	for i, e := range entries {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?)")
		src := e.Source
		if src == "" {
			src = source
		}
		// 按列定义长度截断（VARCHAR 按字符、TEXT 按字节），避免单字段超长使整批失败。
		args = append(args,
			time.UnixMilli(e.Time).UTC(),
			truncateRunes(src, 64),           // source VARCHAR(64)
			truncateRunes(e.OwnerType, 32),   // owner_type VARCHAR(32)
			truncateRunes(e.OwnerID, 64),     // owner_id VARCHAR(64)
			truncateRunes(e.Type, 64),        // type VARCHAR(64)
			truncateBytesUTF8(e.Info, 65535), // info TEXT（字节上限）
			truncateRunes(e.SubType, 64),     // sub_type VARCHAR(64)
			truncateRunes(e.Reason, 255),     // reason VARCHAR(255)
			truncateRunes(e.SubReason, 255),  // sub_reason VARCHAR(255)
			truncateRunes(e.Level, 16),       // level VARCHAR(16)
			truncateRunes(e.TraceID, 64),     // trace_id VARCHAR(64)
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.cli.Exec(ctx, sb.String(), args...); err != nil {
		return 0, fmt.Errorf("logsvc/mysql: insert batch(%d): %w", len(entries), err)
	}
	return len(entries), nil
}

// Close 实现 LogService：关闭 MySQL 连接池。
func (s *MySQLState) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cli == nil {
		return nil
	}
	err := s.cli.Close()
	s.cli = nil
	return err
}
