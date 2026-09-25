// #nosec G201 -- 迁移 SQL 的表名 versionTable 在 NewMigrator 中已做白名单校验（identifierRE）；SQL 文本来自内嵌迁移文件而非外部输入，fmt.Sprintf 只用于标识符、不用于值。

// Package migration 提供数据库 Schema 版本化迁移基础架构。
//
// 对标 Flyway / golang-migrate。支持 Up 迁移 + 版本追踪（通过 _schema_versions 表）。
// 迁移以嵌入式方法注册，引擎在 app 启动扫描 data.sqls_dir 时自动运行待执行的迁移。
//
// 接入：
//
// migration.Register(migration.Migration{
// Version: 1,
// Name: "create accounts table",
// Up: "CREATE TABLE IF NOT EXISTS account (...)",
// })
//
// // 引擎启动时自动调用（runSQLMigrations）
// migrator := migration.NewMigrator(db, "")
// migrator.Up(ctx)
//
// ⚠️ 只支持「向前迁移」：需要回滚时人工执行反向 SQL。
package migration

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var identifierRE = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")

// Migration 单条迁移。
type Migration struct {
	Version int    // 单调递增，从 1 开始
	Name    string // 可读描述
	Up      string // 向前迁移 SQL（可多条，分号分隔）
	// Down 回滚 SQL（注册结构保留字段）。
	//
	// ⚠️ 接线状态：**无读取点**。保留该字段只为不破坏既有注册写法与文档示例
	//（配了不报错、也不参与 SQL 注入面），但**引擎当前不执行任何回滚**。
	Down string
}

var (
	mu   sync.Mutex
	list []Migration
)

// Register 注册一条迁移（通常在 init() 调用）。
// 同版本重复注册时保留首个：RunGame 在迁移注册后、NewGame 完成前失败时，
// 已注册项仍留在进程全局，同进程重跑会走一遍注册路径——无去重会重复注册同一版本。
func Register(m Migration) {
	mu.Lock()
	defer mu.Unlock()
	for i := range list {
		if list[i].Version == m.Version {
			return // 已有同版本：保留首个注册（重复注册是重跑路径的常态）
		}
	}
	list = append(list, m)
}

// GetPending 返回所有已注册迁移，按 Version 排序。
func GetPending() []Migration {
	mu.Lock()
	defer mu.Unlock()
	out := make([]Migration, len(list))
	copy(out, list)
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// Migrator 迁移执行器，按 Version 追踪已执行的迁移。
type Migrator struct {
	db           *sql.DB
	versionTable string
}

// NewMigrator 构造迁移执行器。versionTable 为空时使用默认名 "_schema_versions"，
// 不合法标识符时返回错误。
func NewMigrator(db *sql.DB, versionTable string) (*Migrator, error) {
	if versionTable == "" {
		versionTable = "_schema_versions"
	}
	// versionTable 会被拼入 SQL 标识符，必须做白名单校验，防止 SQL 注入。
	if !identifierRE.MatchString(versionTable) {
		return nil, fmt.Errorf("migration: invalid version table name: %q", versionTable)
	}
	return &Migrator{db: db, versionTable: versionTable}, nil
}

// EnsureVersionTable 确保版本追踪表存在。
func (m *Migrator) EnsureVersionTable(ctx context.Context) error {
	// #nosec G201 -- versionTable 在 NewMigrator 中已做白名单校验。
	q := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` ("+
			"version INT NOT NULL PRIMARY KEY,"+
			"name VARCHAR(255) NOT NULL,"+
			"applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"+
			")", m.versionTable)
	_, err := m.db.ExecContext(ctx, q)
	return err
}

// AppliedVersions 返回已执行的版本号集合。
func (m *Migrator) AppliedVersions(ctx context.Context) (map[int]bool, error) {
	out := make(map[int]bool)
	// #nosec G201 -- versionTable 在 NewMigrator 中已做白名单校验。
	rows, err := m.db.QueryContext(ctx, fmt.Sprintf("SELECT version FROM `%s`", m.versionTable))
	if err != nil {
		return nil, err
	}
	// 行集已遍历到底并回传 rows.Err()，关闭失败无补救动作；显式忽略。
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

// splitSQL 按分号拆分为多条语句，忽略纯空白和末尾分号。
//
// 完整跟踪引号（单引号 / 双引号 / 反引号，含 ” / "" 与反斜杠转义）与注释
// （-- 行注释 / # 行注释 / /* */ 块注释）：只盯单引号会把双引号、反引号
// 与注释里的分号当作语句边界，含这些字符的迁移 SQL 会被错误拆分执行；
// 转义判断不依赖「前一字符」（`\\'` 的真转义场景会被误判）。
func splitSQL(s string) []string {
	var out []string
	var buf strings.Builder
	var inSingle, inDouble, inBacktick, inLineComment, inBlockComment bool

	for i := 0; i < len(s); i++ {
		ch := s[i]
		// 引号内的反斜杠转义：连同被转义字符整体消费（保证 \\' 的引号按「闭合」处理）。
		if (inSingle || inDouble || inBacktick) && ch == '\\' && i+1 < len(s) {
			buf.WriteByte(ch)
			i++
			buf.WriteByte(s[i])
			continue
		}
		switch {
		case inLineComment:
			buf.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			buf.WriteByte(ch)
			if ch == '*' && i+1 < len(s) && s[i+1] == '/' {
				buf.WriteByte('/')
				i++
				inBlockComment = false
			}
		case inSingle:
			buf.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' { // '' 转义：仍属字符串内
					buf.WriteByte('\'')
					i++
				} else {
					inSingle = false
				}
			}
		case inDouble:
			buf.WriteByte(ch)
			if ch == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					buf.WriteByte('"')
					i++
				} else {
					inDouble = false
				}
			}
		case inBacktick:
			buf.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
		default:
			switch {
			case ch == '\'':
				inSingle = true
				buf.WriteByte(ch)
			case ch == '"':
				inDouble = true
				buf.WriteByte(ch)
			case ch == '`':
				inBacktick = true
				buf.WriteByte(ch)
			case ch == '-' && i+2 < len(s) && s[i+1] == '-' &&
				(s[i+2] == ' ' || s[i+2] == '\t' || s[i+2] == '\n' || s[i+2] == '\r'):
				inLineComment = true
				buf.WriteByte(ch)
				buf.WriteByte('-')
				i++
			case ch == '#':
				inLineComment = true
				buf.WriteByte(ch)
			case ch == '/' && i+1 < len(s) && s[i+1] == '*':
				inBlockComment = true
				buf.WriteByte(ch)
				buf.WriteByte('*')
				i++
			case ch == ';':
				stmt := strings.TrimSpace(buf.String())
				if stmt != "" {
					out = append(out, stmt)
				}
				buf.Reset()
			default:
				buf.WriteByte(ch)
			}
		}
	}
	// 处理末尾无分号的语句。
	if stmt := strings.TrimSpace(buf.String()); stmt != "" {
		out = append(out, stmt)
	}
	return out
}

// Up 执行所有未执行的迁移。每条迁移在一个事务内执行，多语句按分号拆分逐条执行。
//
// ⚠️ MySQL 的 DDL（CREATE/ALTER/DROP）会隐式提交：迁移里含 DDL 时事务并不能整体回滚——
// 中途失败会留下「部分语句已执行、版本未记录」的状态，重启后该迁移会重跑。
// 因此迁移 SQL 必须写成幂等形式（IF NOT EXISTS / IF EXISTS），并在失败时人工核对半执行状态。
func (m *Migrator) Up(ctx context.Context) error {
	if err := m.EnsureVersionTable(ctx); err != nil {
		return fmt.Errorf("migration: ensure version table: %w", err)
	}
	applied, err := m.AppliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("migration: read versions: %w", err)
	}
	pending := GetPending()
	for _, mg := range pending {
		if applied[mg.Version] {
			continue
		}
		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("migration: begin tx v%d: %w", mg.Version, err)
		}
		statements := splitSQL(mg.Up)
		for idx, stmt := range statements {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				// 带语句序号：DDL 隐式提交后 tx.Rollback 无效，需按序号人工核对半执行状态。
				return fmt.Errorf("migration: v%d %s: statement %d/%d failed (DDL 隐式提交，可能已部分执行): %w",
					mg.Version, mg.Name, idx+1, len(statements), err)
			}
		}
		q := fmt.Sprintf("INSERT INTO `%s` (version, name) VALUES (?, ?)", m.versionTable)
		if _, err := tx.ExecContext(ctx, q, mg.Version, mg.Name); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration: record v%d: %w", mg.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration: commit v%d: %w", mg.Version, err)
		}
	}
	return nil
}

// 需要回滚时人工执行反向 SQL，并手工删 _schema_versions 里对应 version 行。
