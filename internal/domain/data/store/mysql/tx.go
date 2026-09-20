package mysql

import (
	"context"
	"database/sql"
)

// Tx 事务封装。由 Client.Begin 创建，提交/回滚前所有操作在同一事务内执行。
type Tx struct {
	tx *sql.Tx
}

// Exec 在事务中执行写操作。
func (t *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query 在事务中查询并返回 *sql.Rows。
func (t *Tx) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// Get 在事务中查询单行并反射到 dest。
func (t *Tx) Get(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return scanOne(rows, dest)
}

// Select 在事务中查询多行并反射到切片。
func (t *Tx) Select(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return scanSlice(dest, rows)
}

// Commit 提交事务。
func (t *Tx) Commit() error { return t.tx.Commit() }

// Rollback 回滚事务。
func (t *Tx) Rollback() error { return t.tx.Rollback() }

// Raw 返回底层 *sql.Tx。
func (t *Tx) Raw() *sql.Tx { return t.tx }
