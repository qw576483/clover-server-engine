package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// 反射扫描

// scanOne / scanSlice / scanRow / structFields / isScanner 共同实现「SQL 行 → Go 值」
// 的反射映射：结构体按列名（优先 db tag）映射字段；基础类型或实现 sql.Scanner 的
// 类型（如 sql.NullString）直接 rows.Scan。

// scanOne 读取单行反射到 dest，然后关闭 rows。无结果返回 ErrNotFound。
func scanOne(rows *sql.Rows, dest any) error {
	// 结果集已消费到底并把 rows.Err() 作为返回值，关闭失败无补救动作；显式忽略。
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return ErrNotFound
	}
	if err := scanRow(rows, dest); err != nil {
		return err
	}
	// 消费剩余行，确保完整结果集的错误被感知
	for rows.Next() {
	}
	return rows.Err()
}

// scanSlice 读取所有行反射到 dest（切片指针），然后关闭 rows。
func scanSlice(dest any, rows *sql.Rows) error {
	defer func() { _ = rows.Close() }()
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return fmt.Errorf("mysql ScanSlice: dest must be non-nil pointer to slice")
	}
	dv = dv.Elem()
	if dv.Kind() != reflect.Slice {
		return fmt.Errorf("mysql ScanSlice: dest must be pointer to slice, got %s", dv.Kind())
	}
	elemType := dv.Type().Elem()
	var elems []reflect.Value
	for rows.Next() {
		ev := reflect.New(elemType)
		if err := scanRow(rows, ev.Interface()); err != nil {
			return err
		}
		elems = append(elems, ev.Elem())
	}
	if err := rows.Err(); err != nil {
		return err
	}
	out := reflect.MakeSlice(dv.Type(), len(elems), len(elems))
	for i, e := range elems {
		out.Index(i).Set(e)
	}
	dv.Set(out)
	return nil
}

// scanRow 将当前行反射写入 dest：结构体指针按列名映射；基础类型/实现了 sql.Scanner
// 的类型（如 sql.NullString）直接 rows.Scan。
// isScanner 判在前（短路优先），先判断是否为 Scanner 类型再判断 Kind。
func scanRow(rows *sql.Rows, dest any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return fmt.Errorf("mysql scan: dest must be non-nil pointer, got %T", dest)
	}
	dv = dv.Elem()
	if isScanner(dv) || dv.Kind() != reflect.Struct {
		return rows.Scan(dest)
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	fields, err := structFields(dv)
	if err != nil {
		return err
	}
	// 列名统一小写后与 struct fields map（同样小写）比对，
	// 消除 MySQL 返回列名大小写不一致的映射失败。
	ptrs := make([]any, len(cols))
	for i, col := range cols {
		if f, ok := fields[strings.ToLower(col)]; ok {
			ptrs[i] = f.Addr().Interface()
		} else {
			var ignore sql.RawBytes
			ptrs[i] = &ignore // 列在结构体中无对应字段时丢弃
		}
	}
	return rows.Scan(ptrs...)
}

// structFields 收集结构体的可导出字段，键为小写列名（优先取 db tag）。
//
// 匿名字段（嵌入结构体）会递归展平到字段级：若把嵌入结构体按类型名整体入 map，
// scanRow 会把 driver 值 Scan 进整个结构体（报 unsupported Scan）。
// 不同字段映射到同一列名（重复 tag）会直接报错，避免后者静默覆盖前者。
func structFields(v reflect.Value) (map[string]reflect.Value, error) {
	fields := make(map[string]reflect.Value, v.NumField())
	if err := collectStructFields(v, fields); err != nil {
		return nil, err
	}
	return fields, nil
}

// collectStructFields 递归收集字段；v 须是可寻址的结构体值。
func collectStructFields(v reflect.Value, out map[string]reflect.Value) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" { // 非导出字段跳过
			continue
		}
		fv := v.Field(i)
		if sf.Anonymous {
			ft := sf.Type
			if ft.Kind() == reflect.Ptr {
				if fv.IsNil() {
					if !fv.CanSet() {
						continue
					}
					fv.Set(reflect.New(ft.Elem())) // 匿名的 nil 指针：先分配再展平
				}
				ft = ft.Elem()
				fv = fv.Elem()
			}
			if ft.Kind() == reflect.Struct {
				if err := collectStructFields(fv, out); err != nil {
					return err
				}
				continue
			}
			// 匿名非结构体（如嵌入 string）：按普通字段处理
		}
		name := sf.Name
		if tag := sf.Tag.Get("db"); tag != "" {
			if tag == "-" {
				continue
			}
			name = tag
		}
		key := strings.ToLower(name)
		if _, dup := out[key]; dup {
			return fmt.Errorf("mysql scan: duplicate column name %q in struct %s", key, t)
		}
		out[key] = fv
	}
	return nil
}

// isScanner 判断 v 是否实现了 sql.Scanner（如 sql.NullString/自定义类型）。
func isScanner(v reflect.Value) bool {
	if !v.CanAddr() {
		return false
	}
	_, ok := v.Addr().Interface().(sql.Scanner)
	return ok
}
