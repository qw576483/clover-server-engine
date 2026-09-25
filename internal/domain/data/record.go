package data

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/object"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
)

// Record 强类型表：由「列名 + 列类型」定义 schema，
// 单元格按 (行, 列) 强类型读写。任何修改都会记录「哪一行 / 哪一格变动」，可精准增量同步。

// 典型用途：背包格子、邮件列表、任务进度、排行榜……一切「多行同构」的数据。
// Record 并发安全。
type Record struct {
	mu        sync.RWMutex
	cols      []string
	colType   []object.Type
	rows      [][]object.Value
	dirty     map[int]struct{}                              // 变动行索引（SetCell / AddRow）
	deleted   []int                                         // 删除的行索引（DeleteRow 追加，MarkClean 清空）
	knownRows int                                           // 客户端已见行数（上次 MarkClean/载入时的行数）。新增行只会尾部追加，故已见行恒为前缀；用于跳过「删除客户端从未见过的行」的幻影删除
	cleared   bool                                          // 整表清空（Clear 设置，MarkClean 清空）
	fullDirty bool                                          // 结构性 schema 变动（AddColumn 加列时设置）
	maxRows   int                                           // 建议容量上限（AddRow 超出返回 -1），0 表示不限
	onChange  func(row, col int, v object.Value, full bool) // 可选：表变动即时回调（行级自动同步底座）
}

// NewRecord 按列名 + 列类型构造空表（两者长度必须一致）。
func NewRecord(cols []string, colTypes []object.Type) *Record {
	if len(cols) != len(colTypes) {
		n := min(len(cols), len(colTypes))
		cols = cols[:n]
		colTypes = colTypes[:n]
	}
	cc := make([]string, len(cols))
	ct := make([]object.Type, len(colTypes))
	copy(cc, cols)
	copy(ct, colTypes)
	return &Record{
		cols:    cc,
		colType: ct,
		rows:    nil,
		dirty:   make(map[int]struct{}),
	}
}

// ColCount 列数。
func (s *Record) ColCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cols)
}

// ColName 第 col 列名。
func (s *Record) ColName(col int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if col < 0 || col >= len(s.cols) {
		return ""
	}
	return s.cols[col]
}

// ColType 第 col 列类型。
func (s *Record) ColType(col int) object.Type {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if col < 0 || col >= len(s.colType) {
		return object.TypeInt
	}
	return s.colType[col]
}

// RowCount 当前行数。
func (s *Record) RowCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// MaxRows 建议容量上限。
func (s *Record) MaxRows() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxRows
}

// SetMaxRows 设置建议容量上限。
func (s *Record) SetMaxRows(n int) {
	s.mu.Lock()
	s.maxRows = n
	s.mu.Unlock()
}

// AddColumn 追加一列（扩展 schema），现有行用该类型零值补齐；结构性变动标记整表重同步。
func (s *Record) AddColumn(name string, typ object.Type) {
	s.mu.Lock()
	s.cols = append(s.cols, name)
	s.colType = append(s.colType, typ)
	for i := range s.rows {
		s.rows[i] = append(s.rows[i], object.NewZeroValue(typ))
	}
	s.fullDirty = true
	fn := s.onChange
	s.mu.Unlock()
	if fn != nil {
		fn(-1, -1, object.Value{}, true)
	}
}

// AddRow 追加一行全零值，返回行索引；超过 MaxRows（>0）时返回 -1。
func (s *Record) AddRow() int {
	s.mu.Lock()
	if s.maxRows > 0 && len(s.rows) >= s.maxRows {
		s.mu.Unlock()
		return -1
	}
	row := make([]object.Value, len(s.cols))
	for i := range row {
		row[i] = object.NewZeroValue(s.colType[i])
	}
	s.rows = append(s.rows, row)
	idx := len(s.rows) - 1
	s.dirty[idx] = struct{}{}
	fn := s.onChange
	s.mu.Unlock()
	if fn != nil {
		fn(idx, -1, object.Value{}, false)
	}
	return idx
}

// AddRowValues 追加一行给定值（长度须与列数一致），返回行索引。
// 长度不符时补零/截断为静默行为（调用方应保证一致），若 maxRows 限制触发则返回 -1。
func (s *Record) AddRowValues(vals []object.Value) int {
	s.mu.Lock()
	if s.maxRows > 0 && len(s.rows) >= s.maxRows {
		s.mu.Unlock()
		return -1
	}
	row := make([]object.Value, len(s.cols))
	for i := 0; i < len(s.cols); i++ {
		if i < len(vals) {
			row[i] = vals[i]
		} else {
			row[i] = object.NewZeroValue(s.colType[i])
		}
	}
	s.rows = append(s.rows, row)
	idx := len(s.rows) - 1
	s.dirty[idx] = struct{}{}
	fn := s.onChange
	s.mu.Unlock()
	if fn != nil {
		fn(idx, -1, object.Value{}, false)
	}
	return idx
}

// DeleteRow 删除第 row 行（行级增量同步：仅记录被删索引，不清除脏标记）。返回是否成功。
// 后续通过 Changes() 可提取 deleted 列表供客户端按序移除。
// 若被删的行是本次同步窗口内新增（客户端从未见过），则不记入 deleted：
// 客户端表中不存在该行，下发删除会越界/错删（幻影删除）。
func (s *Record) DeleteRow(row int) bool {
	s.mu.Lock()
	if row < 0 || row >= len(s.rows) {
		s.mu.Unlock()
		return false
	}
	knownRow := row < s.knownRows
	if knownRow {
		s.knownRows--
	}
	// 原地搬移后，底层数组的尾槽仍引用旧行（含 TypeBytes 大块内存）：
	// 显式置空，让 GC 立即回收，而不必等后续 append 覆盖。
	old := s.rows
	s.rows = append(old[:row], old[row+1:]...)
	old[len(old)-1] = nil
	// 脏标记索引平移：>row 的行索引 -1，=row 的行从 dirty 中移除（已被删）
	if len(s.dirty) > 0 {
		newDirty := make(map[int]struct{}, len(s.dirty))
		for i := range s.dirty {
			if i < row {
				newDirty[i] = struct{}{}
			} else if i > row {
				newDirty[i-1] = struct{}{}
			}
			// i == row 丢弃（该行已不存在）
		}
		s.dirty = newDirty
	}
	if knownRow {
		s.deleted = append(s.deleted, row)
	}
	fn := s.onChange
	s.mu.Unlock()
	if fn != nil {
		fn(row, -1, object.Value{}, false)
	}
	return true
}

// Clear 清空所有行（行级增量同步：标记 cleared 而非整表重推）。
func (s *Record) Clear() {
	s.mu.Lock()
	s.rows = nil
	s.dirty = make(map[int]struct{})
	s.deleted = nil // cleared 已覆盖全部删除语义
	s.knownRows = 0 // 清空后客户端视图为空，后续新增行的删除不再下发
	s.cleared = true
	fn := s.onChange
	s.mu.Unlock()
	if fn != nil {
		fn(-1, -1, object.Value{}, false)
	}
}

// SetCell 写单元格 (row, col)；值真正变化才标记该行脏。
func (s *Record) SetCell(row, col int, v object.Value) {
	s.mu.Lock()
	if row < 0 || row >= len(s.rows) || col < 0 || col >= len(s.cols) {
		s.mu.Unlock()
		return
	}
	if object.ValueEqual(s.rows[row][col], v) {
		s.mu.Unlock()
		return
	}
	// TypeBytes 深拷贝后再存储：否则调用方持有的 Y 底层数组与表内共享，
	// 外部修改会绕过本方法（不标脏）直接污染表数据（与 GetCell/Changes 的深拷贝语义一致）。
	if v.Type() == object.TypeBytes && v.Y != nil {
		v.Y = append([]byte{}, v.Y...)
	}
	s.rows[row][col] = v
	s.dirty[row] = struct{}{}
	fn := s.onChange
	s.mu.Unlock()
	if fn != nil {
		fn(row, col, v, false)
	}
}

// SetNotifier 注册「表变动即时回调」（写即自动同步的底座，行级）。
// 传 nil 注销回调。回调在释放内部锁后调用，可安全回读 Record（不会死锁）。
func (s *Record) SetNotifier(fn func(row, col int, v object.Value, full bool)) {
	s.mu.Lock()
	s.onChange = fn
	s.mu.Unlock()
}

// GetCell 读单元格 (row, col)（越界返回零值）。
// TypeBytes 深拷贝底层数组返回，避免调用方修改返回值污染表内部状态（与 Changes 一致）。
func (s *Record) GetCell(row, col int) object.Value {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if row < 0 || row >= len(s.rows) || col < 0 || col >= len(s.cols) {
		return object.Value{}
	}
	v := s.rows[row][col]
	if v.Type() == object.TypeBytes && v.Y != nil {
		v.Y = append([]byte{}, v.Y...)
	}
	return v
}

// Row 返回第 row 行的值拷贝（越界返回 nil）。
// TypeBytes 深拷贝底层数组：仅做结构体层浅拷贝时 Y []byte 仍与表内共享，
// 调用方修改返回值会绕过 SetCell 的脏标记直接污染表内部状态（与 GetCell/Changes 一致）。
func (s *Record) Row(row int) []object.Value {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if row < 0 || row >= len(s.rows) {
		return nil
	}
	cp := make([]object.Value, len(s.rows[row]))
	copy(cp, s.rows[row])
	for j := range cp {
		if cp[j].Type() == object.TypeBytes && cp[j].Y != nil {
			cp[j].Y = append([]byte{}, cp[j].Y...)
		}
	}
	return cp
}

// ColIndex 根据列名返回列索引，未找到返回 -1。
func (s *Record) ColIndex(name string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, n := range s.cols {
		if n == name {
			return i
		}
	}
	return -1
}

// Find 查找指定列中值与 v 相等的第一行，返回行索引，未找到返回 -1。
// col 为列名或列索引（int）。
func (s *Record) Find(col any, v object.Value) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var colIdx int
	switch c := col.(type) {
	case int:
		colIdx = c
	case string:
		colIdx = -1
		for i, n := range s.cols {
			if n == c {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			return -1
		}
	default:
		return -1
	}
	if colIdx < 0 || colIdx >= len(s.cols) {
		return -1
	}
	for row := range s.rows {
		if row < len(s.rows) && colIdx < len(s.rows[row]) {
			if object.ValueEqual(s.rows[row][colIdx], v) {
				return row
			}
		}
	}
	return -1
}

// MarkClean 清除脏标记（落库 / 广播后调用）。
func (s *Record) MarkClean() {
	s.mu.Lock()
	s.dirty = make(map[int]struct{})
	s.deleted = nil
	s.cleared = false
	s.fullDirty = false
	s.knownRows = len(s.rows) // 同步完成，当前全部行均为客户端已见
	s.mu.Unlock()
}

// RowDirty 第 row 行自上次 MarkClean 是否变动。
func (s *Record) RowDirty(row int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.dirty[row]
	return ok
}

// DirtyRows 返回变动行索引集合（升序）。
func (s *Record) DirtyRows() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int, 0, len(s.dirty))
	for i := range s.dirty {
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}

// FullDirty 是否发生结构性变动（需整表重同步）。
func (s *Record) FullDirty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fullDirty
}

// CommitDiff 为增量同步接口：若非全表重推，仅序列化脏行 + 删除索引 + 清空标记；否则序列化整表。
// 增量行采用裸值格式（与 MarshalJSON 一致），客户端已有 cols schema 无需每格带类型。
// 成功序列化后消费脏标记（MarkClean），否则下次调用会重复推送同一批增量。
func (s *Record) CommitDiff() ([]byte, bool) {
	dirtyRows, deletedRows, cleared, full := s.Changes()
	if full {
		b, err := json.Marshal(s)
		if err != nil {
			// 序列化失败属非预期分支：必须留日志（否则上层只看到「没有 diff」）。
			logger.Errorf("data: record CommitDiff full marshal failed (rows=%d cols=%d): %v", s.RowCount(), s.ColCount(), err)
			return nil, true
		}
		s.MarkClean()
		return b, true
	}
	// 把 []object.Value 转成裸值 JSON，与全量格式一致。
	// 使用 [][2]any（[index, cells]）代替 map[int]，避免 JSON 序列化时 int key 变 string，
	// 客户端可按行号精准 patch。
	type rowEntry struct {
		Index int               `json:"index"`
		Cells []json.RawMessage `json:"cells"`
	}
	rawRows := make([]rowEntry, 0, len(dirtyRows))
	for idx, row := range dirtyRows {
		cells := make([]json.RawMessage, len(row))
		for j, cell := range row {
			raw, err := cell.MarshalValueOnly()
			if err != nil {
				logger.Errorf("data: record CommitDiff cell marshal failed (row=%d col=%d): %v", idx, j, err)
				return nil, false
			}
			cells[j] = raw
		}
		rawRows = append(rawRows, rowEntry{Index: idx, Cells: cells})
	}
	patch := struct {
		Rows    []rowEntry `json:"rows"`
		Deleted []int      `json:"deleted,omitempty"`
		Cleared bool       `json:"cleared,omitempty"`
	}{Rows: rawRows, Deleted: deletedRows, Cleared: cleared}
	b, err := json.Marshal(patch)
	if err != nil {
		logger.Errorf("data: record CommitDiff patch marshal failed (rows=%d): %v", len(rawRows), err)
		return nil, false
	}
	s.MarkClean()
	return b, false
}

// Changes 返回：变动行的快照 + 删除的行索引（按服务器端操作顺序） + 是否清空 + 是否整表重同步（schema 变更）。
// 客户端处理顺序：① 若 Full → 整表替换 ② 若 Cleared → 清空 ③ 按 forward 顺序移除 Deleted 行 ④ 应用 Rows。
func (s *Record) Changes() (dirtyRows map[int][]object.Value, deletedRows []int, cleared bool, full bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[int][]object.Value, len(s.dirty))
	for i := range s.dirty {
		if i < len(s.rows) {
			cp := make([]object.Value, len(s.rows[i]))
			copy(cp, s.rows[i])
			// 深拷贝共享引用的 []byte（底层数组独立，防止外部修改污染内部状态）。
			for j := range cp {
				if cp[j].Type() == object.TypeBytes && cp[j].Y != nil {
					cp[j].Y = append([]byte{}, cp[j].Y...)
				}
			}
			out[i] = cp
		}
	}
	del := make([]int, len(s.deleted))
	copy(del, s.deleted)
	return out, del, s.cleared, s.fullDirty
}

// JSON 线化（标准格式，客户端 / 服务器共用）
type recordWire struct {
	Cols [][]json.RawMessage `json:"cols"`
	Rows [][]json.RawMessage `json:"rows"`
}

// MarshalJSON 紧凑线化：{cols:[[name,type],...], rows:[[v1,v2],...]}。
// cols 每项为 [列名, 列类型]，客户端按列类型解析裸值即可，不再每格冗余 {"t","v"}。
func (s *Record) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := make([][]json.RawMessage, len(s.rows))
	for i, row := range s.rows {
		cells := make([]json.RawMessage, len(row))
		for j, cell := range row {
			raw, err := cell.MarshalValueOnly()
			if err != nil {
				return nil, err
			}
			cells[j] = raw
		}
		rows[i] = cells
	}
	cols := make([][]json.RawMessage, len(s.cols))
	for i := range s.cols {
		nameJSON, err := json.Marshal(s.cols[i])
		if err != nil {
			return nil, err
		}
		typeJSON, err := json.Marshal(s.colType[i])
		if err != nil {
			return nil, err
		}
		cols[i] = []json.RawMessage{nameJSON, typeJSON}
	}
	w := recordWire{
		Cols: cols,
		Rows: rows,
	}
	return json.Marshal(w)
}

// UnmarshalJSON 从紧凑格式载入（覆盖 schema 与数据）。
//
// 全部解析先在临时变量中完成，成功后才一次性替换内部状态：
// 先替换 s.cols/s.colType 再逐列解析时，中途失败会留下「新 cols + 旧 rows」
// 的半更新态（对象残缺，且调用方已经拿到 error 无从修复）。
func (s *Record) UnmarshalJSON(data []byte) error {
	var w recordWire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	cols := make([]string, len(w.Cols))
	colType := make([]object.Type, len(w.Cols))
	for i, entry := range w.Cols {
		if len(entry) < 2 {
			// 容忍残缺列定义，但留日志：静默留空列名会让后续按列名取值全部 miss。
			logger.Warnf("data: record UnmarshalJSON col %d has %d fields (want name+type), defaulting to empty name/TypeInt", i, len(entry))
			continue
		}
		if err := json.Unmarshal(entry[0], &cols[i]); err != nil {
			return err
		}
		if err := json.Unmarshal(entry[1], &colType[i]); err != nil {
			return err
		}
	}
	rows := make([][]object.Value, len(w.Rows))
	for i, rowRaw := range w.Rows {
		if len(rowRaw) > len(colType) {
			logger.Warnf("data: record UnmarshalJSON row %d has %d cells, exceeding %d columns (extra cells decoded as TypeInt)", i, len(rowRaw), len(colType))
		}
		rows[i] = make([]object.Value, len(rowRaw))
		for j, cellRaw := range rowRaw {
			colT := object.TypeInt
			if j < len(colType) {
				colT = colType[j]
			}
			rows[i][j] = object.NewValueFromRaw(colT, cellRaw)
		}
	}
	s.mu.Lock()
	s.cols = cols
	s.colType = colType
	s.rows = rows
	s.resetDirtyLocked()
	s.mu.Unlock()
	return nil
}

// resetDirtyLocked 重置变更追踪（调用方须持有 s.mu）。
func (s *Record) resetDirtyLocked() {
	s.dirty = make(map[int]struct{})
	s.deleted = nil
	s.cleared = false
	s.fullDirty = false
	s.knownRows = len(s.rows)
}

// ————————————————————————————————————————————————
// RecordSchema 真身在 pkg/domain/data/types.go，通过别名引用。
// ————————————————————————————————————————————————
