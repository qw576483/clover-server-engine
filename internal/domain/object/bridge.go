package object

// bridge.go 将 pkg/domain/object 的核心类型重导出到 internal，
// 使同包内的 game_object / bag / codec / sync_entity 等文件无需修改引用。

import (
	pobj "github.com/qw576483/clover-server-engine/pkg/domain/object"
)

// 类型重导出（pkg 本体）
type (
	ObjectID        = pobj.ObjectID
	Object          = pobj.Object
	Value           = pobj.Value
	Message         = pobj.Message
	MessageReceiver = pobj.MessageReceiver
	Handler         = pobj.Handler
	EventHandler    = pobj.EventHandler
	BasicObject     = pobj.BasicObject
	Manager         = pobj.Manager
	AttrID          = pobj.AttrID
	AttrKind        = pobj.AttrKind
	AttrDef         = pobj.AttrDef
	AttrChangeFn    = pobj.AttrChangeFn
	AttrSet         = pobj.AttrSet
	// Type 值类型标签（uint8）：同时被 schema / bag / codec / binary / json 引用。
	Type = pobj.Type
)

// 常量重导出
const (
	TypePlayer    = pobj.TypePlayer
	TypeScene     = pobj.TypeScene
	AttrKindInt   = pobj.AttrKindInt
	AttrKindFloat = pobj.AttrKindFloat

	// 值类型标签常量（Value.Type / RecordSchema.ColTypes 使用）。
	TypeNil    = pobj.TypeNil
	TypeInt    = pobj.TypeInt
	TypeFloat  = pobj.TypeFloat
	TypeString = pobj.TypeString
	TypeBytes  = pobj.TypeBytes
	TypeBool   = pobj.TypeBool
	TypeObject = pobj.TypeObject
)

// 函数重导出
var (
	NewObjectID    = pobj.NewObjectID
	ParseObjectID  = pobj.ParseObjectID
	FromUint64     = pobj.FromUint64
	NewManager     = pobj.NewManager
	NewBasicObject = pobj.NewBasicObject
	NewAttrSet     = pobj.NewAttrSet

	// Value 构造函数。
	NewInt          = pobj.NewInt
	NewFloat        = pobj.NewFloat
	NewString       = pobj.NewString
	NewBytes        = pobj.NewBytes
	NewBool         = pobj.NewBool
	NewObject       = pobj.NewObject
	NewZeroValue    = pobj.NewZeroValue
	NewValueFromRaw = pobj.NewValueFromRaw

	// 值比较。
	ValueEqual = pobj.ValueEqual
)

// 错误重导出
var (
	ErrObjectNotFound = pobj.ErrObjectNotFound
	ErrNoHandler      = pobj.ErrNoHandler
	ErrNoEventHandler = pobj.ErrNoEventHandler
)
