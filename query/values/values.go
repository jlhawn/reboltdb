package values

import (
	"math"

	"gopkg.in/rethinkdb/rethinkdb-go.v5/ql2"
)

type Error struct {
	Type    ql2.Response_ErrorType
	Message string
}

type Top interface {
	IsDatum() bool
	IsSequence() bool
	IsDatabase() bool
	IsFunction() bool
	IsOrdering() bool
	IsPathSpec() bool
}

type Datum interface {
	Top
	IsNull() bool
	IsMinVal() bool
	IsMaxVal() bool
	IsBool() bool
	IsNumber() bool
	IsString() bool
	IsObject() bool
	IsArray() bool
	IsTime() bool
	IsBinary() bool
	IsGeometry() bool
	AsBool() Bool
	AsNumber() Number
	AsString() String
	AsObject() Object
	AsArray() Array
	AsTime() Time
	AsBinary() Binary
	AsGeometry() Geometry
}

type top struct{}

func (top) IsDatum() bool    { return false }
func (top) IsSequence() bool { return false }
func (top) IsDatabase() bool { return false }
func (top) IsFunction() bool { return false }
func (top) IsOrdering() bool { return false }
func (top) IsPathSpec() bool { return false }

type datum struct{ top }

func (datum) IsDatum() bool { return true }

func (datum) IsNull() bool         { return false }
func (datum) IsMinVal() bool       { return false }
func (datum) IsMaxVal() bool       { return false }
func (datum) IsBool() bool         { return false }
func (datum) IsNumber() bool       { return false }
func (datum) IsString() bool       { return false }
func (datum) IsObject() bool       { return false }
func (datum) IsArray() bool        { return false }
func (datum) IsTime() bool         { return false }
func (datum) IsBinary() bool       { return false }
func (datum) IsGeometry() bool     { return false }
func (datum) AsBool() Bool         { return Bool{} }
func (datum) AsNumber() Number     { return Number{} }
func (datum) AsString() String     { return String{} }
func (datum) AsObject() Object     { return object{} }
func (datum) AsArray() Array       { return Array{} }
func (datum) AsTime() Time         { return Time{} }
func (datum) AsBinary() Binary     { return Binary{} }
func (datum) AsGeometry() Geometry { return Geometry{} }

type Null struct{ datum }

func (Null) IsNull() bool { return true }

type MinVal struct{ datum }

func (MinVal) IsMinVal() bool { return true }

type MaxVal struct{ datum }

func (MaxVal) IsMaxVal() bool { return true }

type Bool struct {
	datum
	val bool
}

func (Bool) IsBool() bool   { return true }
func (b Bool) AsBool() Bool { return b }

func (b Bool) Value() bool { return b.val }

type Number struct {
	datum
	val float64
}

func (Number) IsNumber() bool     { return true }
func (n Number) AsNumber() Number { return n }

func (n Number) IsInteger() bool  { return n.val == math.Trunc(n.val) }
func (n Number) Int64() int64     { return int64(n.val) }
func (n Number) Float64() float64 { return n.val }

type String struct {
	datum
	val string
}

func (String) IsString() bool     { return true }
func (s String) AsString() String { return s }

func (s String) Value() string { return s.val }

type Object interface {
	Datum
	Items() map[string]Datum
	IsSelection() bool
	AsSelection() Selection
}

type object struct {
	datum
	items map[string]Datum
}

func (object) IsObject() bool     { return true }
func (o object) AsObject() Object { return o }

func (o object) Items() map[string]Datum { return o.items }
func (o object) IsSelection() bool       { return false }
func (o object) AsSelection() Selection  { return selection{} }

type Array struct {
	sequence
	datum
	items []Datum
}

func (Array) IsArray() bool    { return true }
func (a Array) AsArray() Array { return a }

func (a Array) Items() []Datum { return a.items }

type Time struct{ datum }

func (Time) IsTime() bool   { return true }
func (t Time) AsTime() Time { return t }

type Binary struct{ datum }

func (Binary) IsBinary() bool     { return true }
func (b Binary) AsBinary() Binary { return b }

type Geometry struct{ datum }

func (Geometry) IsGeometry() bool       { return true }
func (g Geometry) AsGeometry() Geometry { return g }

type Selection interface {
	Object
	TableDescriptor
	Changes(options Object) Stream
}

type TableDescriptor interface {
	DB() string
	Table() string
}

type tableDescriptor struct {
	db, table string
}

func (td tableDescriptor) DB() string    { return td.db }
func (td tableDescriptor) Table() string { return td.table }

type selection struct {
	object
	tableDescriptor
}

func (selection) IsSelection() bool             { return true }
func (s selection) AsSelection() Selection      { return s }
func (selection) Changes(options Object) Stream { return stream{} }

type Sequence interface {
	Top
	IsArray() bool
	IsStream() bool
	AsArray() Array
	AsStream() Stream
}

type sequence struct{ top }

func (sequence) IsArray() bool    { return false }
func (sequence) IsStream() bool   { return false }
func (sequence) AsArray() Array   { return Array{} }
func (sequence) AsStream() Stream { return stream{} }

type Stream interface {
	Sequence
	IsSelectionStream() bool
	AsSelectionStream() SelectionStream
	NextItem() (Datum, *Error)
	Changes(options Object) Stream
}

type stream struct {
	sequence
}

func (stream) IsSelectionStream() bool            { return false }
func (stream) AsSelectionStream() SelectionStream { return selectionStream{} }
func (stream) NextItem() (Datum, *Error)          { return nil, nil }
func (s stream) Changes(options Object) Stream    { return s }

type SelectionStream interface {
	Stream
	TableDescriptor
	IsTable() bool
	AsTable() Table
	Next() (Selection, *Error)
}

type selectionStream struct {
	stream
	tableDescriptor
}

func (selectionStream) IsSelectionStream() bool              { return true }
func (s selectionStream) AsSelectionStream() SelectionStream { return s }

func (selectionStream) IsTable() bool               { return false }
func (selectionStream) AsTable() Table              { return nil }
func (s selectionStream) Next() (Selection, *Error) { return nil, nil }

type IndexOrderedSelectionStream interface {
	SelectionStream
	Between(lowerKey, upperKey Datum, options Object) SelectionStream
}

type Table interface {
	SelectionStream
	Name() string
	Get(key Datum) Selection
	GetAll(keys []Datum, index string) SelectionStream
	Between(lowerKey, upperKey Datum, index string, options Object) SelectionStream
	OrderBy(index string, descending bool, nextOrdering Ordering) (IndexOrderedSelectionStream, *Error)
	Distinct(index string) (Stream, *Error)
	InsertObject(obj Object, conflict, durability string, returnChanges bool) Object
	InsertSequence(seq Sequence, conflict, durability string, returnChanges bool) Object
	Wait() Object
	Sync() Object
	IndexCreate(name string, indexFunc Function, multi bool) (Object, *Error)
	IndexDrop(name string) (Object, *Error)
	IndexList() Array
	IndexStatus(names ...string) Array
	IndexWait(names ...string) Array
	IndexRename(oldName, newName string, overwrite bool) (Object, *Error)
}

type Database interface {
	Top
	Name()
}

type database struct {
	top
	name string
}

func (database) IsDatabase() bool { return true }

type Function interface {
	Args() []int64
	Eval(env map[int64]Datum) Datum
}

type Ordering interface {
	Key(d Datum) string
	Descending() bool
}
