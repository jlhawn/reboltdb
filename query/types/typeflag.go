package types

type TypeFlag int64

// TODO: GroupedData, GroupedStream
const (
	Datum TypeFlag = 1 << iota
	Sequence
	Database
	Function
	Ordering
	Null = Datum | (1 << iota)
	MinVal
	MaxVal
	Bool
	Number
	String
	Object
	Time
	Binary
	Geometry
	Selection       = Object | (1 << iota)
	Array           = Datum | Sequence | (1 << iota)
	SelectionArray  = Array | (1 << iota)
	Stream          = Sequence | (1 << iota)
	SelectionStream = Stream | (1 << iota)
	TableSlice      = SelectionStream | (1 << iota)
	Table           = SelectionStream | (1 << iota)
)

var allFlags = map[TypeFlag]string{
	Datum:           "DATUM",
	Sequence:        "SEQUENCE",
	Database:        "DATABASE",
	Function:        "FUNCTION",
	Ordering:        "ORDERING",
	Null:            "NULL",
	MinVal:          "MINVAL",
	MaxVal:          "MAXVAL",
	Bool:            "BOOL",
	Number:          "NUMBER",
	String:          "STRING",
	Object:          "OBJECT",
	Time:            "PTYPE<TIME>",
	Binary:          "PTYPE<BINARY>",
	Geometry:        "PTYPE<GEOMETRY>",
	Selection:       "SELECTION<OBJECT>",
	Array:           "ARRAY",
	SelectionArray:  "SELECTION<ARRAY>",
	Stream:          "STREAM",
	SelectionStream: "SELECTION<STREAM>",
	TableSlice:      "TABLE_SLICE",
	Table:           "TABLE",
}

func (t TypeFlag) String() string {
	return allFlags[t]
}

func (t TypeFlag) IsSubTypeOf(other TypeFlag) bool {
	return t&other == other
}
