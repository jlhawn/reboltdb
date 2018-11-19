package query

import (
	"fmt"
	"strings"

	"gopkg.in/rethinkdb/rethinkdb-go.v5/ql2"

	"github.com/jlhawn/reboltdb/json"
	"github.com/jlhawn/reboltdb/query/types"
)

type Term struct {
	Type ql2.Term_TermType

	Args    []*Term
	OptArgs map[string]*Term

	Datum json.Value // This is nil unless Type is DATUM.
}

// TODO: This should return a compile error type with frames
func MakeTermTree(value json.Value) (*Term, error) {
	// An object must be recursively evaluated as a MAKE_OBJECT term.
	if value.IsObject() {
		return makeObjectTerm(value.AsObject())
	}

	// An array is actually a recursive Term.
	// Any other type is just a datum.
	if !value.IsArray() {
		return &Term{
			Type:  ql2.Term_DATUM,
			Datum: value,
		}, nil
	}

	termArray := value.AsArray()
	if len(termArray) == 0 || len(termArray) > 3 {
		return nil, fmt.Errorf("expected 1 to 3 entries in term array, but got %d", len(termArray))
	}

	if !termArray[0].IsNumber() {
		return nil, fmt.Errorf("expected term type to be number, but got %s", termArray[0].ValueType())
	}
	termType := ql2.Term_TermType(termArray[0].AsInt64())

	var termArgs []*Term
	if len(termArray) > 1 {
		if !termArray[1].IsArray() {
			return nil, fmt.Errorf("expected term args to by type array, but got %s", termArray[1].ValueType())
		}
		argVals := termArray[1].AsArray()
		termArgs = make([]*Term, len(argVals))
		for i, argVal := range argVals {
			var err error
			if termArgs[i], err = MakeTermTree(argVal); err != nil {
				return nil, fmt.Errorf("%s arg[%d] -> %s", ql2.Term_TermType_name[int32(termType)], i, err)
			}
		}
	}

	var termOptArgs map[string]*Term
	if len(termArray) > 2 {
		if !termArray[2].IsObject() {
			return nil, fmt.Errorf("expected term args to by type object, but got %s", termArray[2].ValueType())
		}
		optArgVals := termArray[2].AsObject()
		termOptArgs = make(map[string]*Term, len(termOptArgs))
		for key, optArgVal := range optArgVals {
			var err error
			if termOptArgs[key], err = MakeTermTree(optArgVal); err != nil {
				return nil, fmt.Errorf("%s optArg[%q] -> %s", ql2.Term_TermType_name[int32(termType)], key, err)
			}
		}
	}

	return &Term{
		Type:    termType,
		Args:    termArgs,
		OptArgs: termOptArgs,
	}, nil
}

func makeObjectTerm(object json.Object) (*Term, error) {
	termOptArgs := make(map[string]*Term, len(object))
	for key, val := range object {
		var err error
		termOptArgs[key], err = MakeTermTree(val)
		if err != nil {
			return nil, fmt.Errorf("obj[%q] -> %s", key, err)
		}
	}

	return &Term{
		Type:    ql2.Term_MAKE_OBJ,
		OptArgs: termOptArgs,
	}, nil
}

func (t *Term) String() string {
	var b strings.Builder
	t.format(&b, 0)
	return b.String()
}

func (t *Term) format(b *strings.Builder, i int) {
	if t.IsDatum() {
		switch {
		case t.Datum.IsNull():
			fmt.Fprint(b, "null")
		case t.Datum.IsString():
			fmt.Fprintf(b, "%q", t.Datum)
		default:
			fmt.Fprintf(b, "%v", t.Datum)
		}
		return
	}

	// Start with term name.
	fmt.Fprintf(b, "(%s", ql2.Term_TermType_name[int32(t.Type)])

	t.formatArgs(b, i)
	t.formatOptArgs(b, i)

	fmt.Fprint(b, ")")
}

func (t *Term) formatArgs(b *strings.Builder, i int) {
	if t.Args == nil {
		return
	}

	indent := strings.Repeat(" ", 2*i)
	multiArg := len(t.Args) > 1
	if multiArg {
		i += 1 // Indent args by an extra level.
	}

	fmt.Fprint(b, " [")
	for _, arg := range t.Args {
		if multiArg {
			// Indent multiple args each on its own line.
			fmt.Fprintf(b, "\n%s  ", indent)
		}
		arg.format(b, i)
	}
	if multiArg {
		fmt.Fprintf(b, "\n%s", indent)
	}
	fmt.Fprint(b, "]")
}

func (t *Term) formatOptArgs(b *strings.Builder, i int) {
	if t.OptArgs == nil {
		return
	}

	indent := strings.Repeat(" ", 2*i)
	multiArg := len(t.OptArgs) > 1
	if multiArg {
		i += 1 // Indent args by an extra level.
	}

	fmt.Fprint(b, " {")
	for key, arg := range t.OptArgs {
		if multiArg {
			// Indent multiple args each on its own line.
			fmt.Fprintf(b, "\n%s  ", indent)
		}
		fmt.Fprintf(b, "%q: ", key)
		arg.format(b, i)
	}
	if multiArg {
		fmt.Fprintf(b, "\n%s", indent)
	}
	fmt.Fprint(b, "}")
}

func (t *Term) IsDatum() bool {
	return t.Type == ql2.Term_DATUM
}

func valueType(val json.Value) types.TypeFlag {
	switch {
	case val.IsNull():
		return types.Null
	case val.IsBool():
		return types.Bool
	case val.IsNumber():
		return types.Number
	case val.IsString():
		return types.String
	case val.IsObject():
		return types.Object
	case val.IsArray():
		return types.Array
	}
	return 0
}

var returnTypeMap = map[ql2.Term_TermType]types.TypeFlag{
	ql2.Term_MAKE_ARRAY:       types.Array,
	ql2.Term_MAKE_OBJ:         types.Object,
	ql2.Term_VAR:              types.Datum,
	ql2.Term_JAVASCRIPT:       0, // DO NOT IMPLEMENT.
	ql2.Term_UUID:             types.String,
	ql2.Term_HTTP:             0, // DO NOT IMPLEMENT.
	ql2.Term_ERROR:            0, // No return type; Raises error.
	ql2.Term_IMPLICIT_VAR:     types.Datum,
	ql2.Term_DB:               types.Database,
	ql2.Term_TABLE:            types.Table,
	ql2.Term_GET:              types.Selection,
	ql2.Term_GET_ALL:          types.SelectionStream,
	ql2.Term_EQ:               types.Bool,
	ql2.Term_NE:               types.Bool,
	ql2.Term_LT:               types.Bool,
	ql2.Term_LE:               types.Bool,
	ql2.Term_GT:               types.Bool,
	ql2.Term_GE:               types.Bool,
	ql2.Term_NOT:              types.Bool,
	ql2.Term_ADD:              types.Number | types.String | types.Time,
	ql2.Term_SUB:              types.Number | types.Time,
	ql2.Term_MUL:              types.Number | types.Array,
	ql2.Term_DIV:              types.Number,
	ql2.Term_MOD:              types.Number,
	ql2.Term_FLOOR:            types.Number,
	ql2.Term_CEIL:             types.Number,
	ql2.Term_ROUND:            types.Number,
	ql2.Term_APPEND:           types.Array,
	ql2.Term_PREPEND:          types.Array,
	ql2.Term_DIFFERENCE:       types.Array,
	ql2.Term_SET_INSERT:       types.Array,
	ql2.Term_SET_INTERSECTION: types.Array,
	ql2.Term_SET_UNION:        types.Array,
	ql2.Term_SET_DIFFERENCE:   types.Array,
	ql2.Term_SLICE:            types.String | types.Binary | types.Array | types.Stream | types.SelectionStream,
	ql2.Term_SKIP:             types.Stream | types.Array,
	ql2.Term_LIMIT:            types.Stream | types.Array,
	ql2.Term_OFFSETS_OF:       types.Array,
	ql2.Term_CONTAINS:         types.Bool,
	ql2.Term_GET_FIELD:        0,
	ql2.Term_KEYS:             0,
	ql2.Term_VALUES:           0,
	ql2.Term_OBJECT:           0,
	ql2.Term_HAS_FIELDS:       0,
	ql2.Term_WITH_FIELDS:      0,
	ql2.Term_PLUCK:            0,
	ql2.Term_WITHOUT:          0,
	ql2.Term_MERGE:            0,
	ql2.Term_BETWEEN:          0,
	ql2.Term_REDUCE:           0,
	ql2.Term_MAP:              0,
	ql2.Term_FOLD:             0,
	ql2.Term_FILTER:           0,
	ql2.Term_CONCAT_MAP:       0,
	ql2.Term_ORDER_BY:         0,
	ql2.Term_DISTINCT:         0,
	ql2.Term_COUNT:            0,
	ql2.Term_IS_EMPTY:         0,
	ql2.Term_UNION:            0,
	ql2.Term_NTH:              0,
	ql2.Term_BRACKET:          0,
	ql2.Term_INNER_JOIN:       0,
	ql2.Term_OUTER_JOIN:       0,
	ql2.Term_EQ_JOIN:          0,
	ql2.Term_ZIP:              0,
	ql2.Term_RANGE:            0,
	ql2.Term_INSERT_AT:        0,
	ql2.Term_DELETE_AT:        0,
	ql2.Term_CHANGE_AT:        0,
	ql2.Term_SPLICE_AT:        0,
	ql2.Term_COERCE_TO:        0,
	ql2.Term_TYPE_OF:          0,
	ql2.Term_UPDATE:           0,
	ql2.Term_DELETE:           0,
	ql2.Term_REPLACE:          0,
	ql2.Term_INSERT:           0,
	ql2.Term_DB_CREATE:        0,
	ql2.Term_DB_DROP:          0,
	ql2.Term_DB_LIST:          0,
	ql2.Term_TABLE_CREATE:     0,
	ql2.Term_TABLE_DROP:       0,
	ql2.Term_TABLE_LIST:       0,
	ql2.Term_CONFIG:           0,
	ql2.Term_STATUS:           0,
	ql2.Term_WAIT:             0,
	ql2.Term_RECONFIGURE:      0,
	ql2.Term_REBALANCE:        0,
	ql2.Term_SYNC:             0,
	ql2.Term_GRANT:            0,
	ql2.Term_INDEX_CREATE:     0,
	ql2.Term_INDEX_DROP:       0,
	ql2.Term_INDEX_STATUS:     0,
	ql2.Term_INDEX_WAIT:       0,
	ql2.Term_INDEX_RENAME:     0,
	ql2.Term_SET_WRITE_HOOK:   0,
	ql2.Term_GET_WRITE_HOOK:   0,
	ql2.Term_FUNCALL:          0,
	ql2.Term_BRANCH:           0,
	ql2.Term_OR:               0,
	ql2.Term_AND:              0,
	ql2.Term_FOR_EACH:         0,
	ql2.Term_FUNC:             0,
	ql2.Term_ASC:              0,
	ql2.Term_DESC:             0,
	ql2.Term_INFO:             0,
	ql2.Term_MATCH:            0,
	ql2.Term_UPCASE:           0,
	ql2.Term_DOWNCASE:         0,
	ql2.Term_SAMPLE:           0,
	ql2.Term_DEFAULT:          0,
	ql2.Term_JSON:             0,
	ql2.Term_ISO8601:          0,
	ql2.Term_TO_ISO8601:       0,
	ql2.Term_EPOCH_TIME:       0,
	ql2.Term_TO_EPOCH_TIME:    0,
	ql2.Term_NOW:              0,
	ql2.Term_IN_TIMEZONE:      0,
	ql2.Term_DURING:           0,
	ql2.Term_DATE:             0,
	ql2.Term_TIME_OF_DAY:      0,
	ql2.Term_TIMEZONE:         0,
	ql2.Term_YEAR:             0,
	ql2.Term_MONTH:            0,
	ql2.Term_DAY:              0,
	ql2.Term_DAY_OF_WEEK:      0,
	ql2.Term_DAY_OF_YEAR:      0,
	ql2.Term_HOURS:            0,
	ql2.Term_MINUTES:          0,
	ql2.Term_SECONDS:          0,
	ql2.Term_TIME:             0,
	ql2.Term_MONDAY:           0,
	ql2.Term_TUESDAY:          0,
	ql2.Term_WEDNESDAY:        0,
	ql2.Term_THURSDAY:         0,
	ql2.Term_FRIDAY:           0,
	ql2.Term_SATURDAY:         0,
	ql2.Term_SUNDAY:           0,
	ql2.Term_JANUARY:          0,
	ql2.Term_FEBRUARY:         0,
	ql2.Term_MARCH:            0,
	ql2.Term_APRIL:            0,
	ql2.Term_MAY:              0,
	ql2.Term_JUNE:             0,
	ql2.Term_JULY:             0,
	ql2.Term_AUGUST:           0,
	ql2.Term_SEPTEMBER:        0,
	ql2.Term_OCTOBER:          0,
	ql2.Term_NOVEMBER:         0,
	ql2.Term_DECEMBER:         0,
	ql2.Term_LITERAL:          0,
	ql2.Term_GROUP:            0,
	ql2.Term_SUM:              0,
	ql2.Term_AVG:              0,
	ql2.Term_MIN:              0,
	ql2.Term_MAX:              0,
	ql2.Term_SPLIT:            0,
	ql2.Term_UNGROUP:          0,
	ql2.Term_RANDOM:           0,
	ql2.Term_CHANGES:          0,
	ql2.Term_ARGS:             0,
	ql2.Term_BINARY:           0,
	ql2.Term_GEOJSON:          0,
	ql2.Term_TO_GEOJSON:       0,
	ql2.Term_POINT:            0,
	ql2.Term_LINE:             0,
	ql2.Term_POLYGON:          0,
	ql2.Term_DISTANCE:         0,
	ql2.Term_INTERSECTS:       0,
	ql2.Term_INCLUDES:         0,
	ql2.Term_CIRCLE:           0,
	ql2.Term_GET_INTERSECTING: 0,
	ql2.Term_FILL:             0,
	ql2.Term_GET_NEAREST:      0,
	ql2.Term_POLYGON_SUB:      0,
	ql2.Term_TO_JSON_STRING:   0,
	ql2.Term_MINVAL:           0,
	ql2.Term_MAXVAL:           0,
	ql2.Term_BIT_AND:          0,
	ql2.Term_BIT_OR:           0,
	ql2.Term_BIT_XOR:          0,
	ql2.Term_BIT_NOT:          0,
	ql2.Term_BIT_SAL:          0,
	ql2.Term_BIT_SAR:          0,
}

func (t *Term) returnType() types.TypeFlag {
	if t.IsDatum() {
		return valueType(t.Datum)
	}

	return returnTypeMap[t.Type]
}
