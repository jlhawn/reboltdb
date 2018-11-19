package json

import (
	"fmt"

	json "github.com/buger/jsonparser"
)

type Value interface {
	ValueType() json.ValueType

	// These methods return whether or not the underlying value is of a
	// given type.
	IsNull() bool
	IsBool() bool
	IsNumber() bool
	IsString() bool
	IsArray() bool
	IsObject() bool

	// These methods always return the zero value for the given type unless it
	// is the actual underlying value.
	AsBool() bool
	AsInt64() int64
	AsFloat64() float64
	AsString() string
	AsArray() []Value
	AsObject() map[string]Value
}

type Null struct{}

func (Null) ValueType() json.ValueType { return json.Null }

func (Null) IsNull() bool   { return true }
func (Null) IsBool() bool   { return false }
func (Null) IsNumber() bool { return false }
func (Null) IsString() bool { return false }
func (Null) IsArray() bool  { return false }
func (Null) IsObject() bool { return false }

func (Null) AsBool() bool               { return false }
func (Null) AsInt64() int64             { return 0 }
func (Null) AsFloat64() float64         { return 0 }
func (Null) AsString() string           { return "" }
func (Null) AsArray() []Value           { return nil }
func (Null) AsObject() map[string]Value { return nil }

type Bool bool

func (Bool) ValueType() json.ValueType { return json.Boolean }

func (Bool) IsNull() bool   { return false }
func (Bool) IsBool() bool   { return true }
func (Bool) IsNumber() bool { return false }
func (Bool) IsString() bool { return false }
func (Bool) IsArray() bool  { return false }
func (Bool) IsObject() bool { return false }

func (b Bool) AsBool() bool             { return bool(b) }
func (Bool) AsInt64() int64             { return 0 }
func (Bool) AsFloat64() float64         { return 0 }
func (Bool) AsString() string           { return "" }
func (Bool) AsArray() []Value           { return nil }
func (Bool) AsObject() map[string]Value { return nil }

type Number float64

func (Number) ValueType() json.ValueType { return json.Number }

func (Number) IsNull() bool   { return false }
func (Number) IsBool() bool   { return false }
func (Number) IsNumber() bool { return true }
func (Number) IsString() bool { return false }
func (Number) IsArray() bool  { return false }
func (Number) IsObject() bool { return false }

func (Number) AsBool() bool               { return false }
func (n Number) AsInt64() int64           { return int64(n) }
func (n Number) AsFloat64() float64       { return float64(n) }
func (Number) AsString() string           { return "" }
func (Number) AsArray() []Value           { return nil }
func (Number) AsObject() map[string]Value { return nil }

type String string

func (String) ValueType() json.ValueType { return json.String }

func (String) IsNull() bool   { return false }
func (String) IsBool() bool   { return false }
func (String) IsNumber() bool { return false }
func (String) IsString() bool { return true }
func (String) IsArray() bool  { return false }
func (String) IsObject() bool { return false }

func (String) AsBool() bool               { return false }
func (String) AsInt64() int64             { return 0 }
func (String) AsFloat64() float64         { return 0 }
func (s String) AsString() string         { return string(s) }
func (String) AsArray() []Value           { return nil }
func (String) AsObject() map[string]Value { return nil }

type Array []Value

func (Array) ValueType() json.ValueType { return json.Array }

func (Array) IsNull() bool   { return false }
func (Array) IsBool() bool   { return false }
func (Array) IsNumber() bool { return false }
func (Array) IsString() bool { return false }
func (Array) IsArray() bool  { return true }
func (Array) IsObject() bool { return false }

func (Array) AsBool() bool               { return false }
func (Array) AsInt64() int64             { return 0 }
func (Array) AsFloat64() float64         { return 0 }
func (Array) AsString() string           { return "" }
func (a Array) AsArray() []Value         { return []Value(a) }
func (Array) AsObject() map[string]Value { return nil }

type Object map[string]Value

func (Object) ValueType() json.ValueType { return json.Object }

func (Object) IsNull() bool   { return false }
func (Object) IsBool() bool   { return false }
func (Object) IsNumber() bool { return false }
func (Object) IsString() bool { return false }
func (Object) IsArray() bool  { return false }
func (Object) IsObject() bool { return true }

func (Object) AsBool() bool                 { return false }
func (Object) AsInt64() int64               { return 0 }
func (Object) AsFloat64() float64           { return 0 }
func (Object) AsString() string             { return "" }
func (Object) AsArray() []Value             { return nil }
func (o Object) AsObject() map[string]Value { return map[string]Value(o) }

type ParseError struct {
	field string
	err   error
}

func parseError(field string, err error) error {
	return &ParseError{
		field: field,
		err:   err,
	}
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("unable to parse JSON at field %s: %s", e.field, e.err)
}

func Parse(data []byte) (Value, error) {
	data, dataType, _, err := json.Get(data)
	if err != nil {
		return nil, fmt.Errorf("unable to parse JSON: %s", err)
	}

	return parse(data, dataType, ".")
}

func parse(data []byte, dataType json.ValueType, field string) (Value, error) {
	switch dataType {
	case json.String:
		strVal, err := json.ParseString(data)
		if err != nil {
			return nil, parseError(field, err)
		}
		return String(strVal), nil
	case json.Boolean:
		boolVal, err := json.ParseBoolean(data)
		if err != nil {
			return nil, parseError(field, err)
		}
		return Bool(boolVal), nil
	case json.Number:
		numberVal, err := json.ParseFloat(data)
		if err != nil {
			return nil, parseError(field, err)
		}
		return Number(numberVal), nil
	case json.Null:
		return Null{}, nil
	case json.Array:
		arrayVal := Array{}
		_, err := json.ArrayEach(data, func(data []byte, dataType json.ValueType, offset int) error {
			val, err := parse(data, dataType, fmt.Sprintf("%s[%d]", field, len(arrayVal)))
			if err != nil {
				return err
			}
			arrayVal = append(arrayVal, val)
			return nil
		})
		if err != nil {
			if _, ok := err.(*ParseError); !ok {
				err = parseError(field, err)
			}
			return nil, err
		}
		return arrayVal, nil
	case json.Object:
		objectVal := Object{}
		err := json.ObjectEach(data, func(key, data []byte, dataType json.ValueType, offset int) error {
			val, err := parse(data, dataType, fmt.Sprintf("%s[\"%s\"]", field, string(key)))
			if err != nil {
				return err
			}
			objectVal[string(key)] = val
			return nil
		})
		if err != nil {
			if _, ok := err.(*ParseError); !ok {
				err = parseError(field, err)
			}
			return nil, err
		}
		return objectVal, nil
	default:
		return nil, fmt.Errorf("unable to handle type %s", dataType)
	}
}
