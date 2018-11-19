package types

import (
	"testing"
)

func TestTypeFlagIsSubTypeOf(t *testing.T) {
	type testCasePair struct {
		first, second TypeFlag
	}

	// We use a map rather than write out a long table of expectations for the
	// cross product of all types because the table is very sparse with very
	// few pairs having a positive expectation of type equality.
	expectations := map[testCasePair]bool{
		{Datum, Datum}:                     true,
		{Sequence, Sequence}:               true,
		{Database, Database}:               true,
		{Function, Function}:               true,
		{Ordering, Ordering}:               true,
		{Null, Datum}:                      true,
		{Null, Null}:                       true,
		{MinVal, Datum}:                    true,
		{MinVal, MinVal}:                   true,
		{MaxVal, Datum}:                    true,
		{MaxVal, MaxVal}:                   true,
		{Bool, Datum}:                      true,
		{Bool, Bool}:                       true,
		{Number, Datum}:                    true,
		{Number, Number}:                   true,
		{String, Datum}:                    true,
		{String, String}:                   true,
		{Object, Datum}:                    true,
		{Object, Object}:                   true,
		{Time, Datum}:                      true,
		{Time, Time}:                       true,
		{Binary, Datum}:                    true,
		{Binary, Binary}:                   true,
		{Geometry, Datum}:                  true,
		{Geometry, Geometry}:               true,
		{Selection, Datum}:                 true,
		{Selection, Object}:                true,
		{Selection, Selection}:             true,
		{Array, Datum}:                     true,
		{Array, Sequence}:                  true,
		{Array, Array}:                     true,
		{SelectionArray, Datum}:            true,
		{SelectionArray, Sequence}:         true,
		{SelectionArray, Array}:            true,
		{SelectionArray, SelectionArray}:   true,
		{Stream, Sequence}:                 true,
		{Stream, Stream}:                   true,
		{SelectionStream, Sequence}:        true,
		{SelectionStream, Stream}:          true,
		{SelectionStream, SelectionStream}: true,
		{TableSlice, Sequence}:             true,
		{TableSlice, Stream}:               true,
		{TableSlice, SelectionStream}:      true,
		{TableSlice, TableSlice}:           true,
		{Table, Sequence}:                  true,
		{Table, Stream}:                    true,
		{Table, SelectionStream}:           true,
		{Table, Table}:                     true,
	}

	for first := range allFlags {
		for second := range allFlags {
			expectation := expectations[testCasePair{first, second}]
			if first.IsSubTypeOf(second) != expectation {
				t.Fail()
				t.Errorf("Expected typeFlag(%q).IsSubTypeOf(typeFlag(%q)) to be %t", first, second, expectation)
			}
		}
	}
}
