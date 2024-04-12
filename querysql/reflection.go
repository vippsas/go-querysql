package querysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

func getPointersToFields(rows *sql.Rows, pointerToStruct interface{}) ([]interface{}, error) {
	// Gets the names of columns in the query
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for i, name := range columns {
		columns[i] = canonicalName(name)
	}

	// Get the names of struct fields, recursing into embedded structs
	names := DeepFieldNames(pointerToStruct)
	for i, name := range names {
		names[i] = canonicalName(name)
	}

	// Build a mapping from name to index, this index is
	// both for names[i] and origPtrs[i]
	name2index := make(map[string]int, len(names))
	for i, name := range names {
		name2index[name] = i
	}

	// Get pointers in ordering determined by struct
	origPtrs := DeepFieldPointers(pointerToStruct)

	// Reorder pointers to match query column order
	ptrs := make([]interface{}, 0, len(columns))
	mappedNames := make([]string, 0, len(columns))
	n := 0
	for _, col := range columns {
		if j, ok := name2index[col]; ok {
			ptrs = append(ptrs, origPtrs[j])
			mappedNames = append(mappedNames, names[j])
			n++
		}
	}

	// Demand that all fields in struct gets filled
	if n != len(names) {
		diff := stringSliceDiff(names, columns)
		return nil, fmt.Errorf("failed to map all struct fields to query columns (names: %v, columns: %v, diff: %v)", names, columns, diff)
	}

	// Demand that all query columns gets scanned
	if len(columns) > len(ptrs) {
		diff := stringSliceDiff(names, columns)
		return nil, fmt.Errorf("failed to map all query columns to struct fields (names: %v, columns: %v, diff: %v)", names, columns, diff)
	}
	return ptrs, nil
}

func stringSliceDiff(a, b []string) map[string]int {
	diff := map[string]int{}
	for _, name := range a {
		diff[name] = diff[name] + 1
	}
	for _, name := range b {
		diff[name] = diff[name] - 1
	}
	for name, count := range diff {
		if count == 0 {
			delete(diff, name)
		}
	}
	return diff
}

func canonicalName(name string) string {
	return strings.ToLower(name)
}

func MustStructValue(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	// Note not else-if, allowing unwrapping of an interface containing a pointer to a struct
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("Expecting a struct or pointer to struct, possibly wrapped in interface.")
	}
	return v
}

func MustStructType(v reflect.Type) reflect.Type {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	// Note not else-if, allowing unwrapping of an interface containing a pointer to a struct
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("Expecting a struct or pointer to struct, possibly wrapped in interface.")
	}
	return v
}

func deepFieldsOfStructValue(val reflect.Value) []reflect.Value {
	v := MustStructValue(val)
	n := v.NumField()
	fields := make([]reflect.Value, 0, n)
	for i := 0; i < n; i++ {
		f := v.Field(i)
		tf := v.Type().Field(i)
		k := tf.Type.Kind()
		if k == reflect.Struct && (tf.Anonymous || tf.Tag.Get("refl") == "recurse") {
			fields = append(fields, deepFieldsOfStructValue(f)...)
		} else {
			fields = append(fields, f)
		}
	}
	return fields
}

// Return names of fields of struct instance v, recursing into embedded structs (but not named struct members)
func DeepFieldNames(v interface{}) []string {
	return deepFieldNamesOfStructType(reflect.TypeOf(v))
}

func deepFieldNamesOfStructType(typ reflect.Type) []string {
	t := MustStructType(typ)
	n := t.NumField()
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		f := t.Field(i)
		k := f.Type.Kind()
		if k == reflect.Struct && (f.Anonymous || f.Tag.Get("refl") == "recurse") {
			names = append(names, deepFieldNamesOfStructType(f.Type)...)
		} else {
			names = append(names, t.Field(i).Name)
		}
	}
	return names
}

// Return pointers to fields of struct instance v, recursing into embedded structs (but not named struct members)
func DeepFieldPointers(obj interface{}) []interface{} {
	fields := deepFieldsOfStructValue(reflect.ValueOf(obj))
	pointers := make([]interface{}, len(fields))
	for i, f := range fields {
		if f.CanInterface() {
			pointers[i] = f.Addr().Interface()
		}
	}
	return pointers
}

type NameAndTag struct {
	Name string
	Tag  string
}
