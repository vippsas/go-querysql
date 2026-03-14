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

	// Get the names of fields marked refl:"optional"; these are allowed to
	// be absent from the query result and will keep their zero value.
	optionalSet := make(map[string]bool)
	for _, name := range deepOptionalFieldNamesOfStructType(reflect.TypeOf(pointerToStruct)) {
		optionalSet[canonicalName(name)] = true
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

	// Demand that all non-optional fields in the struct get filled.
	// Optional fields that have no matching column simply keep their zero value.
	requiredCount := 0
	requiredNames := []string{}
	for _, name := range names {
		if !optionalSet[name] {
			requiredCount++
			requiredNames = append(requiredNames, name)
		}
	}
	if n < requiredCount {
		diff := stringSliceDiff(requiredNames, columns)
		return nil, fmt.Errorf("failed to map all struct fields to query columns (names: %v, columns: %v, diff: %v)", requiredNames, columns, diff)
	}

	// Demand that all query columns gets scanned
	if len(columns) > len(ptrs) {
		diff := stringSliceDiff(requiredNames, columns)
		return nil, fmt.Errorf("failed to map all query columns to struct fields (names: %v, columns: %v, diff: %v)", requiredNames, columns, diff)
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

// hasReflTag reports whether the struct tag's "refl" value contains option.
// The value may be a comma-separated list, e.g. `refl:"recurse,optional"`.
func hasReflTag(tag reflect.StructTag, option string) bool {
	for _, part := range strings.Split(tag.Get("refl"), ",") {
		if part == option {
			return true
		}
	}
	return false
}

func deepFieldsOfStructValue(val reflect.Value) []reflect.Value {
	v := MustStructValue(val)
	n := v.NumField()
	fields := make([]reflect.Value, 0, n)
	for i := 0; i < n; i++ {
		f := v.Field(i)
		tf := v.Type().Field(i)
		k := tf.Type.Kind()
		if k == reflect.Struct && (tf.Anonymous || hasReflTag(tf.Tag, "recurse")) {
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
		if k == reflect.Struct && (f.Anonymous || hasReflTag(f.Tag, "recurse")) {
			names = append(names, deepFieldNamesOfStructType(f.Type)...)
		} else {
			names = append(names, t.Field(i).Name)
		}
	}
	return names
}

// deepOptionalFieldNamesOfStructType returns the names of fields marked with
// the refl:"optional" tag.
//
// When a struct field carries refl:"recurse,optional", all child fields recursed
// from it are also treated as optional.
func deepOptionalFieldNamesOfStructType(typ reflect.Type) []string {
	return deepOptionalFieldNamesHelper(typ, false)
}

func deepOptionalFieldNamesHelper(typ reflect.Type, parentOptional bool) []string {
	t := MustStructType(typ)
	n := t.NumField()
	names := make([]string, 0)
	for i := 0; i < n; i++ {
		f := t.Field(i)
		k := f.Type.Kind()
		isOptional := parentOptional || hasReflTag(f.Tag, "optional")
		if k == reflect.Struct && (f.Anonymous || hasReflTag(f.Tag, "recurse")) {
			names = append(names, deepOptionalFieldNamesHelper(f.Type, isOptional)...)
		} else if isOptional {
			names = append(names, f.Name)
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
