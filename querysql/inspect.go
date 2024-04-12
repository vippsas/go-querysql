package querysql

import (
	"database/sql"
	"reflect"
)

type typeinfo struct {
	valid    bool
	isStruct bool // use special struct demarshalling; otherwise standard SQL Scan
}

var sqlScannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func inspectType[T any]() typeinfo {
	var zeroValue T
	typ := reflect.TypeOf(zeroValue)
	kind := typ.Kind()

	if kind == reflect.Struct {
		return typeinfo{
			valid:    true,
			isStruct: true,
		}
	} else if isScalar(kind) ||
		// if T = []E or E, we need that *the pointer* *E implements sql.Scanner
		reflect.PointerTo(typ).Implements(sqlScannerType) {
		return typeinfo{
			valid:    true,
			isStruct: false,
		}
	} else if kind == reflect.Slice {
		// []uint8 / []byte? this is a special case that is allowed
		if typ.Elem().Kind() == reflect.Uint8 {
			return typeinfo{
				valid:    true,
				isStruct: false,
			}
		}
	}
	return typeinfo{valid: false}
}

func isScalar(k reflect.Kind) bool {
	switch k {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.String:
		return true
	default:
		return false
	}
}
