package querysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

type funcInfo struct {
	name      string
	numArgs   int
	isClosure bool
	argType   []reflect.Type
	valueOf   reflect.Value
}

func GoMSSQLDispatcher(fs []interface{}) RowsGoDispatcher {
	var knownFuncs string
	var funcMap = map[string]funcInfo{}

	// Check if the `fs` passed in are indeed functions and construct a map of func name to func
	for _, f := range fs {
		var fInfo funcInfo

		funcType := reflect.TypeOf(f)
		if funcType.Kind() != reflect.Func {
			panic("Provided type is not a function")
		}

		fInfo.valueOf = reflect.ValueOf(f)
		getFunctionName := func(fullName string) (string, bool) {
			paths := strings.Split(fullName, "/")
			lastPath := paths[len(paths)-1]
			parts := strings.Split(lastPath, ".")
			fName := parts[len(parts)-1]
			matched, err := regexp.Match(`func\d+`, []byte(fName))
			if err != nil {
				panic(err.Error())
			}
			if matched {
				return parts[len(parts)-2], true // It is a closure
			}
			return fName, false
		}
		fInfo.name, fInfo.isClosure = getFunctionName(runtime.FuncForPC(fInfo.valueOf.Pointer()).Name())

		if knownFuncs == "" {
			knownFuncs = fmt.Sprintf("'%s'", fInfo.name)
		} else {
			knownFuncs = fmt.Sprintf("%s, '%s'", knownFuncs, fInfo.name)
		}

		typeOfFunc := fInfo.valueOf.Type()
		fInfo.numArgs = typeOfFunc.NumIn()
		fInfo.argType = make([]reflect.Type, fInfo.numArgs)

		for i := 0; i < fInfo.numArgs; i++ {
			fInfo.argType[i] = funcType.In(i)
		}
		if _, in := funcMap[fInfo.name]; in {
			panic(fmt.Sprintf("Function already in dispatcher %s (closure==%v)", fInfo.name, fInfo.isClosure))
		}
		funcMap[fInfo.name] = fInfo
	}

	return func(rows *sql.Rows) error {
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}

		fields := make([]interface{}, len(cols))
		scanPointers := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			scanPointers[i] = &fields[i]
		}
		for rows.Next() {
			if err = rows.Scan(scanPointers...); err != nil {
				return err
			}
		}

		// The first argument to the select is expected to be a string
		// with the name of the function to be called
		fname, ok := fields[0].(string)
		if !ok {
			// The first argument is expected to be a string, but we can get nil if we do something like `select _function=... where 1=2`
			// The lack of results is not an error, and it just means there is nothing to do
			if fields[0] == nil {
				return nil
			}
			return fmt.Errorf("first argument to 'select' is expected to be a string. Got '%v' of type '%s' instead", fields[0], reflect.TypeOf(fields[0]).String())
		}
		fInfo, ok := funcMap[fname]
		if !ok {
			return fmt.Errorf("could not find '%s'.  The first argument to 'select' must be the name of a function passed into the dispatcher.  Expected one of %s", fname, knownFuncs)
		}

		if len(cols)-1 != fInfo.numArgs {
			return fmt.Errorf("incorrect number of parameters for function '%s'", fname)
		}

		// Set up the args for calling fo the function
		in := make([]reflect.Value, fInfo.numArgs)
		for i, value := range fields {
			if i == 0 {
				continue // function name
			}

			// Convert MSSQL types to Go types
			switch typedValue := value.(type) {
			case []uint8:
				switch colTypes[i].DatabaseTypeName() {
				case "DECIMAL":
					str := string(typedValue)
					value, err = strconv.ParseFloat(str, 64)
					if err != nil {
						return fmt.Errorf("could not convert argument '%s' of '%s' to float64",
							str,
							colTypes[i].Name())
					}
				case "MONEY":
					str := string(typedValue)
					value, err = strconv.ParseFloat(str, 64)
					if err != nil {
						return fmt.Errorf("could not convert argument '%s' of '%s' to float64",
							str,
							colTypes[i].Name())
					}
				}
			}

			// Check if SQL type and Go func type match
			reflectedValue := reflect.ValueOf(value)
			sqlType := reflect.TypeOf(value)
			fArgType := fInfo.argType[i-1]
			if fArgType != sqlType {
				// Try to convert the sql value to the expected type
				if !reflectedValue.CanConvert(fArgType) {
					return fmt.Errorf("expected parameter '%s' to be of type '%s' but got '%s' instead",
						colTypes[i].Name(),
						fArgType,
						sqlType)
				}
				reflectedValue = reflectedValue.Convert(fArgType)
			}
			in[i-1] = reflectedValue
		}

		fInfo.valueOf.Call(in)

		if err = rows.Err(); err != nil {
			return err
		}

		return nil
	}
}
