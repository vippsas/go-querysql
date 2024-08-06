package querysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

func PrometheusMSSQLMonitor(funcMap map[string]interface{}) RowsMonitor {
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

		/*
			for i := 0; i < len(cols); i++ {
				fmt.Printf("%t\n", fields[i])
			}
			fmt.Printf("****\n")
		*/

		// The first argument to the select is expected to be a string
		// with the name of the function to be called
		fname, ok := fields[0].(string)
		if !ok {
			return fmt.Errorf("first argument to 'select' is expected to be a string. Got '%s' of type '%s' instead", fname, reflect.TypeOf(fname).String())
		}
		f, ok := funcMap[fname]
		if !ok {
			return fmt.Errorf("could not find '%s'.  The first argument to 'select' is expected to be the name of a function passed into PrometheusMSSQLMonitor", fname)
		}

		/*
			fmt.Printf("%t\n", f)
			fmt.Printf("****\n")
		*/

		funcType := reflect.TypeOf(f)
		if funcType.Kind() != reflect.Func {
			return fmt.Errorf("expected '%s' to be a function", fname)
		}

		funcValue := reflect.ValueOf(f)
		if len(cols)-1 != funcValue.Type().NumIn() {
			return fmt.Errorf("incorrect number of parameters for function '%s'", fname)
		}

		// Args
		in := make([]reflect.Value, len(cols)-1)
		for i, value := range fields {
			if i == 0 {
				continue // function name
			}

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
				}
			}

			var reflectValue reflect.Value

			paramType := funcType.In(i - 1)
			if paramType == reflect.TypeOf(value) {
				reflectValue = reflect.ValueOf(value)
			} else {
				// Try to convert the sql value to the expected type
				reflectValue = reflect.ValueOf(value)
				if !reflectValue.CanConvert(paramType) {
					return fmt.Errorf("expected parameter '%s' to be of type '%s' but got '%s' instead\n",
						colTypes[i].Name(),
						paramType,
						reflect.TypeOf(value))
				}
				reflectValue = reflectValue.Convert(paramType)
			}
			in[i-1] = reflectValue
		}

		funcValue.Call(in)

		if err = rows.Err(); err != nil {
			return err
		}

		return nil
	}
}
