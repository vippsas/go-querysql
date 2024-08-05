package querysql

import (
	"database/sql"
	"fmt"
)

func PrometheusMSSQLMonitor() RowsMonitor {
	return func(rows *sql.Rows) error {

		cols, err := rows.Columns()
		if err != nil {
			return err
		}

		// For logging just scan *everything* into a string type straight from SQL driver to make things simple here...
		// The first column is the log level by protocol of RowsLogger.
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

		for i := 0; i < len(cols); i++ {
			// First argument could be a function name, other arguments could be parameters
			fmt.Printf("%t\n", fields[i])
		}

		if err = rows.Err(); err != nil {
			return err
		}

		return nil
	}
}
