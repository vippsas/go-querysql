package querysql

import (
	"database/sql"
)

func PrometheusMSSQLMonitor() RowsMonitor {
	return func(rows *sql.Rows) error {
		return nil
	}
}
