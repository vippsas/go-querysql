package querysql_test

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
)

var sqldb *sql.DB

func TestMain(m *testing.M) {
	dsn := os.Getenv("SQL_DSN")
	if dsn == "" {
		dsn = "sqlserver://127.0.0.1:1433?database=master&user id=sa&password=VippsPw1"
		//panic("Must set SQL_DSN to run tests")
	}
	var err error
	sqldb, err = sql.Open("sqlserver", dsn)
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}
