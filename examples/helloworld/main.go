package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	"github.com/vippsas/go-querysql/querysql"

	_ "github.com/denisenkom/go-mssqldb"
)

func initdb() (*sql.DB, error) {
	dsn := os.Getenv("SQL_DSN")
	if dsn == "" {
		dsn = "sqlserver://127.0.0.1:1433?database=master&user id=sa&password=VippsPw1"
	}
	return sql.Open("sqlserver", dsn)
}

func main() {
	sqldb, err := initdb()
	if err != nil {
		panic(err.Error())
	}

	logger := log.Default()
	ctx := querysql.WithLogger(context.Background(), querysql.StdMSSQLLogger(logger))

	qry := `select _log='info', message='hello world from a query'`
	_, err = querysql.ExecContext(ctx, sqldb, qry, "world")
	if err != nil {
		panic(err.Error())
	}
}
