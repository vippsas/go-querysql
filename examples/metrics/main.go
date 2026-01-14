package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/sirupsen/logrus"
	"github.com/vippsas/go-querysql/querysql"
)

func initdb() (*sql.DB, error) {
	dsn := os.Getenv("SQL_DSN")
	if dsn == "" {
		dsn = "sqlserver://127.0.0.1:1433?database=master&user id=sa&password=VippsPw1"
	}
	return sql.Open("sqlserver", dsn)
}

func populatedb(ctx context.Context, sqldb *sql.DB) error {
	qry := `drop table if exists
	create table MyUsers (
		Id int identity(1,1) primary key,
		UserName nvarchar(50) not null,
		UserAge int
	);
	insert into MyUsers (UserName, UserAge) values ('Bob Doe', 42);
	insert into MyUsers (UserName, UserAge) values ('Johny Doe', 10);
	`
	_, err := querysql.ExecContext(ctx, sqldb, qry, "world")
	return err
}

func processUsers(ctx context.Context, sqldb *sql.DB) error {
	qry := `select * from MyUsers;
select _function='UserMetrics', label='size.MyUsers', numProcessed=@@rowcount;
`
	type rowType struct {
		Id       int
		UserName string
		UserAge  int
	}
	users, err := querysql.Slice[rowType](ctx, sqldb, qry, "world")
	fmt.Println(users)
	return err
}

// This function is going to be called when the query in processUsers is run
func UserMetrics(label string, count int) {
	fmt.Println(label, count)
}

func main() {
	sqldb, err := initdb()
	if err != nil {
		panic(err.Error())
	}

	logger := logrus.StandardLogger()
	ctx := querysql.WithLogger(context.Background(), querysql.LogrusMSSQLLogger(logger, logrus.InfoLevel))
	ctx = querysql.WithDispatcher(ctx, querysql.GoMSSQLDispatcher([]interface{}{
		UserMetrics,
	}))

	err = populatedb(ctx, sqldb)
	if err != nil {
		panic(err.Error())
	}

	err = processUsers(ctx, sqldb)
	if err != nil {
		panic(err.Error())
	}

}
