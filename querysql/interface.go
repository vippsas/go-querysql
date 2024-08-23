package querysql

import (
	"context"
	"database/sql"
)

type RowsAndResult struct {
	*sql.Rows
}

var _ sql.Result = RowsAndResult{}

func (r RowsAndResult) LastInsertId() (int64, error) {
	panic("Not implemented")
}

func (r RowsAndResult) RowsAffected() (int64, error) {
	return 0, nil // TODO(dsf)
}

type CtxQuerier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type CtxExecuter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Exec(query string, args ...any) (sql.Result, error)
}

type CtxQueryAndExec interface {
	QueryAndExec(ctx context.Context, query string, args ...interface{}) (RowsAndResult, error)
}
