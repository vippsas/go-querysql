package querysql

import (
	"context"
	"database/sql"
)

type BeginTxer interface {
	BeginTx(ctx context.Context, txOptions *sql.TxOptions) (*sql.Tx, error)
}

type Committer interface {
	Commit() error
	Rollback() error
}

type CtxQuerier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type CtxExecuter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Exec(query string, args ...any) (sql.Result, error)
}
