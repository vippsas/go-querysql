// Package querysql is a newer interface to the capabilities in query ("query v2" in a sense).
// Intention is to also replace misc testutils.Scan functions ec.
// May be separated into another repo longer term, but is convenient to test it together with testdb package for now.
package querysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
)

var ErrNotDone = fmt.Errorf("there are more result sets after reading last expected result")
var ErrNoMoreSets = fmt.Errorf("no more result sets")

// RowsLogger takes a sql.Rows and logs it. A default implementation is available, but
// sometimes one might wish to improve on the formatting of different data types, hence
// this low level interface is available.
//
// The convention is that the first column will always contain the log level. You are
// encouraged to treat dummy values (such as "1") as a default loglevel such as INFO,
// for brevity during debugging
type RowsLogger func(rows *sql.Rows) error

type SqlResult struct {
	// TODO(dsf)
}

func (r SqlResult) LastInsertId() (int64, error) {
	panic("Not implemented yet") // TODO(dsf)
	return 0, nil
}

func (r SqlResult) RowsAffected() (int64, error) {
	return 0, nil
}

var _ sql.Result = SqlResult{}

// ResultSets is a tiny wrapper around sql.Rows to help managing whether to call NextResultSet or not.
// It is fine to instantiate this struct yourself.
//
// Template methods are not allowed in Go, so the "methods" of this struct are provided as functions
// prefixed with ScanRow; NextStructSlice, NextInt, etc
type ResultSets struct {
	Rows *sql.Rows
	// Err is set to defer errors from constructors until the first method call
	Err error

	// Set DoneAfterNext to enable a mode where an error is returned if we did not exhaust the resultset
	DoneAfterNext bool

	// Logger is used for outputting select statements with the special log column (see README)
	// By default it is set by New to the value provided by Logger(ctx), but feel free to set or change it.
	Logger RowsLogger

	// By default, the use of an underscore column, "select _=1, ...", will trigger logging
	// This lets you specify a custom key such as "loglevel" for the same purpose in addition.
	// It will be compared with the lowercase name of the column.
	LogKeyLowercase string

	started bool
}

// hook for tests
var _closeHook = func(r io.Closer) error {
	return r.Close()
}

func New(ctx context.Context, querier CtxQuerier, qry string, args ...any) *ResultSets {
	rows, err := querier.QueryContext(ctx, qry, args...)
	return &ResultSets{
		Rows:    rows,
		started: false,
		Err:     err, // important to return the error unadorned here, as some code e.g. casts it directly to mssql.Error
		Logger:  Logger(ctx),
	}
}

// EnsureDoneAfterNext sets the DoneAfterNext flag. The receiver rs is returned for syntactical
// brevity, a copy is not made
func (rs *ResultSets) EnsureDoneAfterNext() *ResultSets {
	rs.DoneAfterNext = true
	return rs
}

func (rs *ResultSets) Close() error {
	rows := rs.Rows
	rs.Rows = nil
	if rows != nil {
		return _closeHook(rows)
	}
	return nil
}

func (rs *ResultSets) hasLogColumn(cols []string) bool {
	return len(cols) > 0 && (cols[0] == "_" || (rs.LogKeyLowercase != "" && strings.ToLower(cols[0]) == rs.LogKeyLowercase))
}

func (rs *ResultSets) processLogSelect() error {
	if rs.Logger == nil {
		// Just exhaust Rows...not an error to attempt logging to /dev/null
		for rs.Rows.Next() {
		}
		return rs.Rows.Err()
	}

	if err := rs.Logger(rs.Rows); err != nil {
		return err
	}
	// a well-written RowsLogger would return rs.Rows.Err(), but just be certain this isn't overlooked...
	return rs.Rows.Err()
}

// NextResult reads the next result set from `rs`, into the type/scanner provided in the `typ`
// argument. Typical arguments for `typ` is `SliceOf[int]`, `SingleOf[MyStruct]`,
// `Call[MyStruct](func(MyStruct) error { ... })`
func NextResult[T any](rs *ResultSets, typ func() Result[T]) (T, error) {
	result := typ()
	if err := Next(rs, result); err != nil {
		var zero T
		return zero, err
	}

	return result.Result()
}

func MustNextResult[T any](rs *ResultSets, typ func() Result[T]) T {
	result, err := NextResult(rs, typ)
	if err != nil {
		panic(err)
	}
	return result
}

func (rs *ResultSets) processAllLogSelects() (hadColumns bool, err error) {
	for !rs.Done() {
		var cols []string
		cols, err = rs.Rows.Columns()
		if err != nil {
			return false, err
		}
		if len(cols) == 0 {
			// This happens in the event that there's no result sets in the query at all
			return false, nil
		}

		if rs.hasLogColumn(cols) {
			if err = rs.processLogSelect(); err != nil {
				return false, err
			}

			if err = rs.nextResultSet(); err != nil {
				return false, err
			}
		} else {
			// non-logging select; return
			return true, nil
		}
	}
	return true, nil
}

func (rs *ResultSets) Done() bool {
	return rs.Rows == nil
}

func (rs *ResultSets) nextResultSet() error {
	if rs.Rows.NextResultSet() {
		return nil
	} else {
		// we have exhausted the results; automatically close Rows; this also ensures Done() returns true
		return rs.Close()
	}
}

// Next reads the next result set from `rs`, passing each row to `scanner`;
// taking care of checking errors and advancing result sets. On errors, `rs`
// will be closed. If EnsureDoneAfterNext is used, `rs` will also be closed on successful return.
func Next(rs *ResultSets, scanner Target) error {
	_, err := NextWithSqlResult(rs, scanner)
	return err
}

func NextWithSqlResult(rs *ResultSets, scanner Target) (sql.Result, error) {
	sqlResult := SqlResult{}

	if rs.Err != nil {
		return nil, rs.Err
	}

	if rs.Done() {
		// No need to `defer closeRS()`, already closed
		return nil, ErrNoMoreSets
	}

	if !rs.started {
		hadColumns, err := rs.processAllLogSelects()
		if err != nil {
			defer func() { _ = rs.Close() }()
			return nil, err
		}
		if !hadColumns {
			// the *sql.Rows interface typically treats a 'select' with 0 rows and the lack of a select
			// very similar; but there is a slight difference in whether Columns() is available or not
			// We make use of this to give a consistent API where you always get ErrNoMoreSets if a `select`
			// statement is missing
			defer func() { _ = rs.Close() }()
			return sqlResult, ErrNoMoreSets
		}
		rs.started = true

		if rs.Done() {
			// No need to `defer closeRS()`, already closed
			return nil, ErrNoMoreSets
		}
	}

	for rs.Rows.Next() {
		if scanner == nil {
			continue
		}
		if err := scanner.ScanRow(rs.Rows); err != nil {
			defer func() { _ = rs.Close() }()
			return nil, err
		}
	}

	if err := rs.Rows.Err(); err != nil {
		defer func() { _ = rs.Close() }()
		// If we return the error here, we'll miss processing the result sets up to this point
		// Instead of returning the error, we set rs.Err so that next call to Next will return the error
		rs.Err = err
		return sqlResult, nil // TODO(dsf)
	}

	if err := rs.nextResultSet(); err != nil {
		defer func() { _ = rs.Close() }()
		return nil, err
	}

	if _, err := rs.processAllLogSelects(); err != nil {
		return nil, err
	}

	if rs.DoneAfterNext {
		if !rs.Done() {
			_ = rs.Close()
			return nil, ErrNotDone
		}
	}

	return sqlResult, nil // TODO(dsf)
}

func MustNext(rs *ResultSets, scanner Target) {
	err := Next(rs, scanner)
	if err != nil {
		panic(err)
	}
}

func must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

//
// Convenience shorthands; single-select
//

func Single[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) (T, error) {
	return NextResult[T](New(ctx, querier, qry, args...).EnsureDoneAfterNext(), SingleOf[T])
}

func MustSingle[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) T {
	return must(Single[T](ctx, querier, qry, args...))
}

func Slice[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) ([]T, error) {
	return NextResult(New(ctx, querier, qry, args...).EnsureDoneAfterNext(), SliceOf[T])
}

func MustSlice[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) []T {
	return must(Slice[T](ctx, querier, qry, args...))
}

func Iter[T any](ctx context.Context, querier CtxQuerier, visit func(T) error, qry string, args ...any) error {
	_, err := NextResult(New(ctx, querier, qry, args...).EnsureDoneAfterNext(), Call(visit))
	return err
}

func MustIter[T any](ctx context.Context, querier CtxQuerier, visit func(T) error, qry string, args ...any) {
	if err := Iter[T](ctx, querier, visit, qry, args...); err != nil {
		panic(err)
	}
}

func Query(
	targets []Target,
	ctx context.Context,
	querier CtxQuerier,
	qry string,
	args ...any,
) error {
	rs := New(ctx, querier, qry, args...)
	var success bool
	defer func() {
		if !success {
			defer rs.Close()
		}
	}()

	for _, target := range targets {
		if err := Next(rs, target); err != nil {
			return err
		}
	}
	success = true
	if err := rs.Close(); err != nil {
		return err
	}
	return nil
}

func Query2[T1 any, T2 any](
	type1 func() Result[T1],
	type2 func() Result[T2],
	ctx context.Context,
	querier CtxQuerier,
	qry string,
	args ...any) (T1, T2, error) {
	rs := New(ctx, querier, qry, args...)
	var success bool
	defer func() {
		if !success {
			defer rs.Close()
		}
	}()

	var zero1 T1
	var zero2 T2

	t1, err := NextResult(rs, type1)
	if err != nil {
		return zero1, zero2, err
	}

	t2, err := NextResult(rs, type2)
	if err != nil {
		return zero1, zero2, err
	}

	success = true
	if err = rs.Close(); err != nil {
		return zero1, zero2, err
	}
	return t1, t2, nil
}

func Query3[T1 any, T2 any, T3 any](
	type1 func() Result[T1],
	type2 func() Result[T2],
	type3 func() Result[T3],
	ctx context.Context,
	querier CtxQuerier,
	qry string,
	args ...any,
) (T1, T2, T3, error) {
	rs := New(ctx, querier, qry, args...)
	var success bool
	defer func() {
		if !success {
			defer rs.Close()
		}
	}()

	var zero1 T1
	var zero2 T2
	var zero3 T3

	t1, err := NextResult(rs, type1)
	if err != nil {
		return zero1, zero2, zero3, err
	}

	t2, err := NextResult(rs, type2)
	if err != nil {
		return zero1, zero2, zero3, err
	}

	t3, err := NextResult(rs, type3)
	if err != nil {
		return zero1, zero2, zero3, err
	}

	success = true
	if err = rs.Close(); err != nil {
		return zero1, zero2, zero3, err
	}
	return t1, t2, t3, nil
}

func Query4[T1 any, T2 any, T3 any, T4 any](
	type1 func() Result[T1],
	type2 func() Result[T2],
	type3 func() Result[T3],
	type4 func() Result[T4],
	ctx context.Context,
	querier CtxQuerier,
	qry string,
	args ...any,
) (T1, T2, T3, T4, error) {
	rs := New(ctx, querier, qry, args...)
	var success bool
	defer func() {
		if !success {
			defer rs.Close()
		}
	}()

	var zero1 T1
	var zero2 T2
	var zero3 T3
	var zero4 T4

	t1, err := NextResult(rs, type1)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	t2, err := NextResult(rs, type2)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	t3, err := NextResult(rs, type3)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	t4, err := NextResult(rs, type4)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	success = true
	if err = rs.Close(); err != nil {
		return zero1, zero2, zero3, zero4, err
	}
	return t1, t2, t3, t4, nil
}

func ExecContext(
	ctx context.Context,
	querier CtxQuerier,
	qry string,
	args ...any,
) (sql.Result, error) {
	rs := New(ctx, querier, qry, args...)
	var err error
	var res sql.Result

	for {
		res, err = NextWithSqlResult(rs, nil)
		if err != nil {
			if err == ErrNoMoreSets {
				return res, nil
			}
			return nil, err
		}
	}
	return res, nil
}

func Exec(querier CtxQuerier, qry string, args ...any) (sql.Result, error) {
	return ExecContext(context.Background(), querier, qry, args...)
}
