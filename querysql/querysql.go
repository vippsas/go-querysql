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

// RowsLogger takes a sql.Rows and logs it. A default implementation is available, but
// sometimes one might wish to improve on the formatting of different data types, hence
// this low level interface is available.
//
// The convention is that the first column will always contain the log level. You are
// encouraged to treat dummy values (such as "1") as a default loglevel such as INFO,
// for brevity during debugging
type RowsLogger func(rows *sql.Rows) error

// ResultSets is a tiny wrapper around sql.Rows to help managing whether to call NextResultSet or not.
// It is fine to instantiate this struct yourself.
//
// Template methods are not allowed in Go, so the "methods" of this struct are provided as functions
// prefixed with ScanRow; NextStructSlice, NextInt, etc
type ResultSets struct {
	Rows *sql.Rows
	// Started should be set to true if Rows has already been exhausted for one rowset, and is ready for a call
	// to NextResultSet.
	Started bool
	// Err is set to defer errors from constructors until the first method call
	Err error
	// Set CloseAfterNext to enable a mode where Rows is closed after the next resultset has been processed
	CloseAfterNext bool

	// Logger is used for outputting select statements with the special log column (see README)
	// By default it is set by New to the value provided by Logger(ctx), but feel free to set or change it.
	Logger RowsLogger

	// By default, the use of an underscore column, "select _=1, ...", will trigger logging
	// This lets you specify a custom key such as "loglevel" for the same purpose in addition.
	// It will be compared with the lowercase name of the column.
	LogKeyLowercase string
}

// hook for tests
var _closeHook = func(r io.Closer) error {
	return r.Close()
}

func New(ctx context.Context, querier CtxQuerier, qry string, args ...any) *ResultSets {
	rows, err := querier.QueryContext(ctx, qry, args...)
	if err != nil {
		err = fmt.Errorf("rows.New: %w", err)
	}
	return &ResultSets{
		Rows:    rows,
		Started: false,
		Err:     err,
		Logger:  Logger(ctx),
	}
}

func AutoClose(rs *ResultSets) *ResultSets {
	rs.CloseAfterNext = true
	return rs
}

func (rs *ResultSets) Close() error {
	if rs.Rows != nil {
		return _closeHook(rs.Rows)
	}
	return nil
}

// BeginResultSet is like sql.Rows.NextResultSet, except it should be called *before*
// every result, instead of after
func (rs *ResultSets) BeginResultSet() bool {
	if rs.Started {
		return rs.Rows.NextResultSet()
	} else {
		rs.Started = true
		return true
	}
}

func (rs *ResultSets) onReturn() error {
	if rs.CloseAfterNext {
		return rs.Close()
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

// Next reads the next result set from `rs`, passing each row to `scanner`;
// taking care of checking errors and advancing result sets. On errors, `rs`
// will be closed. If AutoClose is used, `rs` will also be closed on successful return.
func Next(rs *ResultSets, scanner Target) error {
	if rs.Err != nil {
		return rs.Err
	}

	success := false
	defer func() {
		if !success {
			_ = rs.Rows.Close()
		}
	}()

	for {

		if !rs.BeginResultSet() {
			return ErrNoMoreSets
		}

		cols, err := rs.Rows.Columns()
		if err != nil {
			return err
		}

		if rs.hasLogColumn(cols) {
			rs.processLogSelect()
		} else {
			break
		}
	}

	for rs.Rows.Next() {
		if err := scanner.ScanRow(rs.Rows); err != nil {
			return err
		}
	}
	// important and easily forgotten final check on rows.Err()
	if err := rs.Rows.Err(); err != nil {
		return err
	}

	success = true // disable defer-Close

	return rs.onReturn()
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
	return NextResult[T](AutoClose(New(ctx, querier, qry, args...)), SingleOf[T])
}

func MustSingle[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) T {
	return must(Single[T](ctx, querier, qry, args...))
}

func Slice[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) ([]T, error) {
	return NextResult(AutoClose(New(ctx, querier, qry, args...)), SliceOf[T])
}

func MustSlice[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) []T {
	return must(Slice[T](ctx, querier, qry, args...))
}

func Iter[T any](ctx context.Context, querier CtxQuerier, visit func(T) error, qry string, args ...any) error {
	_, err := NextResult(AutoClose(New(ctx, querier, qry, args...)), Call(visit))
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
