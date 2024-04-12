// Package sqlquery is a newer interface to the capabilities in query ("query v2" in a sense).
// Intention is to also replace misc testutils.Scan functions ec.
// May be separated into another repo longer term, but is convenient to test it together with testdb package for now.
package querysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
)

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

// Next reads the next result set from `rs`, into the type/scanner provided in the `typ`
// argument. Typical arguments for `typ` is `SliceOf[int]`, `SingleOf[MyStruct]`,
// `Call[MyStruct](func(MyStruct) error { ... })`
func Next[T any](rs *ResultSets, typ func() Result[T]) (T, error) {
	var zero T

	if rs.Err != nil {
		return zero, rs.Err
	}

	success := false
	defer func() {
		if !success {
			_ = rs.Rows.Close()
		}
	}()

	if !rs.BeginResultSet() {
		return zero, ErrNoMoreSets
	}

	scanner := typ() // just syntax candy to avoid ()

	for rs.Rows.Next() {
		if err := scanner.ScanRow(rs.Rows); err != nil {
			return zero, err
		}
	}
	// important and easily forgotten final check on rows.Err()
	if err := rs.Rows.Err(); err != nil {
		return zero, err
	}

	result, err := scanner.Result()
	if err != nil {
		return zero, err
	}

	success = true // disable defer-Close

	err = rs.onReturn()
	if err != nil {
		return zero, err
	}

	return result, nil
}

func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

//
// Convenience shorthands; single-select
//

func Single[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) (T, error) {
	return Next[T](AutoClose(New(ctx, querier, qry, args...)), SingleOf[T])
}

func MustSingle[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) T {
	return Must(Single[T](ctx, querier, qry, args...))
}

func Slice[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) ([]T, error) {
	return Next(AutoClose(New(ctx, querier, qry, args...)), SliceOf[T])
}

func MustSlice[T any](ctx context.Context, querier CtxQuerier, qry string, args ...any) []T {
	return Must(Slice[T](ctx, querier, qry, args...))
}

func Iter[T any](ctx context.Context, querier CtxQuerier, visit func(T) error, qry string, args ...any) error {
	_, err := Next(AutoClose(New(ctx, querier, qry, args...)), Call(visit))
	return err
}

func MustIter[T any](ctx context.Context, querier CtxQuerier, visit func(T) error, qry string, args ...any) {
	if err := Iter[T](ctx, querier, visit, qry, args...); err != nil {
		panic(err)
	}
}

// Multi-select shorthands
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

	t1, err := Next(rs, type1)
	if err != nil {
		return zero1, zero2, err
	}

	t2, err := Next(rs, type2)
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

	t1, err := Next(rs, type1)
	if err != nil {
		return zero1, zero2, zero3, err
	}

	t2, err := Next(rs, type2)
	if err != nil {
		return zero1, zero2, zero3, err
	}

	t3, err := Next(rs, type3)
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

	t1, err := Next(rs, type1)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	t2, err := Next(rs, type2)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	t3, err := Next(rs, type3)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	t4, err := Next(rs, type4)
	if err != nil {
		return zero1, zero2, zero3, zero4, err
	}

	success = true
	if err = rs.Close(); err != nil {
		return zero1, zero2, zero3, zero4, err
	}
	return t1, t2, t3, t4, nil
}

/*
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
		if _, err := Next(rs, target); err != nil {
			return err
		}
	}
	success = true
	if err := rs.Close(); err != nil {
		return err
	}
	return nil

}
*/
