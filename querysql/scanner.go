package querysql

import (
	"database/sql"
	"fmt"
)

var ErrNoMoreSets = fmt.Errorf("no more result sets")
var ErrZeroRowsExpectedOne = fmt.Errorf("query: 0 rows, expected 1: %w", sql.ErrNoRows)
var ErrManyRowsExpectedOne = fmt.Errorf("query: more than 1 row (use sliceScanner?)")

type RowScanner interface {
	ScanRow(*sql.Rows) error
}

type Target interface {
	RowScanner
	Done() error
}

type Result[T any] interface {
	RowScanner
	Result() (T, error)
}

type BaseResult[T any] struct {
	typeinfo
	init         bool
	row          T
	scanPointers []any
}

// scanRow calls rows.Scan to populate scanner.row
func (scanner *BaseResult[T]) scanRow(rows *sql.Rows) error {
	if !scanner.init {
		scanner.init = true
		scanner.typeinfo = inspectType[T]()
		if !scanner.typeinfo.valid {
			return fmt.Errorf("query.ScanRow: illegal type parameter T")
		}

		if scanner.isStruct {
			var err error
			scanner.scanPointers, err = getPointersToFields(rows, &scanner.row)
			if err != nil {
				return err
			}
		} else {
			scanner.scanPointers = []any{&scanner.row}
		}
	}

	if err := rows.Scan(scanner.scanPointers...); err != nil {
		return err
	}
	return nil
}

type singleScanner[T any] struct {
	BaseResult[T]
	hasRead bool
}

// SingleOf declares that you want to enforce that the resultset only has a single row,
// and scan that single row into a value of type T.
func SingleOf[T any]() Result[T] {
	return &singleScanner[T]{}
}

func (rv *singleScanner[T]) Result() (T, error) {
	if !rv.hasRead {
		var zero T
		return zero, ErrZeroRowsExpectedOne
	}
	return rv.row, nil
}

func (rv *singleScanner[T]) ScanRow(rows *sql.Rows) error {
	if rv.hasRead {
		return ErrManyRowsExpectedOne
	}
	if err := rv.scanRow(rows); err != nil {
		return err
	}
	rv.hasRead = true
	return nil
}

type sliceScanner[T any] struct {
	BaseResult[T]
	slice []T
}

// SliceOf declares that you want to scan the result into a slice of type T.
func SliceOf[T any]() Result[[]T] {
	return &sliceScanner[T]{}
}

func (rv *sliceScanner[T]) Result() ([]T, error) {
	return rv.slice, nil
}

func (rv *sliceScanner[T]) ScanRow(rows *sql.Rows) error {
	if err := rv.scanRow(rows); err != nil {
		return err
	}
	rv.slice = append(rv.slice, rv.row)
	return nil
}

type iterScanner[T any] struct {
	BaseResult[T]
	count int
	visit func(T) error
}

func (scanner *iterScanner[T]) Result() (int, error) {
	return scanner.count, nil
}

func (scanner *iterScanner[T]) ScanRow(rows *sql.Rows) error {
	if err := scanner.scanRow(rows); err != nil {
		return err
	}
	scanner.count++
	return scanner.visit(scanner.row)
}

// Call lets you provide a callback visit function that is called for each row.
// The returned data is the number of rows scanned.
func Call[T any](visit func(row T) error) func() Result[int] {
	// return a factory function (this system is mainly used for syntax candy for SliceOf and SingleOf,
	// although having a factory protocol can be useful for other reasons too),
	return func() Result[int] {
		return &iterScanner[T]{visit: visit}
	}
}

type pointerTarget[T any] struct {
	target  *T
	scanner Result[T]
}

func (pt pointerTarget[T]) ScanRow(rows *sql.Rows) error {
	return pt.scanner.ScanRow(rows)
}

func (pt pointerTarget[T]) Done() error {
	val, err := pt.scanner.Result()
	if err != nil {
		return err
	}
	*pt.target = val
	return nil
}

func Into[T any](scanner func() Result[T], target *T) Target {
	return pointerTarget[T]{
		target:  target,
		scanner: scanner(),
	}
}

func SliceInto[T any](target *[]T) Target {
	return Into[[]T](SliceOf[T], target)
}

func SingleInto[T any](target *T) Target {
	return Into[T](SingleOf[T], target)
}
