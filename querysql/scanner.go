package querysql

import (
	"database/sql"
	"fmt"
)

var ErrZeroRowsExpectedOne = fmt.Errorf("query: 0 rows, expected 1: %w", sql.ErrNoRows)
var ErrManyRowsExpectedOne = fmt.Errorf("query: more than 1 row (use sliceScanner?)")

type Target interface {
	ScanRow(*sql.Rows) error
}

type Result[T any] interface {
	Target
	Result() (T, error)
}

type RowScanner[T any] struct {
	typeinfo
	init         bool
	target       *T
	scanPointers []any
}

// scanRow calls rows.Scan to populate scanner.row
func (scanner *RowScanner[T]) scanRow(rows *sql.Rows) error {
	if !scanner.init {
		scanner.init = true
		scanner.typeinfo = inspectType[T]()
		if !scanner.typeinfo.valid {
			return fmt.Errorf("query.ScanRow: illegal type parameter T")
		}

		if scanner.isStruct {
			var err error
			scanner.scanPointers, err = getPointersToFields(rows, scanner.target)
			if err != nil {
				return err
			}
		} else {
			scanner.scanPointers = []any{scanner.target}
		}
	}

	if err := rows.Scan(scanner.scanPointers...); err != nil {
		return err
	}
	return nil
}

//
// single values
//

type singleScanner[T any] struct {
	RowScanner[T]
	hasRead bool
}

func singleInto[T any](target *T) Result[T] {
	result := &singleScanner[T]{}
	result.target = target
	return result
}

// SingleInto set up reading a single row into `target`. If there is not exactly 1 row
// in the resultset an error is returned.
func SingleInto[T any](target *T) Target {
	result := &singleScanner[T]{}
	result.target = target
	return result
}

// SingleOf declares that you want to enforce that the resultset only has a single row,
// and scan that single row into a value of type T that is returned.
func SingleOf[T any]() Result[T] {
	var value T
	return singleInto(&value)
}

func (rv *singleScanner[T]) Result() (T, error) {
	if !rv.hasRead {
		var zero T
		return zero, ErrZeroRowsExpectedOne
	}
	return *rv.target, nil
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

//
// slices
//

type sliceScanner[T any] struct {
	RowScanner[T]
	row          T
	slicePointer *[]T
}

// SliceInto declares that you want to scan the result into a slice of type T
// at the given `target`
func sliceInto[T any](target *[]T) Result[[]T] {
	result := &sliceScanner[T]{}
	result.slicePointer = target
	result.target = &result.row
	return result
}

// SliceInto declares that you want to scan the result into a slice of type T
// at the given `target`
func SliceInto[T any](target *[]T) Target {
	return sliceInto(target)
}

// SliceOf declares that you want to scan the result into a slice of type T.
func SliceOf[T any]() Result[[]T] {
	var result []T
	return sliceInto(&result)
}

func (rv *sliceScanner[T]) Result() ([]T, error) {
	return *rv.slicePointer, nil
}

func (rv *sliceScanner[T]) ScanRow(rows *sql.Rows) error {
	if err := rv.scanRow(rows); err != nil {
		return err
	}
	*rv.slicePointer = append(*rv.slicePointer, rv.row)
	return nil
}

//
// callbacks
//

type iterScanner[T any] struct {
	RowScanner[T]
	row   T
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
		result := &iterScanner[T]{visit: visit}
		result.target = &result.row
		return result
	}
}
