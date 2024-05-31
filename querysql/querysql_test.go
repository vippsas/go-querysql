package querysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MyArray [5]byte

// Scan implements the sql.Scanner interface.
func (u *MyArray) Scan(src interface{}) error {
	copy(u[:], src.([]byte))
	return nil
}

func isClosed(r *sql.Rows) bool {
	err := r.Scan()
	return err != nil && err.Error() == "sql: Rows are closed"
}

func TestInspectType(t *testing.T) {
	type mystruct struct {
		x int
	}
	for i, tc := range []struct {
		expected, got typeinfo
	}{
		{
			expected: typeinfo{true, false},
			got:      inspectType[int](),
		},
		{
			expected: typeinfo{true, false},
			got:      inspectType[[]byte](),
		},
		{
			expected: typeinfo{true, true},
			got:      inspectType[mystruct](),
		},
		{
			expected: typeinfo{valid: false},
			got:      inspectType[[]mystruct](),
		},
		{
			expected: typeinfo{true, false},
			got:      inspectType[MyArray](),
		},
		{
			expected: typeinfo{valid: false},
			got:      inspectType[[]MyArray](),
		},
		{
			expected: typeinfo{valid: false},
			got:      inspectType[*int](),
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.got)
		})
	}
}

func TestMultipleRowsetsResultsHappyDay(t *testing.T) {
	qry := `
-- single scalar
select 2;

-- single struct
select X = 1, Y = 'one';

-- log something
select _=1, x = 'hello world', y = 1;

-- multiple scalar
select 'hello' union all select @p1;

-- log something
select _=1, x = 'hello world2', y = 2;
-- log something again without a result in between
select _=1, x = 'hello world3', y = 3
union all select _=2, x='hello world3', y= 4

-- logging of 0 rows
select _=1, x=1 from (select 1 as y where 1 = 0) tmp

-- empty struct slice
select X = 1, Y = 'one' where 1 = 0;

-- multiple struct
select X = 1, Y = 'one'
union all select X = 2, Y = 'two';

-- multiple sql.Scanner
select 0x0102030405 union all select 0x0102030406

-- more types of single scalar
select concat('hello ', @p1);
select 0x0102030405
select newid()

-- logging in the end
select _=1, log='at end'

`

	type row struct {
		X int
		Y string
	}

	var hook LogHook
	logger := logrus.StandardLogger()
	logger.Hooks.Add(&hook)
	ctx := WithLogger(context.Background(), LogrusMSSQLLogger(logger, logrus.InfoLevel))
	rs := New(ctx, sqldb, qry, "world")
	rows := rs.Rows

	assert.Equal(t, 2, MustNextResult(rs, SingleOf[int]))
	assert.Equal(t, row{1, "one"}, MustNextResult(rs, SingleOf[row]))
	assert.Equal(t, []string{"hello", "world"}, MustNextResult(rs, SliceOf[string]))
	assert.Equal(t, []row(nil), MustNextResult(rs, SliceOf[row]))
	assert.Equal(t, []row{{1, "one"}, {2, "two"}}, MustNextResult(rs, SliceOf[row]))
	assert.Equal(t, []MyArray{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 6}}, MustNextResult(rs, SliceOf[MyArray]))
	assert.Equal(t, "hello world", MustNextResult(rs, SingleOf[string]))
	assert.Equal(t, MyArray{1, 2, 3, 4, 5}, MustNextResult(rs, SingleOf[MyArray]))
	assert.Equal(t, 16, len(MustNextResult(rs, SingleOf[[]uint8])))

	// Check that we have exhausted the logging select before we do the call that gets ErrNoMoreSets
	assert.Equal(t, []logrus.Fields{
		{"x": "hello world", "y": int64(1)},
		{"x": "hello world2", "y": int64(2)},
		{"x": "hello world3", "y": int64(3)},
		{"x": "hello world3", "y": int64(4)},
		{"_norows": true, "x": ""},
		{"log": "at end"},
	}, hook.lines)

	_, err := NextResult(rs, SingleOf[int])
	assert.Equal(t, ErrNoMoreSets, err)
	assert.True(t, isClosed(rows))
	assert.True(t, rs.Done())

	rs.Close()
	assert.True(t, isClosed(rows))

}

func TestMultipleRowsetsPointers(t *testing.T) {
	qry := `
-- single scalar
select 2;

-- single struct
select X = 1, Y = 'one';

-- multiple scalar
select 'hello' union all select @p1;

-- empty struct slice
select X = 1, Y = 'one' where 1 = 0;

-- multiple struct
select X = 1, Y = 'one'
union all select X = 2, Y = 'two';

-- piggy-back a test for logging selects when no logger is configured on the ctx
select _=1, this='will never be seen'
union all select _=1, this='also silenced';

-- multiple sql.Scanner
select 0x0102030405 union all select 0x0102030406

-- more types of single scalar
select concat('hello ', @p1);
select 0x0102030405
select newid()

`

	type row struct {
		X int
		Y string
	}

	rs := New(context.Background(), sqldb, qry, "world")
	rows := rs.Rows

	var intValue int
	MustNext(rs, SingleInto(&intValue))
	assert.Equal(t, 2, intValue)

	var rowValue row
	MustNext(rs, SingleInto(&rowValue))
	assert.Equal(t, row{1, "one"}, rowValue)

	var stringSlice []string
	MustNext(rs, SliceInto(&stringSlice))
	assert.Equal(t, []string{"hello", "world"}, stringSlice)

	var structSlice []row
	MustNext(rs, SliceInto(&structSlice))
	assert.Equal(t, []row(nil), structSlice)

	structSlice = nil
	MustNext(rs, SliceInto(&structSlice))
	assert.Equal(t, []row{{1, "one"}, {2, "two"}}, structSlice)

	var myArraySlice []MyArray
	MustNext(rs, SliceInto(&myArraySlice))
	assert.Equal(t, []MyArray{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 6}}, myArraySlice)

	var stringValue string
	MustNext(rs, SingleInto(&stringValue))
	assert.Equal(t, "hello world", stringValue)

	var myArray MyArray
	MustNext(rs, SingleInto(&myArray))
	assert.Equal(t, MyArray{1, 2, 3, 4, 5}, myArray)

	var byteslice []uint8
	MustNext(rs, SingleInto(&byteslice))
	assert.Equal(t, 16, len(byteslice))

	var dummy int
	err := Next(rs, SingleInto(&dummy))
	assert.Equal(t, ErrNoMoreSets, err)

	assert.True(t, rs.Done())
	assert.True(t, isClosed(rows))
}

func TestEmptyScalar(t *testing.T) {
	qry := `select 1 where 1 = 2`
	rs := New(context.Background(), sqldb, qry)
	rows := rs.Rows
	_, err := NextResult(rs, SingleOf[int])
	assert.Equal(t, ErrZeroRowsExpectedOne, err)
	assert.True(t, isClosed(rows))
}

func TestEmptyStruct(t *testing.T) {
	type row struct {
		X int
		Y string
	}

	qry := `select 1 as X, 'one' as Y where 1 = 2`
	rs := New(context.Background(), sqldb, qry)
	rows := rs.Rows
	_, err := NextResult(rs, SingleOf[row])
	assert.Equal(t, ErrZeroRowsExpectedOne, err)
	assert.True(t, isClosed(rows))
	assert.True(t, rs.Done())
}

func TestManyScalar(t *testing.T) {
	qry := `select 1 union all select 2`
	rs := New(context.Background(), sqldb, qry)
	rows := rs.Rows

	_, err := NextResult(rs, SingleOf[int])
	assert.Equal(t, ErrManyRowsExpectedOne, err)
	assert.True(t, isClosed(rows))
	assert.True(t, rs.Done())
}

func TestAutoClose(t *testing.T) {
	// automatically close rows when all results are read
	qry := `select 1`
	rs := New(context.Background(), sqldb, qry)
	rows := rs.Rows

	assert.Equal(t, 1, MustNextResult(rs, SingleOf[int]))
	assert.True(t, isClosed(rows))
	assert.True(t, rs.Rows == nil)
}

func TestEnsureDoneAfterNext(t *testing.T) {
	qry := `select 1; select 2;`
	_, err := Single[int](context.Background(), sqldb, qry)
	require.Error(t, err)
	assert.Equal(t, ErrNotDone, err)
}

func TestNoResultSets(t *testing.T) {
	// when there are 0 result sets in the query, make sure it's ErrNoMoreSets, for consistency
	qry := `declare @x int = 1`
	_, err := Slice[int](context.Background(), sqldb, qry)
	require.NotNil(t, err)
	require.Equal(t, ErrNoMoreSets, err)
}

func TestOnlyLoggingResultSets(t *testing.T) {
	// when there are only logging result sets, make sure error is ErrNoMoreSets
	qry := `select _=1, x=1;`
	_, err := Slice[int](context.Background(), sqldb, qry)
	require.NotNil(t, err)
	require.Equal(t, ErrNoMoreSets, err)
}

func TestSingleCloseModeErrorPropagates(t *testing.T) {
	origCloseHook := _closeHook
	_closeHook = func(r io.Closer) error {
		_ = r.Close()
		return fmt.Errorf("from hook")
	}
	defer func() {
		_closeHook = origCloseHook
	}()

	qry := `select 1`

	// Implementation in Single
	_, err := Single[int](context.Background(), sqldb, qry)
	require.Error(t, err)
	assert.Equal(t, "from hook", err.Error())

	// Implementation in Slice
	_, err = Slice[int](context.Background(), sqldb, qry)
	require.Error(t, err)
	assert.Equal(t, "from hook", err.Error())

}

func TestQuery4(t *testing.T) {
	a, b, c, d, err := Query4(
		SingleOf[int], SliceOf[int], SliceOf[string], SliceOf[int],
		context.Background(), sqldb, `
		select 1;
		select 3 union all select 4;
		select 'hello';
		select 1 where 1 = 0;
	`)
	require.NoError(t, err)
	assert.Equal(t, 1, a)
	assert.Equal(t, []int{3, 4}, b)
	assert.Equal(t, []string{"hello"}, c)
	assert.Equal(t, []int(nil), d)
}

func TestQueryPointers(t *testing.T) {
	var a int
	var b []int
	var c []string
	var d []int

	err := Query(
		[]Target{SingleInto(&a), SliceInto(&b), SliceInto(&c), SliceInto(&d)},
		context.Background(), sqldb, `
		select 1;
		select 3 union all select 4;
		select 'hello';
		select 1 where 1 = 0;
	`)
	require.NoError(t, err)
	assert.Equal(t, 1, a)
	assert.Equal(t, []int{3, 4}, b)
	assert.Equal(t, []string{"hello"}, c)
	assert.Equal(t, []int(nil), d)
}

func TestPropagateSyntaxError1(t *testing.T) {
	_, _, _, _, err := Query4(
		SingleOf[int], SliceOf[int], SliceOf[string], SliceOf[int],
		context.Background(), sqldb, `
		syntax < error
	`)
	assert.Error(t, err)
}

func TestPropagateSyntaxError2(t *testing.T) {
	rs := New(context.Background(), sqldb, `
		syntax < error
	`)
	_, err := NextResult(rs, SingleOf[int])
	assert.Error(t, err)
	_, err = NextResult(rs, SliceOf[int])
	assert.Error(t, err)
}

func TestStructScanError(t *testing.T) {
	type mismatchingStruct struct {
		X int
		Y int
	}

	_, err := Slice[mismatchingStruct](context.Background(), sqldb, `
		select 1 as X, 2 as Y, 3 as Z
	`)
	assert.Error(t, err)
}
