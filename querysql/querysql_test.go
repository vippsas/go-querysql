package querysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/go-querysql/querysql/testhelper"
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
select _log='info', x = 'hello world', y = 1;

-- multiple scalar
select 'hello' union all select @p1;

-- log something
select _log='info', x = 'hello world2', y = 2;
-- log something again without a result in between
select _log='info', x = 'hello world3', y = 3
union all select _log='info', x='hello world3', y= 4

-- logging of 0 rows
select _log='info', x=1 from (select 1 as y where 1 = 0) tmp

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
select _log='info', log='at end'

-- dispatcher
select _function='TestFunction', component = 'abc', val=1, time=1.23;

-- dispatcher with MONEY type
select _function='OtherTestFunction', time=42, money=convert(money, 12345.67);
`

	type row struct {
		X int
		Y string
	}

	var hook LogHook
	logger := logrus.StandardLogger()
	logger.Hooks.Add(&hook)
	ctx := WithLogger(context.Background(), LogrusMSSQLLogger(logger, logrus.InfoLevel))
	ctx = WithDispatcher(ctx, GoMSSQLDispatcher([]interface{}{
		testhelper.TestFunction,
		testhelper.OtherTestFunction,
	}))
	rs := New(ctx, sqldb, qry, "world")
	rows := rs.Rows
	testhelper.ResetTestFunctionsCalled()

	// select 2
	assert.Equal(t, 2, MustNextResult(rs, SingleOf[int]))

	// select X = 1, Y = 'one';
	assert.Equal(t, row{1, "one"}, MustNextResult(rs, SingleOf[row]))

	// select 'hello' union all select @p1;
	assert.Equal(t, []string{"hello", "world"}, MustNextResult(rs, SliceOf[string]))

	// select X = 1, Y = 'one' where 1 = 0;
	assert.Equal(t, []row(nil), MustNextResult(rs, SliceOf[row]))

	// select X = 1, Y = 'one'
	// union all select X = 2, Y = 'two';
	assert.Equal(t, []row{{1, "one"}, {2, "two"}}, MustNextResult(rs, SliceOf[row]))

	// select 0x0102030405 union all select 0x0102030406
	assert.Equal(t, []MyArray{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 6}}, MustNextResult(rs, SliceOf[MyArray]))

	// select concat('hello ', @p1);
	assert.Equal(t, "hello world", MustNextResult(rs, SingleOf[string]))

	// select 0x0102030405
	assert.Equal(t, MyArray{1, 2, 3, 4, 5}, MustNextResult(rs, SingleOf[MyArray]))

	// select newid()
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

	NextResult(rs, SliceOf[string]) // This will process all dispatcher function calls
	assert.True(t, testhelper.TestFunctionsCalled["TestFunction"])
	assert.True(t, testhelper.TestFunctionsCalled["OtherTestFunction"])

	_, err := NextResult(rs, SingleOf[int])
	assert.Equal(t, ErrNoMoreSets, err)
	assert.True(t, isClosed(rows))
	assert.True(t, rs.Done())

	rs.Close()
	assert.True(t, isClosed(rows))

}

func TestInvalidLogLevel(t *testing.T) {
	qry := `
-- log something
select _log=1, x = 'hello world', y = 1;
`

	var hook LogHook
	logger := logrus.StandardLogger()
	logger.Hooks.Add(&hook)
	ctx := WithLogger(context.Background(), LogrusMSSQLLogger(logger, logrus.InfoLevel))
	rs := New(ctx, sqldb, qry, "world")
	err := NextNoScanner(rs)
	assert.Error(t, err)
	assert.Equal(t, "no more result sets", err.Error())

	// Check that we have exhausted the logging select before we do the call that gets ErrNoMoreSets
	assert.Equal(t, []logrus.Fields{
		{"event": "invalid.log.level", "invalid.level": "1"},
		{"x": "hello world", "y": int64(1)},
	}, hook.lines)
}

func Test_LogAndException(t *testing.T) {
	qry := `
-- single scalar
select 2;
-- single struct
select X = 1, Y = 'one';
-- log something
select _log='info', x = 'hello world', y = 1;
-- single struct
select X = 2, Y = 'two';
throw 55002, 'Here is an error', 1;
select 2;
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

	// select 2
	v1, err := NextResult(rs, SingleOf[int])
	assert.NoError(t, err)
	assert.Equal(t, 2, v1)

	// select X = 1, Y = 'one'
	v2, err := NextResult(rs, SingleOf[row])
	assert.NoError(t, err)
	assert.Equal(t, row{1, "one"}, v2)

	// select X = 2, Y = 'two'
	v3, err := NextResult(rs, SingleOf[row])
	assert.NoError(t, err)
	assert.Equal(t, row{2, "two"}, v3)

	// throw 55002, 'Here is an error.', 1;
	_, err = NextResult(rs, SingleOf[row])
	assert.Equal(t, "mssql: Here is an error", err.Error())

	// Check that we have exhausted the logging select before we do the call that gets ErrNoMoreSets
	assert.Equal(t, []logrus.Fields{
		{"x": "hello world", "y": int64(1)},
	}, hook.lines)
}

func TestDispatcherSetupError(t *testing.T) {
	var mustNotBeTrue bool
	var hook LogHook
	logger := logrus.StandardLogger()
	logger.Hooks.Add(&hook)
	defer func() {
		r := recover()
		assert.NotNil(t, r) // nil if a panic didn't happen, not nil if a panic happened
		assert.False(t, mustNotBeTrue)
	}()

	ctx := WithLogger(context.Background(), LogrusMSSQLLogger(logger, logrus.InfoLevel))
	ctx = WithDispatcher(ctx, GoMSSQLDispatcher([]interface{}{
		"SomethingThatIsNotAFunctionPointer", // This should cause a panic
	}))
	// Nothing here gets executed because we expect the WithDispatcher to have panicked
	mustNotBeTrue = true
}

func TestDispatcherRuntimeErrorsAndCornerCases(t *testing.T) {
	testcases := []struct {
		name                   string
		query                  string
		function               string
		expectedError          string
		expectedFunctionCalled bool
	}{
		{
			name: "Function does not exist",
			query: `
						select _function='FunctionDoesNotExist'; -- Blows up here
						select _function='TestFunction', component = 'abc', val=1, time=1.23; -- This does not get processed
			`,
			function:      "FunctionDoesNotExist",
			expectedError: "could not find 'FunctionDoesNotExist'.  The first argument to 'select' must be the name of a function passed into the dispatcher.  Expected one of 'TestFunction', 'OtherTestFunction'",
		},
		{
			name: "_function is not a string",
			query: `
						select _function=4; -- Blows up here
						select _function='TestFunction', component = 'abc', val=1, time=1.23; -- This does not get processed
			`,
			expectedError: "first argument to 'select' is expected to be a string. Got '4' of type 'int64' instead",
		},
		{
			name: "Function exist, but wrong number of args",
			query: `
						select _function='TestFunction', component = 'abc', val=1; -- Blows up here
						select _function='TestFunction', component = 'abc', val=1, time=1.23; -- This does not get processed
			`,
			function:      "TestFunction",
			expectedError: "incorrect number of parameters for function 'TestFunction'",
		},
		{
			name: "Function exist, can't convert args",
			query: `
						select _function='TestFunction', component = 'abc', val=1, time='apple'; -- Blows up here
						select _function='TestFunction', component = 'abc', val=1, time=1.23; -- This does not get processed
			`,
			function:      "TestFunction",
			expectedError: "expected parameter 'time' to be of type 'float64' but got 'string' instead",
		},
		{
			name:     "Function exists, but try to print nil values",
			query:    `select _function='TestFunction', component = 'abc', val=1, time='apple' where 1=2; -- will return all nils`,
			function: "TestFunction",
		},
	}

	var hook LogHook
	logger := logrus.StandardLogger()
	logger.Hooks.Add(&hook)
	ctx := WithLogger(context.Background(), LogrusMSSQLLogger(logger, logrus.InfoLevel))
	ctx = WithDispatcher(ctx, GoMSSQLDispatcher([]interface{}{
		testhelper.TestFunction,
		testhelper.OtherTestFunction,
	}))
	var err error
	for _, tc := range testcases {
		rs := New(ctx, sqldb, tc.query, "world")
		rows := rs.Rows

		testhelper.ResetTestFunctionsCalled()

		_, err = NextResult(rs, SliceOf[string])
		if tc.expectedError != "" {
			assert.Error(t, err)
			assert.Equal(t, tc.expectedError, err.Error())
		}
		assert.Equal(t, tc.expectedFunctionCalled, testhelper.TestFunctionsCalled[tc.function])

		_, err = NextResult(rs, SingleOf[int])
		assert.Equal(t, ErrNoMoreSets, err)
		assert.True(t, isClosed(rows))
		assert.True(t, rs.Done())

		rs.Close()
		assert.True(t, isClosed(rows))
	}
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
select _log='info', this='will never be seen'
union all select _log='info', this='also silenced';

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
	assert.Equal(t, NewZeroRowsExpectedOne(sql.ErrNoRows), err)
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
	assert.Equal(t, NewZeroRowsExpectedOne(sql.ErrNoRows), err)
	assert.True(t, isClosed(rows))
	assert.True(t, rs.Done())
}

func TestEmptyResultWithError(t *testing.T) {
	qry := `
if OBJECT_ID('dbo.MyUsers', 'U') is not null drop table MyUsers
create table MyUsers (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Username NVARCHAR(50) not null,
    Userage int
);
insert into MyUsers (Userage) 
output inserted.ID
values (42);
`
	// We run the query above in two ways:
	// - first with ExecContext
	// - second with SingleOf
	// The run with ExecContext returns an error E
	// The run with SingleOf returns a QuerySqlError wrapped around E

	// ExecContext error
	_, errExec := ExecContext(context.Background(), sqldb, qry, "world")
	assert.Error(t, errExec)
	assert.Equal(t,
		"mssql: Cannot insert the value NULL into column 'Username', table 'master.dbo.MyUsers'; column does not allow nulls. INSERT fails.",
		errExec.Error(),
	)

	// SingleOf error
	rs := New(context.Background(), sqldb, qry)
	_ = rs.Rows
	_, errSingle := NextResult(rs, SingleOf[int])
	assert.Error(t, errSingle)
	// The errSingle has the same underlying error as the errExec
	assert.True(t, errors.Is(errSingle, errExec))
	// But the errSingle is not the same error as the errExec because,
	// in addition to the underlying error, errSingle also contains
	// the information that we called Single and didn't get any value back
	assert.False(t, errors.Is(errExec, errSingle))
}

func TestManyScalar(t *testing.T) {
	qry := `select 1 union all select 2`
	rs := New(context.Background(), sqldb, qry)
	rows := rs.Rows

	_, err := NextResult(rs, SingleOf[int])
	assert.Equal(t, NewManyRowsExpectedOne(), err)
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
	qry := `select _log='info', x=1;`
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

func TestExecContext(t *testing.T) {
	qry := `
if OBJECT_ID('dbo.MyUsers', 'U') is not null drop table MyUsers
create table MyUsers (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Username NVARCHAR(50)
);
insert into MyUsers (Username) values ('JohnDoe');

-- logging
select _log='info', Y = 'one';

-- dispatcher
select _function='TestFunction', component = 'abc', val=1, time=1.23;
`

	var hook LogHook
	logger := logrus.StandardLogger()
	logger.Hooks.Add(&hook)
	ctx := WithLogger(context.Background(), LogrusMSSQLLogger(logger, logrus.InfoLevel))
	ctx = WithDispatcher(ctx, GoMSSQLDispatcher([]interface{}{
		testhelper.TestFunction,
	}))
	testhelper.ResetTestFunctionsCalled()

	res, err := ExecContext(ctx, sqldb, qry, "world")
	assert.NoError(t, err)

	_, err = res.RowsAffected()
	assert.Error(t, err)
	_, err = res.LastInsertId()
	assert.Error(t, err)

	// Check that we have exhausted the logging select before we do the call that gets ErrNoMoreSets
	assert.Equal(t, []logrus.Fields{
		{"Y": "one"},
	}, hook.lines)

	assert.True(t, testhelper.TestFunctionsCalled["TestFunction"])
}
