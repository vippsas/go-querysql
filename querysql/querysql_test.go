package querysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"

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

func TestMultipleRowsetsHappyDay(t *testing.T) {
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

	assert.Equal(t, 2, Must(Next(rs, SingleOf[int])))
	assert.Equal(t, row{1, "one"}, Must(Next(rs, SingleOf[row])))
	assert.Equal(t, []string{"hello", "world"}, Must(Next(rs, SliceOf[string])))
	assert.Equal(t, []row(nil), Must(Next(rs, SliceOf[row])))
	assert.Equal(t, []row{{1, "one"}, {2, "two"}}, Must(Next(rs, SliceOf[row])))
	assert.Equal(t, []MyArray{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 6}}, Must(Next(rs, SliceOf[MyArray])))
	assert.Equal(t, "hello world", Must(Next(rs, SingleOf[string])))
	assert.Equal(t, MyArray{1, 2, 3, 4, 5}, Must(Next(rs, SingleOf[MyArray])))
	assert.Equal(t, 16, len(Must(Next(rs, SingleOf[[]uint8]))))

	_, err := Next(rs, SingleOf[int])
	assert.Equal(t, ErrNoMoreSets, err)

	rs.Close()
	assert.True(t, isClosed(rs.Rows))
}

func TestEmptyScalar(t *testing.T) {
	qry := `select 1 where 1 = 2`
	rs := New(context.Background(), sqldb, qry)
	_, err := Next(rs, SingleOf[int])
	assert.Equal(t, ErrZeroRowsExpectedOne, err)
	assert.True(t, isClosed(rs.Rows))
}

func TestEmptyStruct(t *testing.T) {
	type row struct {
		X int
		Y string
	}

	qry := `select 1 as X, 'one' as Y where 1 = 2`
	rs := New(context.Background(), sqldb, qry)
	_, err := Next(rs, SingleOf[row])
	assert.Equal(t, ErrZeroRowsExpectedOne, err)
	assert.True(t, isClosed(rs.Rows))
}

func TestManyScalar(t *testing.T) {
	qry := `select 1 union all select 2`
	rs := New(context.Background(), sqldb, qry)

	_, err := Next(rs, SingleOf[int])
	assert.Equal(t, ErrManyRowsExpectedOne, err)
	assert.True(t, isClosed(rs.Rows))
}

func TestAutoClose(t *testing.T) {
	qry := `select 1`
	rs := AutoClose(New(context.Background(), sqldb, qry))

	assert.Equal(t, 1, Must(Next(rs, SingleOf[int])))
	assert.True(t, isClosed(rs.Rows))
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
	_, err := Next(rs, SingleOf[int])
	assert.Error(t, err)
	_, err = Next(rs, SliceOf[int])
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
