# querysql - Syntax candy for querying SQL from Go

The querysql package provides several layers; all of them
provide safety against common mistakes and avoids a lot of iterator
boilerplate.

All of the methods allows conveniently fetching results into
your own structs, using reflection.

* To execute a single select statement, use:
    * `querysql.Slice` returns a slice
    * `querysql.Single` validates that the result is exactly one row,
       and returns that single row 
    * `querysql.Iter` calls a callback function for each row 
* To execute multiple select statements in the same database
  roundtrip, `querysql.Query2`, `querysql.Query3`, ...
  is available
* `querysql.New` offers a lower-level API with more options, used to build
  the primitives above

Below the variables `ctx` and `db` are used for `context.Context` and `*sql.DB`.

## Single select statement

The most common mode is to process a single `select` statement per query:
 
```go
// Read a single integer; errors if not exactly 1 row
n := querysql.MustSingle[int](ctx, db, `select 1`)
n, err := querysql.Single[int](ctx, db, `select 1`)

// Read a slice of integers; can have any number of elements
slice := querysql.Slice[int](ctx, db, `select @p1`, 3)
slice, err := querysql.MustSlice[int](ctx, db, `select @p1`, 3)

// Support for reading into structs, or slices of structs
type row {
	X int
	Y string
}
singleRow, err := querysql.QuerySingle[row](ctx, db, `select 1 as X, "hello" as Y`)
sliceOfRows, err := querysql.QuerySlice[row](ctx, db, `select 1 as X, "hello" as Y`)

// Avoid allocating slices by passing a callback 
err := querysql.QueryIter(ctx, db, func(row int) error {
	fmt.Println("Row %d", row)
}, `select 1 union all select 2`)
```

## Multiple select statements

SQL supports doing several selects on the same round-trip, which
can be very useful to get information from several tables in the same
query. In this case a somewhat special syntax is used to pass the
specifications for how to read each type:

```go
qry := `
    select 1
    union all select 2;

    select 1 as X, "one" as Y 
` 
singleInteger, sliceOfStruct, err := querysql.Query2(
	querysql.SingleOf[int], 
	querysql.SliceOf[MyStruct],
	ctx, db, qry, arg1, arg2)
```
We have defined `Query2`, `Query3` and `Query4` for this use up
to 4 select statements.

If you prefer, you can instead scan into pointers; this also allows
using a single function for any number of results or dynamic number
of results:
```go
var singleInteger int
var sliceOfStruct []MyStruct
singleInteger, sliceOfStruct, err := querysql.Query(
	[]querysql.Target{
		querysql.SingleInto(&singleInteger),
		querysql.SliceInto(&sliceOfStruct),
	},
	ctx, db, qry, arg1, arg2)
```


## Advanced use

For more advanced usecase you may use `querysql.New`.
In the following example we first do a select to get a "command"
back, then scan into *different* types depending on the value of
the first `select`:
```go
rs := querysql.New(ctx, dbi, `
    select 'ints';

    select 1 union all select 2;
`)
defer rs.Close()

mode, err := querysql.NextResult(rs, querysql.SingleOf[string])
if mode == "ints" {
	data, err := querysql.NextResult(rs, querysql.SliceOf[int])
	...
} else if mode == "structs" {
	type SomeStruct struct {
		X int
		Y string
	}
	data, err := querysql.Next(rs, querysql.Call(func (row MyStruct)) {
		fmt.Println(row)
		return nil
    })
}
```


## Future plans

* Allow querying directly into `map` types, using the first columns
  as the key
