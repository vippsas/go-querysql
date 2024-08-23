# querysql - Syntax candy for querying SQL from Go

The querysql package provides several layers; all of them
provide safety against common mistakes and avoids a lot of iterator
boilerplate. Also included is a convenient logging mechanism.

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
slice := querysql.MustSlice[int](ctx, db, `select @p1`, 3)
slice, err := querysql.Slice[int](ctx, db, `select @p1`, 3)

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

This mode still supports [logging](#logging-from-sql) and will return `ErrNotDone`
if there are several non-logging select statements.

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

## Logging from SQL

When writing longer multi-statement SQL queries the lack of
debugging between statements can be a real problem. A work-around
is provided in this library. Any target-less `select` statements
where the name of the first column is `_log` will be re-directed
to a logger (if one is configured; and otherwise the data will be
ignored). Example:

```go
qry := `
    declare @a = 'world';

    select A='one';

    -- logging
    select _log='info', hello=@a;
    
    select B='two';

    -- log one entry per row, at a non-standard level
    select _log='info', hello=@a from SomeTable;

` 

// configure a logger on ctx
ctx := querysql.WithLogger(context.Background(), LogrusMSSQLLogger(logger, logrus.InfoLevel))

// do the query like normal; the middle select will be directed to the logger
firstResult, secondResult, err := querysql.Query2(ctx, ...)
```

The LogrusMSSQLLogger above is, as given by the name, specific
to one combination of tools, and you may need to write your
own implementation of `RowsLogger` based on the one provided in this library.
The `*sql.Rows` is passed straight through to the `RowsLogger`,
but by convention the first column in the result will be the special
column `_log`, which may either contain a log-level (`info`, `debug`, `warning`, `error`).

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
// rs is automatically closed after processing the last select statement,
// but it is still a good idea to defer a Close in case you do not process
// all the select statements
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

You may also process a dynamic number of results; `rs.Done()` will be be true
when there are no more result sets available. At this point, `rs` has also been
automatically closed; although it is still a good idea to `defer rs.Close()` in
case you do not reach the point where `rs.Done() == true`.


## Future plans

* Allow querying directly into `map` types, using the first columns
  as the key
* Automatically deserialize XML or JSON results from SQL to structs
