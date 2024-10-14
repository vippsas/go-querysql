package querysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleCloseModeErrorPropagates(t *testing.T) {
	dsn := os.Getenv("SQL_DSN")
	if dsn == "" {
		dsn = "sqlserver://127.0.0.1:1433?database=master&user id=sa&password=VippsPw1"
		//panic("Must set SQL_DSN to run tests")
	}
	sqldb, err := sql.Open("sqlserver", dsn)
	if err != nil {
		panic(err)
	}

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
	_, err = Single[int](context.Background(), sqldb, qry)
	require.Error(t, err)
	assert.Equal(t, "from hook", err.Error())

	// Implementation in Slice
	_, err = Slice[int](context.Background(), sqldb, qry)
	require.Error(t, err)
	assert.Equal(t, "from hook", err.Error())
}
