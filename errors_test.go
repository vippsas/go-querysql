package go_querysql

import (
	"fmt"
	"testing"

	"github.com/vippsas/go-querysql/querysql"
)

func Test_IsMssqlError(t *testing.T) {
	testcases := []struct {
		err        error
		isSQLError bool
	}{
		{
			fmt.Errorf("Not an SQL error"),
			false,
		},
	}

	for _, tc := range testcases {
		//assert.Equal(t, tc.isSQLError, querysql.IsMssqlError(tc.err, querysql.MssqlErrorUniqueKeyViolated))
		querysql.IsMssqlError(tc.err, querysql.MssqlErrorUniqueKeyViolated)
	}
}
