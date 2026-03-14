package querysql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/go-querysql/querysql"
)

type mappingErrorExpectation struct {
	errorPrefix string
	names       []string
	columns     []string
	diff        map[string]int
}

// assertMappingError checks that err matches the expected getPointersToFields error format:
//
//	"<errorPrefix> (names: <names>, columns: <columns>, diff: <diff>)"
func assertMappingError(t *testing.T, err error, expected mappingErrorExpectation) {
	t.Helper()
	require.Error(t, err)

	expectedMsg := fmt.Sprintf("%s (names: %v, columns: %v, diff: %v)",
		expected.errorPrefix, expected.names, expected.columns, expected.diff)
	assert.Equal(t, expectedMsg, err.Error())
}

func TestGetPointersToFields_StructFieldMissingFromQuery(t *testing.T) {
	type row struct {
		X int
		Y string
	}
	rs := querysql.New(context.Background(), sqldb, `select X = 1`)
	defer rs.Close()

	_, err := querysql.NextResult(rs, querysql.SingleOf[row])
	assertMappingError(t, err, mappingErrorExpectation{
		errorPrefix: "failed to map all struct fields to query columns",
		names:       []string{"x", "y"},
		columns:     []string{"x"},
		diff:        map[string]int{"y": 1},
	})
}

func TestGetPointersToFields_QueryColumnMissingFromStruct(t *testing.T) {
	type row struct {
		X int
		Y string
	}
	rs := querysql.New(context.Background(), sqldb, `select X = 1, Y = 'one', Z = 2`)
	defer rs.Close()

	_, err := querysql.NextResult(rs, querysql.SingleOf[row])
	assertMappingError(t, err, mappingErrorExpectation{
		errorPrefix: "failed to map all query columns to struct fields",
		names:       []string{"x", "y"},
		columns:     []string{"x", "y", "z"},
		diff:        map[string]int{"z": -1},
	})
}

func TestGetPointersToFields_OptionalFieldMissingIsNotAnError(t *testing.T) {
	type row struct {
		X int
		Y string `refl:"optional"`
	}
	rs := querysql.New(context.Background(), sqldb, `select X = 1`)
	defer rs.Close()

	val, err := querysql.NextResult(rs, querysql.SingleOf[row])
	assert.NoError(t, err)
	assert.Equal(t, 1, val.X)
	assert.Equal(t, "", val.Y) // zero value since it's optional and missing
}

func TestGetPointersToFields_RequiredFieldMissingWithOptionalPresent(t *testing.T) {
	type row struct {
		X int
		Y string
		Z int `refl:"optional"`
	}
	rs := querysql.New(context.Background(), sqldb, `select X = 1`)
	defer rs.Close()

	_, err := querysql.NextResult(rs, querysql.SingleOf[row])
	assertMappingError(t, err, mappingErrorExpectation{
		errorPrefix: "failed to map all struct fields to query columns",
		names:       []string{"x", "y"},
		columns:     []string{"x"},
		diff:        map[string]int{"y": 1},
	})
}

func TestGetPointersToFields_MultipleFieldsMissingFromQuery(t *testing.T) {
	type row struct {
		X int
		Y string
		Z int
	}
	rs := querysql.New(context.Background(), sqldb, `select X = 1`)
	defer rs.Close()

	_, err := querysql.NextResult(rs, querysql.SingleOf[row])
	assertMappingError(t, err, mappingErrorExpectation{
		errorPrefix: "failed to map all struct fields to query columns",
		names:       []string{"x", "y", "z"},
		columns:     []string{"x"},
		diff:        map[string]int{"y": 1, "z": 1},
	})
}

func TestGetPointersToFields_MultipleExtraQueryColumns(t *testing.T) {
	type row struct {
		X int
	}
	rs := querysql.New(context.Background(), sqldb, `select X = 1, Y = 'one', Z = 2, W = 3`)
	defer rs.Close()

	_, err := querysql.NextResult(rs, querysql.SingleOf[row])
	assertMappingError(t, err, mappingErrorExpectation{
		errorPrefix: "failed to map all query columns to struct fields",
		names:       []string{"x"},
		columns:     []string{"x", "y", "z", "w"},
		diff:        map[string]int{"y": -1, "z": -1, "w": -1},
	})
}
