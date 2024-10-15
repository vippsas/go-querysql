package querysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type myArray [5]byte

// Scan implements the sql.Scanner interface.
func (u *myArray) Scan(src interface{}) error {
	copy(u[:], src.([]byte))
	return nil
}

func TestInspectType(t *testing.T) {
	type mystruct struct {
		x int
	}
	for i, tc := range []struct {
		expected, got typeinfo
	}{
		{
			expected: typeinfo{true, false, false, false},
			got:      inspectType[int](),
		},
		{
			expected: typeinfo{true, false, false, false},
			got:      inspectType[[]byte](),
		},
		{
			expected: typeinfo{true, true, false, false},
			got:      inspectType[mystruct](),
		},
		{
			expected: typeinfo{valid: false},
			got:      inspectType[[]mystruct](),
		},
		{
			expected: typeinfo{true, false, true, false},
			got:      inspectType[myArray](),
		},
		{
			expected: typeinfo{valid: false},
			got:      inspectType[[]myArray](),
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
