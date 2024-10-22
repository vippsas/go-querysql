package testhelper

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"strconv"
	"strings"
)

var TestFunctionsCalled = map[string]bool{
	"TestFunction":      false,
	"OtherTestFunction": false,
}

func TestFunction(component string, val int64, t float64) {
	TestFunctionsCalled[getFunctionName()] = true
}

func OtherTestFunction(time float64, money float64) {
	TestFunctionsCalled[getFunctionName()] = true
}

func ResetTestFunctionsCalled() {
	for k, _ := range TestFunctionsCalled {
		TestFunctionsCalled[k] = false
	}
}

func getFunctionName() string {
	pc, _, _, ok := runtime.Caller(1) // 1 means we get the caller of the function
	if !ok {
		panic("Could not get function name")
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		panic("Could not get function name")
	}

	cleanUpName := func(fullName string) string {
		paths := strings.Split(fullName, "/")
		lastPath := paths[len(paths)-1]
		parts := strings.Split(lastPath, ".")
		return parts[len(parts)-1]
	}
	return cleanUpName(fn.Name())
}

type Money struct {
	Data int64
}

// Parse constructs money from a string. If you have an integer value
// that you know how to interpret, just multiply it up and construct
// Money{Data: ..} directly.
func Parse(value string) (Money, error) {
	if len(value) == 0 {
		return Money{}, errors.New("empty string is not value money")
	}
	sign := int64(1)
	if value[0] == '-' {
		sign = -1
		value = value[1:]
	}

	parts := strings.Split(value, ".")
	if len(parts) != 2 || (len(parts[1]) != 2 && len(parts[1]) != 4) {
		return Money{}, fmt.Errorf("not valid money: %s, must have 2 or 4 decimals", value)
	}

	if len(parts[0]) > 1 && parts[0][0] == '0' {
		return Money{}, fmt.Errorf("not valid money: %s, cannot have leading zeros", value)
	}

	// Note the 63 bit here. We are converting to int64 later on, which means that 63 is all
	// we are going to be able to report.
	whole, err := strconv.ParseUint(parts[0], 10, 63)
	if err != nil {
		return Money{}, fmt.Errorf("not valid money: %s (parts[0]=%s)", value, parts[0])
	}
	fraccent, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return Money{}, fmt.Errorf("not valid money: %s (parts[1]=%s)", value, parts[1])
	}

	// This will never overflow, since the max we parse in fraccent is 16 bit, and we
	// have 64 bits to play with here
	if len(parts[1]) == 2 {
		fraccent *= 100
	}
	if fraccent%100 != 0 {
		return Money{}, errors.New("Money asserts that only whole-cent values are taken (todo: implement FractionalMoney)")
	}

	hi, lo := bits.Mul64(whole, 10000)
	if hi != 0 {
		return Money{}, fmt.Errorf("The amount %s would overflow an uint64 if converted to cents", value)
	}

	cc, carry := bits.Add64(lo, fraccent, 0)
	if carry != 0 {
		return Money{}, fmt.Errorf("The amount %s would overflow an uint64 if converted to cents", value)
	}

	if sign == 1 {
		if cc > math.MaxInt64 {
			return Money{}, fmt.Errorf("The amount %s will not fit into an int64", value)
		}
	} else {
		if cc > (1 << 63) {
			return Money{}, fmt.Errorf("The amount %s will not fit into an int64", value)
		}
	}

	return Money{Data: sign * int64(cc)}, nil
}

func (target *Money) Scan(value interface{}) error {
	// if value is nil, false
	if value != nil {
		var strvalue string
		switch s := value.(type) {
		case []uint8:
			strvalue = string(s)
		case string:
			strvalue = s
		default:
			return fmt.Errorf("not valid money: %v", value)
		}
		parsed, err := Parse(strvalue)
		if err != nil {
			return err
		}
		*target = parsed
		return nil
	}
	return errors.New("not valid money: nil")
}

func (m Money) String() string {
	sign, absM := "", m.Data
	if absM < 0 {
		sign, absM = "-", -absM
	}

	whole := absM / 10000
	cents := (absM % 10000) / 100

	return fmt.Sprintf("%s%d.%02d", sign, whole, cents)
}
