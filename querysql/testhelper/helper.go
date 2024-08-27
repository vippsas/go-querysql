package testhelper

import (
	"runtime"
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
