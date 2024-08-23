package testhelper

var TestFunctionCalled bool

func TestFunction(component string, val int64, t float64) {
	TestFunctionCalled = true
}
