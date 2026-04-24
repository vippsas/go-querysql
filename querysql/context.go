package querysql

import (
	"golang.org/x/net/context"
)

type contextKey int

const ckRowsLogger contextKey = 0
const ckRowsDispatcher contextKey = 1

// WithLogger will return the context with a logger registered for use with querysql;
// during queries, querysql will use Logger() to extract the logger from the context
func WithLogger(ctx context.Context, logger RowsLogger) context.Context {
	return context.WithValue(ctx, ckRowsLogger, logger)
}

func Logger(ctx context.Context) RowsLogger {
	l := ctx.Value(ckRowsLogger)
	if l != nil {
		return l.(RowsLogger)
	}
	return nil
}

// WithDispatcher registers either a RowsGoDispatcher or a DeferredRowsGoDispatcher on the context.
func WithDispatcher(ctx context.Context, dispatcher any) context.Context {
	return context.WithValue(ctx, ckRowsDispatcher, dispatcher)
}

func dispatcherValue(ctx context.Context) any {
	l := ctx.Value(ckRowsDispatcher)
	return l
}

// Dispatcher returns the legacy immediate dispatcher, if one was registered.
// Deferred dispatchers are available internally through the context value used by ResultSets.
func Dispatcher(ctx context.Context) RowsGoDispatcher {
	l := ctx.Value(ckRowsDispatcher)
	if l == nil {
		return nil
	}

	dispatcher, ok := l.(RowsGoDispatcher)
	if !ok {
		return nil
	}

	return dispatcher
}
