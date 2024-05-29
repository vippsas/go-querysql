package querysql

import (
	"golang.org/x/net/context"
)

type contextKey int

const ckRowsLogger contextKey = 0

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
