package querysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// Try executing txFunc within a transaction once and ensure either
// tx.Commit() is called when there is no error and tx.Rollback() on error or panic.
func DoTransactional(ctx context.Context, log logrus.FieldLogger, dbi BeginTxer, isolation sql.IsolationLevel, txFunc func(tx *sql.Tx) error) error {
	// Start transaction
	tx, err := dbi.BeginTx(ctx, &sql.TxOptions{Isolation: isolation, ReadOnly: false})
	if err != nil {
		return fmt.Errorf("Failed to begin tx: %w", err)
	}
	// Make sure a rollback occurs on panic
	defer RollbackOnPanic(log, tx)

	// Do the actual transaction work, then commit or rollback
	err = txFunc(tx)
	if err == nil {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("%w while attempting commit", err)
		}
		return nil
	} else if IsRollbackRequest(err) {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			if IsRedundantRollbackError(rollbackErr) {
				// return original error if the problem is that rollback had already been done, e.g. due to xact_abort or similar.
				// See https://www.sommarskog.se/error_handling/Part1.html
				return err
			}
			return fmt.Errorf("%w while attempting rollback. \nOriginal error before rollback attempt:\n%+v\n", rollbackErr, err)
		}
		return nil
	} else {
		log.Debugf("Function failed, rolling back: %+v", err)
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			if IsRedundantRollbackError(rollbackErr) {
				// return original error if the problem is that rollback had already been done, e.g. due to xact_abort or similar.
				// See https://www.sommarskog.se/error_handling/Part1.html
				return err
			}
			return fmt.Errorf("%w while attempting rollback. \nOriginal error before rollback attempt:\n%+v\n", rollbackErr, err)
		}
		return fmt.Errorf("%w successfully rolled back", err)
	}
}

func RetryTransactional(ctx context.Context, log logrus.FieldLogger, dbi BeginTxer, attemptLimit int, sleepDuration time.Duration, backoffFactor float64, txFunc func(tx *sql.Tx) error) error {
	var err error
	if backoffFactor < 1.0 {
		backoffFactor = 1.0
	}
	for attempt := 1; attempt <= attemptLimit; attempt++ {
		err = DoTransactional(ctx, log.WithField("attempt", attempt), dbi, sql.LevelSnapshot, txFunc)
		if err == nil {
			break
		}
		time.Sleep(sleepDuration)
		sleepDuration = time.Duration(float64(sleepDuration) * backoffFactor)
	}
	return err
}
