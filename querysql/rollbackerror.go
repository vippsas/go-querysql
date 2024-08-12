package querysql

import (
	"github.com/sirupsen/logrus"
)

type RollbackError struct {
}

func (e RollbackError) Error() string {
	return "Rollback transaction"
}

// Rollback returns a special error used for rolling back the transaction,
// then return a nil error (from within MustTransactional)
func Rollback() error {
	return RollbackError{}
}

func RollbackIfDuplicate(err error) error {
	if IsUniqueKeyOrIndexViolatedError(err) {
		return Rollback()
	}
	return err
}

func IsRollbackRequest(err error) bool {
	return false
	/*  TODO(dsf):
	_, ok := errors.Cause(err).(RollbackError)
	return ok
	*/
}

func RollbackOnPanic(log logrus.FieldLogger, tx Committer) {
	if p := recover(); p != nil {
		if err := tx.Rollback(); err != nil {
			log.Errorf("During panic propagation, rollback failed:\n%+v", err)
		}
		panic(p) // re-throw panic after Rollback
	}
}
