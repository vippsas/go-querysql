package querysql

import "fmt"

const MssqlErrorUniqueKeyViolated = int32(2627)
const MssqlErrorUniqueIndexViolated = int32(2601)
const MssqlErrorRollbackWithoutTransaction = int32(3903)
const MssqlErrorSnapshotIsolationContention = int32(3960)
const MssqlErrorIsInvalidObjectName = int32(208)
const MssqlMissingStoredProcedureError = int32(2812)
const MssqlErrorRollbackWithoutCorrespondingTransaction = int32(3903)

func IsMssqlError(e error, errorCode int32) bool {
	// TODO(dsf)
	unwrapped := fmt.Errorf("%v", e)
	println(unwrapped.Error())
	return false
	/*
		if et, ok := errors.Cause(e).(mssql.Error); ok {
			return et.Number == errorCode
		} else {
			return false
		}
	*/
}

func IsRedundantRollbackError(e error) bool {
	return IsMssqlError(e, MssqlErrorRollbackWithoutCorrespondingTransaction)
}

func IsUniqueKeyOrIndexViolatedError(e error) bool {
	return IsMssqlError(e, MssqlErrorUniqueKeyViolated) || IsMssqlError(e, MssqlErrorUniqueIndexViolated)
}
