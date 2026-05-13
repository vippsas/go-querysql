package querysql

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/sirupsen/logrus"
)

func StdMSSQLLogger(logger *log.Logger) RowsLogger {
	defaultLogLevel := logrus.InfoLevel
	return func(rows *sql.Rows) error {
		var logLevel string

		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}

		// For logging just scan *everything* into a string type straight from SQL driver to make things simple here...
		// The first column is the log level by protocol of RowsLogger.
		fields := make([]interface{}, len(cols))
		scanPointers := make([]interface{}, len(cols))
		scanPointers[0] = &logLevel
		for i := 1; i < len(cols); i++ {
			scanPointers[i] = &fields[i]
		}

		hadRow := false
		for rows.Next() {
			hadRow = true
			if err = rows.Scan(scanPointers...); err != nil {
				return err
			}
			parsedLogLevel, err := logrus.ParseLevel(logLevel)
			if err != nil {
				emitLogEntry(logger, logrus.Fields{
					"event":         "invalid.log.level",
					"invalid.level": logLevel,
				}, logrus.ErrorLevel)
				parsedLogLevel = defaultLogLevel
			}

			logrusFields := logrus.Fields{}
			for i, value := range fields {
				if i == 0 {
					continue
				}
				// we post-process the types of the values a bit to make some types more readable in logs
				switch typedValue := value.(type) {
				case []uint8:
					switch colTypes[i].DatabaseTypeName() {
					case "MONEY":
						value = string(typedValue)
					case "UNIQUEIDENTIFIER":
						value, err = ParseSQLUUIDBytes(typedValue)
						if err != nil {
							return fmt.Errorf("could not decode UUID from SQL: %w", err)
						}
					default:
						value = "0x" + hex.EncodeToString(typedValue)
					}
				}
				logrusFields[cols[i]] = value
			}
			emitLogEntry(logger, logrusFields, parsedLogLevel)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		if !hadRow {
			// it can be quite annoying to have logging of empty tables turn into nothing, so log
			// an indication that the log statement was there, with an empty table
			// in this case loglevel is unreachable, and we really can only log the keys,
			// but let's hope INFO isn't overboard
			logrusFields := logrus.Fields{}
			logrusFields["_norows"] = true
			for _, col := range cols[1:] {
				logrusFields[col] = ""
			}
			emitLogEntry(logger, logrusFields, defaultLogLevel)
		}
		return nil
	}
}

func emitLogEntry(logger *log.Logger, fields logrus.Fields, level logrus.Level) {
	str := ""
	for k, v := range fields {
		str += fmt.Sprintf(" %s='%v'", k, v)
	}
	switch level {
	case logrus.PanicLevel:
		logger.Panic(str)
	case logrus.FatalLevel:
		logger.Fatal(str)
	case logrus.ErrorLevel, logrus.WarnLevel, logrus.DebugLevel, logrus.TraceLevel, logrus.InfoLevel:
		str = fmt.Sprintf("level=%s %s", level.String(), str)
		logger.Print(str)
	default:
		panic(fmt.Sprintf("Log level %d not handled in emitLogEntry", level))
	}
}
