package querysql

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// LogrusMSSQLLogger returns a basic RowsLogger suitable for the combination of MS SQL and logrus
func LogrusMSSQLLogger(logger logrus.FieldLogger, defaultLogLevel logrus.Level) RowsLogger {
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
				logrusEmitLogEntry(logger.WithFields(logrus.Fields{
					"event":         "invalid.log.level",
					"invalid.level": logLevel,
				}), logrus.ErrorLevel)
				parsedLogLevel = defaultLogLevel
			}

			sublogger := logger
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
				sublogger = sublogger.WithField(cols[i], value)
			}
			logrusEmitLogEntry(sublogger, parsedLogLevel)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		if !hadRow {
			// it can be quite annoying to have logging of empty tables turn into nothing, so log
			// an indication that the log statement was there, with an empty table
			// in this case loglevel is unreachable, and we really can only log the keys,
			// but let's hope INFO isn't overboard
			l := logger.WithField("_norows", true)
			for _, col := range cols[1:] {
				l = l.WithField(col, "")
			}
			logrusEmitLogEntry(l, defaultLogLevel)
		}
		return nil
	}
}

func logrusEmitLogEntry(logger logrus.FieldLogger, level logrus.Level) {
	switch level {
	case logrus.PanicLevel:
		logger.Panic()
	case logrus.FatalLevel:
		logger.Fatal()
	case logrus.ErrorLevel:
		logger.Error()
	case logrus.WarnLevel:
		logger.Warning()
	case logrus.InfoLevel:
		logger.Info()
	case logrus.DebugLevel:
		logger.Debug()
	case logrus.TraceLevel:
		logger.Debug()
	default:
		panic(fmt.Sprintf("Log level %d not handled in logrusEmitLogEntry", level))
	}
}

func ParseSQLUUIDBytes(v []uint8) (uuid.UUID, error) {
	if len(v) != 16 {
		return uuid.UUID{}, errors.New("ParseSQLUUIDBytes: did not get 16 bytes")
	}
	var shuffled [16]uint8
	// This: select convert(uniqueidentifier, '00010203-0405-0607-0809-0a0b0c0d0e0f')
	// Returns this when passed to uuid.FromBytes:
	// 03020100-0504-0706-0809-0a0b0c0d0e0f
	// So, shuffling first
	shuffled[0x0] = v[0x3]
	shuffled[0x1] = v[0x2]
	shuffled[0x2] = v[0x1]
	shuffled[0x3] = v[0x0]

	shuffled[0x4] = v[0x5]
	shuffled[0x5] = v[0x4]

	shuffled[0x6] = v[0x7]
	shuffled[0x7] = v[0x6]

	// The rest are not shuffled :shrug:
	shuffled[0x8] = v[0x8]
	shuffled[0x9] = v[0x9]

	shuffled[0xa] = v[0xa]
	shuffled[0xb] = v[0xb]
	shuffled[0xc] = v[0xc]
	shuffled[0xd] = v[0xd]
	shuffled[0xe] = v[0xe]
	shuffled[0xf] = v[0xf]

	return uuid.FromBytes(shuffled[:])
}
