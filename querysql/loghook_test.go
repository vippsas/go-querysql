package querysql

import (
	"github.com/sirupsen/logrus"
)

type LogHook struct {
	lines []logrus.Fields
}

func (hook *LogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook *LogHook) Fire(entry *logrus.Entry) error {
	hook.lines = append(hook.lines, entry.Data)
	return nil
}
