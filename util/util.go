package util

import (
	"reflect"

	filename "github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
)

func SetupLogs() {
	customFormatter := &logrus.TextFormatter{}
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)
	logrus.SetLevel(logrus.DebugLevel)
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	logrus.AddHook(filenameHook)
}

func StringInSlice(a string, list []reflect.Value) bool {
	for _, b := range list {
		if b.String() == a {
			return true
		}
	}
	return false
}

func MapValueByKey(a string, mp reflect.Value) reflect.Value {
	keys := mp.MapKeys()
	for _, b := range keys {
		if b.String() == a {
			return mp.MapIndex(b)
		}
	}
	return reflect.Value{}
}
