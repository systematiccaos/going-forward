// has basic convenience functions that are often needed in different microservices
package util

import (
	"reflect"

	filename "github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
)

// sets up logrus to display time and linenumber in each message
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

// looks if a slice contains a string
func StringInSlice(a string, list []reflect.Value) bool {
	for _, b := range list {
		if b.String() == a {
			return true
		}
	}
	return false
}

// gets a value of a map by its key
func MapValueByKey(a string, mp reflect.Value) reflect.Value {
	keys := mp.MapKeys()
	for _, b := range keys {
		if b.String() == a {
			return mp.MapIndex(b)
		}
	}
	return reflect.Value{}
}
