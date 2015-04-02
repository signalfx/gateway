package structdefaults

import (
	"github.com/golang/glog"
	"reflect"
)

// FillDefaultFrom fills default non null values into a null existing interface
func FillDefaultFrom(existing interface{}, defaults interface{}) {
	existingVal := reflect.ValueOf(existing).Elem()
	defaultsVal := reflect.ValueOf(defaults).Elem()
	for i := 0; i < existingVal.NumField(); i++ {
		fieldType := existingVal.Type().Field(i)
		if existingVal.Field(i).Kind() == reflect.Ptr && existingVal.Field(i).IsNil() {
			glog.V(3).Infof("On null field %s", fieldType)
			defaultValue := defaultsVal.Field(i).Interface()
			glog.V(3).Infof("default is %s", defaultValue)
			if defaultValue != reflect.ValueOf(nil) {
				glog.V(3).Infof("Setting to %s", defaultsVal.Field(i))
				existingVal.Field(i).Set(defaultsVal.Field(i))
			}
		}
	}
}
