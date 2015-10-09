package structdefaults

import (
	"reflect"
)

// FillDefaultFrom fills default non null values into a null existing interface
func FillDefaultFrom(existing interface{}, defaults interface{}) {
	existingVal := reflect.ValueOf(existing).Elem()
	defaultsVal := reflect.ValueOf(defaults).Elem()
	for i := 0; i < existingVal.NumField(); i++ {
		if existingVal.Field(i).Kind() == reflect.Ptr && existingVal.Field(i).IsNil() {
			defaultValue := defaultsVal.Field(i).Interface()
			if defaultValue != reflect.ValueOf(nil) {
				existingVal.Field(i).Set(defaultsVal.Field(i))
			}
		}
	}
}
