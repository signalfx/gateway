package stringhelper

import (
	"reflect"
	"strings"
)

// GenericFromString is a readable String() implementation for generic interfaces
func GenericFromString(object interface{}) string {
	parts := []string{}
	existingVal := reflect.ValueOf(object).Elem()
	for i := 0; i < existingVal.NumField(); i++ {
		fieldName := existingVal.Type().Field(i)
		if existingVal.Field(i).Kind() == reflect.Ptr && !existingVal.Field(i).IsNil() {
			pointedToElem := existingVal.Field(i).Elem()
			if pointedToElem.Kind() == reflect.String {
				parts = append(parts, fieldName.Name+"="+existingVal.Field(i).Elem().String())
			} else {
				strMethod := pointedToElem.MethodByName("String")
				if strMethod.Kind() == reflect.Func {
					result := strMethod.Call([]reflect.Value{})[0]
					valueOfField := result.String()
					parts = append(parts, fieldName.Name+"="+valueOfField)
				}
			}
		} else if existingVal.Field(i).Kind() == reflect.String {
			parts = append(parts, fieldName.Name+"="+existingVal.Field(i).String())
		}
	}
	return reflect.TypeOf(object).String() + "[" + strings.Join(parts, "|") + "]"
}

