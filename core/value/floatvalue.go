package value

import (
	"errors"
	"strconv"
)

type floatWire struct {
	val float64
}

func (wireVal *floatWire) WireValue() string {
	return strconv.FormatFloat(wireVal.val, 'f', -1, 64)
}

func (wireVal *floatWire) FloatValue() (float64, error) {
	return wireVal.val, nil
}

func (wireVal *floatWire) IntValue() (int64, error) {
	return 0, errors.New("no exact integer representation")
}

// NewFloatWire creates new datapoint value is a float
func NewFloatWire(val float64) DatapointValue {
	return &floatWire{val: val}
}
