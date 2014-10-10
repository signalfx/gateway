package value

import (
	"errors"
	"strconv"
)

type floatWire float64

func (wireVal floatWire) WireValue() string {
	return strconv.FormatFloat(float64(wireVal), 'f', -1, 64)
}

func (wireVal floatWire) FloatValue() (float64, error) {
	return float64(wireVal), nil
}

func (wireVal floatWire) IntValue() (int64, error) {
	return 0, errors.New("no exact integer representation")
}

// NewFloatWire creates new datapoint value is a float
func NewFloatWire(val float64) DatapointValue {
	return floatWire(val)
}
