package value

import (
	"strconv"
)

type floatWire float64

// A FloatValue is a datapoint whos raw type is a 64 bit float
type FloatValue interface {
	DatapointValue
	FloatValue() float64
}

func (wireVal floatWire) FloatValue() float64 {
	return float64(wireVal)
}

func (wireVal floatWire) String() string {
	return strconv.FormatFloat(float64(wireVal), 'f', -1, 64)
}

// NewFloatWire creates new datapoint value is a float
func NewFloatWire(val float64) FloatValue {
	return floatWire(val)
}
