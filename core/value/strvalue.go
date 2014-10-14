package value

import (
	"errors"
)

type strWire string

func (wireVal strWire) WireValue() string {
	return string(wireVal)
}

func (wireVal strWire) FloatValue() (float64, error) {
	return 0, errors.New("no exact float representation")
}

func (wireVal strWire) IntValue() (int64, error) {
	return 0, errors.New("no exact integer representation")
}

func (wireVal strWire) String() string {
	return wireVal.WireValue()
}

// NewStrWire creates new datapoint value is a float
func NewStrWire(val string) DatapointValue {
	return strWire(val)
}
