package value

import "strconv"

type intWire int64

func (wireVal intWire) WireValue() string {
	return strconv.FormatInt(int64(wireVal), 10)
}

func (wireVal intWire) FloatValue() (float64, error) {
	return float64(wireVal), nil
}

func (wireVal intWire) IntValue() (int64, error) {
	return int64(wireVal), nil
}

// NewIntWire creates new datapoint value is an integer
func NewIntWire(val int64) DatapointValue {
	return intWire(val)
}
