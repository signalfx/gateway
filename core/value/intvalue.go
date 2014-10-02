package value

import "strconv"

type intWire struct {
	val int64
}

func (wireVal *intWire) WireValue() string {
	return strconv.FormatInt(wireVal.val, 10)
}

func (wireVal *intWire) FloatValue() (float64, error) {
	return float64(wireVal.val), nil
}

func (wireVal *intWire) IntValue() (int64, error) {
	return wireVal.val, nil
}

// NewIntWire creates new datapoint value is an integer
func NewIntWire(val int64) DatapointValue {
	return &intWire{val: val}
}
