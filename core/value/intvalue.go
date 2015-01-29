package value

import "strconv"

type intWire int64

// An IntDatapoint is a datapoint whos raw value is a 64 bit integer
type IntDatapoint interface {
	DatapointValue
	IntValue() int64
}

func (wireVal intWire) IntValue() int64 {
	return int64(wireVal)
}

func (wireVal intWire) String() string {
	return strconv.FormatInt(int64(wireVal), 10)
}

// NewIntWire creates new datapoint value is an integer
func NewIntWire(val int64) IntDatapoint {
	return intWire(val)
}
