package value

type strWire string

// A StringDatapoint is a datapoint whos raw value is a string
type StringDatapoint interface {
	DatapointValue
}

func (wireVal strWire) String() string {
	return string(wireVal)
}

// NewStrWire creates new datapoint value is a string
func NewStrWire(val string) StringDatapoint {
	return strWire(val)
}
