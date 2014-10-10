package value

// DatapointValue is the value of a datapoint being written
type DatapointValue interface {
	WireValue() string
	FloatValue() (float64, error)
	IntValue() (int64, error)
	String() string
}
