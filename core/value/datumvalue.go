package value

import (
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
)

// NewDatumWire creates new datapoint value referenced from a value of the datum protobuf
func NewDatumWire(val *com_signalfuse_metrics_protobuf.Datum) DatapointValue {
	if val.DoubleValue != nil {
		return NewFloatWire(val.GetDoubleValue())
	}
	if val.IntValue != nil {
		return NewIntWire(val.GetIntValue())
	}
	return NewStrWire(val.GetStrValue())
}
